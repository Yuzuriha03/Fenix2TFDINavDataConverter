use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{Map, Number, Value};

use crate::db_json::{
    configure_read_connection, fetch_table_rows, fetch_table_rows_after_id, format_row, json_to_i64,
};
use crate::stats::TableExportStats;
use crate::tables::{
    ExistingJsonIndex, PreformattedJsonExport, export_preformatted_rows_to_json,
    load_existing_id_index,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct ReferenceIdIndex {
    output_id_by_db_id: HashMap<i64, i64>,
    db_row_count: usize,
}

pub(crate) type AirportIdIndex = ReferenceIdIndex;
pub(crate) type IlsIdIndex = ReferenceIdIndex;
pub(crate) type NavaidIdIndex = ReferenceIdIndex;
pub(crate) type RunwayIdIndex = ReferenceIdIndex;
pub(crate) type WaypointIdIndex = ReferenceIdIndex;

impl ReferenceIdIndex {
    #[must_use]
    pub(crate) fn output_id_for_db(&self, db_id: i64) -> i64 {
        self.output_id_by_db_id
            .get(&db_id)
            .copied()
            .unwrap_or(db_id)
    }

    #[must_use]
    pub(crate) const fn db_row_count(&self) -> usize {
        self.db_row_count
    }
}

struct ForeignKeyRemap<'a> {
    column_name: &'a str,
    index: &'a ReferenceIdIndex,
}

pub(crate) fn build_airport_id_index(
    db_path: &Path,
    base_json_dir: Option<&Path>,
) -> Result<AirportIdIndex> {
    build_reference_id_index(
        db_path,
        base_json_dir,
        "Airports",
        "Airports.json",
        &["ICAO", "Latitude", "Longitude"],
        &[],
    )
}

pub(crate) fn build_runway_id_index(
    db_path: &Path,
    base_json_dir: Option<&Path>,
    airport_id_index: &AirportIdIndex,
) -> Result<RunwayIdIndex> {
    build_reference_id_index(
        db_path,
        base_json_dir,
        "Runways",
        "Runways.json",
        &["AirportID", "Ident", "Latitude", "Longitude"],
        &[ForeignKeyRemap {
            column_name: "AirportID",
            index: airport_id_index,
        }],
    )
}

pub(crate) fn build_ils_id_index(
    db_path: &Path,
    base_json_dir: Option<&Path>,
    runway_id_index: &RunwayIdIndex,
) -> Result<IlsIdIndex> {
    build_reference_id_index(
        db_path,
        base_json_dir,
        "Ilses",
        "Ilses.json",
        &["RunwayID", "Ident", "LocCourse"],
        &[ForeignKeyRemap {
            column_name: "RunwayID",
            index: runway_id_index,
        }],
    )
}

pub(crate) fn build_navaid_id_index(
    db_path: &Path,
    base_json_dir: Option<&Path>,
) -> Result<NavaidIdIndex> {
    build_reference_id_index(
        db_path,
        base_json_dir,
        "Navaids",
        "Navaids.json",
        &["Ident", "Type", "Latitude", "Longitude"],
        &[],
    )
}

pub(crate) fn build_waypoint_id_index(
    db_path: &Path,
    base_json_dir: Option<&Path>,
) -> Result<WaypointIdIndex> {
    build_reference_id_index(
        db_path,
        base_json_dir,
        "Waypoints",
        "Waypoints.json",
        &["Ident", "Latitude", "Longitude"],
        &[],
    )
}

pub(crate) fn export_airports_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    airport_id_index: &AirportIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "Airports",
        airport_id_index,
        &[ForeignKeyRemap {
            column_name: "PrimaryID",
            index: airport_id_index,
        }],
        preloaded_existing_index,
    )
}

pub(crate) fn export_airport_lookup_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    airport_id_index: &AirportIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "AirportLookup",
        airport_id_index,
        &[],
        preloaded_existing_index,
    )
}

pub(crate) fn export_runways_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    runway_id_index: &RunwayIdIndex,
    airport_id_index: &AirportIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "Runways",
        runway_id_index,
        &[ForeignKeyRemap {
            column_name: "AirportID",
            index: airport_id_index,
        }],
        preloaded_existing_index,
    )
}

pub(crate) fn export_ilses_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    ils_id_index: &IlsIdIndex,
    runway_id_index: &RunwayIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "Ilses",
        ils_id_index,
        &[ForeignKeyRemap {
            column_name: "RunwayID",
            index: runway_id_index,
        }],
        preloaded_existing_index,
    )
}

pub(crate) fn export_navaids_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    navaid_id_index: &NavaidIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "Navaids",
        navaid_id_index,
        &[],
        preloaded_existing_index,
    )
}

pub(crate) fn export_navaid_lookup_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    navaid_id_index: &NavaidIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "NavaidLookup",
        navaid_id_index,
        &[],
        preloaded_existing_index,
    )
}

pub(crate) fn export_waypoints_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    waypoint_id_index: &WaypointIdIndex,
    navaid_id_index: &NavaidIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "Waypoints",
        waypoint_id_index,
        &[ForeignKeyRemap {
            column_name: "NavaidID",
            index: navaid_id_index,
        }],
        preloaded_existing_index,
    )
}

pub(crate) fn export_waypoint_lookup_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    waypoint_id_index: &WaypointIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table(
        db_path,
        output_dir,
        base_json_dir,
        "WaypointLookup",
        waypoint_id_index,
        &[],
        preloaded_existing_index,
    )
}

pub(crate) fn export_terminals_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    airport_id_index: &AirportIdIndex,
    runway_id_index: &RunwayIdIndex,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    export_reference_table_with_preserved_ids(
        db_path,
        output_dir,
        base_json_dir,
        "Terminals",
        &[
            ForeignKeyRemap {
                column_name: "AirportID",
                index: airport_id_index,
            },
            ForeignKeyRemap {
                column_name: "RwyID",
                index: runway_id_index,
            },
        ],
        preloaded_existing_index,
    )
}

fn build_reference_id_index(
    db_path: &Path,
    base_json_dir: Option<&Path>,
    table_name: &str,
    json_file_name: &str,
    key_columns: &[&str],
    foreign_key_remaps: &[ForeignKeyRemap<'_>],
) -> Result<ReferenceIdIndex> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    configure_read_connection(&conn);

    let db_rows = fetch_reference_rows(&conn, table_name, None, foreign_key_remaps)?;
    let existing_rows = load_existing_json_rows(base_json_dir, json_file_name)?;

    Ok(build_reference_id_index_from_rows(
        &db_rows,
        &existing_rows,
        key_columns,
    ))
}

fn export_reference_table(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    table_name: &str,
    primary_id_index: &ReferenceIdIndex,
    foreign_key_remaps: &[ForeignKeyRemap<'_>],
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    configure_read_connection(&conn);

    let existing_load_start = Instant::now();
    let existing_merge = match preloaded_existing_index {
        Some(existing) => Some(existing),
        None => load_existing_id_index(base_json_dir, table_name)?,
    };
    let existing_load_time = existing_load_start.elapsed();

    let db_read_start = Instant::now();
    let rows = fetch_table_rows(&conn, table_name)?;
    let db_read_time = db_read_start.elapsed();
    let source_rows = rows.len();

    let format_start = Instant::now();
    let mut seen_output_ids = existing_merge
        .as_ref()
        .map_or_else(HashSet::new, |existing| existing.ids.clone());
    let formatted_rows = rows
        .into_iter()
        .filter_map(|mut row| {
            let db_id = json_to_i64(row.get("ID"))?;
            let output_id = primary_id_index.output_id_for_db(db_id);
            if !seen_output_ids.insert(output_id) {
                return None;
            }

            row.insert("ID".to_string(), Value::Number(Number::from(output_id)));
            apply_foreign_key_remaps(&mut row, foreign_key_remaps);
            Some(format_row(row, table_name, None))
        })
        .collect::<Vec<_>>();
    let format_time = format_start.elapsed();

    export_preformatted_rows_to_json(PreformattedJsonExport {
        output_dir,
        table_name,
        formatted_rows: &formatted_rows,
        existing_merge: existing_merge.as_ref(),
        source_rows,
        existing_load_time,
        db_read_time,
        format_time,
    })
}

fn export_reference_table_with_preserved_ids(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    table_name: &str,
    foreign_key_remaps: &[ForeignKeyRemap<'_>],
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    configure_read_connection(&conn);

    let existing_load_start = Instant::now();
    let existing_merge = match preloaded_existing_index {
        Some(existing) => Some(existing),
        None => load_existing_id_index(base_json_dir, table_name)?,
    };
    let existing_load_time = existing_load_start.elapsed();

    let db_read_start = Instant::now();
    let rows = if let Some(existing) = &existing_merge {
        if let Some(max_id) = existing.ids.iter().max().copied() {
            fetch_table_rows_after_id(&conn, table_name, max_id)?
        } else {
            fetch_table_rows(&conn, table_name)?
        }
    } else {
        fetch_table_rows(&conn, table_name)?
    };
    let db_read_time = db_read_start.elapsed();
    let source_rows = rows.len();

    let format_start = Instant::now();
    let formatted_rows = rows
        .into_iter()
        .filter_map(|mut row| {
            let keep_row = existing_merge.as_ref().is_none_or(|existing| {
                json_to_i64(row.get("ID")).is_none_or(|id| !existing.ids.contains(&id))
            });
            if !keep_row {
                return None;
            }

            apply_foreign_key_remaps(&mut row, foreign_key_remaps);
            Some(format_row(row, table_name, None))
        })
        .collect::<Vec<_>>();
    let format_time = format_start.elapsed();

    export_preformatted_rows_to_json(PreformattedJsonExport {
        output_dir,
        table_name,
        formatted_rows: &formatted_rows,
        existing_merge: existing_merge.as_ref(),
        source_rows,
        existing_load_time,
        db_read_time,
        format_time,
    })
}

fn fetch_reference_rows(
    conn: &Connection,
    table_name: &str,
    primary_id_index: Option<&ReferenceIdIndex>,
    foreign_key_remaps: &[ForeignKeyRemap<'_>],
) -> Result<Vec<Map<String, Value>>> {
    let db_rows = fetch_table_rows(conn, table_name)?;
    let mut seen_output_ids = HashSet::new();
    let mut rows = Vec::with_capacity(db_rows.len());

    for mut row in db_rows {
        if let Some(primary_id_index) = primary_id_index {
            let Some(db_id) = json_to_i64(row.get("ID")) else {
                continue;
            };
            let output_id = primary_id_index.output_id_for_db(db_id);
            if !seen_output_ids.insert(output_id) {
                continue;
            }
            row.insert("ID".to_string(), Value::Number(Number::from(output_id)));
        }

        apply_foreign_key_remaps(&mut row, foreign_key_remaps);
        rows.push(format_row(row, table_name, None));
    }

    if primary_id_index.is_some() {
        rows.sort_by_key(|row| json_to_i64(row.get("ID")).unwrap_or(i64::MAX));
    }

    Ok(rows)
}

fn apply_foreign_key_remaps(
    row: &mut Map<String, Value>,
    foreign_key_remaps: &[ForeignKeyRemap<'_>],
) {
    for remap in foreign_key_remaps {
        let Some(db_id) = json_to_i64(row.get(remap.column_name)) else {
            continue;
        };
        row.insert(
            remap.column_name.to_string(),
            Value::Number(Number::from(remap.index.output_id_for_db(db_id))),
        );
    }
}

fn load_existing_json_rows(
    base_json_dir: Option<&Path>,
    json_file_name: &str,
) -> Result<Vec<Map<String, Value>>> {
    let Some(base_json_dir) = base_json_dir else {
        return Ok(Vec::new());
    };

    let path = base_json_dir.join(json_file_name);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(&path)
        .with_context(|| format!("failed to open reference json: {}", path.display()))?;
    let reader = BufReader::with_capacity(1024 * 1024, file);
    serde_json::from_reader(reader)
        .with_context(|| format!("failed to parse reference json: {}", path.display()))
}

fn build_reference_id_index_from_rows(
    db_rows: &[Map<String, Value>],
    existing_rows: &[Map<String, Value>],
    key_columns: &[&str],
) -> ReferenceIdIndex {
    let mut existing_id_by_key = HashMap::new();
    let mut used_ids = HashSet::new();

    for row in existing_rows {
        let Some(existing_id) = json_to_i64(row.get("ID")) else {
            continue;
        };
        used_ids.insert(existing_id);
        if let Some(key) = build_row_key(row, key_columns) {
            existing_id_by_key.entry(key).or_insert(existing_id);
        }
    }

    let max_existing_id = used_ids.iter().copied().max().unwrap_or_default();
    let max_db_id = db_rows
        .iter()
        .filter_map(|row| json_to_i64(row.get("ID")))
        .max()
        .unwrap_or_default();
    let mut next_generated_id = max_existing_id.max(max_db_id);
    let mut output_id_by_db_id = HashMap::with_capacity(db_rows.len());

    for row in db_rows {
        let Some(db_id) = json_to_i64(row.get("ID")) else {
            continue;
        };

        let output_id = if let Some(key) = build_row_key(row, key_columns) {
            existing_id_by_key.get(&key).map_or_else(
                || {
                    if used_ids.contains(&db_id) {
                        next_unused_id(&mut next_generated_id, &used_ids)
                    } else {
                        db_id
                    }
                },
                |existing_id| *existing_id,
            )
        } else if !used_ids.contains(&db_id) {
            db_id
        } else {
            next_unused_id(&mut next_generated_id, &used_ids)
        };

        used_ids.insert(output_id);
        output_id_by_db_id.insert(db_id, output_id);
    }

    ReferenceIdIndex {
        output_id_by_db_id,
        db_row_count: db_rows.len(),
    }
}

fn next_unused_id(next_generated_id: &mut i64, used_ids: &HashSet<i64>) -> i64 {
    loop {
        *next_generated_id = next_generated_id.saturating_add(1);
        if !used_ids.contains(next_generated_id) {
            return *next_generated_id;
        }
    }
}

fn build_row_key(row: &Map<String, Value>, key_columns: &[&str]) -> Option<String> {
    let mut any_value = false;
    let mut parts = Vec::with_capacity(key_columns.len());

    for &column in key_columns {
        let part = match row.get(column) {
            None | Some(Value::Null) => "null".to_string(),
            Some(Value::String(text)) => {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    any_value = true;
                }
                format!("s:{trimmed}")
            }
            Some(Value::Number(number)) => {
                any_value = true;
                format!("n:{number}")
            }
            Some(Value::Bool(value)) => {
                any_value = true;
                format!("b:{value}")
            }
            Some(other) => {
                any_value = true;
                serde_json::to_string(other).unwrap_or_else(|_| "json_error".to_string())
            }
        };
        parts.push(part);
    }

    any_value.then(|| parts.join("\u{1f}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(id: i64, ident: &str, latitude: f64, longitude: f64) -> Map<String, Value> {
        let mut row = Map::new();
        row.insert("ID".to_string(), Value::Number(Number::from(id)));
        row.insert("Ident".to_string(), Value::String(ident.to_string()));
        row.insert(
            "Latitude".to_string(),
            Value::Number(Number::from_f64(latitude).unwrap()),
        );
        row.insert(
            "Longitude".to_string(),
            Value::Number(Number::from_f64(longitude).unwrap()),
        );
        row
    }

    #[test]
    fn prefers_existing_json_ids_for_matching_rows() {
        let db_rows = vec![
            make_row(10, "DJT", 32.123_456_79, 118.123_456_79),
            make_row(20, "ELNEX", 31.0, 117.0),
        ];
        let existing_rows = vec![make_row(5010, "DJT", 32.123_456_79, 118.123_456_79)];

        let index = build_reference_id_index_from_rows(
            &db_rows,
            &existing_rows,
            &["Ident", "Latitude", "Longitude"],
        );

        assert_eq!(index.output_id_for_db(10), 5010);
        assert_eq!(index.output_id_for_db(20), 20);
        assert_eq!(index.db_row_count(), 2);
    }

    #[test]
    fn allocates_new_ids_when_db_id_collides_without_a_matching_json_row() {
        let db_rows = vec![
            make_row(10, "AAA", 10.0, 10.0),
            make_row(20, "BBB", 20.0, 20.0),
        ];
        let existing_rows = vec![make_row(10, "ZZZ", 99.0, 99.0)];

        let index = build_reference_id_index_from_rows(
            &db_rows,
            &existing_rows,
            &["Ident", "Latitude", "Longitude"],
        );

        assert!(index.output_id_for_db(10) > 20);
        assert_eq!(index.output_id_for_db(20), 20);
    }
}
