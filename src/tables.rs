use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::db_json::{
    fetch_table_rows, format_row, json_to_i64, read_json_object_array, write_json_objects,
};
use crate::stats::{PhaseDurations, TableExportStats};

pub(crate) fn export_table_to_json(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    table_name: &str,
    waypoints: &HashMap<i64, String>,
) -> Result<TableExportStats> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    let db_read_start = Instant::now();
    let rows = fetch_table_rows(&conn, table_name)?;
    let db_read_time = db_read_start.elapsed();
    let json_transform_start = Instant::now();
    let formatted_rows: Vec<Map<String, Value>> = rows
        .into_iter()
        .map(|row| format_row(row, table_name, waypoints))
        .collect();
    let merged_rows = merge_rows_by_id(base_json_dir, table_name, formatted_rows)?;
    let json_transform_time = json_transform_start.elapsed();

    let output_path = output_dir.join(format!("{table_name}.json"));
    let json_write_start = Instant::now();
    write_json_objects(&output_path, &merged_rows)?;

    Ok(TableExportStats {
        table_name: table_name.to_string(),
        row_count: merged_rows.len(),
        phase: PhaseDurations {
            db_read: db_read_time,
            json_transform: json_transform_time,
            json_write: json_write_start.elapsed(),
        },
    })
}

pub(crate) fn fetch_waypoints(conn: &Connection) -> Result<HashMap<i64, String>> {
    let mut statement = conn
        .prepare("SELECT ID, Ident FROM Waypoints")
        .context("failed to query Waypoints")?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .context("failed to iterate Waypoints")?;
    rows.collect::<rusqlite::Result<HashMap<_, _>>>()
        .context("failed to build Waypoints lookup")
}

fn merge_rows_by_id(
    base_json_dir: Option<&Path>,
    table_name: &str,
    formatted_rows: Vec<Map<String, Value>>,
) -> Result<Vec<Map<String, Value>>> {
    let Some(base_json_dir) = base_json_dir else {
        return Ok(formatted_rows);
    };

    let base_json_path = base_json_dir.join(format!("{table_name}.json"));
    if !base_json_path.exists() {
        return Ok(formatted_rows);
    }

    let mut existing_rows = read_json_object_array(&base_json_path)?;
    let mut existing_ids = existing_rows
        .iter()
        .filter_map(|row| json_to_i64(row.get("ID")))
        .collect::<std::collections::HashSet<_>>();

    for row in formatted_rows {
        let Some(id) = json_to_i64(row.get("ID")) else {
            existing_rows.push(row);
            continue;
        };
        if existing_ids.insert(id) {
            existing_rows.push(row);
        }
    }

    Ok(existing_rows)
}
