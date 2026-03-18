use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;

use anyhow::{Context, Result, bail};
use encoding_rs::GBK;
use rusqlite::Connection;
use rusqlite::params;
use rusqlite::types::Value as SqlValue;
use serde::Serialize;
use serde_json::{Map, Number, Value};
use std::string::ToString;

const DEFAULT_JSON_WRITE_BUFFER_CAPACITY: usize = 1024 * 1024;

pub(crate) fn configure_read_connection(conn: &Connection) {
    // Best-effort read tuning; ignore unsupported pragmas on some SQLite builds.
    let _ = conn.pragma_update(None, "cache_size", -200_000i64);
    let _ = conn.pragma_update(None, "temp_store", "MEMORY");
    let _ = conn.pragma_update(None, "mmap_size", 256_i64 * 1024 * 1024);
}

pub(crate) fn fetch_table_rows(
    conn: &Connection,
    table_name: &str,
) -> Result<Vec<Map<String, Value>>> {
    let sql = table_select_sql(conn, table_name)?;
    let mut statement = conn
        .prepare(&sql)
        .with_context(|| format!("failed to query table {table_name}"))?;
    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(ToString::to_string)
        .collect();
    let rows = statement
        .query_map([], |row| row_to_map(row, &column_names))
        .with_context(|| format!("failed to iterate table {table_name}"))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("failed to read table {table_name}"))
}

pub(crate) fn fetch_table_rows_after_id(
    conn: &Connection,
    table_name: &str,
    min_exclusive_id: i64,
) -> Result<Vec<Map<String, Value>>> {
    let base_sql = table_select_sql(conn, table_name)?;
    let sql = format!("{base_sql} WHERE ID > ?");
    let mut statement = conn
        .prepare(&sql)
        .with_context(|| format!("failed to query table {table_name} incrementally"))?;
    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(ToString::to_string)
        .collect();
    let rows = statement
        .query_map(params![min_exclusive_id], |row| {
            row_to_map(row, &column_names)
        })
        .with_context(|| format!("failed to iterate table {table_name} incrementally"))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("failed to read incremental rows from table {table_name}"))
}

fn table_select_sql(conn: &Connection, table_name: &str) -> Result<String> {
    match table_name {
        "AirportLookup" => Ok("SELECT extID, ID FROM AirportLookup".to_string()),
        "NavaidLookup" => {
            Ok("SELECT Ident, Type, Country, NavKeyCode, ID FROM NavaidLookup".to_string())
        }
        "WaypointLookup" => Ok("SELECT Ident, Country, ID FROM WaypointLookup".to_string()),
        "Terminals" => Ok(
            "SELECT ID, AirportID, Proc, ICAO, FullName, Name, Rwy, RwyID FROM Terminals"
                .to_string(),
        ),
        "AirwayLegs" => Ok(
            "SELECT ID, AirwayID, Level, Waypoint1ID, Waypoint2ID, IsStart, IsEnd FROM AirwayLegs"
                .to_string(),
        ),
        "Waypoints" => {
            let longitude_col = resolve_longitude_column(conn, "Waypoints")?;
            Ok(format!(
                "SELECT ID, Ident, Name, Latitude, NavaidID, {longitude_col} AS Longitude, Collocated FROM Waypoints"
            ))
        }
        "Runways" => {
            let longitude_col = resolve_longitude_column(conn, "Runways")?;
            Ok(format!(
                "SELECT ID, AirportID, Ident, TrueHeading, Length, Width, Surface, Latitude, {longitude_col} AS Longitude, Elevation FROM Runways"
            ))
        }
        _ => Ok(format!("SELECT * FROM {table_name}")),
    }
}

fn resolve_longitude_column(conn: &Connection, table_name: &str) -> Result<&'static str> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table_name})"))
        .with_context(|| format!("failed to inspect schema for table {table_name}"))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .with_context(|| format!("failed to iterate schema for table {table_name}"))?;

    let mut has_longitude = false;
    let mut has_legacy_longtitude = false;
    for row in rows {
        let name =
            row.with_context(|| format!("failed to read schema row for table {table_name}"))?;
        if name == "Longitude" {
            has_longitude = true;
        }
        if name == "Longtitude" {
            has_legacy_longtitude = true;
        }
    }

    if has_longitude {
        Ok("Longitude")
    } else if has_legacy_longtitude {
        Ok("Longtitude")
    } else {
        bail!("{table_name} table has neither Longitude nor Longtitude column")
    }
}

pub(crate) fn sql_value_to_json(value: SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(number) => Value::Number(Number::from(number)),
        SqlValue::Real(number) => Number::from_f64(number).map_or(Value::Null, Value::Number),
        SqlValue::Text(text) => Value::String(text),
        SqlValue::Blob(bytes) => Value::Array(
            bytes
                .into_iter()
                .map(|byte| Value::Number(Number::from(byte)))
                .collect(),
        ),
    }
}

pub(crate) fn format_row(
    mut row: Map<String, Value>,
    table_name: &str,
    waypoints: Option<&HashMap<i64, String>>,
) -> Map<String, Value> {
    if let Some(value) = row.remove("Longtitude") {
        row.insert("Longitude".to_string(), round_json_number(&value));
    }
    if let Some(value) = row.get_mut("Latitude") {
        *value = round_json_number(value);
    }

    match table_name {
        "Ilses" => select_columns(
            row,
            &[
                "ID",
                "RunwayID",
                "Freq",
                "GsAngle",
                "Latitude",
                "Longitude",
                "Category",
                "Ident",
                "LocCourse",
                "CrossingHeight",
                "Elevation",
                "HasDme",
            ],
        ),
        "Terminals" => {
            if let Some(proc_code) = json_to_i64(row.get("Proc")) {
                row.insert("Proc".to_string(), Value::Number(Number::from(proc_code)));
            }
            row
        }
        "Airports" => {
            if let Some(value) = row.remove("TransitionAltitude") {
                row.insert("TransAlt".to_string(), value);
            }
            select_columns(
                row,
                &[
                    "Elevation",
                    "ICAO",
                    "ID",
                    "Latitude",
                    "Longitude",
                    "Name",
                    "PrimaryID",
                    "TransAlt",
                ],
            )
        }
        "AirwayLegs" => {
            if let Some(waypoint_id) = json_to_i64(row.get("Waypoint1ID")) {
                row.insert(
                    "Waypoint1".to_string(),
                    waypoints
                        .and_then(|lookup| lookup.get(&waypoint_id))
                        .cloned()
                        .map_or(Value::Null, Value::String),
                );
            } else {
                row.insert("Waypoint1".to_string(), Value::Null);
            }

            if let Some(waypoint_id) = json_to_i64(row.get("Waypoint2ID")) {
                row.insert(
                    "Waypoint2".to_string(),
                    waypoints
                        .and_then(|lookup| lookup.get(&waypoint_id))
                        .cloned()
                        .map_or(Value::Null, Value::String),
                );
            } else {
                row.insert("Waypoint2".to_string(), Value::Null);
            }

            row
        }
        "Navaids" => {
            row.remove("MagneticVariation");
            row.remove("Range");
            row
        }
        _ => row,
    }
}

pub(crate) fn json_to_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64().or_else(|| {
            number
                .as_f64()
                .and_then(integral_f64_to_i64)
        }),
        Value::String(text) => text
            .parse::<i64>()
            .ok()
            .or_else(|| text.parse::<f64>().ok().and_then(integral_f64_to_i64)),
        _ => None,
    }
}

pub(crate) fn write_json_objects<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    write_json_objects_with_buffer(path, rows, DEFAULT_JSON_WRITE_BUFFER_CAPACITY)
}

pub(crate) fn write_json_objects_with_buffer<T: Serialize>(
    path: &Path,
    rows: &[T],
    buffer_capacity: usize,
) -> Result<()> {
    let file =
        File::create(path).with_context(|| format!("failed to create file: {}", path.display()))?;
    let writer = BufWriter::with_capacity(buffer_capacity, file);
    serde_json::to_writer(writer, rows)
        .with_context(|| format!("failed to write json objects: {}", path.display()))?;
    Ok(())
}

pub(crate) fn write_json_objects_if_changed_with_buffer<T: Serialize>(
    path: &Path,
    rows: &[T],
    buffer_capacity: usize,
) -> Result<bool> {
    if !path.exists() {
        write_json_objects_with_buffer(path, rows, buffer_capacity)?;
        return Ok(true);
    }

    let encoded = serde_json::to_vec(rows)
        .with_context(|| format!("failed to encode json objects: {}", path.display()))?;

    if let Ok(existing) = fs::read(path)
        && existing == encoded
    {
        return Ok(false);
    }

    fs::write(path, encoded)
        .with_context(|| format!("failed to write json objects: {}", path.display()))?;
    Ok(true)
}

pub(crate) fn read_text_gbk(path: &Path) -> Result<String> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read file: {}", path.display()))?;
    let (decoded, _, _) = GBK.decode(&bytes);
    Ok(decoded.into_owned())
}

pub(crate) fn trim_csv_field(field: &str) -> &str {
    field.trim_matches(|ch| matches!(ch, ' ' | '\t' | '\r' | '\n'))
}

pub(crate) fn extract_csv_fields_simple<'a, const N: usize>(
    line: &'a str,
    indices: &[usize; N],
) -> [Option<&'a str>; N] {
    let trimmed = line.trim_end_matches(['\r', '\n']);
    let max_target = indices.iter().copied().max().unwrap_or(0);
    let mut out: [Option<&'a str>; N] = [None; N];
    let mut field_index = 0usize;
    let mut start = 0usize;

    loop {
        let end = trimmed[start..]
            .find(',')
            .map_or(trimmed.len(), |offset| start + offset);
        for (slot, target_index) in indices.iter().enumerate() {
            if *target_index == field_index {
                out[slot] = Some(trim_csv_field(&trimmed[start..end]));
            }
        }
        if end == trimmed.len() || field_index >= max_target {
            break;
        }
        field_index += 1;
        start = end + 1;
    }

    out
}

fn row_to_map(
    row: &rusqlite::Row<'_>,
    column_names: &[String],
) -> rusqlite::Result<Map<String, Value>> {
    let mut map = Map::with_capacity(column_names.len());
    for (index, column_name) in column_names.iter().enumerate() {
        map.insert(column_name.clone(), sql_value_to_json(row.get(index)?));
    }
    Ok(map)
}

fn select_columns(mut row: Map<String, Value>, ordered_columns: &[&str]) -> Map<String, Value> {
    let mut ordered = Map::with_capacity(ordered_columns.len());
    for column in ordered_columns {
        if let Some(value) = row.remove(*column) {
            ordered.insert((*column).to_string(), value);
        }
    }
    ordered
}

fn round_json_number(value: &Value) -> Value {
    value.as_f64().map_or_else(
        || value.clone(),
        |number| {
        let rounded = (number * 100_000_000.0).round() / 100_000_000.0;
            Number::from_f64(rounded).map_or(Value::Null, Value::Number)
        },
    )
}

fn integral_f64_to_i64(float: f64) -> Option<i64> {
    if !float.is_finite() || float.fract() != 0.0 {
        return None;
    }
    format!("{float:.0}").parse::<i64>().ok()
}
