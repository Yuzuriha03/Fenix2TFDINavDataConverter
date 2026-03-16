use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;

use anyhow::{Context, Result, bail};
use encoding_rs::GBK;
use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use serde_json::{Map, Number, Value};

pub(crate) fn fetch_table_rows(
    conn: &Connection,
    table_name: &str,
) -> Result<Vec<Map<String, Value>>> {
    let sql = format!("SELECT * FROM {table_name}");
    let mut statement = conn
        .prepare(&sql)
        .with_context(|| format!("failed to query table {table_name}"))?;
    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect();
    let rows = statement
        .query_map([], |row| row_to_map(row, &column_names))
        .with_context(|| format!("failed to iterate table {table_name}"))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("failed to read table {table_name}"))
}

pub(crate) fn sql_value_to_json(value: SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(number) => Value::Number(Number::from(number)),
        SqlValue::Real(number) => Number::from_f64(number)
            .map(Value::Number)
            .unwrap_or(Value::Null),
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
    waypoints: &HashMap<i64, String>,
) -> Map<String, Value> {
    if let Some(value) = row.remove("Longtitude") {
        row.insert("Longitude".to_string(), round_json_number(&value));
    }
    if let Some(value) = row.get_mut("Latitude") {
        *value = round_json_number(value);
    }

    match table_name {
        "AirportLookup" => select_columns(row, &["ID", "extID"]),
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
        "Terminals" => select_columns(
            {
                if let Some(proc_code) = json_to_i64(row.get("Proc")) {
                    row.insert("Proc".to_string(), Value::Number(Number::from(proc_code)));
                }
                row
            },
            &[
                "ID",
                "AirportID",
                "Proc",
                "ICAO",
                "FullName",
                "Name",
                "Rwy",
                "RwyID",
            ],
        ),
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
                        .get(&waypoint_id)
                        .cloned()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                );
            } else {
                row.insert("Waypoint1".to_string(), Value::Null);
            }

            if let Some(waypoint_id) = json_to_i64(row.get("Waypoint2ID")) {
                row.insert(
                    "Waypoint2".to_string(),
                    waypoints
                        .get(&waypoint_id)
                        .cloned()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                );
            } else {
                row.insert("Waypoint2".to_string(), Value::Null);
            }

            select_columns(
                row,
                &[
                    "ID",
                    "AirwayID",
                    "Level",
                    "Waypoint1ID",
                    "Waypoint2ID",
                    "IsStart",
                    "IsEnd",
                    "Waypoint1",
                    "Waypoint2",
                ],
            )
        }
        "Navaids" => {
            row.remove("MagneticVariation");
            row.remove("Range");
            row
        }
        "Waypoints" => select_columns(
            row,
            &[
                "ID",
                "Ident",
                "Name",
                "Latitude",
                "NavaidID",
                "Longitude",
                "Collocated",
            ],
        ),
        "Runways" => select_columns(
            row,
            &[
                "ID",
                "AirportID",
                "Ident",
                "TrueHeading",
                "Length",
                "Width",
                "Surface",
                "Latitude",
                "Longitude",
                "Elevation",
            ],
        ),
        _ => row,
    }
}

pub(crate) fn json_to_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64().or_else(|| {
            number.as_f64().and_then(|float| {
                if float.fract() == 0.0 {
                    Some(float as i64)
                } else {
                    None
                }
            })
        }),
        Value::String(text) => text.parse::<i64>().ok().or_else(|| {
            text.parse::<f64>().ok().and_then(|float| {
                if float.fract() == 0.0 {
                    Some(float as i64)
                } else {
                    None
                }
            })
        }),
        _ => None,
    }
}

pub(crate) fn json_string(value: Option<&Value>) -> Option<String> {
    let text = json_text(value);
    if text.is_empty() { None } else { Some(text) }
}

pub(crate) fn json_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Bool(flag)) => flag.to_string(),
        _ => String::new(),
    }
}

pub(crate) fn write_json(path: &Path, data: &[Value]) -> Result<()> {
    let file =
        File::create(path).with_context(|| format!("failed to create file: {}", path.display()))?;
    let writer = BufWriter::with_capacity(1024 * 1024, file);
    serde_json::to_writer(writer, data)
        .with_context(|| format!("failed to write json: {}", path.display()))?;
    Ok(())
}

pub(crate) fn write_json_objects(path: &Path, rows: &[Map<String, Value>]) -> Result<()> {
    let values: Vec<Value> = rows.iter().cloned().map(Value::Object).collect();
    write_json(path, &values)
}

pub(crate) fn read_text_gbk(path: &Path) -> Result<String> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read file: {}", path.display()))?;
    let (decoded, _, _) = GBK.decode(&bytes);
    Ok(decoded.into_owned())
}

pub(crate) fn read_json_array(path: &Path) -> Result<Vec<Value>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open json file: {}", path.display()))?;
    let json: Value = serde_json::from_reader(file)
        .with_context(|| format!("failed to parse json file: {}", path.display()))?;
    match json {
        Value::Array(items) => Ok(items),
        _ => bail!("top-level json value is not an array: {}", path.display()),
    }
}

pub(crate) fn read_json_object_array(path: &Path) -> Result<Vec<Map<String, Value>>> {
    read_json_array(path)?
        .into_iter()
        .map(|value| match value {
            Value::Object(object) => Ok(object),
            _ => bail!("json array member is not an object: {}", path.display()),
        })
        .collect()
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
        let end = match trimmed[start..].find(',') {
            Some(offset) => start + offset,
            None => trimmed.len(),
        };
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
    let mut map = Map::new();
    for (index, column_name) in column_names.iter().enumerate() {
        map.insert(column_name.clone(), sql_value_to_json(row.get(index)?));
    }
    Ok(map)
}

fn select_columns(mut row: Map<String, Value>, ordered_columns: &[&str]) -> Map<String, Value> {
    let mut ordered = Map::new();
    for column in ordered_columns {
        if let Some(value) = row.remove(*column) {
            ordered.insert((*column).to_string(), value);
        }
    }
    ordered
}

fn round_json_number(value: &Value) -> Value {
    if let Some(number) = value.as_f64() {
        let rounded = (number * 100_000_000.0).round() / 100_000_000.0;
        Number::from_f64(rounded)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    } else {
        value.clone()
    }
}
