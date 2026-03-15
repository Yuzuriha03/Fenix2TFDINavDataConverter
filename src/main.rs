use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, params};
use serde_json::{Map, Number, Value};
use sevenz_rust::SevenZWriter;

const REQUIRED_TABLES: &[&str] = &[
    "AirportCommunication",
    "AirportLookup",
    "Airports",
    "AirwayLegs",
    "Airways",
    "config",
    "Gls",
    "GridMora",
    "Holdings",
    "ILSes",
    "Markers",
    "MarkerTypes",
    "NavaidLookup",
    "Navaids",
    "NavaidTypes",
    "Runways",
    "SurfaceTypes",
    "TerminalLegs",
    "TerminalLegsEx",
    "Terminals",
    "TrmLegTypes",
    "WaypointLookup",
    "Waypoints",
];

const EXPORT_TABLES: &[&str] = &[
    "AirportLookup",
    "Airports",
    "AirwayLegs",
    "Airways",
    "Ilses",
    "NavaidLookup",
    "Navaids",
    "Runways",
    "Terminals",
    "WaypointLookup",
    "Waypoints",
];

const TERMINAL_LEG_COLUMNS: &[&str] = &[
    "ID",
    "TerminalID",
    "Type",
    "Transition",
    "TrackCode",
    "WptID",
    "WptLat",
    "WptLon",
    "TurnDir",
    "NavID",
    "NavLat",
    "NavLon",
    "NavBear",
    "NavDist",
    "Course",
    "Distance",
    "Alt",
    "Vnav",
    "CenterID",
    "CenterLat",
    "CenterLon",
    "IsFlyOver",
    "SpeedLimit",
    "IsFAF",
    "IsMAP",
];

fn main() {
    if let Err(error) = run() {
        eprintln!("错误: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    prepare_output_directory()?;

    let db_path = prompt_db3_path()?;
    let start_terminal_id = prompt_terminal_id()?;

    let conn = Connection::open(&db_path)
        .with_context(|| format!("无法打开数据库: {}", db_path.display()))?;
    validate_required_tables(&conn)?;
    drop(conn);

    export_db3_to_json(&db_path, start_terminal_id)?;
    compress_and_cleanup()?;

    println!("数据已成功导出并压缩为Nav-Primary.7z");
    Ok(())
}

fn prepare_output_directory() -> Result<()> {
    if Path::new("Nav-Primary").exists() {
        fs::remove_dir_all("Nav-Primary").context("无法清理旧的 Nav-Primary 目录")?;
    }

    fs::create_dir_all("Nav-Primary/ProcedureLegs").context("无法创建输出目录")?;

    if Path::new("Nav-Primary.7z").exists() {
        fs::remove_file("Nav-Primary.7z").context("无法删除旧的 Nav-Primary.7z")?;
    }

    Ok(())
}

fn prompt_db3_path() -> Result<PathBuf> {
    loop {
        let input = prompt("请输入Fenix NDB文件路径：")?;
        let trimmed = input.trim().trim_matches(['\'', '"']);
        let path = PathBuf::from(trimmed);

        if !path.exists() || path.extension().and_then(|ext| ext.to_str()) != Some("db3") {
            println!("文件路径无效或不是一个.db3文件，请重新输入。");
            continue;
        }

        let validation_conn = match Connection::open(&path) {
            Ok(conn) => conn,
            Err(error) => {
                println!("无法打开数据库: {error}，请重新输入。");
                continue;
            }
        };

        match validate_required_tables(&validation_conn) {
            Ok(()) => return Ok(path),
            Err(_) => println!("所读取文件不是fenix数据库格式，请重新输入db3文件路径："),
        }
    }
}

fn prompt_terminal_id() -> Result<i64> {
    loop {
        let input = prompt("请输入要转换的起始TerminalID：")?;
        match input.trim().parse::<i64>() {
            Ok(value) => return Ok(value),
            Err(_) => println!("TerminalID 必须是整数，请重新输入。"),
        }
    }
}

fn prompt(message: &str) -> Result<String> {
    print!("{message}");
    io::stdout().flush().context("无法刷新输出")?;

    let mut buffer = String::new();
    io::stdin()
        .read_line(&mut buffer)
        .context("无法读取输入")?;
    Ok(buffer)
}

fn validate_required_tables(conn: &Connection) -> Result<()> {
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .context("无法读取数据库表信息")?;

    let table_rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .context("无法遍历数据库表信息")?;

    let tables: HashSet<String> = table_rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("读取数据库表信息失败")?
        .into_iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();

    let missing: Vec<&str> = REQUIRED_TABLES
        .iter()
        .copied()
        .filter(|table| !tables.contains(&table.to_ascii_lowercase()))
        .collect();

    if missing.is_empty() {
        Ok(())
    } else {
        bail!("缺少必要数据表: {}", missing.join(", "))
    }
}

fn export_db3_to_json(db_path: &Path, start_terminal_id: i64) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("无法打开数据库: {}", db_path.display()))?;
    let waypoints = fetch_waypoints(&conn)?;
    drop(conn);

    let (table_export_result, terminal_legs_result) = rayon::join(
        || {
            EXPORT_TABLES
                .par_iter()
                .try_for_each(|table_name| export_table_to_json(db_path, table_name, &waypoints))
        },
        || export_terminal_legs(db_path, start_terminal_id),
    );

    table_export_result?;
    terminal_legs_result?;
    Ok(())
}

fn export_table_to_json(db_path: &Path, table_name: &str, waypoints: &HashMap<i64, String>) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("无法打开数据库: {}", db_path.display()))?;
    let rows = fetch_table_rows(&conn, table_name)?;
    let formatted_rows: Vec<Value> = rows
        .into_par_iter()
        .map(|row| Value::Object(format_row(row, table_name, waypoints)))
        .collect();

    let output_path = Path::new("Nav-Primary").join(format!("{table_name}.json"));
    write_json(&output_path, &formatted_rows)
}

fn fetch_waypoints(conn: &Connection) -> Result<HashMap<i64, String>> {
    let mut statement = conn
        .prepare("SELECT ID, Ident FROM Waypoints")
        .context("无法读取 Waypoints")?;
    let rows = statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))
        .context("无法遍历 Waypoints")?;

    rows.collect::<rusqlite::Result<HashMap<_, _>>>()
        .context("构建 Waypoints 索引失败")
}

fn fetch_table_rows(conn: &Connection, table_name: &str) -> Result<Vec<Map<String, Value>>> {
    let sql = format!("SELECT * FROM {table_name}");
    let mut statement = conn
        .prepare(&sql)
        .with_context(|| format!("无法查询数据表 {table_name}"))?;

    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect();

    let rows = statement
        .query_map([], |row| row_to_map(row, &column_names))
        .with_context(|| format!("无法遍历数据表 {table_name}"))?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("读取数据表 {table_name} 失败"))
}

fn row_to_map(row: &rusqlite::Row<'_>, column_names: &[String]) -> rusqlite::Result<Map<String, Value>> {
    let mut map = Map::new();

    for (index, column_name) in column_names.iter().enumerate() {
        let value = sql_value_to_json(row.get(index)?);
        map.insert(column_name.clone(), value);
    }

    Ok(map)
}

fn sql_value_to_json(value: SqlValue) -> Value {
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

fn format_row(
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
            row,
            &["ID", "AirportID", "Proc", "ICAO", "FullName", "Name", "Rwy", "RwyID"],
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
                let value = waypoints
                    .get(&waypoint_id)
                    .cloned()
                    .map(Value::String)
                    .unwrap_or(Value::Null);
                row.insert("Waypoint1".to_string(), value);
            } else {
                row.insert("Waypoint1".to_string(), Value::Null);
            }

            if let Some(waypoint_id) = json_to_i64(row.get("Waypoint2ID")) {
                let value = waypoints
                    .get(&waypoint_id)
                    .cloned()
                    .map(Value::String)
                    .unwrap_or(Value::Null);
                row.insert("Waypoint2".to_string(), value);
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
            &["ID", "Ident", "Name", "Latitude", "NavaidID", "Longitude", "Collocated"],
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

fn export_terminal_legs(db_path: &Path, start_terminal_id: i64) -> Result<()> {
    let (terminal_data_result, coordinate_data_result) = rayon::join(
        || {
            let terminal_legs = with_connection(db_path, |conn| fetch_terminal_legs(conn, start_terminal_id))?;
            let terminal_legs_ex = with_connection(db_path, |conn| fetch_table_rows(conn, "TerminalLegsEx"))?;
            let runway_ids_by_terminal = with_connection(db_path, fetch_runway_ids_by_terminal)?;
            Ok::<_, anyhow::Error>((terminal_legs, terminal_legs_ex, runway_ids_by_terminal))
        },
        || {
            let runway_coords = with_connection(db_path, |conn| fetch_coordinates_map(conn, "Runways"))?;
            let waypoint_coords = with_connection(db_path, |conn| fetch_coordinates_map(conn, "Waypoints"))?;
            Ok::<_, anyhow::Error>((runway_coords, waypoint_coords))
        },
    );

    let (terminal_legs, terminal_legs_ex, runway_ids_by_terminal) = terminal_data_result?;
    let (runway_coords, waypoint_coords) = coordinate_data_result?;

    let terminal_legs_ex_by_id: HashMap<i64, Map<String, Value>> = terminal_legs_ex
        .into_iter()
        .filter_map(|row| json_to_i64(row.get("ID")).map(|id| (id, row)))
        .collect();

    let mut terminal_groups: BTreeMap<i64, Vec<Map<String, Value>>> = BTreeMap::new();

    for mut leg in terminal_legs {
        let terminal_id = json_to_i64(leg.get("TerminalID"))
            .context("TerminalLegs 记录缺少 TerminalID")?;

        let is_map = matches!(leg.get("Alt"), Some(Value::String(text)) if text == "MAP");
        leg.insert("IsFAF".to_string(), Value::Number(Number::from(0)));
        leg.insert(
            "IsMAP".to_string(),
            Value::Number(Number::from(if is_map { -1 } else { 0 })),
        );

        if let Some(leg_id) = json_to_i64(leg.get("ID")) {
            if let Some(leg_ex) = terminal_legs_ex_by_id.get(&leg_id) {
                leg.insert(
                    "IsFlyOver".to_string(),
                    leg_ex.get("IsFlyOver").cloned().unwrap_or(Value::Null),
                );
                leg.insert(
                    "SpeedLimit".to_string(),
                    leg_ex.get("SpeedLimit").cloned().unwrap_or(Value::Null),
                );
            } else {
                leg.insert("IsFlyOver".to_string(), Value::Null);
                leg.insert("SpeedLimit".to_string(), Value::Null);
            }
        }

        if leg.get("IsFlyOver").is_none() {
            leg.insert("IsFlyOver".to_string(), Value::Null);
        }
        if leg.get("SpeedLimit").is_none() {
            leg.insert("SpeedLimit".to_string(), Value::Null);
        }

        fill_terminal_leg_coordinates(
            &mut leg,
            terminal_id,
            &runway_ids_by_terminal,
            &runway_coords,
            &waypoint_coords,
        );

        terminal_groups.entry(terminal_id).or_default().push(leg);
    }

    let terminal_jobs: Vec<(i64, Vec<Map<String, Value>>)> = terminal_groups.into_iter().collect();

    terminal_jobs.into_par_iter().try_for_each(|(terminal_id, mut legs)| {
        mark_final_approach_fix(&mut legs);
        let ordered_legs: Vec<Value> = legs
            .iter()
            .map(|leg| Value::Object(order_terminal_leg(leg)))
            .collect();

        let output_path = Path::new("Nav-Primary/ProcedureLegs")
            .join(format!("TermID_{terminal_id}.json"));
        write_json(&output_path, &ordered_legs)
    })?;

    Ok(())
}

fn with_connection<T>(db_path: &Path, action: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("无法打开数据库: {}", db_path.display()))?;
    action(&conn)
}

fn fetch_terminal_legs(conn: &Connection, start_terminal_id: i64) -> Result<Vec<Map<String, Value>>> {
    let mut statement = conn
        .prepare("SELECT * FROM TerminalLegs WHERE TerminalID >= ?")
        .context("无法查询 TerminalLegs")?;

    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect();

    let rows = statement
        .query_map(params![start_terminal_id], |row| row_to_map(row, &column_names))
        .context("无法遍历 TerminalLegs")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("读取 TerminalLegs 失败")
}

fn fetch_runway_ids_by_terminal(conn: &Connection) -> Result<HashMap<i64, i64>> {
    let mut statement = conn
        .prepare("SELECT ID, RwyID FROM Terminals")
        .context("无法读取 Terminals 的跑道信息")?;

    let rows = statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)))
        .context("无法遍历 Terminals 的跑道信息")?;

    let mut map = HashMap::new();
    for row in rows {
        let (terminal_id, runway_id) = row.context("读取 Terminals 跑道信息失败")?;
        if let Some(runway_id) = runway_id {
            map.insert(terminal_id, runway_id);
        }
    }

    Ok(map)
}

fn fetch_coordinates_map(conn: &Connection, table_name: &str) -> Result<HashMap<i64, (f64, f64)>> {
    let sql = format!("SELECT ID, Latitude, Longtitude FROM {table_name}");
    let mut statement = conn
        .prepare(&sql)
        .with_context(|| format!("无法读取 {table_name} 坐标信息"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, Option<f64>>(2)?,
            ))
        })
        .with_context(|| format!("无法遍历 {table_name} 坐标信息"))?;

    let mut map = HashMap::new();
    for row in rows {
        let (id, latitude, longitude) = row.with_context(|| format!("读取 {table_name} 坐标失败"))?;
        if let (Some(latitude), Some(longitude)) = (latitude, longitude) {
            map.insert(id, (latitude, longitude));
        }
    }

    Ok(map)
}

fn fill_terminal_leg_coordinates(
    leg: &mut Map<String, Value>,
    terminal_id: i64,
    runway_ids_by_terminal: &HashMap<i64, i64>,
    runway_coords: &HashMap<i64, (f64, f64)>,
    waypoint_coords: &HashMap<i64, (f64, f64)>,
) {
    let missing_wpt = all_null(leg, &["WptID", "WptLat", "WptLon"]);
    let is_map = matches!(leg.get("Alt"), Some(Value::String(text)) if text == "MAP");

    if missing_wpt && is_map {
        if let Some(runway_id) = runway_ids_by_terminal.get(&terminal_id) {
            if let Some(coords) = runway_coords.get(runway_id) {
                update_coordinates(leg, "WptLat", "WptLon", *coords);
            }
        }
        return;
    }

    if should_fill_point(leg, "WptID", "WptLat", "WptLon") {
        if let Some(point_id) = json_to_i64(leg.get("WptID")) {
            if let Some(coords) = waypoint_coords.get(&point_id) {
                update_coordinates(leg, "WptLat", "WptLon", *coords);
            }
        }
        return;
    }

    if should_fill_point(leg, "CenterID", "CenterLat", "CenterLon") {
        if let Some(point_id) = json_to_i64(leg.get("CenterID")) {
            if let Some(coords) = waypoint_coords.get(&point_id) {
                update_coordinates(leg, "CenterLat", "CenterLon", *coords);
            }
        }
        return;
    }

    if should_fill_point(leg, "NavID", "NavLat", "NavLon") {
        if let Some(point_id) = json_to_i64(leg.get("NavID")) {
            if let Some(coords) = waypoint_coords.get(&point_id) {
                update_coordinates(leg, "NavLat", "NavLon", *coords);
            }
        }
    }
}

fn all_null(leg: &Map<String, Value>, keys: &[&str]) -> bool {
    keys.iter()
        .all(|key| leg.get(*key).is_none_or(Value::is_null))
}

fn should_fill_point(leg: &Map<String, Value>, id_key: &str, lat_key: &str, lon_key: &str) -> bool {
    leg.get(id_key).is_some_and(|value| !value.is_null())
        && leg.get(lat_key).is_none_or(Value::is_null)
        && leg.get(lon_key).is_none_or(Value::is_null)
}

fn update_coordinates(leg: &mut Map<String, Value>, lat_key: &str, lon_key: &str, coords: (f64, f64)) {
    leg.insert(lat_key.to_string(), rounded_number_value(coords.0));
    leg.insert(lon_key.to_string(), rounded_number_value(coords.1));
}

fn rounded_number_value(value: f64) -> Value {
    let rounded = (value * 100_000_000.0).round() / 100_000_000.0;
    Number::from_f64(rounded)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn mark_final_approach_fix(legs: &mut [Map<String, Value>]) {
    if legs.len() < 3 {
        return;
    }

    for index in 1..(legs.len() - 1) {
        let valid = (0..=index).rev().all(|check_index| match parse_vnav(legs[check_index].get("Vnav")) {
            Some(value) => value < 2.5,
            None => legs[check_index].get("Vnav").is_none_or(Value::is_null),
        });

        if !valid {
            continue;
        }

        if let Some(next_vnav) = parse_vnav(legs[index + 1].get("Vnav")) {
            if next_vnav > 2.5 {
                legs[index].insert("IsFAF".to_string(), Value::Number(Number::from(-1)));
            }
        }
    }
}

fn parse_vnav(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => {
            if text.chars().filter(|ch| *ch == '.').count() <= 1
                && text.chars().all(|ch| ch.is_ascii_digit() || ch == '.')
            {
                text.parse::<f64>().ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

fn order_terminal_leg(leg: &Map<String, Value>) -> Map<String, Value> {
    let mut ordered = Map::new();

    for column in TERMINAL_LEG_COLUMNS {
        let value = leg.get(*column).cloned().unwrap_or(Value::Null);
        let normalized = match *column {
            "TurnDir" | "Transition" | "Alt" if value.is_null() => Value::String(String::new()),
            "SpeedLimit" => json_to_i64(Some(&value))
                .map(Number::from)
                .map(Value::Number)
                .unwrap_or(value),
            "IsFlyOver" if json_to_i64(Some(&value)) == Some(1) => Value::Number(Number::from(-1)),
            _ => value,
        };
        ordered.insert((*column).to_string(), normalized);
    }

    ordered
}

fn json_to_i64(value: Option<&Value>) -> Option<i64> {
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

fn write_json(path: &Path, data: &[Value]) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("无法创建输出文件: {}", path.display()))?;
    serde_json::to_writer(file, data)
        .with_context(|| format!("无法写入 JSON 文件: {}", path.display()))?;
    Ok(())
}

fn compress_and_cleanup() -> Result<()> {
    let source_dir = Path::new("Nav-Primary");
    let archive_path = Path::new("Nav-Primary.7z");

    let mut archive = SevenZWriter::create(archive_path)
        .context("无法创建 Nav-Primary.7z 压缩文件")?;
    archive
        .push_source_path_non_solid(source_dir, |_| true)
        .context("压缩 Nav-Primary 目录失败")?;
    archive.finish().context("无法完成 Nav-Primary.7z 写入")?;
    fs::remove_dir_all(source_dir).context("删除 Nav-Primary 目录失败")?;
    Ok(())
}
