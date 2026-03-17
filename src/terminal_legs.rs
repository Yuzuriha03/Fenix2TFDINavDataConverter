use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use rayon::prelude::*;
use rusqlite::{Connection, params, params_from_iter};
use serde_json::{Map, Number, Value};

use crate::db_json::{
    configure_read_connection, json_to_i64, sql_value_to_json, write_json,
};
use crate::stats::{PhaseDurations, TerminalLegExportStats};

#[derive(Clone, Debug)]
struct TerminalLegRecord {
    id: i64,
    terminal_id: i64,
    leg_type: Value,
    transition: Value,
    track_code: Value,
    wpt_id: Value,
    wpt_lat: Value,
    wpt_lon: Value,
    turn_dir: Value,
    nav_id: Value,
    nav_lat: Value,
    nav_lon: Value,
    nav_bear: Value,
    nav_dist: Value,
    course: Value,
    distance: Value,
    alt: Value,
    vnav: Value,
    center_id: Value,
    center_lat: Value,
    center_lon: Value,
    is_fly_over: Value,
    speed_limit: Value,
    is_faf: i64,
    is_map: i64,
}

impl TerminalLegRecord {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let id = row.get::<_, i64>(0)?;
        let terminal_id = row.get::<_, i64>(1)?;
        let alt = sql_value_to_json(row.get(16)?);
        let is_map = matches!(&alt, Value::String(text) if text == "MAP");
        Ok(Self {
            id,
            terminal_id,
            leg_type: sql_value_to_json(row.get(2)?),
            transition: sql_value_to_json(row.get(3)?),
            track_code: sql_value_to_json(row.get(4)?),
            wpt_id: sql_value_to_json(row.get(5)?),
            wpt_lat: sql_value_to_json(row.get(6)?),
            wpt_lon: sql_value_to_json(row.get(7)?),
            turn_dir: sql_value_to_json(row.get(8)?),
            nav_id: sql_value_to_json(row.get(9)?),
            nav_lat: sql_value_to_json(row.get(10)?),
            nav_lon: sql_value_to_json(row.get(11)?),
            nav_bear: sql_value_to_json(row.get(12)?),
            nav_dist: sql_value_to_json(row.get(13)?),
            course: sql_value_to_json(row.get(14)?),
            distance: sql_value_to_json(row.get(15)?),
            alt,
            vnav: sql_value_to_json(row.get(17)?),
            center_id: sql_value_to_json(row.get(18)?),
            center_lat: sql_value_to_json(row.get(19)?),
            center_lon: sql_value_to_json(row.get(20)?),
            is_fly_over: sql_value_to_json(row.get(21)?),
            speed_limit: sql_value_to_json(row.get(22)?),
            is_faf: 0,
            is_map: if is_map { -1 } else { 0 },
        })
    }

    fn fill_coordinates(
        &mut self,
        runway_ids_by_terminal: &HashMap<i64, i64>,
        runway_coords: &HashMap<i64, (f64, f64)>,
        waypoint_coords: &HashMap<i64, (f64, f64)>,
    ) {
        let missing_wpt = value_is_null(&self.wpt_id)
            && value_is_null(&self.wpt_lat)
            && value_is_null(&self.wpt_lon);
        let is_map = matches!(&self.alt, Value::String(text) if text == "MAP");
        if missing_wpt && is_map {
            if let Some(runway_id) = runway_ids_by_terminal.get(&self.terminal_id)
                && let Some(coords) = runway_coords.get(runway_id)
            {
                self.wpt_lat = rounded_number_value(coords.0);
                self.wpt_lon = rounded_number_value(coords.1);
            }
            return;
        }

        if should_fill_value(&self.wpt_id, &self.wpt_lat, &self.wpt_lon) {
            if let Some(point_id) = json_to_i64(Some(&self.wpt_id))
                && let Some(coords) = waypoint_coords.get(&point_id)
            {
                self.wpt_lat = rounded_number_value(coords.0);
                self.wpt_lon = rounded_number_value(coords.1);
            }
            return;
        }

        if should_fill_value(&self.center_id, &self.center_lat, &self.center_lon) {
            if let Some(point_id) = json_to_i64(Some(&self.center_id))
                && let Some(coords) = waypoint_coords.get(&point_id)
            {
                self.center_lat = rounded_number_value(coords.0);
                self.center_lon = rounded_number_value(coords.1);
            }
            return;
        }

        if should_fill_value(&self.nav_id, &self.nav_lat, &self.nav_lon)
            && let Some(point_id) = json_to_i64(Some(&self.nav_id))
            && let Some(coords) = waypoint_coords.get(&point_id)
        {
            self.nav_lat = rounded_number_value(coords.0);
            self.nav_lon = rounded_number_value(coords.1);
        }
    }

    fn vnav(&self) -> Option<f64> {
        parse_vnav(Some(&self.vnav))
    }

    fn to_output_map(&self) -> Map<String, Value> {
        let mut ordered = Map::new();
        ordered.insert("ID".to_string(), Value::Number(Number::from(self.id)));
        ordered.insert(
            "TerminalID".to_string(),
            Value::Number(Number::from(self.terminal_id)),
        );
        ordered.insert("Type".to_string(), self.leg_type.clone());
        ordered.insert(
            "Transition".to_string(),
            normalize_leg_text(&self.transition),
        );
        ordered.insert("TrackCode".to_string(), self.track_code.clone());
        ordered.insert("WptID".to_string(), self.wpt_id.clone());
        ordered.insert("WptLat".to_string(), self.wpt_lat.clone());
        ordered.insert("WptLon".to_string(), self.wpt_lon.clone());
        ordered.insert("TurnDir".to_string(), normalize_leg_text(&self.turn_dir));
        ordered.insert("NavID".to_string(), self.nav_id.clone());
        ordered.insert("NavLat".to_string(), self.nav_lat.clone());
        ordered.insert("NavLon".to_string(), self.nav_lon.clone());
        ordered.insert("NavBear".to_string(), self.nav_bear.clone());
        ordered.insert("NavDist".to_string(), self.nav_dist.clone());
        ordered.insert("Course".to_string(), self.course.clone());
        ordered.insert("Distance".to_string(), self.distance.clone());
        ordered.insert("Alt".to_string(), normalize_leg_text(&self.alt));
        ordered.insert("Vnav".to_string(), self.vnav.clone());
        ordered.insert("CenterID".to_string(), self.center_id.clone());
        ordered.insert("CenterLat".to_string(), self.center_lat.clone());
        ordered.insert("CenterLon".to_string(), self.center_lon.clone());
        ordered.insert(
            "IsFlyOver".to_string(),
            if json_to_i64(Some(&self.is_fly_over)) == Some(1) {
                Value::Number(Number::from(-1))
            } else {
                self.is_fly_over.clone()
            },
        );
        ordered.insert(
            "SpeedLimit".to_string(),
            json_to_i64(Some(&self.speed_limit))
                .map(Number::from)
                .map(Value::Number)
                .unwrap_or_else(|| self.speed_limit.clone()),
        );
        ordered.insert(
            "IsFAF".to_string(),
            Value::Number(Number::from(self.is_faf)),
        );
        ordered.insert(
            "IsMAP".to_string(),
            Value::Number(Number::from(self.is_map)),
        );
        ordered
    }
}

pub(crate) fn export_terminal_legs(
    db_path: &Path,
    start_terminal_id: i64,
    output_dir: &Path,
) -> Result<TerminalLegExportStats> {
    let db_read_start = Instant::now();
    let (terminal_legs, runway_ids_by_terminal, terminal_ids, runway_coords, waypoint_coords) =
        with_connection(db_path, |conn| {
            let terminal_legs = fetch_terminal_legs(conn, start_terminal_id)?;
            let (runway_ids_by_terminal, terminal_ids) = fetch_terminal_metadata(conn)?;

            let runway_ids: HashSet<i64> = runway_ids_by_terminal.values().copied().collect();
            let waypoint_ids = collect_waypoint_reference_ids(&terminal_legs);

            let runway_coords = fetch_runway_coordinates_by_ids(conn, &runway_ids)?;
            let waypoint_coords = fetch_waypoint_coordinates_by_ids(conn, &waypoint_ids)?;

            Ok::<_, anyhow::Error>(
                (
                    terminal_legs,
                    runway_ids_by_terminal,
                    terminal_ids,
                    runway_coords,
                    waypoint_coords,
                ),
            )
        })?;
    let db_read_time = db_read_start.elapsed();
    let row_count = terminal_legs.len();

    let json_transform_start = Instant::now();
    let mut terminal_groups: HashMap<i64, Vec<TerminalLegRecord>> = HashMap::new();
    for mut leg in terminal_legs {
        leg.fill_coordinates(&runway_ids_by_terminal, &runway_coords, &waypoint_coords);
        terminal_groups
            .entry(leg.terminal_id)
            .or_default()
            .push(leg);
    }
    let terminal_jobs: Vec<(i64, Vec<TerminalLegRecord>)> = terminal_groups
        .into_iter()
        .filter(|(terminal_id, _)| terminal_ids.contains(terminal_id))
        .collect();
    let file_count = terminal_jobs.len();
    let json_transform_time = json_transform_start.elapsed();

    let json_write_start = Instant::now();
    terminal_jobs
        .into_par_iter()
        .try_for_each(|(terminal_id, mut legs)| {
            mark_final_approach_fix(&mut legs);
            let ordered_legs: Vec<Value> = legs
                .iter()
                .map(|leg| Value::Object(leg.to_output_map()))
                .collect();
            let output_path = output_dir
                .join("ProcedureLegs")
                .join(format!("TermID_{terminal_id}.json"));
            write_json(&output_path, &ordered_legs)
        })?;

    let _ = cleanup_extra_procedure_files(output_dir, &terminal_ids)?;

    Ok(TerminalLegExportStats {
        row_count,
        file_count,
        phase: PhaseDurations {
            db_read: db_read_time,
            json_transform: json_transform_time,
            json_write: json_write_start.elapsed(),
        },
    })
}

fn with_connection<T>(db_path: &Path, action: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    configure_read_connection(&conn);
    action(&conn)
}

fn fetch_terminal_legs(
    conn: &Connection,
    start_terminal_id: i64,
) -> Result<Vec<TerminalLegRecord>> {
    let mut statement = conn
        .prepare(
            "SELECT l.ID, l.TerminalID, l.Type, l.Transition, l.TrackCode, l.WptID, l.WptLat, l.WptLon, l.TurnDir, l.NavID, l.NavLat, l.NavLon, l.NavBear, l.NavDist, l.Course, l.Distance, l.Alt, l.Vnav, l.CenterID, l.CenterLat, l.CenterLon, ex.IsFlyOver, ex.SpeedLimit FROM TerminalLegs l LEFT JOIN TerminalLegsEx ex ON ex.ID = l.ID WHERE l.TerminalID >= ?",
        )
        .context("failed to query TerminalLegs")?;
    let rows = statement
        .query_map(params![start_terminal_id], TerminalLegRecord::from_row)
        .context("failed to iterate TerminalLegs")?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read TerminalLegs")
}

fn fetch_terminal_metadata(conn: &Connection) -> Result<(HashMap<i64, i64>, HashSet<i64>)> {
    let mut statement = conn
        .prepare("SELECT ID, RwyID FROM Terminals")
        .context("failed to query terminal runway ids")?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .context("failed to iterate terminal runway ids")?;
    let mut runway_ids_by_terminal = HashMap::new();
    let mut terminal_ids = HashSet::new();
    for row in rows {
        let (terminal_id, runway_id) = row.context("failed to read terminal runway id row")?;
        terminal_ids.insert(terminal_id);
        if let Some(runway_id) = runway_id {
            runway_ids_by_terminal.insert(terminal_id, runway_id);
        }
    }
    Ok((runway_ids_by_terminal, terminal_ids))
}

fn cleanup_extra_procedure_files(
    output_dir: &Path,
    allowed_terminal_ids: &HashSet<i64>,
) -> Result<usize> {
    let procedure_dir = output_dir.join("ProcedureLegs");
    let mut removed = 0;

    if !procedure_dir.exists() {
        return Ok(0);
    }

    for entry in fs::read_dir(&procedure_dir).with_context(|| {
        format!(
            "failed to read ProcedureLegs directory: {}",
            procedure_dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read ProcedureLegs entry: {}",
                procedure_dir.display()
            )
        })?;
        let file_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        let Some(number_text) = file_name
            .strip_prefix("TermID_")
            .and_then(|v| v.strip_suffix(".json"))
        else {
            continue;
        };
        let Ok(terminal_id) = number_text.parse::<i64>() else {
            continue;
        };
        if !allowed_terminal_ids.contains(&terminal_id) {
            fs::remove_file(entry.path()).with_context(|| {
                format!(
                    "failed to remove stale ProcedureLegs file: {}",
                    entry.path().display()
                )
            })?;
            removed += 1;
        }
    }

    Ok(removed)
}

fn collect_waypoint_reference_ids(terminal_legs: &[TerminalLegRecord]) -> HashSet<i64> {
    let mut ids = HashSet::new();
    for leg in terminal_legs {
        if let Some(id) = json_to_i64(Some(&leg.wpt_id)) {
            ids.insert(id);
        }
        if let Some(id) = json_to_i64(Some(&leg.center_id)) {
            ids.insert(id);
        }
        if let Some(id) = json_to_i64(Some(&leg.nav_id)) {
            ids.insert(id);
        }
    }
    ids
}

fn fetch_waypoint_coordinates_by_ids(
    conn: &Connection,
    ids: &HashSet<i64>,
) -> Result<HashMap<i64, (f64, f64)>> {
    fetch_coordinates_by_ids(conn, "Waypoints", ids)
}

fn fetch_runway_coordinates_by_ids(
    conn: &Connection,
    ids: &HashSet<i64>,
) -> Result<HashMap<i64, (f64, f64)>> {
    fetch_coordinates_by_ids(conn, "Runways", ids)
}

fn fetch_coordinates_by_ids(
    conn: &Connection,
    table_name: &str,
    ids: &HashSet<i64>,
) -> Result<HashMap<i64, (f64, f64)>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let longitude_col = resolve_longitude_column(conn, table_name)?;
    let mut map = HashMap::new();

    let mut id_list: Vec<i64> = ids.iter().copied().collect();
    id_list.sort_unstable();
    for chunk in id_list.chunks(900) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "SELECT ID, Latitude, {longitude_col} FROM {table_name} WHERE ID IN ({placeholders})"
        );
        let mut statement = conn
            .prepare(&query)
            .with_context(|| format!("failed to query {table_name} coordinates"))?;
        let rows = statement
            .query_map(params_from_iter(chunk.iter()), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                ))
            })
            .with_context(|| format!("failed to iterate {table_name} coordinates"))?;

        for row in rows {
            let (id, latitude, longitude) =
                row.with_context(|| format!("failed to read {table_name} coordinate row"))?;
            let (Some(latitude), Some(longitude)) = (latitude, longitude) else {
                continue;
            };
            map.insert(id, (latitude, longitude));
        }
    }

    Ok(map)
}

fn resolve_longitude_column(conn: &Connection, table_name: &str) -> Result<&'static str> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table_name})"))
        .with_context(|| format!("failed to inspect schema for {table_name}"))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .with_context(|| format!("failed to iterate schema for {table_name}"))?;

    let mut has_longitude = false;
    let mut has_longtitude = false;
    for row in rows {
        let name = row.with_context(|| format!("failed to read schema row for {table_name}"))?;
        if name == "Longitude" {
            has_longitude = true;
        }
        if name == "Longtitude" {
            has_longtitude = true;
        }
    }

    if has_longitude {
        Ok("Longitude")
    } else if has_longtitude {
        Ok("Longtitude")
    } else {
        anyhow::bail!("{table_name} table has neither Longitude nor Longtitude column")
    }
}

fn value_is_null(value: &Value) -> bool {
    value.is_null()
}

fn should_fill_value(id_value: &Value, lat_value: &Value, lon_value: &Value) -> bool {
    !id_value.is_null() && lat_value.is_null() && lon_value.is_null()
}

fn rounded_number_value(value: f64) -> Value {
    let rounded = (value * 100_000_000.0).round() / 100_000_000.0;
    Number::from_f64(rounded)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn mark_final_approach_fix(legs: &mut [TerminalLegRecord]) {
    if legs.len() < 3 {
        return;
    }

    let mut prefix_valid = match legs[0].vnav() {
        Some(value) => value < 2.5,
        None => legs[0].vnav.is_null(),
    };

    for index in 1..(legs.len() - 1) {
        let current_valid = match legs[index].vnav() {
            Some(value) => value < 2.5,
            None => legs[index].vnav.is_null(),
        };
        prefix_valid = prefix_valid && current_valid;
        if !prefix_valid {
            continue;
        }
        if let Some(next_vnav) = legs[index + 1].vnav()
            && next_vnav > 2.5
        {
            legs[index].is_faf = -1;
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

fn normalize_leg_text(value: &Value) -> Value {
    if value.is_null() {
        Value::String(String::new())
    } else {
        value.clone()
    }
}
