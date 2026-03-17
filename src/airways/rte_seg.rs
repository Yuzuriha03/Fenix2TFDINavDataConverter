use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Cursor};
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use rusqlite::Connection;
use rusqlite::params_from_iter;
use serde_json::{Map, Number, Value};

use crate::db_json::{
    configure_read_connection, extract_csv_fields_simple, read_text_gbk, trim_csv_field,
};

use super::{AirwayMirrorReference, directed_airway_route_key};

type AirwayRows = Vec<Map<String, Value>>;
type AirwayBuildOutput = (AirwayRows, AirwayRows);

#[derive(Clone, Debug)]
pub(crate) struct PreloadedRteSegData {
    pub(super) mirror_reference: AirwayMirrorReference,
    pub(super) airway_rows: Vec<ParsedRteSegAirwayRow>,
    pub(super) required_waypoint_idents: HashSet<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RteSegDirection {
    Bidirectional,
    Forward,
    Reverse,
    Other,
}

impl RteSegDirection {
    fn parse(value: &str) -> Self {
        match value.trim() {
            "X" => Self::Bidirectional,
            "F" => Self::Forward,
            "B" => Self::Reverse,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ParsedRteSegAirwayRow {
    ident: String,
    start_ident: String,
    start_latitude: Option<f64>,
    start_longitude: Option<f64>,
    end_ident: String,
    end_latitude: Option<f64>,
    end_longitude: Option<f64>,
    direction: RteSegDirection,
}

#[derive(Clone, Debug)]
pub(crate) struct WaypointCandidate {
    id: i64,
    latitude: f64,
    longitude: f64,
}

#[derive(Clone, Debug)]
struct DirectedAirwaySegment {
    ident: String,
    start_ident: String,
    start_id: i64,
    end_ident: String,
    end_id: i64,
}

pub(super) fn load_waypoint_candidates_from_db(
    db_path: &Path,
    required_idents: &HashSet<String>,
) -> Result<HashMap<String, Vec<WaypointCandidate>>> {
    if required_idents.is_empty() {
        return Ok(HashMap::new());
    }

    let conn = Connection::open(db_path)?;
    configure_read_connection(&conn);

    let longitude_col = resolve_waypoint_longitude_column(&conn)?;

    let mut candidates: HashMap<String, Vec<WaypointCandidate>> = HashMap::new();
    let mut ident_list: Vec<String> = required_idents.iter().cloned().collect();
    ident_list.sort();
    for chunk in ident_list.chunks(900) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT ID, Ident, Latitude, {longitude_col} FROM Waypoints WHERE Ident IN ({placeholders}) AND Latitude IS NOT NULL AND {longitude_col} IS NOT NULL"
        );
        let mut statement = conn.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(chunk.iter()), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })?;

        for row in rows {
            let (id, ident, latitude, longitude) = row?;
            let ident = ident.trim();
            if ident.is_empty() {
                continue;
            }

            candidates
                .entry(ident.to_string())
                .or_default()
                .push(WaypointCandidate {
                    id,
                    latitude,
                    longitude,
                });
        }
    }
    Ok(candidates)
}

pub(super) fn collect_required_waypoint_idents(rows: &[ParsedRteSegAirwayRow]) -> HashSet<String> {
    let mut idents = HashSet::new();
    for row in rows {
        if !row.start_ident.trim().is_empty() {
            idents.insert(row.start_ident.clone());
        }
        if !row.end_ident.trim().is_empty() {
            idents.insert(row.end_ident.clone());
        }
    }
    idents
}

fn resolve_waypoint_longitude_column(conn: &Connection) -> Result<&'static str> {
    let mut statement = conn.prepare("PRAGMA table_info(Waypoints)")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_longitude = false;
    let mut has_longtitude = false;
    for row in rows {
        let column = row?;
        if column == "Longitude" {
            has_longitude = true;
        }
        if column == "Longtitude" {
            has_longtitude = true;
        }
    }
    if has_longitude {
        Ok("Longitude")
    } else if has_longtitude {
        Ok("Longtitude")
    } else {
        bail!("Waypoints table has neither Longitude nor Longtitude column")
    }
}

pub(super) fn build_airway_tables_from_rows(
    rows: &[ParsedRteSegAirwayRow],
    waypoint_candidates: &HashMap<String, Vec<WaypointCandidate>>,
) -> AirwayBuildOutput {
    let mut ident_order = Vec::new();
    let mut segments_by_ident: HashMap<String, Vec<DirectedAirwaySegment>> = HashMap::new();

    for row in rows {
        let Some(segment) = normalize_rte_seg_airway_row(row, waypoint_candidates) else {
            continue;
        };
        if !segments_by_ident.contains_key(&segment.ident) {
            ident_order.push(segment.ident.clone());
        }
        segments_by_ident
            .entry(segment.ident.clone())
            .or_default()
            .push(segment);
    }

    let mut next_airway_id = 1i64;
    let mut next_leg_id = 1i64;
    let mut airways = Vec::new();
    let mut airway_legs = Vec::new();
    for ident in ident_order {
        let Some(segments) = segments_by_ident.get(&ident) else {
            continue;
        };
        let airway_id = next_airway_id;
        next_airway_id += 1;

        let mut airway_row = Map::new();
        airway_row.insert("ID".to_string(), Value::Number(Number::from(airway_id)));
        airway_row.insert("Ident".to_string(), Value::String(ident.clone()));
        airways.push(airway_row);

        for chain in split_rte_seg_segments_into_chains(segments) {
            for (index, segment) in chain.iter().enumerate() {
                let mut leg_row = Map::new();
                leg_row.insert("ID".to_string(), Value::Number(Number::from(next_leg_id)));
                next_leg_id += 1;
                leg_row.insert(
                    "AirwayID".to_string(),
                    Value::Number(Number::from(airway_id)),
                );
                leg_row.insert("Level".to_string(), Value::String("B".to_string()));
                leg_row.insert(
                    "Waypoint1ID".to_string(),
                    Value::Number(Number::from(segment.start_id)),
                );
                leg_row.insert(
                    "Waypoint2ID".to_string(),
                    Value::Number(Number::from(segment.end_id)),
                );
                leg_row.insert(
                    "IsStart".to_string(),
                    Value::Number(Number::from(if index == 0 { 1 } else { 0 })),
                );
                leg_row.insert(
                    "IsEnd".to_string(),
                    Value::Number(Number::from(if index + 1 == chain.len() { 1 } else { 0 })),
                );
                leg_row.insert(
                    "Waypoint1".to_string(),
                    Value::String(segment.start_ident.clone()),
                );
                leg_row.insert(
                    "Waypoint2".to_string(),
                    Value::String(segment.end_ident.clone()),
                );
                airway_legs.push(leg_row);
            }
        }
    }

    (airways, airway_legs)
}

pub(super) fn load_rte_seg_mirror_reference(rte_seg_path: &Path) -> Result<AirwayMirrorReference> {
    let content = read_text_gbk(rte_seg_path)?;
    load_rte_seg_mirror_reference_from_bufread(Cursor::new(content))
}

pub(crate) fn preload_rte_seg(rte_seg_path: &Path) -> Result<PreloadedRteSegData> {
    let content = read_text_gbk(rte_seg_path)?;
    let (mirror_reference, airway_rows) = rayon::join(
        || load_rte_seg_mirror_reference_from_bufread(Cursor::new(content.as_bytes())),
        || parse_rte_seg_airway_rows_from_bufread(Cursor::new(content.as_bytes())),
    );
    let airway_rows = airway_rows?;
    let required_waypoint_idents = collect_required_waypoint_idents(&airway_rows);

    Ok(PreloadedRteSegData {
        mirror_reference: mirror_reference?,
        airway_rows,
        required_waypoint_idents,
    })
}

fn normalize_rte_seg_airway_row(
    row: &ParsedRteSegAirwayRow,
    waypoint_candidates: &HashMap<String, Vec<WaypointCandidate>>,
) -> Option<DirectedAirwaySegment> {
    let (start_ident, start_latitude, start_longitude, end_ident, end_latitude, end_longitude) =
        match row.direction {
            RteSegDirection::Bidirectional | RteSegDirection::Forward => (
                row.start_ident.as_str(),
                row.start_latitude,
                row.start_longitude,
                row.end_ident.as_str(),
                row.end_latitude,
                row.end_longitude,
            ),
            RteSegDirection::Reverse => (
                row.end_ident.as_str(),
                row.end_latitude,
                row.end_longitude,
                row.start_ident.as_str(),
                row.start_latitude,
                row.start_longitude,
            ),
            RteSegDirection::Other => return None,
        };

    let start_id = resolve_waypoint_id(
        waypoint_candidates,
        start_ident,
        start_latitude,
        start_longitude,
    )?;
    let end_id = resolve_waypoint_id(waypoint_candidates, end_ident, end_latitude, end_longitude)?;

    Some(DirectedAirwaySegment {
        ident: row.ident.clone(),
        start_ident: start_ident.to_string(),
        start_id,
        end_ident: end_ident.to_string(),
        end_id,
    })
}

fn split_rte_seg_segments_into_chains(
    segments: &[DirectedAirwaySegment],
) -> Vec<Vec<DirectedAirwaySegment>> {
    let mut chains = Vec::new();
    let mut current_chain = Vec::new();
    for segment in segments {
        let starts_new_chain = current_chain
            .last()
            .map(|last: &DirectedAirwaySegment| {
                last.end_id != segment.start_id || last.end_ident != segment.start_ident
            })
            .unwrap_or(true);
        if starts_new_chain && !current_chain.is_empty() {
            chains.push(std::mem::take(&mut current_chain));
        }
        current_chain.push(segment.clone());
    }
    if !current_chain.is_empty() {
        chains.push(current_chain);
    }
    chains
}

fn resolve_waypoint_id(
    waypoint_candidates: &HashMap<String, Vec<WaypointCandidate>>,
    ident: &str,
    latitude: Option<f64>,
    longitude: Option<f64>,
) -> Option<i64> {
    let candidates = waypoint_candidates.get(ident)?;
    if candidates.len() == 1 || latitude.is_none() || longitude.is_none() {
        return candidates.first().map(|candidate| candidate.id);
    }

    let (latitude, longitude) = (latitude?, longitude?);
    candidates
        .iter()
        .min_by(|left, right| {
            let left_distance =
                coordinate_distance_sq(latitude, longitude, left.latitude, left.longitude);
            let right_distance =
                coordinate_distance_sq(latitude, longitude, right.latitude, right.longitude);
            left_distance
                .partial_cmp(&right_distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|candidate| candidate.id)
}

fn coordinate_distance_sq(
    left_latitude: f64,
    left_longitude: f64,
    right_latitude: f64,
    right_longitude: f64,
) -> f64 {
    let latitude_delta = left_latitude - right_latitude;
    let longitude_delta = left_longitude - right_longitude;
    latitude_delta * latitude_delta + longitude_delta * longitude_delta
}

fn load_rte_seg_mirror_reference_from_bufread<R: BufRead>(
    mut reader: R,
) -> Result<AirwayMirrorReference> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(AirwayMirrorReference::default());
    }
    let [ident_idx, start_idx, end_idx, dir_idx] = parse_rte_seg_header_indices_simple(&line)?;
    let mut directed_edges = HashSet::new();
    let mut normalized_edges = Vec::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let [ident, start, end, code_dir] =
            extract_csv_fields_simple(&line, &[ident_idx, start_idx, end_idx, dir_idx]);
        let ident = ident.unwrap_or("").trim();
        let start = start.unwrap_or("").trim();
        let end = end.unwrap_or("").trim();
        if ident.is_empty() || start.is_empty() || end.is_empty() {
            continue;
        }

        let direction = RteSegDirection::parse(code_dir.unwrap_or(""));
        let (waypoint1, waypoint2) = match direction {
            RteSegDirection::Bidirectional | RteSegDirection::Forward => {
                (start.to_string(), end.to_string())
            }
            RteSegDirection::Reverse => (end.to_string(), start.to_string()),
            RteSegDirection::Other => continue,
        };
        directed_edges.insert(directed_airway_route_key(ident, &waypoint1, &waypoint2));
        if direction == RteSegDirection::Bidirectional {
            directed_edges.insert(directed_airway_route_key(ident, &waypoint2, &waypoint1));
        }
        normalized_edges.push((ident.to_string(), waypoint1, waypoint2));
    }

    let mut mirrored_edge_keys = HashSet::new();
    for (ident, waypoint1, waypoint2) in normalized_edges {
        let reverse_key = directed_airway_route_key(&ident, &waypoint2, &waypoint1);
        if directed_edges.contains(&reverse_key) {
            let key = if waypoint1 <= waypoint2 {
                directed_airway_route_key(&ident, &waypoint1, &waypoint2)
            } else {
                directed_airway_route_key(&ident, &waypoint2, &waypoint1)
            };
            mirrored_edge_keys.insert(key);
        }
    }
    Ok(AirwayMirrorReference { mirrored_edge_keys })
}

fn parse_rte_seg_header_indices_simple(line: &str) -> Result<[usize; 4]> {
    let header = line.strip_prefix('\u{feff}').unwrap_or(line);
    let mut index_map = HashMap::new();
    for (idx, field) in header.trim_end_matches(['\r', '\n']).split(',').enumerate() {
        index_map.insert(trim_csv_field(field), idx);
    }
    let required = |name: &str| {
        index_map
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("required RTE_SEG.csv column missing: {}", name))
    };
    Ok([
        required("TXT_DESIG")?,
        required("CODE_POINT_START")?,
        required("CODE_POINT_END")?,
        required("CODE_DIR")?,
    ])
}

pub(super) fn load_rte_seg_airway_rows(rte_seg_path: &Path) -> Result<Vec<ParsedRteSegAirwayRow>> {
    let content = read_text_gbk(rte_seg_path)?;
    parse_rte_seg_airway_rows_from_bufread(Cursor::new(content))
}

fn parse_rte_seg_airway_rows_from_bufread<R: BufRead>(
    mut reader: R,
) -> Result<Vec<ParsedRteSegAirwayRow>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(Vec::new());
    }
    let [
        ident_idx,
        start_ident_idx,
        start_longitude_idx,
        start_latitude_idx,
        end_ident_idx,
        end_longitude_idx,
        end_latitude_idx,
        dir_idx,
    ] = parse_rte_seg_airway_header_indices_simple(&line)?;
    let mut rows = Vec::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let [
            ident,
            start_ident,
            start_longitude,
            start_latitude,
            end_ident,
            end_longitude,
            end_latitude,
            code_dir,
        ] = extract_csv_fields_simple(
            &line,
            &[
                ident_idx,
                start_ident_idx,
                start_longitude_idx,
                start_latitude_idx,
                end_ident_idx,
                end_longitude_idx,
                end_latitude_idx,
                dir_idx,
            ],
        );
        let ident = ident.unwrap_or("").trim();
        let start_ident = start_ident.unwrap_or("").trim();
        let end_ident = end_ident.unwrap_or("").trim();
        if ident.is_empty() || start_ident.is_empty() || end_ident.is_empty() {
            continue;
        }

        rows.push(ParsedRteSegAirwayRow {
            ident: ident.to_string(),
            start_ident: start_ident.to_string(),
            start_latitude: start_latitude.and_then(parse_dms_to_decimal),
            start_longitude: start_longitude.and_then(parse_dms_to_decimal),
            end_ident: end_ident.to_string(),
            end_latitude: end_latitude.and_then(parse_dms_to_decimal),
            end_longitude: end_longitude.and_then(parse_dms_to_decimal),
            direction: RteSegDirection::parse(code_dir.unwrap_or("")),
        });
    }
    Ok(rows)
}

fn parse_rte_seg_airway_header_indices_simple(line: &str) -> Result<[usize; 8]> {
    let header = line.strip_prefix('\u{feff}').unwrap_or(line);
    let mut index_map = HashMap::new();
    for (idx, field) in header.trim_end_matches(['\r', '\n']).split(',').enumerate() {
        index_map.insert(trim_csv_field(field), idx);
    }
    let required = |name: &str| {
        index_map
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("required RTE_SEG.csv column missing: {}", name))
    };
    Ok([
        required("TXT_DESIG")?,
        required("CODE_POINT_START")?,
        required("GEO_LONG_START_ACCURACY")?,
        required("GEO_LAT_START_ACCURACY")?,
        required("CODE_POINT_END")?,
        required("GEO_LONG_END_ACCURACY")?,
        required("GEO_LAT_END_ACCURACY")?,
        required("CODE_DIR")?,
    ])
}

fn parse_dms_to_decimal(dms: &str) -> Option<f64> {
    let trimmed = dms.trim();
    if trimmed.len() < 5 {
        return None;
    }
    let mut chars = trimmed.chars();
    let direction = chars.next()?;
    let digits: String = chars.collect();
    let (degree_digits, minute_digits, second_digits) = match direction {
        'N' | 'S' => {
            if digits.len() < 6 {
                return None;
            }
            (&digits[..2], &digits[2..4], &digits[4..6])
        }
        'E' | 'W' => {
            if digits.len() < 7 {
                return None;
            }
            (&digits[..3], &digits[3..5], &digits[5..7])
        }
        _ => return None,
    };
    let degrees = degree_digits.parse::<f64>().ok()?;
    let minutes = minute_digits.parse::<f64>().ok()?;
    let seconds = second_digits.parse::<f64>().ok()?;
    let mut value = degrees + minutes / 60.0 + seconds / 3600.0;
    if matches!(direction, 'S' | 'W') {
        value = -value;
    }
    Some(value)
}
