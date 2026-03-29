use std::io::{BufRead, Cursor};
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use rusqlite::Connection;
use rusqlite::params_from_iter;
use rustc_hash::{FxBuildHasher as BuildHasher, FxHashMap as HashMap, FxHashSet as HashSet};

use crate::db_json::{
    configure_read_connection, extract_csv_fields_simple, read_text_gbk, trim_csv_field,
};
use crate::waypoints::WaypointIdIndex;

use super::{
    AirwayLegOutputRow, AirwayMirrorReference, AirwayOutputRow, directed_airway_route_key,
};

type AirwayBuildOutput = (Vec<AirwayOutputRow>, Vec<AirwayLegOutputRow>);
type AirwayNodeKey = i64;

const fn fast_hash_map<K, V>() -> HashMap<K, V> {
    HashMap::with_hasher(BuildHasher)
}

fn fast_hash_map_with_capacity<K, V>(capacity: usize) -> HashMap<K, V> {
    HashMap::with_capacity_and_hasher(capacity, BuildHasher)
}

const fn fast_hash_set<T>() -> HashSet<T> {
    HashSet::with_hasher(BuildHasher)
}

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
    source_order: usize,
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
    source_order: usize,
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
        return Ok(fast_hash_map());
    }

    let conn = Connection::open(db_path)?;
    configure_read_connection(&conn);

    let longitude_col = resolve_waypoint_longitude_column(&conn)?;

    let mut candidates: HashMap<String, Vec<WaypointCandidate>> =
        fast_hash_map_with_capacity(required_idents.len());
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
    let mut idents = fast_hash_set();
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
    let mut has_legacy_longtitude = false;
    for row in rows {
        let column = row?;
        if column == "Longitude" {
            has_longitude = true;
        }
        if column == "Longtitude" {
            has_legacy_longtitude = true;
        }
    }
    if has_longitude {
        Ok("Longitude")
    } else if has_legacy_longtitude {
        Ok("Longtitude")
    } else {
        bail!("Waypoints table has neither Longitude nor Longtitude column")
    }
}

pub(super) fn build_airway_tables_from_rows_with_id_index(
    rows: &[ParsedRteSegAirwayRow],
    waypoint_candidates: &HashMap<String, Vec<WaypointCandidate>>,
    waypoint_id_index: Option<&WaypointIdIndex>,
) -> AirwayBuildOutput {
    let mut ident_order = Vec::new();
    let mut segments_by_ident: HashMap<String, Vec<DirectedAirwaySegment>> = fast_hash_map();

    for row in rows {
        let Some(segment) =
            normalize_rte_seg_airway_row(row, waypoint_candidates, waypoint_id_index)
        else {
            continue;
        };
        let ident = segment.ident.clone();
        let entry = segments_by_ident.entry(ident.clone()).or_default();
        if entry.is_empty() {
            ident_order.push(ident);
        }
        entry.push(segment);
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

        airways.push(AirwayOutputRow {
            id: airway_id,
            ident: ident.clone(),
        });

        for chain in split_rte_seg_segments_into_chains(segments) {
            for (index, segment) in chain.iter().enumerate() {
                airway_legs.push(AirwayLegOutputRow {
                    id: next_leg_id,
                    airway_id,
                    level: "B".to_string(),
                    waypoint1: segment.start_ident.clone(),
                    waypoint2: segment.end_ident.clone(),
                    waypoint1_id: Some(segment.start_id),
                    waypoint2_id: Some(segment.end_id),
                    is_start: i64::from(index == 0),
                    is_end: i64::from(index + 1 == chain.len()),
                });
                next_leg_id += 1;
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
    waypoint_id_index: Option<&WaypointIdIndex>,
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
    )
    .map(|id| waypoint_id_index.map_or(id, |index| index.output_id_for_db(id)))?;
    let end_id = resolve_waypoint_id(waypoint_candidates, end_ident, end_latitude, end_longitude)
        .map(|id| waypoint_id_index.map_or(id, |index| index.output_id_for_db(id)))?;

    Some(DirectedAirwaySegment {
        ident: row.ident.clone(),
        source_order: row.source_order,
        start_ident: start_ident.to_string(),
        start_id,
        end_ident: end_ident.to_string(),
        end_id,
    })
}

fn split_rte_seg_segments_into_chains(
    segments: &[DirectedAirwaySegment],
) -> Vec<Vec<DirectedAirwaySegment>> {
    if segments.is_empty() {
        return Vec::new();
    }

    let mut outgoing_by_node: HashMap<AirwayNodeKey, Vec<usize>> = fast_hash_map();
    let mut incoming_count_by_node: HashMap<AirwayNodeKey, usize> = fast_hash_map();
    let mut outgoing_count_by_node: HashMap<AirwayNodeKey, usize> = fast_hash_map();

    for (index, segment) in segments.iter().enumerate() {
        let start_node = segment_start_node(segment);
        let end_node = segment_end_node(segment);

        outgoing_by_node.entry(start_node).or_default().push(index);
        *outgoing_count_by_node.entry(start_node).or_default() += 1;
        *incoming_count_by_node.entry(end_node).or_default() += 1;
    }

    for segment_indices in outgoing_by_node.values_mut() {
        segment_indices.sort_by_key(|index| {
            let segment = &segments[*index];
            (
                segment.source_order,
                segment.start_ident.as_str(),
                segment.end_ident.as_str(),
            )
        });
    }

    let mut start_nodes: Vec<&AirwayNodeKey> = outgoing_by_node.keys().collect();
    start_nodes.sort_by_key(|node| {
        outgoing_by_node
            .get(node)
            .and_then(|segment_indices| segment_indices.first())
            .map_or(usize::MAX, |index| segments[*index].source_order)
    });

    let mut chains = Vec::new();
    let mut used_segments = vec![false; segments.len()];

    for node in start_nodes {
        let outgoing_count = outgoing_count_by_node.get(node).copied().unwrap_or(0);
        let incoming_count = incoming_count_by_node.get(node).copied().unwrap_or(0);
        if outgoing_count == 0 || (incoming_count == 1 && outgoing_count == 1) {
            continue;
        }

        let Some(segment_indices) = outgoing_by_node.get(node) else {
            continue;
        };

        for &segment_index in segment_indices {
            if used_segments[segment_index] {
                continue;
            }
            chains.push(build_airway_segment_chain(
                segments,
                &outgoing_by_node,
                &incoming_count_by_node,
                &outgoing_count_by_node,
                &mut used_segments,
                segment_index,
            ));
        }
    }

    let mut remaining_indices: Vec<usize> = (0..segments.len())
        .filter(|index| !used_segments[*index])
        .collect();
    remaining_indices.sort_by_key(|index| segments[*index].source_order);
    for segment_index in remaining_indices {
        if used_segments[segment_index] {
            continue;
        }
        chains.push(build_airway_segment_chain(
            segments,
            &outgoing_by_node,
            &incoming_count_by_node,
            &outgoing_count_by_node,
            &mut used_segments,
            segment_index,
        ));
    }

    chains.sort_by_key(|chain| {
        chain
            .first()
            .map_or(usize::MAX, |segment| segment.source_order)
    });
    chains
}

fn build_airway_segment_chain(
    segments: &[DirectedAirwaySegment],
    outgoing_by_node: &HashMap<AirwayNodeKey, Vec<usize>>,
    incoming_count_by_node: &HashMap<AirwayNodeKey, usize>,
    outgoing_count_by_node: &HashMap<AirwayNodeKey, usize>,
    used_segments: &mut [bool],
    start_index: usize,
) -> Vec<DirectedAirwaySegment> {
    let mut chain = Vec::new();
    let mut current_index = start_index;

    loop {
        if used_segments[current_index] {
            break;
        }

        used_segments[current_index] = true;
        let segment = segments[current_index].clone();
        let next_node = segment_end_node(&segment);
        chain.push(segment);

        let incoming_count = incoming_count_by_node.get(&next_node).copied().unwrap_or(0);
        let outgoing_count = outgoing_count_by_node.get(&next_node).copied().unwrap_or(0);
        if incoming_count != 1 || outgoing_count != 1 {
            break;
        }

        let Some(next_segment_index) =
            outgoing_by_node
                .get(&next_node)
                .and_then(|segment_indices| {
                    segment_indices
                        .iter()
                        .copied()
                        .find(|index| !used_segments[*index])
                })
        else {
            break;
        };

        current_index = next_segment_index;
    }

    chain
}

const fn segment_start_node(segment: &DirectedAirwaySegment) -> AirwayNodeKey {
    segment.start_id
}

const fn segment_end_node(segment: &DirectedAirwaySegment) -> AirwayNodeKey {
    segment.end_id
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
    latitude_delta.mul_add(latitude_delta, longitude_delta * longitude_delta)
}

fn load_rte_seg_mirror_reference_from_bufread<R: BufRead>(
    mut reader: R,
) -> Result<AirwayMirrorReference> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(AirwayMirrorReference::default());
    }
    let [ident_idx, start_idx, end_idx, dir_idx] = parse_rte_seg_header_indices_simple(&line)?;
    let mut directed_edges = fast_hash_set();
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

    let mut mirror_reference = AirwayMirrorReference::default();
    for (ident, waypoint1, waypoint2) in normalized_edges {
        let reverse_key = directed_airway_route_key(&ident, &waypoint2, &waypoint1);
        if directed_edges.contains(&reverse_key) {
            mirror_reference.insert_mirrored_pair(ident, waypoint1, waypoint2);
        }
    }
    Ok(mirror_reference)
}

fn parse_rte_seg_header_indices_simple(line: &str) -> Result<[usize; 4]> {
    let header = line.strip_prefix('\u{feff}').unwrap_or(line);
    let mut index_map = fast_hash_map();
    for (idx, field) in header.trim_end_matches(['\r', '\n']).split(',').enumerate() {
        index_map.insert(trim_csv_field(field), idx);
    }
    let required = |name: &str| {
        index_map
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("required RTE_SEG.csv column missing: {name}"))
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
    let mut source_order = 0usize;

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
            source_order,
            ident: ident.to_string(),
            start_ident: start_ident.to_string(),
            start_latitude: start_latitude.and_then(parse_dms_to_decimal),
            start_longitude: start_longitude.and_then(parse_dms_to_decimal),
            end_ident: end_ident.to_string(),
            end_latitude: end_latitude.and_then(parse_dms_to_decimal),
            end_longitude: end_longitude.and_then(parse_dms_to_decimal),
            direction: RteSegDirection::parse(code_dir.unwrap_or("")),
        });
        source_order += 1;
    }
    Ok(rows)
}

fn parse_rte_seg_airway_header_indices_simple(line: &str) -> Result<[usize; 8]> {
    let header = line.strip_prefix('\u{feff}').unwrap_or(line);
    let mut index_map = fast_hash_map();
    for (idx, field) in header.trim_end_matches(['\r', '\n']).split(',').enumerate() {
        index_map.insert(trim_csv_field(field), idx);
    }
    let required = |name: &str| {
        index_map
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("required RTE_SEG.csv column missing: {name}"))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: i64) -> WaypointCandidate {
        WaypointCandidate {
            id,
            latitude: 0.0,
            longitude: 0.0,
        }
    }

    #[test]
    fn builds_forward_airway_chain_with_single_start_and_end() {
        let rows = vec![
            ParsedRteSegAirwayRow {
                source_order: 0,
                ident: "H100".to_string(),
                start_ident: "A".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "B".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Forward,
            },
            ParsedRteSegAirwayRow {
                source_order: 1,
                ident: "H100".to_string(),
                start_ident: "B".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "C".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Forward,
            },
            ParsedRteSegAirwayRow {
                source_order: 2,
                ident: "H100".to_string(),
                start_ident: "C".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "D".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Forward,
            },
        ];
        let waypoint_candidates: HashMap<String, Vec<WaypointCandidate>> = [
            ("A".to_string(), vec![candidate(1)]),
            ("B".to_string(), vec![candidate(2)]),
            ("C".to_string(), vec![candidate(3)]),
            ("D".to_string(), vec![candidate(4)]),
        ]
        .into_iter()
        .collect();

        let (_airways, legs) =
            build_airway_tables_from_rows_with_id_index(&rows, &waypoint_candidates, None);

        assert_eq!(legs.len(), 3);
        assert_eq!(legs[0].waypoint1, "A");
        assert_eq!(legs[0].waypoint2, "B");
        assert_eq!(legs[0].is_start, 1);
        assert_eq!(legs[0].is_end, 0);

        assert_eq!(legs[1].waypoint1, "B");
        assert_eq!(legs[1].waypoint2, "C");
        assert_eq!(legs[1].is_start, 0);
        assert_eq!(legs[1].is_end, 0);

        assert_eq!(legs[2].waypoint1, "C");
        assert_eq!(legs[2].waypoint2, "D");
        assert_eq!(legs[2].is_start, 0);
        assert_eq!(legs[2].is_end, 1);
    }

    #[test]
    fn builds_reverse_airway_chain_without_splitting_every_segment() {
        let rows = vec![
            ParsedRteSegAirwayRow {
                source_order: 0,
                ident: "H166".to_string(),
                start_ident: "ELNEX".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "P643".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Reverse,
            },
            ParsedRteSegAirwayRow {
                source_order: 1,
                ident: "H166".to_string(),
                start_ident: "P643".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "P644".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Reverse,
            },
            ParsedRteSegAirwayRow {
                source_order: 2,
                ident: "H166".to_string(),
                start_ident: "P644".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "P645".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Reverse,
            },
            ParsedRteSegAirwayRow {
                source_order: 3,
                ident: "H166".to_string(),
                start_ident: "P645".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "P652".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Reverse,
            },
        ];
        let waypoint_candidates: HashMap<String, Vec<WaypointCandidate>> = [
            ("ELNEX".to_string(), vec![candidate(1)]),
            ("P643".to_string(), vec![candidate(2)]),
            ("P644".to_string(), vec![candidate(3)]),
            ("P645".to_string(), vec![candidate(4)]),
            ("P652".to_string(), vec![candidate(5)]),
        ]
        .into_iter()
        .collect();

        let (_airways, legs) =
            build_airway_tables_from_rows_with_id_index(&rows, &waypoint_candidates, None);

        assert_eq!(legs.len(), 4);
        assert_eq!(legs[0].waypoint1, "P652");
        assert_eq!(legs[0].waypoint2, "P645");
        assert_eq!(legs[0].is_start, 1);
        assert_eq!(legs[0].is_end, 0);

        assert_eq!(legs[1].waypoint1, "P645");
        assert_eq!(legs[1].waypoint2, "P644");
        assert_eq!(legs[1].is_start, 0);
        assert_eq!(legs[1].is_end, 0);

        assert_eq!(legs[2].waypoint1, "P644");
        assert_eq!(legs[2].waypoint2, "P643");
        assert_eq!(legs[2].is_start, 0);
        assert_eq!(legs[2].is_end, 0);

        assert_eq!(legs[3].waypoint1, "P643");
        assert_eq!(legs[3].waypoint2, "ELNEX");
        assert_eq!(legs[3].is_start, 0);
        assert_eq!(legs[3].is_end, 1);
    }

    #[test]
    fn keeps_branching_airway_as_two_chains_when_segments_merge_into_one_fix() {
        let rows = vec![
            ParsedRteSegAirwayRow {
                source_order: 0,
                ident: "J89".to_string(),
                start_ident: "ANLAM".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "IGUNI".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Forward,
            },
            ParsedRteSegAirwayRow {
                source_order: 1,
                ident: "J89".to_string(),
                start_ident: "IGUNI".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "WLY".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Forward,
            },
            ParsedRteSegAirwayRow {
                source_order: 2,
                ident: "J89".to_string(),
                start_ident: "WLY".to_string(),
                start_latitude: None,
                start_longitude: None,
                end_ident: "VEDPO".to_string(),
                end_latitude: None,
                end_longitude: None,
                direction: RteSegDirection::Reverse,
            },
        ];
        let waypoint_candidates: HashMap<String, Vec<WaypointCandidate>> = [
            ("ANLAM".to_string(), vec![candidate(1)]),
            ("IGUNI".to_string(), vec![candidate(2)]),
            ("WLY".to_string(), vec![candidate(3)]),
            ("VEDPO".to_string(), vec![candidate(4)]),
        ]
        .into_iter()
        .collect();

        let (_airways, legs) =
            build_airway_tables_from_rows_with_id_index(&rows, &waypoint_candidates, None);

        assert_eq!(legs.len(), 3);

        assert_eq!(legs[0].waypoint1, "ANLAM");
        assert_eq!(legs[0].waypoint2, "IGUNI");
        assert_eq!(legs[0].is_start, 1);
        assert_eq!(legs[0].is_end, 0);

        assert_eq!(legs[1].waypoint1, "IGUNI");
        assert_eq!(legs[1].waypoint2, "WLY");
        assert_eq!(legs[1].is_start, 0);
        assert_eq!(legs[1].is_end, 1);

        assert_eq!(legs[2].waypoint1, "VEDPO");
        assert_eq!(legs[2].waypoint2, "WLY");
        assert_eq!(legs[2].is_start, 1);
        assert_eq!(legs[2].is_end, 1);
    }
}
