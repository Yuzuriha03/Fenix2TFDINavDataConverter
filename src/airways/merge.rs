use std::collections::{HashMap, HashSet};

use serde_json::{Map, Number, Value};

use crate::db_json::{json_string, json_text, json_to_i64};

use super::{AirwayMirrorReference, AirwayReferenceData};

type AirwayRows = Vec<Map<String, Value>>;
type AirwayMergeOutput = (AirwayRows, AirwayRows);

#[derive(Clone, Debug)]
struct RouteSegmentTemplate {
    level: Value,
    waypoint1_id: Value,
    waypoint2_id: Value,
    waypoint1: Value,
}

#[derive(Clone, Debug)]
struct RouteLegComponent {
    rows: Vec<Map<String, Value>>,
    points: Vec<String>,
    point_set: HashSet<String>,
    pair_keys: HashSet<String>,
    is_simple_path: bool,
}

pub(super) fn merge_airway_outputs(
    source_airways: &[Map<String, Value>],
    source_legs: &[Map<String, Value>],
    airway_reference: &AirwayReferenceData,
) -> AirwayMergeOutput {
    let reference = airway_reference;
    if reference.airways.is_empty() && reference.airway_legs.is_empty() {
        let source_airway_ident_by_id = build_airway_ident_map(source_airways);
        let mut source_only_legs = source_legs.to_vec();
        let mut next_leg_id = source_only_legs
            .iter()
            .filter_map(|row| json_to_i64(row.get("ID")))
            .max()
            .unwrap_or(0)
            + 1;
        apply_route_mirror_reference(
            &mut source_only_legs,
            &source_airway_ident_by_id,
            &reference.mirror_reference,
            &mut next_leg_id,
        );
        return (source_airways.to_vec(), source_only_legs);
    }

    let source_airway_ident_by_id = build_airway_ident_map(source_airways);
    let reference_airway_ident_by_id = build_airway_ident_map(&reference.airways);
    let source_airways_by_ident = build_airway_row_map_by_ident(source_airways);
    let reference_airways_by_ident = build_airway_row_map_by_ident(&reference.airways);
    let source_legs_by_ident = group_airway_legs_by_ident(source_legs, &source_airway_ident_by_id);
    let reference_legs_by_ident =
        group_airway_legs_by_ident(&reference.airway_legs, &reference_airway_ident_by_id);
    let source_idents: HashSet<String> = source_airways_by_ident.keys().cloned().collect();

    let mut final_airways: AirwayRows = reference
        .airways
        .iter()
        .filter(|row| {
            row.get("Ident")
                .and_then(Value::as_str)
                .map(|ident| !source_idents.contains(ident))
                .unwrap_or(true)
        })
        .cloned()
        .collect();
    let mut final_legs: AirwayRows = reference
        .airway_legs
        .iter()
        .filter(|row| {
            let Some(airway_id) = json_to_i64(row.get("AirwayID")) else {
                return true;
            };
            let Some(ident) = reference_airway_ident_by_id.get(&airway_id) else {
                return true;
            };
            !source_idents.contains(ident)
        })
        .cloned()
        .collect();

    let mut next_airway_id = reference
        .airways
        .iter()
        .filter_map(|row| json_to_i64(row.get("ID")))
        .max()
        .unwrap_or(0)
        + 1;
    let mut next_leg_id = reference
        .airway_legs
        .iter()
        .filter_map(|row| json_to_i64(row.get("ID")))
        .max()
        .unwrap_or(0)
        + 1;

    let mut idents: Vec<String> = source_airways_by_ident.keys().cloned().collect();
    idents.sort();
    for ident in idents {
        let Some(source_airway_row) = source_airways_by_ident.get(&ident).cloned() else {
            continue;
        };
        let source_route_legs = source_legs_by_ident
            .get(&ident)
            .cloned()
            .unwrap_or_default();
        let existing_airway_row = reference_airways_by_ident.get(&ident).cloned();
        let existing_route_legs = reference_legs_by_ident
            .get(&ident)
            .cloned()
            .unwrap_or_default();

        let airway_row = if let Some(existing_airway_row) = existing_airway_row {
            existing_airway_row
        } else {
            let mut new_airway_row = source_airway_row;
            new_airway_row.insert(
                "ID".to_string(),
                Value::Number(Number::from(next_airway_id)),
            );
            next_airway_id += 1;
            new_airway_row
        };
        let airway_id = json_to_i64(airway_row.get("ID")).unwrap_or(0);
        final_airways.push(airway_row.clone());
        let merged_route_legs = build_merged_route_legs(
            &ident,
            airway_id,
            &source_route_legs,
            &existing_route_legs,
            &reference.mirror_reference,
            &mut next_leg_id,
        );
        final_legs.extend(merged_route_legs);
    }

    final_airways.sort_by_key(|row| json_to_i64(row.get("ID")).unwrap_or(i64::MAX));
    final_legs.sort_by_key(|row| json_to_i64(row.get("ID")).unwrap_or(i64::MAX));
    (final_airways, final_legs)
}

fn build_airway_ident_map(rows: &[Map<String, Value>]) -> HashMap<i64, String> {
    rows.iter()
        .filter_map(|row| {
            let airway_id = json_to_i64(row.get("ID"))?;
            let ident = row.get("Ident")?.as_str()?.to_string();
            Some((airway_id, ident))
        })
        .collect()
}

fn swap_map_values(row: &mut Map<String, Value>, left: &str, right: &str) {
    let left_value = row.get(left).cloned().unwrap_or(Value::Null);
    let right_value = row.get(right).cloned().unwrap_or(Value::Null);
    row.insert(left.to_string(), right_value);
    row.insert(right.to_string(), left_value);
}

fn directed_key_from_row(
    row: &Map<String, Value>,
    airway_ident_by_id: &HashMap<i64, String>,
) -> Option<String> {
    let airway_id = json_to_i64(row.get("AirwayID"))?;
    let ident = airway_ident_by_id.get(&airway_id)?;
    let level = json_text(row.get("Level"));
    let waypoint1 = json_text(row.get("Waypoint1"));
    let waypoint2 = json_text(row.get("Waypoint2"));
    if waypoint1.is_empty() || waypoint2.is_empty() {
        return None;
    }
    Some(directed_airway_edge_key(
        ident, &level, &waypoint1, &waypoint2,
    ))
}

fn build_airway_row_map_by_ident(
    rows: &[Map<String, Value>],
) -> HashMap<String, Map<String, Value>> {
    rows.iter()
        .filter_map(|row| {
            let ident = row.get("Ident")?.as_str()?.to_string();
            Some((ident, row.clone()))
        })
        .collect()
}

fn group_airway_legs_by_ident(
    rows: &[Map<String, Value>],
    airway_ident_by_id: &HashMap<i64, String>,
) -> HashMap<String, Vec<Map<String, Value>>> {
    let mut grouped: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for row in rows {
        let Some(airway_id) = json_to_i64(row.get("AirwayID")) else {
            continue;
        };
        let Some(ident) = airway_ident_by_id.get(&airway_id) else {
            continue;
        };
        grouped.entry(ident.clone()).or_default().push(row.clone());
    }
    grouped
}

fn build_merged_route_legs(
    ident: &str,
    airway_id: i64,
    source_route_legs: &[Map<String, Value>],
    existing_route_legs: &[Map<String, Value>],
    mirror_reference: &AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    if existing_route_legs.is_empty() {
        let mut route_legs =
            clone_route_legs_with_new_identity(source_route_legs, airway_id, next_leg_id);
        apply_route_mirror_reference_for_ident(
            &mut route_legs,
            ident,
            mirror_reference,
            &HashSet::new(),
            next_leg_id,
        );
        return route_legs;
    }

    let source_components = split_route_leg_components(source_route_legs);
    if source_components.is_empty() {
        return existing_route_legs.to_vec();
    }
    let existing_components = split_route_leg_components(existing_route_legs);
    if existing_components.is_empty() {
        let mut route_legs =
            clone_route_legs_with_new_identity(source_route_legs, airway_id, next_leg_id);
        apply_route_mirror_reference_for_ident(
            &mut route_legs,
            ident,
            mirror_reference,
            &HashSet::new(),
            next_leg_id,
        );
        return route_legs;
    }

    let mut existing_matches: Vec<Option<usize>> = vec![None; existing_components.len()];
    let mut matched_source_indices = HashSet::new();
    let mut used_existing_indices = HashSet::new();
    let mut match_candidates = Vec::new();
    for (source_index, source_component) in source_components.iter().enumerate() {
        for (existing_index, existing_component) in existing_components.iter().enumerate() {
            let common_pair_count =
                route_component_common_pair_count(existing_component, source_component);
            if common_pair_count == 0 {
                continue;
            }
            match_candidates.push((
                common_pair_count,
                route_component_common_point_count(existing_component, source_component),
                source_index,
                existing_index,
            ));
        }
    }
    match_candidates.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.cmp(&right.3))
    });
    for (_, _, source_index, existing_index) in match_candidates {
        if matched_source_indices.contains(&source_index)
            || used_existing_indices.contains(&existing_index)
        {
            continue;
        }
        existing_matches[existing_index] = Some(source_index);
        matched_source_indices.insert(source_index);
        used_existing_indices.insert(existing_index);
    }

    let mut merged_route_legs = Vec::new();
    for (existing_index, existing_component) in existing_components.iter().enumerate() {
        if let Some(source_index) = existing_matches[existing_index] {
            merged_route_legs.extend(merge_route_leg_component(
                ident,
                airway_id,
                &source_components[source_index],
                existing_component,
                mirror_reference,
                next_leg_id,
            ));
        } else {
            merged_route_legs.extend(existing_component.rows.clone());
        }
    }

    for (source_index, source_component) in source_components.iter().enumerate() {
        if matched_source_indices.contains(&source_index) {
            continue;
        }
        let mut appended_source =
            clone_route_legs_with_new_identity(&source_component.rows, airway_id, next_leg_id);
        apply_route_mirror_reference_for_ident(
            &mut appended_source,
            ident,
            mirror_reference,
            &HashSet::new(),
            next_leg_id,
        );
        merged_route_legs.extend(appended_source);
    }
    dedup_directed_route_legs(merged_route_legs)
}

fn clone_route_legs_with_new_identity(
    rows: &[Map<String, Value>],
    airway_id: i64,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    rows.iter()
        .map(|row| {
            let mut cloned = row.clone();
            cloned.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
            *next_leg_id += 1;
            cloned.insert(
                "AirwayID".to_string(),
                Value::Number(Number::from(airway_id)),
            );
            cloned
        })
        .collect()
}

fn dedup_directed_route_legs(rows: Vec<Map<String, Value>>) -> Vec<Map<String, Value>> {
    let mut deduped: Vec<Map<String, Value>> = Vec::new();
    let mut index_by_key: HashMap<String, usize> = HashMap::new();
    for row in rows {
        let level = json_text(row.get("Level"));
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            deduped.push(row);
            continue;
        }
        let key = directed_airway_edge_key("", &level, &waypoint1, &waypoint2);
        if let Some(existing_index) = index_by_key.get(&key).copied() {
            let existing = &mut deduped[existing_index];
            if json_to_i64(existing.get("IsStart")).unwrap_or(0) == 0
                && json_to_i64(row.get("IsStart")).unwrap_or(0) == 1
            {
                existing.insert("IsStart".to_string(), Value::Number(Number::from(1)));
            }
            if json_to_i64(existing.get("IsEnd")).unwrap_or(0) == 0
                && json_to_i64(row.get("IsEnd")).unwrap_or(0) == 1
            {
                existing.insert("IsEnd".to_string(), Value::Number(Number::from(1)));
            }
            continue;
        }
        index_by_key.insert(key, deduped.len());
        deduped.push(row);
    }
    deduped
}

fn merge_route_leg_component(
    ident: &str,
    airway_id: i64,
    source_component: &RouteLegComponent,
    existing_component: &RouteLegComponent,
    mirror_reference: &AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    if !source_component.is_simple_path || !existing_component.is_simple_path {
        if source_component.pair_keys == existing_component.pair_keys {
            let mut route_legs = existing_component.rows.clone();
            apply_route_mirror_reference_for_ident(
                &mut route_legs,
                ident,
                mirror_reference,
                &HashSet::new(),
                next_leg_id,
            );
            return route_legs;
        }

        let mut route_legs = existing_component.rows.clone();
        let mut appended_source =
            clone_route_legs_with_new_identity(&source_component.rows, airway_id, next_leg_id);
        apply_route_mirror_reference_for_ident(
            &mut appended_source,
            ident,
            mirror_reference,
            &HashSet::new(),
            next_leg_id,
        );
        route_legs.extend(appended_source);
        return route_legs;
    }

    let merged_order =
        merge_airway_route_order(&existing_component.points, &source_component.points);
    if merged_order.is_empty() {
        let mut route_legs = existing_component.rows.clone();
        let mut appended_source =
            clone_route_legs_with_new_identity(&source_component.rows, airway_id, next_leg_id);
        apply_route_mirror_reference_for_ident(
            &mut appended_source,
            ident,
            mirror_reference,
            &HashSet::new(),
            next_leg_id,
        );
        route_legs.extend(appended_source);
        return route_legs;
    }

    let source_lookup = build_segment_lookup(&source_component.rows);
    let existing_lookup = build_segment_lookup(&existing_component.rows);
    let expanded_mirror_pairs = build_route_mirror_pair_keys(
        ident,
        &existing_component.rows,
        &merged_order,
        mirror_reference,
    );
    let mut rebuilt = rebuild_route_legs_from_order(
        airway_id,
        &merged_order,
        &source_lookup,
        &existing_lookup,
        next_leg_id,
    );
    apply_route_mirror_reference_for_ident(
        &mut rebuilt,
        ident,
        mirror_reference,
        &expanded_mirror_pairs,
        next_leg_id,
    );
    rebuilt
}

fn split_route_leg_components(rows: &[Map<String, Value>]) -> Vec<RouteLegComponent> {
    let mut level_order = Vec::new();
    let mut rows_by_level: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for row in rows {
        let level = json_text(row.get("Level"));
        if !rows_by_level.contains_key(&level) {
            level_order.push(level.clone());
        }
        rows_by_level.entry(level).or_default().push(row.clone());
    }

    let mut components = Vec::new();
    for level in level_order {
        let Some(level_rows) = rows_by_level.remove(&level) else {
            continue;
        };
        components.extend(split_route_leg_components_for_level(&level_rows));
    }
    components
}

fn split_route_leg_components_for_level(rows: &[Map<String, Value>]) -> Vec<RouteLegComponent> {
    if rows.is_empty() {
        return Vec::new();
    }
    let mut components = split_started_route_leg_components(rows);
    if components.is_empty() {
        return split_connected_route_leg_components(rows);
    }

    let assigned_pair_keys: HashSet<String> = components
        .iter()
        .flat_map(|component| component.pair_keys.iter().cloned())
        .collect();
    let remaining_rows: Vec<Map<String, Value>> = rows
        .iter()
        .filter(|row| {
            row_pair_key(row)
                .map(|pair_key| !assigned_pair_keys.contains(&pair_key))
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    components.extend(split_connected_route_leg_components(&remaining_rows));
    components
}

fn split_started_route_leg_components(rows: &[Map<String, Value>]) -> Vec<RouteLegComponent> {
    let mut outgoing: HashMap<String, Vec<usize>> = HashMap::new();
    let mut start_indices = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        outgoing.entry(waypoint1).or_default().push(index);
        if json_to_i64(row.get("IsStart")) == Some(1) {
            start_indices.push(index);
        }
    }
    if start_indices.is_empty() {
        return Vec::new();
    }

    let mut started_paths = Vec::new();
    for start_index in start_indices {
        let Some(path) = collect_started_route_path(start_index, rows, &outgoing) else {
            continue;
        };
        started_paths.push(path);
    }
    started_paths.sort_by(|(left_points, left_pairs), (right_points, right_pairs)| {
        left_pairs
            .len()
            .cmp(&right_pairs.len())
            .then_with(|| left_points.len().cmp(&right_points.len()))
    });

    let mut assigned_pair_keys = HashSet::new();
    let mut components = Vec::new();
    for (points, pair_keys) in started_paths {
        if pair_keys.is_empty() {
            continue;
        }
        if pair_keys
            .iter()
            .any(|pair_key| assigned_pair_keys.contains(pair_key))
        {
            continue;
        }
        let component_rows: Vec<Map<String, Value>> = rows
            .iter()
            .filter(|row| {
                row_pair_key(row)
                    .map(|pair_key| pair_keys.contains(&pair_key))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        let Some(component) = build_route_leg_component_with_points(component_rows, Some(points))
        else {
            continue;
        };
        assigned_pair_keys.extend(component.pair_keys.iter().cloned());
        components.push(component);
    }
    components
}

fn collect_started_route_path(
    start_index: usize,
    rows: &[Map<String, Value>],
    outgoing: &HashMap<String, Vec<usize>>,
) -> Option<(Vec<String>, HashSet<String>)> {
    if start_index >= rows.len() {
        return None;
    }
    let start_row = &rows[start_index];
    let start_waypoint = json_text(start_row.get("Waypoint1"));
    let end_waypoint = json_text(start_row.get("Waypoint2"));
    if start_waypoint.is_empty() || end_waypoint.is_empty() {
        return None;
    }

    let mut points = vec![start_waypoint];
    let mut pair_keys = HashSet::new();
    let mut current_index = start_index;
    let mut previous_point = json_text(rows[start_index].get("Waypoint1"));
    loop {
        let current_row = &rows[current_index];
        let waypoint1 = json_text(current_row.get("Waypoint1"));
        let waypoint2 = json_text(current_row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            break;
        }
        let pair_key = undirected_point_pair_key(&waypoint1, &waypoint2);
        if !pair_keys.insert(pair_key) {
            break;
        }
        points.push(waypoint2.clone());
        if json_to_i64(current_row.get("IsEnd")) == Some(1) {
            break;
        }
        let Some(candidate_indices) = outgoing.get(&waypoint2) else {
            break;
        };
        let mut next_indices: Vec<usize> = candidate_indices
            .iter()
            .copied()
            .filter(|candidate_index| {
                if *candidate_index >= rows.len() {
                    return false;
                }
                let candidate_row = &rows[*candidate_index];
                let candidate_pair_key = row_pair_key(candidate_row);
                let candidate_waypoint2 = json_text(candidate_row.get("Waypoint2"));
                candidate_pair_key
                    .map(|candidate_pair_key| {
                        !pair_keys.contains(&candidate_pair_key)
                            && candidate_waypoint2 != previous_point
                    })
                    .unwrap_or(false)
            })
            .collect();
        if next_indices.is_empty() {
            next_indices = candidate_indices
                .iter()
                .copied()
                .filter(|candidate_index| {
                    if *candidate_index >= rows.len() {
                        return false;
                    }
                    let candidate_row = &rows[*candidate_index];
                    row_pair_key(candidate_row)
                        .map(|candidate_pair_key| !pair_keys.contains(&candidate_pair_key))
                        .unwrap_or(false)
                })
                .collect();
        }
        if next_indices.len() != 1 {
            break;
        }
        current_index = next_indices[0];
        previous_point = json_text(rows[current_index].get("Waypoint1"));
    }

    if pair_keys.is_empty() || points.len() < 2 {
        None
    } else {
        Some((points, pair_keys))
    }
}

fn split_connected_route_leg_components(rows: &[Map<String, Value>]) -> Vec<RouteLegComponent> {
    let unique_rows = dedup_undirected_route_rows(rows);
    if unique_rows.is_empty() {
        return Vec::new();
    }

    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    let mut ordered_edges = Vec::new();
    for row in &unique_rows {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        adjacency
            .entry(waypoint1.clone())
            .or_default()
            .push(waypoint2.clone());
        adjacency
            .entry(waypoint2.clone())
            .or_default()
            .push(waypoint1.clone());
        ordered_edges.push((waypoint1, waypoint2));
    }

    let mut visited_points = HashSet::new();
    let mut components = Vec::new();
    for (waypoint1, waypoint2) in ordered_edges {
        if visited_points.contains(&waypoint1) || visited_points.contains(&waypoint2) {
            continue;
        }
        let mut queue = vec![waypoint1.clone()];
        let mut component_points = HashSet::new();
        while let Some(current) = queue.pop() {
            if !visited_points.insert(current.clone()) {
                continue;
            }
            component_points.insert(current.clone());
            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors {
                    if !visited_points.contains(neighbor) {
                        queue.push(neighbor.clone());
                    }
                }
            }
        }
        if !component_points.contains(&waypoint2) {
            component_points.insert(waypoint2.clone());
        }
        let component_rows: Vec<Map<String, Value>> = rows
            .iter()
            .filter(|row| {
                let left = json_text(row.get("Waypoint1"));
                let right = json_text(row.get("Waypoint2"));
                !left.is_empty()
                    && !right.is_empty()
                    && component_points.contains(&left)
                    && component_points.contains(&right)
            })
            .cloned()
            .collect();
        let Some(component) = build_route_leg_component(component_rows) else {
            continue;
        };
        components.push(component);
    }
    components
}

fn build_route_leg_component(rows: Vec<Map<String, Value>>) -> Option<RouteLegComponent> {
    build_route_leg_component_with_points(rows, None)
}

fn build_route_leg_component_with_points(
    rows: Vec<Map<String, Value>>,
    points: Option<Vec<String>>,
) -> Option<RouteLegComponent> {
    if rows.is_empty() {
        return None;
    }
    let pair_keys = build_route_component_pair_keys(&rows);
    if pair_keys.is_empty() {
        return None;
    }

    let mut point_set = HashSet::new();
    for row in &rows {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if !waypoint1.is_empty() {
            point_set.insert(waypoint1);
        }
        if !waypoint2.is_empty() {
            point_set.insert(waypoint2);
        }
    }

    let is_simple_path = route_component_is_simple_path(&pair_keys, &point_set);
    let points = points.unwrap_or_else(|| extract_route_point_order(&rows));
    Some(RouteLegComponent {
        rows,
        points,
        point_set,
        pair_keys,
        is_simple_path,
    })
}

fn row_pair_key(row: &Map<String, Value>) -> Option<String> {
    let waypoint1 = json_string(row.get("Waypoint1"))?;
    let waypoint2 = json_string(row.get("Waypoint2"))?;
    Some(undirected_point_pair_key(&waypoint1, &waypoint2))
}

fn build_route_component_pair_keys(rows: &[Map<String, Value>]) -> HashSet<String> {
    dedup_undirected_route_rows(rows)
        .into_iter()
        .filter_map(|row| row_pair_key(&row))
        .collect()
}

fn route_component_is_simple_path(
    pair_keys: &HashSet<String>,
    component_points: &HashSet<String>,
) -> bool {
    if pair_keys.is_empty() || component_points.len() < 2 {
        return false;
    }
    let mut degrees: HashMap<&str, usize> = component_points
        .iter()
        .map(|waypoint| (waypoint.as_str(), 0usize))
        .collect();
    for pair_key in pair_keys {
        let Some((waypoint1, waypoint2)) = pair_key.split_once('\u{1f}') else {
            return false;
        };
        if waypoint1 == waypoint2 {
            return false;
        }
        let Some(left_degree) = degrees.get_mut(waypoint1) else {
            return false;
        };
        *left_degree += 1;
        let Some(right_degree) = degrees.get_mut(waypoint2) else {
            return false;
        };
        *right_degree += 1;
    }
    if degrees.values().any(|degree| *degree > 2) {
        return false;
    }
    degrees.values().filter(|degree| **degree == 1).count() == 2
}

fn route_component_common_pair_count(left: &RouteLegComponent, right: &RouteLegComponent) -> usize {
    if left.pair_keys.len() <= right.pair_keys.len() {
        left.pair_keys.intersection(&right.pair_keys).count()
    } else {
        right.pair_keys.intersection(&left.pair_keys).count()
    }
}

fn route_component_common_point_count(
    left: &RouteLegComponent,
    right: &RouteLegComponent,
) -> usize {
    if left.point_set.len() <= right.point_set.len() {
        left.point_set.intersection(&right.point_set).count()
    } else {
        right.point_set.intersection(&left.point_set).count()
    }
}

fn extract_route_point_order(rows: &[Map<String, Value>]) -> Vec<String> {
    let unique_rows = dedup_undirected_route_rows(rows);
    if unique_rows.is_empty() {
        return Vec::new();
    }

    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for row in &unique_rows {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        adjacency
            .entry(waypoint1.clone())
            .or_default()
            .push(waypoint2.clone());
        adjacency.entry(waypoint2).or_default().push(waypoint1);
    }

    let mut endpoints: Vec<String> = adjacency
        .iter()
        .filter_map(|(waypoint, neighbors)| {
            if neighbors.len() == 1 {
                Some(waypoint.clone())
            } else {
                None
            }
        })
        .collect();
    endpoints.sort();

    let start_hints: Vec<String> = rows
        .iter()
        .filter(|row| json_to_i64(row.get("IsStart")) == Some(1))
        .filter_map(|row| json_string(row.get("Waypoint1")))
        .filter(|waypoint| adjacency.contains_key(waypoint))
        .collect();
    let mut start_candidates = Vec::new();
    for waypoint in &start_hints {
        if endpoints.contains(waypoint) {
            start_candidates.push(waypoint.clone());
        }
    }
    start_candidates.extend(endpoints.iter().cloned());
    start_candidates.extend(start_hints);
    start_candidates.extend(adjacency.keys().cloned());

    let mut seen_starts = HashSet::new();
    for start in start_candidates {
        if !seen_starts.insert(start.clone()) {
            continue;
        }
        let order = walk_route_point_order_undirected(&start, &adjacency, unique_rows.len());
        if order.len() == unique_rows.len() + 1 {
            return order;
        }
    }
    extract_route_point_order_directed(&unique_rows)
}

fn walk_route_point_order_undirected(
    start: &str,
    adjacency: &HashMap<String, Vec<String>>,
    edge_count: usize,
) -> Vec<String> {
    let mut order = vec![start.to_string()];
    let mut used_edges = HashSet::new();
    let mut previous: Option<String> = None;
    let mut current = start.to_string();
    while used_edges.len() < edge_count {
        let Some(neighbors) = adjacency.get(&current) else {
            break;
        };
        let next = neighbors
            .iter()
            .find(|candidate| {
                previous.as_deref() != Some(candidate.as_str())
                    && !used_edges.contains(&undirected_point_pair_key(&current, candidate))
            })
            .or_else(|| {
                neighbors.iter().find(|candidate| {
                    !used_edges.contains(&undirected_point_pair_key(&current, candidate))
                })
            })
            .cloned();
        let Some(next) = next else {
            break;
        };
        if !used_edges.insert(undirected_point_pair_key(&current, &next)) {
            break;
        }
        order.push(next.clone());
        previous = Some(current);
        current = next;
    }
    order
}

fn extract_route_point_order_directed(rows: &[Map<String, Value>]) -> Vec<String> {
    let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();
    let mut incoming: HashSet<String> = HashSet::new();
    let mut starts = Vec::new();
    for row in rows {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        outgoing
            .entry(waypoint1.clone())
            .or_default()
            .push(waypoint2.clone());
        incoming.insert(waypoint2);
        if json_to_i64(row.get("IsStart")) == Some(1) {
            starts.push(waypoint1);
        }
    }

    let start = starts
        .into_iter()
        .find(|candidate| outgoing.contains_key(candidate))
        .or_else(|| {
            outgoing
                .keys()
                .find(|candidate| !incoming.contains(*candidate))
                .cloned()
        })
        .or_else(|| json_string(rows[0].get("Waypoint1")));
    let Some(start) = start else {
        return Vec::new();
    };

    let mut order = vec![start.clone()];
    let mut used_edges = HashSet::new();
    let mut current = start;
    loop {
        let Some(next_candidates) = outgoing.get(&current) else {
            break;
        };
        let next = next_candidates
            .iter()
            .find(|candidate| used_edges.insert(undirected_point_pair_key(&current, candidate)))
            .cloned();
        let Some(next) = next else {
            break;
        };
        order.push(next.clone());
        current = next;
    }
    order
}

fn dedup_undirected_route_rows(rows: &[Map<String, Value>]) -> Vec<Map<String, Value>> {
    let mut seen = HashSet::new();
    let mut unique_rows = Vec::new();
    for row in rows {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        let key = undirected_point_pair_key(&waypoint1, &waypoint2);
        if seen.insert(key) {
            unique_rows.push(row.clone());
        }
    }
    unique_rows
}

fn merge_airway_route_order(db_rows: &[String], csv_rows: &[String]) -> Vec<String> {
    let db_points: Vec<&str> = db_rows.iter().map(String::as_str).collect();
    let csv_points_original: Vec<&str> = csv_rows.iter().map(String::as_str).collect();
    let db_set: HashSet<&str> = db_points.iter().copied().collect();
    let csv_set: HashSet<&str> = csv_points_original.iter().copied().collect();
    if !db_set.iter().any(|wp| csv_set.contains(wp)) {
        return Vec::new();
    }

    let first_common_db_index = db_points.iter().position(|wp| csv_set.contains(wp));
    let last_common_db_index = db_points.iter().rposition(|wp| csv_set.contains(wp));
    let (Some(first_common_db_index), Some(last_common_db_index)) =
        (first_common_db_index, last_common_db_index)
    else {
        return Vec::new();
    };
    let first_common = db_points[first_common_db_index];
    let last_common = db_points[last_common_db_index];
    let first_common_csv_index = csv_points_original
        .iter()
        .position(|wp| *wp == first_common);
    let last_common_csv_index = csv_points_original.iter().position(|wp| *wp == last_common);
    let (Some(first_common_csv_index), Some(last_common_csv_index)) =
        (first_common_csv_index, last_common_csv_index)
    else {
        return Vec::new();
    };

    let mut csv_points = csv_points_original.clone();
    if first_common_csv_index > last_common_csv_index {
        csv_points.reverse();
    }

    let mut merged_order: Vec<String> = Vec::new();
    let mut merged_set: HashSet<&str> = HashSet::new();
    let mut csv_index: HashMap<&str, usize> = HashMap::new();
    for (idx, wp) in csv_points.iter().enumerate() {
        csv_index.entry(*wp).or_insert(idx);
    }

    let db_len = db_points.len();
    let mut common_flags = vec![false; db_len];
    for (flag, wp) in common_flags.iter_mut().zip(db_points.iter()) {
        *flag = csv_set.contains(wp);
    }
    if first_common_db_index == 0
        && let Some(prefix_end) = csv_index.get(first_common).copied()
    {
        for wp in &csv_points[..prefix_end] {
            if merged_set.insert(*wp) {
                merged_order.push((*wp).to_string());
            }
        }
    }

    let mut next_common_idx = vec![None; db_len];
    let mut next_idx: Option<usize> = None;
    for i in (0..db_len).rev() {
        next_common_idx[i] = next_idx;
        if common_flags[i] {
            next_idx = Some(i);
        }
    }

    for (i, wp) in db_points.iter().enumerate() {
        if merged_set.insert(*wp) {
            merged_order.push((*wp).to_string());
        }
        if !common_flags[i] {
            continue;
        }
        let Some(nxt_idx) = next_common_idx[i] else {
            continue;
        };
        let next_common = db_points[nxt_idx];
        let Some(idx_csv_current) = csv_index.get(wp) else {
            continue;
        };
        let Some(idx_csv_next) = csv_index.get(next_common) else {
            continue;
        };
        if idx_csv_current < idx_csv_next {
            for m in &csv_points[idx_csv_current + 1..*idx_csv_next] {
                if merged_set.insert(*m) {
                    merged_order.push((*m).to_string());
                }
            }
        } else if idx_csv_next < idx_csv_current {
            for m in csv_points[*idx_csv_next + 1..*idx_csv_current].iter().rev() {
                if merged_set.insert(*m) {
                    merged_order.push((*m).to_string());
                }
            }
        }
    }

    if last_common_db_index + 1 == db_len
        && let Some(suffix_start) = csv_index.get(last_common).copied()
    {
        for wp in &csv_points[suffix_start + 1..] {
            if merged_set.insert(*wp) {
                merged_order.push((*wp).to_string());
            }
        }
    }
    merged_order
}

fn build_segment_lookup(rows: &[Map<String, Value>]) -> HashMap<String, RouteSegmentTemplate> {
    let mut lookup = HashMap::new();
    for row in rows {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        lookup
            .entry(undirected_point_pair_key(&waypoint1, &waypoint2))
            .or_insert_with(|| RouteSegmentTemplate {
                level: row.get("Level").cloned().unwrap_or(Value::Null),
                waypoint1_id: row.get("Waypoint1ID").cloned().unwrap_or(Value::Null),
                waypoint2_id: row.get("Waypoint2ID").cloned().unwrap_or(Value::Null),
                waypoint1: row.get("Waypoint1").cloned().unwrap_or(Value::Null),
            });
    }
    lookup
}

fn rebuild_route_legs_from_order(
    airway_id: i64,
    merged_order: &[String],
    source_lookup: &HashMap<String, RouteSegmentTemplate>,
    existing_lookup: &HashMap<String, RouteSegmentTemplate>,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    if merged_order.len() < 2 {
        return Vec::new();
    }

    let mut rebuilt = Vec::new();
    for index in 0..(merged_order.len() - 1) {
        let waypoint1 = &merged_order[index];
        let waypoint2 = &merged_order[index + 1];
        let key = undirected_point_pair_key(waypoint1, waypoint2);
        let Some(template) = source_lookup
            .get(&key)
            .or_else(|| existing_lookup.get(&key))
        else {
            continue;
        };

        let mut row = Map::new();
        row.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
        *next_leg_id += 1;
        row.insert(
            "AirwayID".to_string(),
            Value::Number(Number::from(airway_id)),
        );
        row.insert("Level".to_string(), template.level.clone());
        let template_waypoint1 = json_text(Some(&template.waypoint1));
        let oriented_same = template_waypoint1 == *waypoint1;
        let (waypoint1_id, waypoint2_id, waypoint1_value, waypoint2_value) = if oriented_same {
            (
                template.waypoint1_id.clone(),
                template.waypoint2_id.clone(),
                Value::String(waypoint1.clone()),
                Value::String(waypoint2.clone()),
            )
        } else {
            (
                template.waypoint2_id.clone(),
                template.waypoint1_id.clone(),
                Value::String(waypoint1.clone()),
                Value::String(waypoint2.clone()),
            )
        };
        row.insert("Waypoint1ID".to_string(), waypoint1_id);
        row.insert("Waypoint2ID".to_string(), waypoint2_id);
        row.insert(
            "IsStart".to_string(),
            Value::Number(Number::from(if index == 0 { 1 } else { 0 })),
        );
        row.insert(
            "IsEnd".to_string(),
            Value::Number(Number::from(if index + 1 == merged_order.len() - 1 {
                1
            } else {
                0
            })),
        );
        row.insert("Waypoint1".to_string(), waypoint1_value);
        row.insert("Waypoint2".to_string(), waypoint2_value);
        rebuilt.push(row);
    }
    rebuilt
}

fn build_route_mirror_pair_keys(
    ident: &str,
    existing_route_legs: &[Map<String, Value>],
    merged_order: &[String],
    mirror_reference: &AirwayMirrorReference,
) -> HashSet<String> {
    if existing_route_legs.is_empty() || merged_order.len() < 2 {
        return HashSet::new();
    }
    let existing_mirrored_pairs = build_existing_mirrored_pair_keys(existing_route_legs);
    if existing_mirrored_pairs.is_empty() && mirror_reference.is_empty() {
        return HashSet::new();
    }

    let merged_index: HashMap<&str, usize> = merged_order
        .iter()
        .enumerate()
        .map(|(index, waypoint)| (waypoint.as_str(), index))
        .collect();
    let mut route_pairs = HashSet::new();
    for row in dedup_undirected_route_rows(existing_route_legs) {
        let level = json_text(row.get("Level"));
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        let pair_key = undirected_point_pair_key(&waypoint1, &waypoint2);
        let should_expand = existing_mirrored_pairs.contains(&pair_key)
            || mirror_reference.should_mirror(ident, &level, &waypoint1, &waypoint2);
        if !should_expand {
            continue;
        }
        let Some(mut start_index) = merged_index.get(waypoint1.as_str()).copied() else {
            continue;
        };
        let Some(mut end_index) = merged_index.get(waypoint2.as_str()).copied() else {
            continue;
        };
        if start_index == end_index {
            continue;
        }
        if start_index > end_index {
            std::mem::swap(&mut start_index, &mut end_index);
        }
        for index in start_index..end_index {
            route_pairs.insert(undirected_point_pair_key(
                &merged_order[index],
                &merged_order[index + 1],
            ));
        }
    }
    route_pairs
}

fn build_existing_mirrored_pair_keys(rows: &[Map<String, Value>]) -> HashSet<String> {
    let mut directed_pairs = HashSet::new();
    for row in rows {
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        directed_pairs.insert((waypoint1, waypoint2));
    }

    let mut mirrored_pairs = HashSet::new();
    for (waypoint1, waypoint2) in &directed_pairs {
        if directed_pairs.contains(&(waypoint2.clone(), waypoint1.clone())) {
            mirrored_pairs.insert(undirected_point_pair_key(waypoint1, waypoint2));
        }
    }
    mirrored_pairs
}

fn apply_route_mirror_reference(
    rows: &mut Vec<Map<String, Value>>,
    airway_ident_by_id: &HashMap<i64, String>,
    airway_reference: &AirwayMirrorReference,
    next_leg_id: &mut i64,
) {
    if airway_reference.is_empty() {
        return;
    }
    let mut directed_edges = HashSet::new();
    for row in rows.iter() {
        if let Some(key) = directed_key_from_row(row, airway_ident_by_id) {
            directed_edges.insert(key);
        }
    }

    let mut mirrored_rows = Vec::new();
    for row in rows.iter() {
        let Some(airway_id) = json_to_i64(row.get("AirwayID")) else {
            continue;
        };
        let Some(ident) = airway_ident_by_id.get(&airway_id) else {
            continue;
        };
        apply_mirror_row_if_needed(
            row,
            ident,
            airway_reference,
            &HashSet::new(),
            &mut directed_edges,
            next_leg_id,
            &mut mirrored_rows,
        );
    }
    rows.extend(mirrored_rows);
}

fn apply_route_mirror_reference_for_ident(
    rows: &mut Vec<Map<String, Value>>,
    ident: &str,
    airway_reference: &AirwayMirrorReference,
    expanded_mirror_pairs: &HashSet<String>,
    next_leg_id: &mut i64,
) {
    if airway_reference.is_empty() && expanded_mirror_pairs.is_empty() {
        return;
    }

    let mut directed_edges = HashSet::new();
    for row in rows.iter() {
        let level = json_text(row.get("Level"));
        let waypoint1 = json_text(row.get("Waypoint1"));
        let waypoint2 = json_text(row.get("Waypoint2"));
        if waypoint1.is_empty() || waypoint2.is_empty() {
            continue;
        }
        directed_edges.insert(directed_airway_edge_key(
            ident, &level, &waypoint1, &waypoint2,
        ));
    }

    let mut mirrored_rows = Vec::new();
    for row in rows.iter() {
        apply_mirror_row_if_needed(
            row,
            ident,
            airway_reference,
            expanded_mirror_pairs,
            &mut directed_edges,
            next_leg_id,
            &mut mirrored_rows,
        );
    }
    rows.extend(mirrored_rows);
}

fn apply_mirror_row_if_needed(
    row: &Map<String, Value>,
    ident: &str,
    airway_reference: &AirwayMirrorReference,
    expanded_mirror_pairs: &HashSet<String>,
    directed_edges: &mut HashSet<String>,
    next_leg_id: &mut i64,
    mirrored_rows: &mut Vec<Map<String, Value>>,
) {
    let level = json_text(row.get("Level"));
    let waypoint1 = json_text(row.get("Waypoint1"));
    let waypoint2 = json_text(row.get("Waypoint2"));
    if waypoint1.is_empty() || waypoint2.is_empty() {
        return;
    }

    let should_mirror = airway_reference.should_mirror(ident, &level, &waypoint1, &waypoint2)
        || expanded_mirror_pairs.contains(&undirected_point_pair_key(&waypoint1, &waypoint2));
    if !should_mirror {
        return;
    }

    let reverse_key = directed_airway_edge_key(ident, &level, &waypoint2, &waypoint1);
    if directed_edges.contains(&reverse_key) {
        return;
    }

    let mut mirrored = row.clone();
    mirrored.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
    *next_leg_id += 1;
    swap_map_values(&mut mirrored, "Waypoint1ID", "Waypoint2ID");
    swap_map_values(&mut mirrored, "Waypoint1", "Waypoint2");
    swap_map_values(&mut mirrored, "IsStart", "IsEnd");
    directed_edges.insert(reverse_key);
    mirrored_rows.push(mirrored);
}

fn undirected_point_pair_key(left: &str, right: &str) -> String {
    if left <= right {
        format!("{left}\u{1f}{right}")
    } else {
        format!("{right}\u{1f}{left}")
    }
}

fn directed_airway_edge_key(ident: &str, level: &str, waypoint1: &str, waypoint2: &str) -> String {
    format!("{ident}\u{1f}|{level}\u{1f}|{waypoint1}\u{1f}|{waypoint2}")
}
