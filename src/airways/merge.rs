use std::collections::{HashMap, HashSet};

use serde_json::{Map, Number, Value};

use crate::db_json::{json_text, json_to_i64};

use super::{AirwayLegReferenceRow, AirwayReferenceData};

type AirwayRows = Vec<Map<String, Value>>;
type AirwayMergeOutput = (AirwayRows, AirwayRows);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct DirectedEdge {
    level: String,
    from: String,
    to: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct UndirectedEdge {
    level: String,
    left: String,
    right: String,
}

#[derive(Clone, Debug)]
struct SourceDirectionality {
    directed: HashSet<DirectedEdge>,
    undirected: HashSet<UndirectedEdge>,
}

#[derive(Clone, Debug)]
struct SegmentTemplate {
    level: Value,
    from_id: Value,
    to_id: Value,
    from_name: String,
}

#[derive(Clone, Debug)]
struct Chain {
    points: Vec<String>,
    first_index_by_point: HashMap<String, usize>,
}

impl Chain {
    fn from_points(points: Vec<String>) -> Self {
        let mut first_index_by_point = HashMap::new();
        for (idx, point) in points.iter().enumerate() {
            first_index_by_point.entry(point.clone()).or_insert(idx);
        }
        Self {
            points,
            first_index_by_point,
        }
    }

    fn point_index(&self, point: &str) -> Option<usize> {
        self.first_index_by_point.get(point).copied()
    }
}

pub(super) fn merge_airway_outputs(
    source_airways: &[Map<String, Value>],
    source_legs: &[Map<String, Value>],
    airway_reference: &AirwayReferenceData,
) -> AirwayMergeOutput {
    let source_ident_by_id = build_airway_ident_map(source_airways);
    let reference_ident_by_id = build_airway_ident_map(&airway_reference.airways);

    let source_airway_by_ident = build_airway_row_map_by_ident(source_airways);
    let mut final_airway_by_ident = build_airway_row_map_by_ident(&airway_reference.airways);

    let source_legs_by_ident = group_airway_legs_by_ident(source_legs, &source_ident_by_id);
    let mut final_legs_by_ident =
        group_reference_airway_legs_by_ident(&airway_reference.airway_legs, &reference_ident_by_id);

    let mut next_airway_id = next_id_from_rows(&airway_reference.airways, "ID");
    let mut next_leg_id = airway_reference
        .airway_legs
        .iter()
        .map(|row| row.id)
        .max()
        .unwrap_or(0)
        + 1;

    let mut source_idents: Vec<String> = source_airway_by_ident.keys().cloned().collect();
    source_idents.sort();

    for ident in source_idents {
        let source_legs_for_ident = source_legs_by_ident
            .get(&ident)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if source_legs_for_ident.is_empty() {
            continue;
        }

        let source_chains = extract_chains(source_legs_for_ident);
        let source_directionality = build_expected_directionality(
            &ident,
            source_legs_for_ident,
            &airway_reference.mirror_reference,
        );

        let existing_legs = final_legs_by_ident
            .get(&ident)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let connected = has_connection(&source_chains, existing_legs);

        let airway_row = ensure_airway_row(
            &ident,
            &source_airway_by_ident,
            &mut final_airway_by_ident,
            &mut next_airway_id,
        );
        let airway_id = json_to_i64(airway_row.get("ID")).unwrap_or(0);

        let merged_legs = if connected {
            merge_with_insertion_strategy(
                &ident,
                airway_id,
                source_legs_for_ident,
                existing_legs,
                &source_chains,
                &source_directionality,
                &airway_reference.mirror_reference,
                &mut next_leg_id,
            )
        } else {
            build_independent_append(
                &ident,
                airway_id,
                source_legs_for_ident,
                &source_directionality,
                airway_reference,
                &mut next_leg_id,
            )
        };

        final_legs_by_ident.insert(ident, merged_legs);
    }

    let mut final_airways: Vec<Map<String, Value>> = final_airway_by_ident.into_values().collect();
    let mut final_legs = Vec::new();

    let mut ordered_idents: Vec<String> = final_legs_by_ident.keys().cloned().collect();
    ordered_idents.sort();
    for ident in ordered_idents {
        if let Some(rows) = final_legs_by_ident.remove(&ident) {
            final_legs.extend(rows);
        }
    }

    renumber_airways_and_legs(&mut final_airways, &mut final_legs);
    (final_airways, final_legs)
}

fn next_id_from_rows(rows: &[Map<String, Value>], id_col: &str) -> i64 {
    rows.iter()
        .filter_map(|row| json_to_i64(row.get(id_col)))
        .max()
        .unwrap_or(0)
        + 1
}

fn ensure_airway_row(
    ident: &str,
    source_airway_by_ident: &HashMap<String, Map<String, Value>>,
    final_airway_by_ident: &mut HashMap<String, Map<String, Value>>,
    next_airway_id: &mut i64,
) -> Map<String, Value> {
    if let Some(existing) = final_airway_by_ident.get(ident).cloned() {
        return existing;
    }

    let mut row = source_airway_by_ident
        .get(ident)
        .cloned()
        .unwrap_or_else(|| {
            let mut fallback = Map::new();
            fallback.insert("Ident".to_string(), Value::String(ident.to_string()));
            fallback
        });
    row.insert("ID".to_string(), Value::Number(Number::from(*next_airway_id)));
    *next_airway_id += 1;
    final_airway_by_ident.insert(ident.to_string(), row.clone());
    row
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

fn build_airway_row_map_by_ident(rows: &[Map<String, Value>]) -> HashMap<String, Map<String, Value>> {
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

fn group_reference_airway_legs_by_ident(
    rows: &[AirwayLegReferenceRow],
    airway_ident_by_id: &HashMap<i64, String>,
) -> HashMap<String, Vec<Map<String, Value>>> {
    let mut grouped: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for row in rows {
        let Some(ident) = airway_ident_by_id.get(&row.airway_id) else {
            continue;
        };
        grouped.entry(ident.clone()).or_default().push(row.to_map());
    }
    grouped
}

fn has_connection(source_chains: &[Chain], existing_legs: &[Map<String, Value>]) -> bool {
    if source_chains.is_empty() || existing_legs.is_empty() {
        return false;
    }

    for row in existing_legs {
        let from = json_text(row.get("Waypoint1"));
        let to = json_text(row.get("Waypoint2"));
        if from.is_empty() || to.is_empty() {
            continue;
        }
        for chain in source_chains {
            if let Some(distance) = path_distance(chain, &from, &to)
                && distance >= 1
            {
                return true;
            }
        }
    }
    false
}

fn path_distance(chain: &Chain, left: &str, right: &str) -> Option<usize> {
    let (Some(li), Some(ri)) = (chain.point_index(left), chain.point_index(right)) else {
        return None;
    };
    Some(li.abs_diff(ri))
}

fn extract_chains(rows: &[Map<String, Value>]) -> Vec<Chain> {
    let mut chains = Vec::new();
    let mut current_points: Vec<String> = Vec::new();

    let is_sorted_by_id = rows.windows(2).all(|window| {
        let left = json_to_i64(window[0].get("ID")).unwrap_or(i64::MAX);
        let right = json_to_i64(window[1].get("ID")).unwrap_or(i64::MAX);
        left <= right
    });

    if is_sorted_by_id {
        for row in rows {
            let from = json_text(row.get("Waypoint1"));
            let to = json_text(row.get("Waypoint2"));
            if from.is_empty() || to.is_empty() {
                continue;
            }

            let is_start = json_to_i64(row.get("IsStart")) == Some(1);
            if is_start || current_points.is_empty() {
                if current_points.len() >= 2 {
                    chains.push(Chain::from_points(std::mem::take(&mut current_points)));
                }
                current_points.push(from.clone());
                current_points.push(to.clone());
            } else {
                if current_points.last().map(|p| p.as_str()) != Some(from.as_str()) {
                    if current_points.len() >= 2 {
                        chains.push(Chain::from_points(std::mem::take(&mut current_points)));
                    }
                    current_points.push(from.clone());
                }
                current_points.push(to.clone());
            }

            let is_end = json_to_i64(row.get("IsEnd")) == Some(1);
            if is_end {
                if current_points.len() >= 2 {
                    chains.push(Chain::from_points(std::mem::take(&mut current_points)));
                } else {
                    current_points.clear();
                }
            }
        }
    } else {
        let mut sorted_rows_storage = rows.to_vec();
        sorted_rows_storage.sort_by_key(|row| json_to_i64(row.get("ID")).unwrap_or(i64::MAX));
        for row in &sorted_rows_storage {
            let from = json_text(row.get("Waypoint1"));
            let to = json_text(row.get("Waypoint2"));
            if from.is_empty() || to.is_empty() {
                continue;
            }

            let is_start = json_to_i64(row.get("IsStart")) == Some(1);
            if is_start || current_points.is_empty() {
                if current_points.len() >= 2 {
                    chains.push(Chain::from_points(std::mem::take(&mut current_points)));
                }
                current_points.push(from.clone());
                current_points.push(to.clone());
            } else {
                if current_points.last().map(|p| p.as_str()) != Some(from.as_str()) {
                    if current_points.len() >= 2 {
                        chains.push(Chain::from_points(std::mem::take(&mut current_points)));
                    }
                    current_points.push(from.clone());
                }
                current_points.push(to.clone());
            }

            let is_end = json_to_i64(row.get("IsEnd")) == Some(1);
            if is_end {
                if current_points.len() >= 2 {
                    chains.push(Chain::from_points(std::mem::take(&mut current_points)));
                } else {
                    current_points.clear();
                }
            }
        }
    }

    if current_points.len() >= 2 {
        chains.push(Chain::from_points(current_points));
    }

    chains
}

fn build_expected_directionality(
    ident: &str,
    rows: &[Map<String, Value>],
    mirror_reference: &super::AirwayMirrorReference,
) -> SourceDirectionality {
    let mut directed = HashSet::new();
    let mut undirected = HashSet::new();

    for row in rows {
        let level = json_text(row.get("Level"));
        let from = json_text(row.get("Waypoint1"));
        let to = json_text(row.get("Waypoint2"));
        if from.is_empty() || to.is_empty() {
            continue;
        }
        directed.insert(DirectedEdge {
            level: level.clone(),
            from: from.clone(),
            to: to.clone(),
        });
        if mirror_reference.should_mirror(ident, &level, &from, &to) {
            directed.insert(DirectedEdge {
                level: level.clone(),
                from: to.clone(),
                to: from.clone(),
            });
        }
        undirected.insert(undirected_edge(&level, &from, &to));
    }

    SourceDirectionality { directed, undirected }
}

#[allow(clippy::too_many_arguments)]
fn merge_with_insertion_strategy(
    ident: &str,
    airway_id: i64,
    source_legs: &[Map<String, Value>],
    existing_legs: &[Map<String, Value>],
    source_chains: &[Chain],
    source_directionality: &SourceDirectionality,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    let template_lookup = build_template_lookup(source_legs, existing_legs);

    let mut merged = Vec::new();
    for row in existing_legs {
        let from = json_text(row.get("Waypoint1"));
        let to = json_text(row.get("Waypoint2"));
        if from.is_empty() || to.is_empty() {
            merged.push(clone_leg_with_new_id(row, airway_id, next_leg_id));
            continue;
        }

        let insert_path = find_insert_path(&from, &to, source_chains);
        if let Some(points) = insert_path
            && points.len() > 2
        {
            let mark_start = json_to_i64(row.get("IsStart")) == Some(1);
            let mark_end = json_to_i64(row.get("IsEnd")) == Some(1);
            let inserted = build_rows_from_points(
                airway_id,
                &points,
                &template_lookup,
                next_leg_id,
                mark_start,
                mark_end,
            );
            merged.extend(inserted);
            continue;
        }

        merged.push(clone_leg_with_new_id(row, airway_id, next_leg_id));
    }

    let mut seen = build_directed_set(&merged);
    for row in source_legs {
        let level = json_text(row.get("Level"));
        let from = json_text(row.get("Waypoint1"));
        let to = json_text(row.get("Waypoint2"));
        if from.is_empty() || to.is_empty() {
            continue;
        }
        let edge = DirectedEdge {
            level,
            from,
            to,
        };
        if seen.insert(edge) {
            merged.push(clone_leg_with_new_id(row, airway_id, next_leg_id));
        }
    }

    let mirrored = apply_mirror_rows(ident, merged, mirror_reference, next_leg_id);
    let mirrored = reconcile_directionality(mirrored, source_directionality);
    dedup_directed(mirrored)
}

fn build_independent_append(
    ident: &str,
    airway_id: i64,
    source_legs: &[Map<String, Value>],
    source_directionality: &SourceDirectionality,
    airway_reference: &AirwayReferenceData,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    let mut rows: Vec<Map<String, Value>> = source_legs
        .iter()
        .map(|row| clone_leg_with_new_id(row, airway_id, next_leg_id))
        .collect();

    rows = apply_mirror_rows(
        ident,
        rows,
        &airway_reference.mirror_reference,
        next_leg_id,
    );
    rows = reconcile_directionality(rows, source_directionality);
    dedup_directed(rows)
}

fn apply_mirror_rows(
    ident: &str,
    mut rows: Vec<Map<String, Value>>,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    let mut existing_directed = HashSet::new();
    for row in &rows {
        if let Some(edge) = directed_edge_from_row(row) {
            existing_directed.insert(edge);
        }
    }

    let original_len = rows.len();
    for idx in (0..original_len).rev() {
        let row = rows[idx].clone();
        let level = json_text(row.get("Level"));
        let from = json_text(row.get("Waypoint1"));
        let to = json_text(row.get("Waypoint2"));
        if from.is_empty() || to.is_empty() {
            continue;
        }

        if !mirror_reference.should_mirror(ident, &level, &from, &to) {
            continue;
        }

        let reverse = DirectedEdge {
            level,
            from: to.clone(),
            to: from.clone(),
        };
        if existing_directed.contains(&reverse) {
            continue;
        }

        let mut mirrored = row;
        mirrored.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
        *next_leg_id += 1;
        swap(&mut mirrored, "Waypoint1ID", "Waypoint2ID");
        swap(&mut mirrored, "Waypoint1", "Waypoint2");
        swap(&mut mirrored, "IsStart", "IsEnd");
        rows.push(mirrored);
        existing_directed.insert(reverse);
    }

    rows
}

fn find_insert_path(from: &str, to: &str, source_chains: &[Chain]) -> Option<Vec<String>> {
    let mut best: Option<(usize, Vec<String>)> = None;

    for chain in source_chains {
        let Some(path) = slice_points_between(chain, from, to) else {
            continue;
        };
        if path.len() < 2 {
            continue;
        }

        let distance = path.len() - 1;
        if best
            .as_ref()
            .map(|(best_dist, _)| distance < *best_dist)
            .unwrap_or(true)
        {
            best = Some((distance, path));
        }
    }

    best.map(|(_, path)| path)
}

fn slice_points_between(chain: &Chain, from: &str, to: &str) -> Option<Vec<String>> {
    let from_idx = chain.point_index(from)?;
    let to_idx = chain.point_index(to)?;

    if from_idx == to_idx {
        return None;
    }

    if from_idx < to_idx {
        Some(chain.points[from_idx..=to_idx].to_vec())
    } else {
        Some(
            chain.points[to_idx..=from_idx]
                .iter()
                .rev()
                .cloned()
                .collect(),
        )
    }
}

fn build_template_lookup(
    source_legs: &[Map<String, Value>],
    existing_legs: &[Map<String, Value>],
) -> HashMap<(String, String), SegmentTemplate> {
    let mut map = HashMap::new();

    for row in source_legs.iter().chain(existing_legs.iter()) {
        let from = json_text(row.get("Waypoint1"));
        let to = json_text(row.get("Waypoint2"));
        if from.is_empty() || to.is_empty() {
            continue;
        }

        map.entry(undirected_pair(&from, &to))
            .or_insert_with(|| SegmentTemplate {
                level: row.get("Level").cloned().unwrap_or(Value::String("B".to_string())),
                from_id: row.get("Waypoint1ID").cloned().unwrap_or(Value::Null),
                to_id: row.get("Waypoint2ID").cloned().unwrap_or(Value::Null),
                from_name: from,
            });
    }

    map
}

fn build_rows_from_points(
    airway_id: i64,
    points: &[String],
    template_lookup: &HashMap<(String, String), SegmentTemplate>,
    next_leg_id: &mut i64,
    mark_start: bool,
    mark_end: bool,
) -> Vec<Map<String, Value>> {
    if points.len() < 2 {
        return Vec::new();
    }

    let mut built = Vec::new();
    for idx in 0..(points.len() - 1) {
        let from = &points[idx];
        let to = &points[idx + 1];

        let key = undirected_pair(from, to);
        let template = template_lookup
            .get(&key)
            .cloned()
            .unwrap_or(SegmentTemplate {
                level: Value::String("B".to_string()),
                from_id: Value::Null,
                to_id: Value::Null,
                from_name: from.clone(),
            });

        let mut row = Map::new();
        row.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
        *next_leg_id += 1;
        row.insert("AirwayID".to_string(), Value::Number(Number::from(airway_id)));
        row.insert("Level".to_string(), template.level.clone());

        let oriented_same = template.from_name == *from;
        if oriented_same {
            row.insert("Waypoint1ID".to_string(), template.from_id.clone());
            row.insert("Waypoint2ID".to_string(), template.to_id.clone());
        } else {
            row.insert("Waypoint1ID".to_string(), template.to_id.clone());
            row.insert("Waypoint2ID".to_string(), template.from_id.clone());
        }

        row.insert(
            "IsStart".to_string(),
            Value::Number(Number::from(if idx == 0 && mark_start { 1 } else { 0 })),
        );
        row.insert(
            "IsEnd".to_string(),
            Value::Number(Number::from(if idx + 1 == points.len() - 1 && mark_end { 1 } else { 0 })),
        );
        row.insert("Waypoint1".to_string(), Value::String(from.clone()));
        row.insert("Waypoint2".to_string(), Value::String(to.clone()));
        built.push(row);
    }

    built
}

fn reconcile_directionality(
    rows: Vec<Map<String, Value>>,
    source_directionality: &SourceDirectionality,
) -> Vec<Map<String, Value>> {
    rows.into_iter()
        .filter_map(|mut row| {
            let level = json_text(row.get("Level"));
            let from = json_text(row.get("Waypoint1"));
            let to = json_text(row.get("Waypoint2"));
            if from.is_empty() || to.is_empty() {
                return Some(row);
            }

            let undirected = undirected_edge(&level, &from, &to);
            if !source_directionality.undirected.contains(&undirected) {
                return Some(row);
            }

            let edge = DirectedEdge {
                level: level.clone(),
                from: from.clone(),
                to: to.clone(),
            };
            if source_directionality.directed.contains(&edge) {
                return Some(row);
            }

            let reverse = DirectedEdge {
                level,
                from: to,
                to: from,
            };
            if source_directionality.directed.contains(&reverse) {
                swap(&mut row, "Waypoint1ID", "Waypoint2ID");
                swap(&mut row, "Waypoint1", "Waypoint2");
                swap(&mut row, "IsStart", "IsEnd");
                return Some(row);
            }

            None
        })
        .collect()
}

fn clone_leg_with_new_id(row: &Map<String, Value>, airway_id: i64, next_leg_id: &mut i64) -> Map<String, Value> {
    let mut cloned = row.clone();
    cloned.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
    *next_leg_id += 1;
    cloned.insert("AirwayID".to_string(), Value::Number(Number::from(airway_id)));
    cloned
}

fn dedup_directed(rows: Vec<Map<String, Value>>) -> Vec<Map<String, Value>> {
    let mut index_by_edge: HashMap<DirectedEdge, usize> = HashMap::new();
    let mut deduped: Vec<Map<String, Value>> = Vec::new();

    for row in rows {
        let Some(edge) = directed_edge_from_row(&row) else {
            deduped.push(row);
            continue;
        };

        if let Some(index) = index_by_edge.get(&edge).copied() {
            let existing = &mut deduped[index];
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

        index_by_edge.insert(edge, deduped.len());
        deduped.push(row);
    }

    deduped
}

fn directed_edge_from_row(row: &Map<String, Value>) -> Option<DirectedEdge> {
    let level = json_text(row.get("Level"));
    let from = json_text(row.get("Waypoint1"));
    let to = json_text(row.get("Waypoint2"));
    if from.is_empty() || to.is_empty() {
        return None;
    }
    Some(DirectedEdge { level, from, to })
}

fn build_directed_set(rows: &[Map<String, Value>]) -> HashSet<DirectedEdge> {
    rows.iter().filter_map(directed_edge_from_row).collect()
}

fn undirected_edge(level: &str, left: &str, right: &str) -> UndirectedEdge {
    if left <= right {
        UndirectedEdge {
            level: level.to_string(),
            left: left.to_string(),
            right: right.to_string(),
        }
    } else {
        UndirectedEdge {
            level: level.to_string(),
            left: right.to_string(),
            right: left.to_string(),
        }
    }
}

fn undirected_pair(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}

fn swap(row: &mut Map<String, Value>, left: &str, right: &str) {
    let left_value = row.get(left).cloned().unwrap_or(Value::Null);
    let right_value = row.get(right).cloned().unwrap_or(Value::Null);
    row.insert(left.to_string(), right_value);
    row.insert(right.to_string(), left_value);
}

fn renumber_airways_and_legs(airways: &mut [Map<String, Value>], legs: &mut [Map<String, Value>]) {
    airways.sort_by(|left, right| {
        let left_ident = left.get("Ident").and_then(Value::as_str).unwrap_or("");
        let right_ident = right.get("Ident").and_then(Value::as_str).unwrap_or("");
        left_ident.cmp(right_ident)
    });

    let mut airway_id_map = HashMap::new();
    for (idx, row) in airways.iter_mut().enumerate() {
        let new_id = idx as i64 + 1;
        if let Some(old_id) = json_to_i64(row.get("ID")) {
            airway_id_map.insert(old_id, new_id);
        }
        row.insert("ID".to_string(), Value::Number(Number::from(new_id)));
    }

    for row in legs.iter_mut() {
        let Some(old_id) = json_to_i64(row.get("AirwayID")) else {
            continue;
        };
        if let Some(new_id) = airway_id_map.get(&old_id) {
            row.insert("AirwayID".to_string(), Value::Number(Number::from(*new_id)));
        }
    }

    legs.sort_by(|left, right| {
        let left_key = (
            json_to_i64(left.get("AirwayID")).unwrap_or(i64::MAX),
            json_to_i64(left.get("ID")).unwrap_or(i64::MAX),
        );
        let right_key = (
            json_to_i64(right.get("AirwayID")).unwrap_or(i64::MAX),
            json_to_i64(right.get("ID")).unwrap_or(i64::MAX),
        );
        left_key.cmp(&right_key)
    });

    for (idx, row) in legs.iter_mut().enumerate() {
        row.insert("ID".to_string(), Value::Number(Number::from(idx as i64 + 1)));
    }
}
