use std::collections::{HashMap, HashSet};

use rayon::prelude::*;
use serde_json::{Map, Number, Value};

use crate::db_json::json_to_i64;

use super::{AirwayLegReferenceRow, AirwayReferenceData, AirwayReferenceRow};

type AirwayRows = Vec<Map<String, Value>>;
type AirwayMergeOutput = (AirwayRows, AirwayRows);
type SourceAirwayRowsByIdent<'a> = HashMap<String, &'a Map<String, Value>>;
type SourceLegRowsByIdent<'a> = HashMap<String, Vec<&'a Map<String, Value>>>;
type ReferenceAirwayRowsByIdent<'a> = HashMap<String, &'a AirwayReferenceRow>;
type ReferenceLegRowsByIdent<'a> = HashMap<String, Vec<&'a AirwayLegReferenceRow>>;

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
    level: String,
    from_id: Option<i64>,
    to_id: Option<i64>,
    from_name: String,
}

#[derive(Debug)]
struct IdentMergeResult {
    ident: String,
    airway_row: Map<String, Value>,
    legs: AirwayRows,
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

fn row_text<'a>(row: &'a Map<String, Value>, key: &str) -> &'a str {
    row.get(key)
        .and_then(Value::as_str)
    .map_or("", str::trim)
}

fn directed_edge(level: &str, from: &str, to: &str) -> DirectedEdge {
    DirectedEdge {
        level: level.to_string(),
        from: from.to_string(),
        to: to.to_string(),
    }
}

pub(super) fn merge_airway_outputs(
    source_airways: &[Map<String, Value>],
    source_legs: &[Map<String, Value>],
    airway_reference: &AirwayReferenceData,
) -> AirwayMergeOutput {
    let source_ident_by_id = build_airway_ident_map(source_airways);
    let reference_ident_by_id = build_reference_airway_ident_map(&airway_reference.airways);

    let source_airway_by_ident = build_airway_row_map_by_ident(source_airways);
    let reference_airway_by_ident =
        build_reference_airway_row_map_by_ident(&airway_reference.airways);

    let source_legs_by_ident = group_airway_legs_by_ident(source_legs, &source_ident_by_id);
    let reference_legs_by_ident =
        group_reference_airway_legs_by_ident(&airway_reference.airway_legs, &reference_ident_by_id);

    let mut source_idents: Vec<String> = source_legs_by_ident.keys().cloned().collect();
    source_idents.sort();

    let mut final_idents: Vec<String> = reference_airway_by_ident.keys().cloned().collect();
    final_idents.extend(source_idents.iter().cloned());
    final_idents.sort();
    final_idents.dedup();

    let airway_id_by_ident = build_temp_airway_id_by_ident(&final_idents);

    let average_legs_per_ident = source_legs.len() / source_idents.len().max(1);
    let should_parallelize =
        source_idents.len() >= rayon::current_num_threads() * 8 && average_legs_per_ident >= 32;

    let merge_ident_range = |idents: &[String]| {
        idents
            .iter()
            .filter_map(|ident| {
                let airway_id = airway_id_by_ident
                    .get(ident.as_str())
                    .copied()
                    .unwrap_or_default();
                merge_ident(
                    ident,
                    airway_id,
                    &source_airway_by_ident,
                    &reference_airway_by_ident,
                    &source_legs_by_ident,
                    &reference_legs_by_ident,
                    &airway_reference.mirror_reference,
                )
            })
            .collect::<Vec<_>>()
    };

    let merged_results: Vec<IdentMergeResult> = if should_parallelize {
        let chunk_size = (source_idents.len() / (rayon::current_num_threads() * 4)).clamp(64, 512);
        source_idents
            .par_chunks(chunk_size)
            .map(merge_ident_range)
            .collect::<Vec<_>>()
            .into_iter()
            .flatten()
            .collect()
    } else {
        merge_ident_range(&source_idents)
    };

    let mut merged_by_ident: HashMap<String, IdentMergeResult> = merged_results
        .into_iter()
        .map(|result| (result.ident.clone(), result))
        .collect();

    let mut final_airways = Vec::with_capacity(final_idents.len());
    let mut final_legs = Vec::with_capacity(airway_reference.airway_legs.len() + source_legs.len());

    for ident in final_idents {
        let airway_id = airway_id_by_ident
            .get(ident.as_str())
            .copied()
            .unwrap_or_default();

        if let Some(result) = merged_by_ident.remove(ident.as_str()) {
            final_airways.push(result.airway_row);
            final_legs.extend(result.legs);
            continue;
        }

        let Some(reference_airway) = reference_airway_by_ident.get(ident.as_str()) else {
            continue;
        };

        final_airways.push(reference_airway.to_map_with_id(airway_id));
        if let Some(reference_legs) = reference_legs_by_ident.get(ident.as_str()) {
            final_legs.extend(reference_legs_to_output_rows(reference_legs, airway_id));
        }
    }

    renumber_airways_and_legs(&mut final_airways, &mut final_legs);
    (final_airways, final_legs)
}

fn merge_ident(
    ident: &str,
    airway_id: i64,
    source_airway_by_ident: &SourceAirwayRowsByIdent<'_>,
    reference_airway_by_ident: &ReferenceAirwayRowsByIdent<'_>,
    source_legs_by_ident: &SourceLegRowsByIdent<'_>,
    reference_legs_by_ident: &ReferenceLegRowsByIdent<'_>,
    mirror_reference: &super::AirwayMirrorReference,
) -> Option<IdentMergeResult> {
    let source_legs_for_ident: &[&Map<String, Value>] = source_legs_by_ident
        .get(ident)
        .map_or(&[][..], Vec::as_slice);
    if source_legs_for_ident.is_empty() {
        return None;
    }

    let source_chains = extract_chains(source_legs_for_ident);
    let source_directionality =
        build_expected_directionality(ident, source_legs_for_ident, mirror_reference);
    let existing_legs: &[&AirwayLegReferenceRow] = reference_legs_by_ident
        .get(ident)
        .map_or(&[][..], Vec::as_slice);

    let airway_row = build_output_airway_row(
        ident,
        airway_id,
        source_airway_by_ident,
        reference_airway_by_ident,
    );

    let mut next_leg_id = 1;
    let legs = if has_connection(&source_chains, existing_legs) {
        merge_with_insertion_strategy(
            ident,
            airway_id,
            source_legs_for_ident,
            existing_legs,
            &source_chains,
            &source_directionality,
            mirror_reference,
            &mut next_leg_id,
        )
    } else {
        build_independent_append(
            ident,
            airway_id,
            source_legs_for_ident,
            &source_directionality,
            mirror_reference,
            &mut next_leg_id,
        )
    };

    Some(IdentMergeResult {
        ident: ident.to_string(),
        airway_row,
        legs,
    })
}

fn build_output_airway_row(
    ident: &str,
    airway_id: i64,
    source_airway_by_ident: &SourceAirwayRowsByIdent<'_>,
    reference_airway_by_ident: &ReferenceAirwayRowsByIdent<'_>,
) -> Map<String, Value> {
    if let Some(source_row) = source_airway_by_ident.get(ident) {
        let mut row = (*source_row).clone();
        row.insert("ID".to_string(), Value::Number(Number::from(airway_id)));
        return row;
    }

    if let Some(reference_row) = reference_airway_by_ident.get(ident) {
        return reference_row.to_map_with_id(airway_id);
    }

    let mut fallback = Map::with_capacity(2);
    fallback.insert("ID".to_string(), Value::Number(Number::from(airway_id)));
    fallback.insert("Ident".to_string(), Value::String(ident.to_string()));
    fallback
}

fn build_temp_airway_id_by_ident(idents: &[String]) -> HashMap<String, i64> {
    idents
        .iter()
        .enumerate()
    .map(|(index, ident)| (ident.clone(), one_based_i64(index)))
        .collect()
}

fn reference_legs_to_output_rows(
    rows: &[&AirwayLegReferenceRow],
    airway_id: i64,
) -> Vec<Map<String, Value>> {
    rows.iter()
        .map(|row| row.to_map_with_ids(row.id, airway_id))
        .collect()
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

fn build_reference_airway_ident_map(rows: &[AirwayReferenceRow]) -> HashMap<i64, String> {
    rows.iter().map(|row| (row.id, row.ident.clone())).collect()
}

fn build_airway_row_map_by_ident(rows: &[Map<String, Value>]) -> SourceAirwayRowsByIdent<'_> {
    rows.iter()
        .filter_map(|row| {
            let ident = row.get("Ident")?.as_str()?.to_string();
            Some((ident, row))
        })
        .collect()
}

fn build_reference_airway_row_map_by_ident(
    rows: &[AirwayReferenceRow],
) -> ReferenceAirwayRowsByIdent<'_> {
    rows.iter().map(|row| (row.ident.clone(), row)).collect()
}

fn group_airway_legs_by_ident<'a>(
    rows: &'a [Map<String, Value>],
    airway_ident_by_id: &HashMap<i64, String>,
) -> SourceLegRowsByIdent<'a> {
    let mut grouped: SourceLegRowsByIdent<'a> = HashMap::new();
    for row in rows {
        let Some(airway_id) = json_to_i64(row.get("AirwayID")) else {
            continue;
        };
        let Some(ident) = airway_ident_by_id.get(&airway_id) else {
            continue;
        };
        grouped.entry(ident.clone()).or_default().push(row);
    }
    grouped
}

fn group_reference_airway_legs_by_ident<'a>(
    rows: &'a [AirwayLegReferenceRow],
    airway_ident_by_id: &HashMap<i64, String>,
) -> ReferenceLegRowsByIdent<'a> {
    let mut grouped: ReferenceLegRowsByIdent<'a> = HashMap::new();
    for row in rows {
        let Some(ident) = airway_ident_by_id.get(&row.airway_id) else {
            continue;
        };
        grouped.entry(ident.clone()).or_default().push(row);
    }
    grouped
}

fn has_connection(source_chains: &[Chain], existing_legs: &[&AirwayLegReferenceRow]) -> bool {
    if source_chains.is_empty() || existing_legs.is_empty() {
        return false;
    }

    for row in existing_legs {
        let from = row.waypoint1.trim();
        let to = row.waypoint2.trim();
        if from.is_empty() || to.is_empty() {
            continue;
        }
        for chain in source_chains {
            if let Some(distance) = path_distance(chain, from, to)
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

fn extract_chains(rows: &[&Map<String, Value>]) -> Vec<Chain> {
    let mut chains = Vec::new();
    let mut current_points: Vec<String> = Vec::new();

    let is_sorted_by_id = rows.windows(2).all(|window| {
        let left = json_to_i64(window[0].get("ID")).unwrap_or(i64::MAX);
        let right = json_to_i64(window[1].get("ID")).unwrap_or(i64::MAX);
        left <= right
    });

    if is_sorted_by_id {
        for row in rows {
            process_chain_row(row, &mut chains, &mut current_points);
        }
    } else {
        let mut sorted_rows_storage = rows.to_vec();
        sorted_rows_storage.sort_by_key(|row| json_to_i64(row.get("ID")).unwrap_or(i64::MAX));
        for row in &sorted_rows_storage {
            process_chain_row(row, &mut chains, &mut current_points);
        }
    }

    if current_points.len() >= 2 {
        chains.push(Chain::from_points(current_points));
    }

    chains
}

fn build_expected_directionality(
    ident: &str,
    rows: &[&Map<String, Value>],
    mirror_reference: &super::AirwayMirrorReference,
) -> SourceDirectionality {
    let mut directed = HashSet::new();
    let mut undirected = HashSet::new();

    for row in rows {
        let level = row_text(row, "Level");
        let from = row_text(row, "Waypoint1");
        let to = row_text(row, "Waypoint2");
        if from.is_empty() || to.is_empty() {
            continue;
        }
        directed.insert(directed_edge(level, from, to));
        if mirror_reference.should_mirror(ident, level, from, to) {
            directed.insert(directed_edge(level, to, from));
        }
        undirected.insert(undirected_edge(level, from, to));
    }

    SourceDirectionality {
        directed,
        undirected,
    }
}

#[allow(clippy::too_many_arguments)]
fn merge_with_insertion_strategy(
    ident: &str,
    airway_id: i64,
    source_legs: &[&Map<String, Value>],
    existing_legs: &[&AirwayLegReferenceRow],
    source_chains: &[Chain],
    source_directionality: &SourceDirectionality,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    let template_lookup = build_template_lookup(source_legs, existing_legs);

    let mut merged = Vec::new();
    let mut seen = HashSet::new();
    for row in existing_legs {
        let from = row.waypoint1.trim();
        let to = row.waypoint2.trim();
        if from.is_empty() || to.is_empty() {
            let cloned = clone_reference_leg_with_new_id(row, airway_id, next_leg_id);
            if let Some(edge) = directed_edge_from_row(&cloned) {
                seen.insert(edge);
            }
            merged.push(cloned);
            continue;
        }

        let insert_path = find_insert_path(from, to, source_chains);
        if let Some(points) = insert_path
            && points.len() > 2
        {
            let mark_start = row.is_start == 1;
            let mark_end = row.is_end == 1;
            let inserted = build_rows_from_points(
                airway_id,
                &points,
                &template_lookup,
                next_leg_id,
                mark_start,
                mark_end,
            );
            for inserted_row in inserted {
                if let Some(edge) = directed_edge_from_row(&inserted_row) {
                    seen.insert(edge);
                }
                merged.push(inserted_row);
            }
            continue;
        }

        let cloned = clone_reference_leg_with_new_id(row, airway_id, next_leg_id);
        if let Some(edge) = directed_edge_from_row(&cloned) {
            seen.insert(edge);
        }
        merged.push(cloned);
    }

    for row in source_legs {
        let level = row_text(row, "Level");
        let from = row_text(row, "Waypoint1");
        let to = row_text(row, "Waypoint2");
        if from.is_empty() || to.is_empty() {
            continue;
        }
        let edge = directed_edge(level, from, to);
        if seen.insert(edge) {
            merged.push(clone_leg_with_new_id(row, airway_id, next_leg_id));
        }
    }

    let mirrored = apply_mirror_rows(ident, merged, &mut seen, mirror_reference, next_leg_id);
    reconcile_and_dedup_directionality(mirrored, source_directionality)
}

fn build_independent_append(
    ident: &str,
    airway_id: i64,
    source_legs: &[&Map<String, Value>],
    source_directionality: &SourceDirectionality,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    let mut rows = Vec::with_capacity(source_legs.len());
    let mut seen = HashSet::new();
    for row in source_legs {
        let cloned = clone_leg_with_new_id(row, airway_id, next_leg_id);
        if let Some(edge) = directed_edge_from_row(&cloned) {
            seen.insert(edge);
        }
        rows.push(cloned);
    }

    rows = apply_mirror_rows(ident, rows, &mut seen, mirror_reference, next_leg_id);
    reconcile_and_dedup_directionality(rows, source_directionality)
}

fn apply_mirror_rows(
    ident: &str,
    mut rows: Vec<Map<String, Value>>,
    existing_directed: &mut HashSet<DirectedEdge>,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> Vec<Map<String, Value>> {
    let original_len = rows.len();
    for idx in (0..original_len).rev() {
        let Some(reverse) = ({
            let row = &rows[idx];
            let level = row_text(row, "Level");
            let from = row_text(row, "Waypoint1");
            let to = row_text(row, "Waypoint2");
            if from.is_empty() || to.is_empty() || !mirror_reference.should_mirror(ident, level, from, to) {
                None
            } else {
                let reverse = directed_edge(level, to, from);
                (!existing_directed.contains(&reverse)).then_some(reverse)
            }
        }) else {
            continue;
        };

        let mut mirrored = rows[idx].clone();
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
            .is_none_or(|(best_dist, _)| distance < *best_dist)
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
    source_legs: &[&Map<String, Value>],
    existing_legs: &[&AirwayLegReferenceRow],
) -> HashMap<(String, String), SegmentTemplate> {
    let mut map = HashMap::new();

    for row in source_legs {
        let from = row_text(row, "Waypoint1");
        let to = row_text(row, "Waypoint2");
        if from.is_empty() || to.is_empty() {
            continue;
        }

        map.entry(undirected_pair(from, to))
            .or_insert_with(|| SegmentTemplate {
                level: row_text(row, "Level").to_string(),
                from_id: json_to_i64(row.get("Waypoint1ID")),
                to_id: json_to_i64(row.get("Waypoint2ID")),
                from_name: from.to_string(),
            });
    }

    for row in existing_legs {
        let from = row.waypoint1.trim();
        let to = row.waypoint2.trim();
        if from.is_empty() || to.is_empty() {
            continue;
        }

        map.entry(undirected_pair(from, to))
            .or_insert_with(|| SegmentTemplate {
                level: row.level.trim().to_string(),
                from_id: Some(row.waypoint1_id),
                to_id: Some(row.waypoint2_id),
                from_name: from.to_string(),
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
            .unwrap_or_else(|| SegmentTemplate {
                level: "B".to_string(),
                from_id: None,
                to_id: None,
                from_name: from.clone(),
            });

        let mut row = Map::with_capacity(9);
        row.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
        *next_leg_id += 1;
        row.insert(
            "AirwayID".to_string(),
            Value::Number(Number::from(airway_id)),
        );
        row.insert("Level".to_string(), Value::String(template.level.clone()));

        let oriented_same = template.from_name == *from;
        if oriented_same {
            row.insert(
                "Waypoint1ID".to_string(),
                template
                    .from_id
                    .map_or(Value::Null, |value| Value::Number(Number::from(value))),
            );
            row.insert(
                "Waypoint2ID".to_string(),
                template
                    .to_id
                    .map_or(Value::Null, |value| Value::Number(Number::from(value))),
            );
        } else {
            row.insert(
                "Waypoint1ID".to_string(),
                template
                    .to_id
                    .map_or(Value::Null, |value| Value::Number(Number::from(value))),
            );
            row.insert(
                "Waypoint2ID".to_string(),
                template
                    .from_id
                    .map_or(Value::Null, |value| Value::Number(Number::from(value))),
            );
        }

        row.insert(
            "IsStart".to_string(),
            Value::Number(Number::from(i32::from(idx == 0 && mark_start))),
        );
        row.insert(
            "IsEnd".to_string(),
            Value::Number(Number::from(i32::from(idx + 1 == points.len() - 1 && mark_end))),
        );
        row.insert("Waypoint1".to_string(), Value::String(from.clone()));
        row.insert("Waypoint2".to_string(), Value::String(to.clone()));
        built.push(row);
    }

    built
}

fn reconcile_and_dedup_directionality(
    rows: Vec<Map<String, Value>>,
    source_directionality: &SourceDirectionality,
) -> Vec<Map<String, Value>> {
    let mut index_by_edge: HashMap<DirectedEdge, usize> = HashMap::new();
    let mut deduped: Vec<Map<String, Value>> = Vec::with_capacity(rows.len());

    for mut row in rows {
        let level = row_text(&row, "Level").to_string();
        let from = row_text(&row, "Waypoint1").to_string();
        let to = row_text(&row, "Waypoint2").to_string();
        if from.is_empty() || to.is_empty() {
            deduped.push(row);
            continue;
        }

        let mut edge = DirectedEdge {
            level: level.clone(),
            from: from.clone(),
            to: to.clone(),
        };

        let undirected = undirected_edge(&level, &from, &to);
        if source_directionality.undirected.contains(&undirected)
            && !source_directionality.directed.contains(&edge)
        {
            let reverse = DirectedEdge {
                level,
                from: to,
                to: from,
            };
            if source_directionality.directed.contains(&reverse) {
                swap(&mut row, "Waypoint1ID", "Waypoint2ID");
                swap(&mut row, "Waypoint1", "Waypoint2");
                swap(&mut row, "IsStart", "IsEnd");
                edge = reverse;
            } else {
                continue;
            }
        }

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

fn clone_leg_with_new_id(
    row: &Map<String, Value>,
    airway_id: i64,
    next_leg_id: &mut i64,
) -> Map<String, Value> {
    let mut cloned = row.clone();
    cloned.insert("ID".to_string(), Value::Number(Number::from(*next_leg_id)));
    *next_leg_id += 1;
    cloned.insert(
        "AirwayID".to_string(),
        Value::Number(Number::from(airway_id)),
    );
    cloned
}

fn clone_reference_leg_with_new_id(
    row: &AirwayLegReferenceRow,
    airway_id: i64,
    next_leg_id: &mut i64,
) -> Map<String, Value> {
    let cloned = row.to_map_with_ids(*next_leg_id, airway_id);
    *next_leg_id += 1;
    cloned
}

fn directed_edge_from_row(row: &Map<String, Value>) -> Option<DirectedEdge> {
    let level = row_text(row, "Level");
    let from = row_text(row, "Waypoint1");
    let to = row_text(row, "Waypoint2");
    if from.is_empty() || to.is_empty() {
        return None;
    }
    Some(directed_edge(level, from, to))
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
        let new_id = one_based_i64(idx);
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
        row.insert(
            "ID".to_string(),
            Value::Number(Number::from(one_based_i64(idx))),
        );
    }
}

fn process_chain_row(
    row: &Map<String, Value>,
    chains: &mut Vec<Chain>,
    current_points: &mut Vec<String>,
) {
    let from = row_text(row, "Waypoint1");
    let to = row_text(row, "Waypoint2");
    if from.is_empty() || to.is_empty() {
        return;
    }

    let is_start = json_to_i64(row.get("IsStart")) == Some(1);
    if is_start
        || current_points.is_empty()
        || current_points.last().map(String::as_str) != Some(from)
    {
        flush_chain(chains, current_points);
        current_points.push(from.to_string());
    }
    current_points.push(to.to_string());

    let is_end = json_to_i64(row.get("IsEnd")) == Some(1);
    if is_end {
        flush_chain(chains, current_points);
    }
}

fn flush_chain(chains: &mut Vec<Chain>, current_points: &mut Vec<String>) {
    if current_points.len() >= 2 {
        chains.push(Chain::from_points(std::mem::take(current_points)));
    } else {
        current_points.clear();
    }
}

fn one_based_i64(index: usize) -> i64 {
    i64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .unwrap_or(i64::MAX)
}
