use rayon::prelude::*;
use rustc_hash::{FxBuildHasher as BuildHasher, FxHashMap as HashMap, FxHashSet as HashSet};

use super::{
    AirwayLegOutputRow, AirwayLegReferenceRow, AirwayOutputRow, AirwayReferenceData,
    AirwayReferenceRow,
};

type AirwayLegRows = Vec<AirwayLegOutputRow>;
type AirwayMergeOutput = (Vec<AirwayOutputRow>, AirwayLegRows);
type SourceAirwayRowsByIdent<'a> = HashMap<String, &'a AirwayOutputRow>;
type SourceLegRowsByIdent<'a> = HashMap<String, Vec<&'a AirwayLegOutputRow>>;
type ReferenceAirwayRowsByIdent<'a> = HashMap<String, &'a AirwayReferenceRow>;
type ReferenceLegRowsByIdent<'a> = HashMap<String, Vec<&'a AirwayLegReferenceRow>>;

const fn fast_hash_map<K, V>() -> HashMap<K, V> {
    HashMap::with_hasher(BuildHasher)
}

fn fast_hash_map_with_capacity<K, V>(capacity: usize) -> HashMap<K, V> {
    HashMap::with_capacity_and_hasher(capacity, BuildHasher)
}

fn fast_hash_set_with_capacity<T>(capacity: usize) -> HashSet<T> {
    HashSet::with_capacity_and_hasher(capacity, BuildHasher)
}

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

#[derive(Clone, Copy, Debug)]
struct SourceMergeState<'a> {
    chains: &'a [Chain],
    directionality: &'a SourceDirectionality,
}

#[derive(Clone, Copy, Debug)]
struct InsertPath<'a> {
    chain: &'a Chain,
    from_idx: usize,
    to_idx: usize,
}


impl InsertPath<'_> {
    const fn point_count(&self) -> usize {
        self.from_idx.abs_diff(self.to_idx) + 1
    }

    const fn segment_count(&self) -> usize {
        self.point_count().saturating_sub(1)
    }

    fn point_at(&self, offset: usize) -> &str {
        if self.from_idx <= self.to_idx {
            self.chain.point(self.from_idx + offset)
        } else {
            self.chain.point(self.from_idx - offset)
        }
    }
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
    airway_row: AirwayOutputRow,
    legs: AirwayLegRows,
}

#[derive(Clone, Debug)]
struct Chain {
    points: Vec<String>,
    first_index_by_point: HashMap<String, usize>,
}

impl Chain {
    fn from_points(points: Vec<String>) -> Self {
        let mut first_index_by_point = fast_hash_map_with_capacity(points.len());
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

    fn point(&self, index: usize) -> &str {
        self.points[index].as_str()
    }
}

fn directed_edge(level: &str, from: &str, to: &str) -> DirectedEdge {
    DirectedEdge {
        level: level.to_string(),
        from: from.to_string(),
        to: to.to_string(),
    }
}

pub(super) fn merge_airway_outputs(
    source_airways: &[AirwayOutputRow],
    source_legs: &[AirwayLegOutputRow],
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

        final_airways.push(reference_airway.to_output_with_id(airway_id));
        if let Some(reference_legs) = reference_legs_by_ident.get(ident.as_str()) {
            final_legs.extend(reference_legs_to_output_rows(reference_legs, airway_id));
        }
    }

    renumber_leg_ids_in_order(&mut final_legs);
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
    let source_legs_for_ident: &[&AirwayLegOutputRow] = source_legs_by_ident
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
    let source_merge_state = SourceMergeState {
        chains: &source_chains,
        directionality: &source_directionality,
    };

    let mut next_leg_id = 1;
    let legs = if has_connection(&source_chains, existing_legs) {
        merge_with_insertion_strategy(
            ident,
            airway_id,
            source_legs_for_ident,
            existing_legs,
            source_merge_state,
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
) -> AirwayOutputRow {
    if let Some(source_row) = source_airway_by_ident.get(ident) {
        let mut row = (*source_row).clone();
        row.id = airway_id;
        return row;
    }

    if let Some(reference_row) = reference_airway_by_ident.get(ident) {
        return reference_row.to_output_with_id(airway_id);
    }

    AirwayOutputRow {
        id: airway_id,
        ident: ident.to_string(),
    }
}

fn build_temp_airway_id_by_ident(idents: &[String]) -> HashMap<String, i64> {
    let mut ids = fast_hash_map_with_capacity(idents.len());
    for (index, ident) in idents.iter().enumerate() {
        ids.insert(ident.clone(), one_based_i64(index));
    }
    ids
}

fn reference_legs_to_output_rows(
    rows: &[&AirwayLegReferenceRow],
    airway_id: i64,
) -> Vec<AirwayLegOutputRow> {
    rows.iter()
        .map(|row| row.to_output_with_ids(row.id, airway_id))
        .collect()
}

fn build_airway_ident_map(rows: &[AirwayOutputRow]) -> HashMap<i64, String> {
    let mut idents = fast_hash_map_with_capacity(rows.len());
    for row in rows {
        idents.insert(row.id, row.ident.clone());
    }
    idents
}

fn build_reference_airway_ident_map(rows: &[AirwayReferenceRow]) -> HashMap<i64, String> {
    let mut idents = fast_hash_map_with_capacity(rows.len());
    for row in rows {
        idents.insert(row.id, row.ident.clone());
    }
    idents
}

fn build_airway_row_map_by_ident(rows: &[AirwayOutputRow]) -> SourceAirwayRowsByIdent<'_> {
    let mut grouped = fast_hash_map_with_capacity(rows.len());
    for row in rows {
        grouped.insert(row.ident.clone(), row);
    }
    grouped
}

fn build_reference_airway_row_map_by_ident(
    rows: &[AirwayReferenceRow],
) -> ReferenceAirwayRowsByIdent<'_> {
    let mut grouped = fast_hash_map_with_capacity(rows.len());
    for row in rows {
        grouped.insert(row.ident.clone(), row);
    }
    grouped
}

fn group_airway_legs_by_ident<'a>(
    rows: &'a [AirwayLegOutputRow],
    airway_ident_by_id: &HashMap<i64, String>,
) -> SourceLegRowsByIdent<'a> {
    let mut grouped: SourceLegRowsByIdent<'a> = fast_hash_map();
    for row in rows {
        let Some(ident) = airway_ident_by_id.get(&row.airway_id) else {
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
    let mut grouped: ReferenceLegRowsByIdent<'a> = fast_hash_map();
    for row in rows {
        let Some(ident) = airway_ident_by_id.get(&row.airway_id) else {
            continue;
        };
        grouped.entry(ident.clone()).or_default().push(row);
    }
    for grouped_rows in grouped.values_mut() {
        grouped_rows.sort_by_key(|row| row.id);
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

fn extract_chains(rows: &[&AirwayLegOutputRow]) -> Vec<Chain> {
    let mut chains = Vec::new();
    let mut current_points: Vec<String> = Vec::new();

    // Source airway legs are produced in ID order by `build_airway_tables_from_rows_with_id_index`,
    // so we can stream them directly without re-sorting every ident during merge.
    for row in rows {
        process_chain_row(row, &mut chains, &mut current_points);
    }

    if current_points.len() >= 2 {
        chains.push(Chain::from_points(current_points));
    }

    chains
}

fn build_expected_directionality(
    ident: &str,
    rows: &[&AirwayLegOutputRow],
    mirror_reference: &super::AirwayMirrorReference,
) -> SourceDirectionality {
    let mut directed = fast_hash_set_with_capacity(rows.len() * 2);
    let mut undirected = fast_hash_set_with_capacity(rows.len());

    for row in rows {
        let from = row.waypoint1.as_str();
        let to = row.waypoint2.as_str();
        if from.is_empty() || to.is_empty() {
            continue;
        }
        directed.insert(directed_edge(row.level.as_str(), from, to));
        if mirror_reference.should_mirror(ident, row.level.as_str(), from, to) {
            directed.insert(directed_edge(row.level.as_str(), to, from));
        }
        undirected.insert(undirected_edge(row.level.as_str(), from, to));
    }

    SourceDirectionality {
        directed,
        undirected,
    }
}

fn merge_with_insertion_strategy(
    ident: &str,
    airway_id: i64,
    source_legs: &[&AirwayLegOutputRow],
    existing_legs: &[&AirwayLegReferenceRow],
    source_merge_state: SourceMergeState<'_>,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> AirwayLegRows {
    let template_lookup = build_template_lookup(source_legs, existing_legs);

    let mut merged = Vec::with_capacity(existing_legs.len() + source_legs.len());
    let mut seen = fast_hash_set_with_capacity(existing_legs.len() + source_legs.len());
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

        let insert_path = find_insert_path(from, to, source_merge_state.chains);
        if let Some(path) = insert_path
            && path.point_count() > 2
        {
            let inserted = build_rows_from_path(
                airway_id,
                path,
                &template_lookup,
                next_leg_id,
                row.is_start == 1,
                row.is_end == 1,
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
        let from = row.waypoint1.as_str();
        let to = row.waypoint2.as_str();
        if from.is_empty() || to.is_empty() {
            continue;
        }
        let edge = directed_edge(row.level.as_str(), from, to);
        if seen.insert(edge) {
            merged.push(clone_leg_with_new_id(row, airway_id, next_leg_id));
        }
    }

    let mirrored = apply_mirror_rows(ident, merged, &mut seen, mirror_reference, next_leg_id);
    reconcile_and_dedup_directionality(mirrored, source_merge_state.directionality)
}

fn build_independent_append(
    ident: &str,
    airway_id: i64,
    source_legs: &[&AirwayLegOutputRow],
    source_directionality: &SourceDirectionality,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> AirwayLegRows {
    let mut rows = Vec::with_capacity(source_legs.len());
    let mut seen = fast_hash_set_with_capacity(source_legs.len());
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
    mut rows: AirwayLegRows,
    existing_directed: &mut HashSet<DirectedEdge>,
    mirror_reference: &super::AirwayMirrorReference,
    next_leg_id: &mut i64,
) -> AirwayLegRows {
    let original_len = rows.len();
    for idx in (0..original_len).rev() {
        let Some(reverse) = ({
            let row = &rows[idx];
            let from = row.waypoint1.as_str();
            let to = row.waypoint2.as_str();
            if from.is_empty()
                || to.is_empty()
                || !mirror_reference.should_mirror(ident, row.level.as_str(), from, to)
            {
                None
            } else {
                let reverse = directed_edge(row.level.as_str(), to, from);
                (!existing_directed.contains(&reverse)).then_some(reverse)
            }
        }) else {
            continue;
        };

        let mut mirrored = rows[idx].clone();
        mirrored.id = *next_leg_id;
        *next_leg_id += 1;
        swap_leg_direction(&mut mirrored);
        rows.push(mirrored);
        existing_directed.insert(reverse);
    }

    rows
}

fn find_insert_path<'a>(
    from: &str,
    to: &str,
    source_chains: &'a [Chain],
) -> Option<InsertPath<'a>> {
    let mut best: Option<InsertPath<'a>> = None;

    for chain in source_chains {
        let (Some(from_idx), Some(to_idx)) = (chain.point_index(from), chain.point_index(to))
        else {
            continue;
        };
        if from_idx == to_idx {
            continue;
        }

        let path = InsertPath {
            chain,
            from_idx,
            to_idx,
        };
        let distance = path.segment_count();
        if best
            .as_ref()
            .is_none_or(|best_path| distance < best_path.segment_count())
        {
            best = Some(path);
        }
    }

    best
}

fn build_template_lookup(
    source_legs: &[&AirwayLegOutputRow],
    existing_legs: &[&AirwayLegReferenceRow],
) -> HashMap<(String, String), SegmentTemplate> {
    let mut map = fast_hash_map_with_capacity(source_legs.len() + existing_legs.len());

    for row in source_legs {
        let from = row.waypoint1.as_str();
        let to = row.waypoint2.as_str();
        if from.is_empty() || to.is_empty() {
            continue;
        }

        map.entry(undirected_pair(from, to))
            .or_insert_with(|| SegmentTemplate {
                level: row.level.clone(),
                from_id: row.waypoint1_id,
                to_id: row.waypoint2_id,
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

fn build_rows_from_path(
    airway_id: i64,
    path: InsertPath<'_>,
    template_lookup: &HashMap<(String, String), SegmentTemplate>,
    next_leg_id: &mut i64,
    mark_start: bool,
    mark_end: bool,
) -> AirwayLegRows {
    let segment_count = path.segment_count();
    if segment_count == 0 {
        return Vec::new();
    }

    let mut built = Vec::with_capacity(segment_count);
    for idx in 0..segment_count {
        let from = path.point_at(idx);
        let to = path.point_at(idx + 1);

        let key = undirected_pair(from, to);
        let template = template_lookup
            .get(&key)
            .cloned()
            .unwrap_or_else(|| SegmentTemplate {
                level: "B".to_string(),
                from_id: None,
                to_id: None,
                from_name: from.to_string(),
            });

        let (waypoint1_id, waypoint2_id) = if template.from_name == from {
            (template.from_id, template.to_id)
        } else {
            (template.to_id, template.from_id)
        };

        built.push(AirwayLegOutputRow {
            id: *next_leg_id,
            airway_id,
            level: template.level,
            waypoint1_id,
            waypoint2_id,
            is_start: i64::from(idx == 0 && mark_start),
            is_end: i64::from(idx + 1 == segment_count && mark_end),
            waypoint1: from.to_string(),
            waypoint2: to.to_string(),
        });
        *next_leg_id += 1;
    }

    built
}

fn reconcile_and_dedup_directionality(
    rows: AirwayLegRows,
    source_directionality: &SourceDirectionality,
) -> AirwayLegRows {
    let mut index_by_edge: HashMap<DirectedEdge, usize> = fast_hash_map();
    let mut deduped: AirwayLegRows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let from = row.waypoint1.as_str();
        let to = row.waypoint2.as_str();
        if from.is_empty() || to.is_empty() {
            deduped.push(row);
            continue;
        }

        let mut edge = DirectedEdge {
            level: row.level.clone(),
            from: row.waypoint1.clone(),
            to: row.waypoint2.clone(),
        };

        let undirected = undirected_edge(row.level.as_str(), from, to);
        if source_directionality.undirected.contains(&undirected)
            && !source_directionality.directed.contains(&edge)
        {
            let reverse = DirectedEdge {
                level: row.level.clone(),
                from: row.waypoint2.clone(),
                to: row.waypoint1.clone(),
            };
            if source_directionality.directed.contains(&reverse) {
                swap_leg_direction(&mut row);
                edge = reverse;
            } else {
                continue;
            }
        }

        if let Some(index) = index_by_edge.get(&edge).copied() {
            let existing = &mut deduped[index];
            if !leg_flag(existing.is_start) && leg_flag(row.is_start) {
                existing.is_start = 1;
            }
            if !leg_flag(existing.is_end) && leg_flag(row.is_end) {
                existing.is_end = 1;
            }
            continue;
        }

        index_by_edge.insert(edge, deduped.len());
        deduped.push(row);
    }

    deduped
}

fn clone_leg_with_new_id(
    row: &AirwayLegOutputRow,
    airway_id: i64,
    next_leg_id: &mut i64,
) -> AirwayLegOutputRow {
    let mut cloned = row.clone();
    cloned.id = *next_leg_id;
    cloned.airway_id = airway_id;
    *next_leg_id += 1;
    cloned
}

fn clone_reference_leg_with_new_id(
    row: &AirwayLegReferenceRow,
    airway_id: i64,
    next_leg_id: &mut i64,
) -> AirwayLegOutputRow {
    let cloned = row.to_output_with_ids(*next_leg_id, airway_id);
    *next_leg_id += 1;
    cloned
}

fn directed_edge_from_row(row: &AirwayLegOutputRow) -> Option<DirectedEdge> {
    let from = row.waypoint1.as_str();
    let to = row.waypoint2.as_str();
    if from.is_empty() || to.is_empty() {
        return None;
    }
    Some(directed_edge(row.level.as_str(), from, to))
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

const fn swap_leg_direction(row: &mut AirwayLegOutputRow) {
    std::mem::swap(&mut row.waypoint1_id, &mut row.waypoint2_id);
    std::mem::swap(&mut row.waypoint1, &mut row.waypoint2);
    std::mem::swap(&mut row.is_start, &mut row.is_end);
}

fn renumber_leg_ids_in_order(legs: &mut [AirwayLegOutputRow]) {
    for (idx, row) in legs.iter_mut().enumerate() {
        row.id = one_based_i64(idx);
    }
}

fn process_chain_row(
    row: &AirwayLegOutputRow,
    chains: &mut Vec<Chain>,
    current_points: &mut Vec<String>,
) {
    let from = row.waypoint1.as_str();
    let to = row.waypoint2.as_str();
    if from.is_empty() || to.is_empty() {
        return;
    }

    if leg_flag(row.is_start)
        || current_points.is_empty()
        || current_points.last().map(String::as_str) != Some(from)
    {
        flush_chain(chains, current_points);
        current_points.push(from.to_string());
    }
    current_points.push(to.to_string());

    if leg_flag(row.is_end) {
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

const fn leg_flag(value: i64) -> bool {
    value == 1
}

fn one_based_i64(index: usize) -> i64 {
    i64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LegParams {
        id: i64,
        airway_id: i64,
        waypoint1_id: i64,
        waypoint2_id: i64,
        is_start: bool,
        is_end: bool,
        waypoint1: String,
        waypoint2: String,
    }

    fn airway_row(id: i64, ident: &str) -> AirwayOutputRow {
        AirwayOutputRow {
            id,
            ident: ident.to_string(),
        }
    }

    fn source_leg_row(params: LegParams) -> AirwayLegOutputRow {
        AirwayLegOutputRow {
            id: params.id,
            airway_id: params.airway_id,
            level: "B".to_string(),
            waypoint1_id: Some(params.waypoint1_id),
            waypoint2_id: Some(params.waypoint2_id),
            is_start: i64::from(params.is_start),
            is_end: i64::from(params.is_end),
            waypoint1: params.waypoint1,
            waypoint2: params.waypoint2,
        }
    }

    fn reference_airway_row(id: i64, ident: &str) -> AirwayReferenceRow {
        AirwayReferenceRow {
            id,
            ident: ident.to_string(),
        }
    }

    fn reference_leg_row(params: LegParams) -> AirwayLegReferenceRow {
        AirwayLegReferenceRow {
            id: params.id,
            airway_id: params.airway_id,
            level: "B".to_string(),
            waypoint1: params.waypoint1,
            waypoint2: params.waypoint2,
            waypoint1_id: params.waypoint1_id,
            waypoint2_id: params.waypoint2_id,
            is_start: i64::from(params.is_start),
            is_end: i64::from(params.is_end),
        }
    }

    #[test]
    fn merge_inserts_missing_segments_and_keeps_global_ids_sequential() {
        let source_airways = vec![airway_row(42, "H100")];
        let source_legs = vec![
            source_leg_row(LegParams {
                id: 1,
                airway_id: 42,
                waypoint1_id: 101,
                waypoint2_id: 102,
                is_start: true,
                is_end: false,
                waypoint1: "A".to_string(),
                waypoint2: "B".to_string(),
            }),
            source_leg_row(LegParams {
                id: 2,
                airway_id: 42,
                waypoint1_id: 102,
                waypoint2_id: 103,
                is_start: false,
                is_end: true,
                waypoint1: "B".to_string(),
                waypoint2: "C".to_string(),
            }),
        ];
        let airway_reference = AirwayReferenceData {
            airways: vec![reference_airway_row(7, "H100")],
            airway_legs: vec![reference_leg_row(LegParams {
                id: 10,
                airway_id: 7,
                waypoint1_id: 101,
                waypoint2_id: 103,
                is_start: true,
                is_end: true,
                waypoint1: "A".to_string(),
                waypoint2: "C".to_string(),
            })],
            mirror_reference: super::super::AirwayMirrorReference::default(),
        };

        let (airways, legs) =
            merge_airway_outputs(&source_airways, &source_legs, &airway_reference);

        assert_eq!(airways.len(), 1);
        assert_eq!(airways[0].ident, "H100");
        assert_eq!(airways[0].id, 1);

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].id, 1);
        assert_eq!(legs[1].id, 2);
        assert_eq!(legs[0].airway_id, 1);
        assert_eq!(legs[1].airway_id, 1);
        assert_eq!(legs[0].waypoint1, "A");
        assert_eq!(legs[0].waypoint2, "B");
        assert_eq!(legs[1].waypoint1, "B");
        assert_eq!(legs[1].waypoint2, "C");
    }

    #[test]
    fn merge_sorts_reference_only_legs_within_each_ident_before_global_renumber() {
        let source_airways = Vec::new();
        let source_legs = Vec::new();
        let airway_reference = AirwayReferenceData {
            airways: vec![reference_airway_row(2, "Z1"), reference_airway_row(1, "A1")],
            airway_legs: vec![
                reference_leg_row(LegParams {
                    id: 20,
                    airway_id: 1,
                    waypoint1_id: 102,
                    waypoint2_id: 103,
                    is_start: false,
                    is_end: true,
                    waypoint1: "B".to_string(),
                    waypoint2: "C".to_string(),
                }),
                reference_leg_row(LegParams {
                    id: 10,
                    airway_id: 1,
                    waypoint1_id: 101,
                    waypoint2_id: 102,
                    is_start: true,
                    is_end: false,
                    waypoint1: "A".to_string(),
                    waypoint2: "B".to_string(),
                }),
            ],
            mirror_reference: super::super::AirwayMirrorReference::default(),
        };

        let (airways, legs) =
            merge_airway_outputs(&source_airways, &source_legs, &airway_reference);

        assert_eq!(airways.len(), 2);
        assert_eq!(airways[0].ident, "A1");
        assert_eq!(airways[1].ident, "Z1");
        assert_eq!(airways[0].id, 1);
        assert_eq!(airways[1].id, 2);

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].id, 1);
        assert_eq!(legs[1].id, 2);
        assert_eq!(legs[0].airway_id, 1);
        assert_eq!(legs[1].airway_id, 1);
        assert_eq!(legs[0].waypoint1, "A");
        assert_eq!(legs[0].waypoint2, "B");
        assert_eq!(legs[1].waypoint1, "B");
        assert_eq!(legs[1].waypoint2, "C");
    }
}
