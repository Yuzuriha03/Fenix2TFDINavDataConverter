mod merge;
pub mod rte_seg;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use serde::{Deserialize, Serialize};

use crate::db_json::write_json_objects;
use crate::stats::{AirwayTimingBreakdown, PhaseDurations, TableExportStats};
use crate::waypoints::WaypointIdIndex;

const CHUNK_SIZE: usize = 2048;

pub(crate) use rte_seg::PreloadedRteSegData;
pub(crate) use rte_seg::WaypointCandidate;

#[derive(Clone, Debug, Default)]
pub(crate) struct AirwayMirrorReference {
    pub(super) mirrored_pairs_by_ident: HashMap<String, HashMap<String, HashSet<String>>>,
}

impl AirwayMirrorReference {
    pub(super) fn insert_mirrored_pair(
        &mut self,
        ident: String,
        waypoint1: String,
        waypoint2: String,
    ) {
        let (left, right) = ordered_pair_owned(waypoint1, waypoint2);
        self.mirrored_pairs_by_ident
            .entry(ident)
            .or_default()
            .entry(left)
            .or_default()
            .insert(right);
    }

    pub(super) fn should_mirror(
        &self,
        ident: &str,
        _level: &str,
        waypoint1: &str,
        waypoint2: &str,
    ) -> bool {
        let (left, right) = ordered_pair_ref(waypoint1, waypoint2);
        self.mirrored_pairs_by_ident
            .get(ident)
            .and_then(|pairs_by_left| pairs_by_left.get(left))
            .is_some_and(|rights| rights.contains(right))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AirwayReferenceData {
    pub(super) airways: Vec<AirwayReferenceRow>,
    pub(super) airway_legs: Vec<AirwayLegReferenceRow>,
    pub(super) mirror_reference: AirwayMirrorReference,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PreloadedAirwayReferenceJson {
    pub(super) airways: Vec<AirwayReferenceRow>,
    pub(super) airway_legs: Vec<AirwayLegReferenceRow>,
}

pub(crate) type PreloadedWaypointCandidates = HashMap<String, Vec<WaypointCandidate>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AirwayOutputRow {
    #[serde(rename = "ID")]
    pub(super) id: i64,
    #[serde(rename = "Ident")]
    pub(super) ident: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AirwayLegOutputRow {
    #[serde(rename = "ID")]
    pub(super) id: i64,
    #[serde(rename = "AirwayID")]
    pub(super) airway_id: i64,
    #[serde(rename = "Level")]
    pub(super) level: String,
    #[serde(rename = "Waypoint1ID")]
    pub(super) waypoint1_id: Option<i64>,
    #[serde(rename = "Waypoint2ID")]
    pub(super) waypoint2_id: Option<i64>,
    #[serde(rename = "IsStart")]
    pub(super) is_start: i64,
    #[serde(rename = "IsEnd")]
    pub(super) is_end: i64,
    #[serde(rename = "Waypoint1")]
    pub(super) waypoint1: String,
    #[serde(rename = "Waypoint2")]
    pub(super) waypoint2: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AirwayReferenceRow {
    #[serde(rename = "ID")]
    pub(super) id: i64,
    #[serde(rename = "Ident")]
    pub(super) ident: String,
}

impl AirwayReferenceRow {
    pub(super) fn to_output_with_id(&self, id: i64) -> AirwayOutputRow {
        AirwayOutputRow {
            id,
            ident: self.ident.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AirwayLegReferenceRow {
    #[serde(rename = "ID")]
    pub(super) id: i64,
    #[serde(rename = "AirwayID")]
    pub(super) airway_id: i64,
    #[serde(rename = "Level")]
    pub(super) level: String,
    #[serde(rename = "Waypoint1")]
    pub(super) waypoint1: String,
    #[serde(rename = "Waypoint2")]
    pub(super) waypoint2: String,
    #[serde(rename = "Waypoint1ID")]
    pub(super) waypoint1_id: i64,
    #[serde(rename = "Waypoint2ID")]
    pub(super) waypoint2_id: i64,
    #[serde(rename = "IsStart")]
    pub(super) is_start: i64,
    #[serde(rename = "IsEnd")]
    pub(super) is_end: i64,
}

impl AirwayLegReferenceRow {
    pub(super) fn to_output_with_ids(&self, id: i64, airway_id: i64) -> AirwayLegOutputRow {
        AirwayLegOutputRow {
            id,
            airway_id,
            level: self.level.clone(),
            waypoint1: self.waypoint1.clone(),
            waypoint2: self.waypoint2.clone(),
            waypoint1_id: Some(self.waypoint1_id),
            waypoint2_id: Some(self.waypoint2_id),
            is_start: self.is_start,
            is_end: self.is_end,
        }
    }
}

fn load_airways_reference(path: &Path) -> Result<Vec<AirwayReferenceRow>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open json file: {}", path.display()))?;
    let reader = BufReader::with_capacity(1024 * 1024, file);
    serde_json::from_reader(reader)
        .with_context(|| format!("failed to parse airway reference json: {}", path.display()))
}

fn load_airway_legs_reference(path: &Path) -> Result<Vec<AirwayLegReferenceRow>> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read json file: {}", path.display()))?;
    let object_ranges = split_top_level_json_object_ranges(&bytes)
        .with_context(|| format!("failed to split airway leg json array: {}", path.display()))?;

    let chunked_rows: Result<Vec<Vec<AirwayLegReferenceRow>>, serde_json::Error> = object_ranges
        .par_chunks(CHUNK_SIZE)
        .map(|chunk| {
            let mut rows = Vec::with_capacity(chunk.len());
            for &(start, end) in chunk {
                rows.push(serde_json::from_slice::<AirwayLegReferenceRow>(
                    &bytes[start..end],
                )?);
            }
            Ok(rows)
        })
        .collect();

    let mut rows = Vec::with_capacity(object_ranges.len());
    for mut chunk in chunked_rows.with_context(|| {
        format!(
            "failed to parse airway leg reference json objects: {}",
            path.display()
        )
    })? {
        rows.append(&mut chunk);
    }
    Ok(rows)
}

fn split_top_level_json_object_ranges(bytes: &[u8]) -> Result<Vec<(usize, usize)>> {
    fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        i
    }

    let mut i = skip_ws(bytes, 0);
    if i >= bytes.len() || bytes[i] != b'[' {
        bail!("expected '[' at top-level");
    }
    i += 1;

    let mut out = Vec::new();
    loop {
        i = skip_ws(bytes, i);
        if i >= bytes.len() {
            bail!("unexpected EOF while parsing top-level array");
        }
        if bytes[i] == b']' {
            break;
        }
        if bytes[i] != b'{' {
            bail!("expected object in top-level array");
        }

        let start = i;
        let mut depth = 0i32;
        let mut in_string = false;
        let mut escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            if in_string {
                if escaped {
                    escaped = false;
                } else if b == b'\\' {
                    escaped = true;
                } else if b == b'"' {
                    in_string = false;
                }
            } else {
                match b {
                    b'"' => in_string = true,
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }

        if depth != 0 {
            bail!("unterminated json object in top-level array");
        }
        out.push((start, i));

        i = skip_ws(bytes, i);
        if i >= bytes.len() {
            bail!("unexpected EOF after json object");
        }
        match bytes[i] {
            b',' => i += 1,
            b']' => break,
            _ => bail!("expected ',' or ']' after object"),
        }
    }

    Ok(out)
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AirwayReferenceLoadTiming {
    pub(crate) mirror_reference: Duration,
    pub(crate) airways_json: Duration,
    pub(crate) airway_legs_json: Duration,
}

pub(crate) fn export_airway_tables(
    db_path: &Path,
    output_dir: &Path,
    airway_reference: &AirwayReferenceData,
    rte_seg_path: &Path,
    preloaded_rte_seg: Option<&PreloadedRteSegData>,
    preloaded_waypoint_candidates: Option<&PreloadedWaypointCandidates>,
    waypoint_id_index: Option<&WaypointIdIndex>,
) -> Result<(Vec<TableExportStats>, AirwayTimingBreakdown)> {
    let airway_db_read_time = Duration::default();

    let waypoint_candidate_start = Instant::now();
    let owned_rte_seg_rows;
    let rte_seg_rows = if let Some(preloaded_rte_seg) = preloaded_rte_seg {
        preloaded_rte_seg.airway_rows.as_slice()
    } else {
        owned_rte_seg_rows = rte_seg::load_rte_seg_airway_rows(rte_seg_path)?;
        owned_rte_seg_rows.as_slice()
    };
    let owned_waypoint_candidates;
    let waypoint_candidates =
        if let Some(preloaded_waypoint_candidates) = preloaded_waypoint_candidates {
            preloaded_waypoint_candidates
        } else {
            let required_idents = rte_seg::collect_required_waypoint_idents(rte_seg_rows);
            owned_waypoint_candidates =
                rte_seg::load_waypoint_candidates_from_db(db_path, &required_idents)?;
            &owned_waypoint_candidates
        };
    let waypoint_candidates_load = if preloaded_waypoint_candidates.is_some() {
        Duration::default()
    } else {
        waypoint_candidate_start.elapsed()
    };

    let airway_transform_start = Instant::now();
    let (formatted_airways, formatted_legs) = rte_seg::build_airway_tables_from_rows_with_id_index(
        rte_seg_rows,
        waypoint_candidates,
        waypoint_id_index,
    );
    let airway_transform_time = airway_transform_start.elapsed();

    let airway_leg_transform_start = Instant::now();
    let (final_airways, final_legs) =
        merge::merge_airway_outputs(&formatted_airways, &formatted_legs, airway_reference);
    let airway_leg_transform_time = airway_leg_transform_start.elapsed();

    let airway_write_start = Instant::now();
    write_json_objects(&output_dir.join("Airways.json"), &final_airways)?;
    let airway_write_time = airway_write_start.elapsed();

    let airway_leg_write_start = Instant::now();
    write_json_objects(&output_dir.join("AirwayLegs.json"), &final_legs)?;
    let airway_leg_write_time = airway_leg_write_start.elapsed();

    Ok((
        vec![
            TableExportStats {
                table_name: "Airways".to_string(),
                row_count: final_airways.len(),
                phase: PhaseDurations {
                    db_read: airway_db_read_time,
                    json_transform: airway_transform_time,
                    json_write: airway_write_time,
                },
                detail: None,
            },
            TableExportStats {
                table_name: "AirwayLegs".to_string(),
                row_count: final_legs.len(),
                phase: PhaseDurations {
                    db_read: Duration::default(),
                    json_transform: airway_leg_transform_time,
                    json_write: airway_leg_write_time,
                },
                detail: None,
            },
        ],
        AirwayTimingBreakdown {
            waypoint_candidates_load,
            build_from_rte_seg: airway_transform_time,
            merge_outputs: airway_leg_transform_time,
            write_airways: airway_write_time,
            write_airway_legs: airway_leg_write_time,
        },
    ))
}

pub(crate) fn load_airway_reference(
    reference_dir: Option<&Path>,
    rte_seg_path: &Path,
    preloaded_rte_seg: Option<&PreloadedRteSegData>,
    preloaded_reference_json: Option<PreloadedAirwayReferenceJson>,
) -> Result<(AirwayReferenceData, AirwayReferenceLoadTiming)> {
    let mut timing = AirwayReferenceLoadTiming::default();

    let mirror_start = Instant::now();
    let mirror_reference = if let Some(preloaded_rte_seg) = preloaded_rte_seg {
        preloaded_rte_seg.mirror_reference.clone()
    } else {
        rte_seg::load_rte_seg_mirror_reference(rte_seg_path)?
    };
    timing.mirror_reference = mirror_start.elapsed();

    let (airways, airway_legs) = if let Some(preloaded_reference_json) = preloaded_reference_json {
        (
            preloaded_reference_json.airways,
            preloaded_reference_json.airway_legs,
        )
    } else {
        let Some(reference_dir) = reference_dir else {
            return Ok((
                AirwayReferenceData {
                    airways: Vec::new(),
                    airway_legs: Vec::new(),
                    mirror_reference,
                },
                timing,
            ));
        };

        let airways_path = reference_dir.join("Airways.json");
        let airway_legs_path = reference_dir.join("AirwayLegs.json");
        if !airways_path.exists() || !airway_legs_path.exists() {
            bail!(
                "reference directory is missing Airways.json or AirwayLegs.json: {}",
                reference_dir.display()
            );
        }

        let airways_start = Instant::now();
        let airways = load_airways_reference(&airways_path)?;
        timing.airways_json = airways_start.elapsed();

        let airway_legs_start = Instant::now();
        let airway_legs = load_airway_legs_reference(&airway_legs_path)?;
        timing.airway_legs_json = airway_legs_start.elapsed();
        (airways, airway_legs)
    };

    Ok((
        AirwayReferenceData {
            airways,
            airway_legs,
            mirror_reference,
        },
        timing,
    ))
}

#[must_use]
pub fn directed_airway_route_key(ident: &str, waypoint1: &str, waypoint2: &str) -> String {
    format!("{ident}\u{1f}|{waypoint1}\u{1f}|{waypoint2}")
}

fn ordered_pair_ref<'a>(left: &'a str, right: &'a str) -> (&'a str, &'a str) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn ordered_pair_owned(left: String, right: String) -> (String, String) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

pub(crate) fn preload_rte_seg(rte_seg_path: &Path) -> Result<PreloadedRteSegData> {
    rte_seg::preload_rte_seg(rte_seg_path)
}

pub(crate) fn preload_airway_reference_json(
    reference_dir: Option<&Path>,
) -> Result<Option<PreloadedAirwayReferenceJson>> {
    let Some(reference_dir) = reference_dir else {
        return Ok(None);
    };

    let airways_path = reference_dir.join("Airways.json");
    let airway_legs_path = reference_dir.join("AirwayLegs.json");
    if !airways_path.exists() || !airway_legs_path.exists() {
        bail!(
            "reference directory is missing Airways.json or AirwayLegs.json: {}",
            reference_dir.display()
        );
    }

    Ok(Some(PreloadedAirwayReferenceJson {
        airways: load_airways_reference(&airways_path)?,
        airway_legs: load_airway_legs_reference(&airway_legs_path)?,
    }))
}

pub(crate) fn preload_waypoint_candidates(
    db_path: &Path,
    preloaded_rte_seg: &PreloadedRteSegData,
) -> Result<PreloadedWaypointCandidates> {
    rte_seg::load_waypoint_candidates_from_db(db_path, &preloaded_rte_seg.required_waypoint_idents)
}

#[cfg(test)]
mod tests {
    use super::AirwayMirrorReference;

    #[test]
    fn mirror_reference_matches_both_directions_without_rebuilding_global_keys() {
        let mut reference = AirwayMirrorReference::default();
        reference.insert_mirrored_pair("H100".to_string(), "C".to_string(), "A".to_string());

        assert!(reference.should_mirror("H100", "B", "A", "C"));
        assert!(reference.should_mirror("H100", "B", "C", "A"));
        assert!(!reference.should_mirror("H100", "B", "A", "D"));
        assert!(!reference.should_mirror("H200", "B", "A", "C"));
    }
}
