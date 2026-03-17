mod merge;
mod rte_seg;

use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Number, Value};

use crate::db_json::write_json_objects;
use crate::stats::{AirwayTimingBreakdown, PhaseDurations, TableExportStats};

#[derive(Clone, Debug, Default)]
pub(crate) struct AirwayMirrorReference {
    pub(super) mirrored_edge_keys: HashSet<String>,
}

impl AirwayMirrorReference {
    pub(super) fn should_mirror(
        &self,
        ident: &str,
        _level: &str,
        waypoint1: &str,
        waypoint2: &str,
    ) -> bool {
        self.mirrored_edge_keys
            .contains(&undirected_airway_route_key(ident, waypoint1, waypoint2))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AirwayReferenceData {
    pub(super) airways: Vec<AirwayReferenceRow>,
    pub(super) airway_legs: Vec<AirwayLegReferenceRow>,
    pub(super) mirror_reference: AirwayMirrorReference,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AirwayReferenceRow {
    #[serde(rename = "ID")]
    pub(super) id: i64,
    #[serde(rename = "Ident")]
    pub(super) ident: String,
}

impl AirwayReferenceRow {
    pub(super) fn to_map(&self) -> Map<String, Value> {
        let mut row = Map::with_capacity(2);
        row.insert("ID".to_string(), Value::Number(Number::from(self.id)));
        row.insert("Ident".to_string(), Value::String(self.ident.clone()));
        row
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
    pub(super) fn to_map(&self) -> Map<String, Value> {
        let mut row = Map::new();
        row.insert("ID".to_string(), Value::Number(Number::from(self.id)));
        row.insert(
            "AirwayID".to_string(),
            Value::Number(Number::from(self.airway_id)),
        );
        row.insert("Level".to_string(), Value::String(self.level.clone()));
        row.insert(
            "Waypoint1".to_string(),
            Value::String(self.waypoint1.clone()),
        );
        row.insert(
            "Waypoint2".to_string(),
            Value::String(self.waypoint2.clone()),
        );
        row.insert(
            "Waypoint1ID".to_string(),
            Value::Number(Number::from(self.waypoint1_id)),
        );
        row.insert(
            "Waypoint2ID".to_string(),
            Value::Number(Number::from(self.waypoint2_id)),
        );
        row.insert(
            "IsStart".to_string(),
            Value::Number(Number::from(self.is_start)),
        );
        row.insert(
            "IsEnd".to_string(),
            Value::Number(Number::from(self.is_end)),
        );
        row
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

    const CHUNK_SIZE: usize = 2048;
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
) -> Result<(Vec<TableExportStats>, AirwayTimingBreakdown)> {
    let airway_db_read_time = Default::default();

    let waypoint_candidate_start = Instant::now();
    let rte_seg_rows = rte_seg::load_rte_seg_airway_rows(rte_seg_path)?;
    let required_idents = rte_seg::collect_required_waypoint_idents(&rte_seg_rows);
    let waypoint_candidates = rte_seg::load_waypoint_candidates_from_db(db_path, &required_idents)?;
    let waypoint_candidates_load = waypoint_candidate_start.elapsed();

    let airway_transform_start = Instant::now();
    let (formatted_airways, formatted_legs) =
        rte_seg::build_airway_tables_from_rows(&rte_seg_rows, &waypoint_candidates);
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
                    db_read: Default::default(),
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
) -> Result<(AirwayReferenceData, AirwayReferenceLoadTiming)> {
    let mut timing = AirwayReferenceLoadTiming::default();

    let mirror_start = Instant::now();
    let mirror_reference = rte_seg::load_rte_seg_mirror_reference(rte_seg_path)?;
    timing.mirror_reference = mirror_start.elapsed();

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

    Ok((
        AirwayReferenceData {
            airways,
            airway_legs,
            mirror_reference,
        },
        timing,
    ))
}

pub(super) fn directed_airway_route_key(ident: &str, waypoint1: &str, waypoint2: &str) -> String {
    format!("{ident}\u{1f}|{waypoint1}\u{1f}|{waypoint2}")
}

fn undirected_airway_route_key(ident: &str, waypoint1: &str, waypoint2: &str) -> String {
    if waypoint1 <= waypoint2 {
        directed_airway_route_key(ident, waypoint1, waypoint2)
    } else {
        directed_airway_route_key(ident, waypoint2, waypoint1)
    }
}
