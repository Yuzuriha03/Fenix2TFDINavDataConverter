mod merge;
mod rte_seg;

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, bail};
use serde_json::{Map, Value};

use crate::db_json::{read_json_object_array, write_json_objects};
use crate::stats::{PhaseDurations, TableExportStats};

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
    pub(super) airways: Vec<Map<String, Value>>,
    pub(super) airway_legs: Vec<Map<String, Value>>,
    pub(super) mirror_reference: AirwayMirrorReference,
}

pub(crate) fn export_airway_tables(
    db_path: &Path,
    output_dir: &Path,
    airway_reference: &AirwayReferenceData,
    rte_seg_path: &Path,
) -> Result<Vec<TableExportStats>> {
    let airway_db_read_time = Default::default();

    let waypoint_candidates = rte_seg::load_waypoint_candidates_from_db(db_path)?;

    let airway_transform_start = Instant::now();
    let (formatted_airways, formatted_legs) =
        rte_seg::build_airway_tables_from_rte_seg(rte_seg_path, &waypoint_candidates)?;
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

    Ok(vec![
        TableExportStats {
            table_name: "Airways".to_string(),
            row_count: final_airways.len(),
            phase: PhaseDurations {
                db_read: airway_db_read_time,
                json_transform: airway_transform_time,
                json_write: airway_write_time,
            },
        },
        TableExportStats {
            table_name: "AirwayLegs".to_string(),
            row_count: final_legs.len(),
            phase: PhaseDurations {
                db_read: Default::default(),
                json_transform: airway_leg_transform_time,
                json_write: airway_leg_write_time,
            },
        },
    ])
}

pub(crate) fn load_airway_reference(
    reference_dir: Option<&Path>,
    rte_seg_path: &Path,
) -> Result<AirwayReferenceData> {
    let mirror_reference = rte_seg::load_rte_seg_mirror_reference(rte_seg_path)?;
    let Some(reference_dir) = reference_dir else {
        return Ok(AirwayReferenceData {
            airways: Vec::new(),
            airway_legs: Vec::new(),
            mirror_reference,
        });
    };

    let airways_path = reference_dir.join("Airways.json");
    let airway_legs_path = reference_dir.join("AirwayLegs.json");
    if !airways_path.exists() || !airway_legs_path.exists() {
        bail!(
            "reference directory is missing Airways.json or AirwayLegs.json: {}",
            reference_dir.display()
        );
    }

    let airways = read_json_object_array(&airways_path)?;
    let airway_legs = read_json_object_array(&airway_legs_path)?;
    Ok(AirwayReferenceData {
        airways,
        airway_legs,
        mirror_reference,
    })
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
