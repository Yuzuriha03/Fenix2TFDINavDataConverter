pub mod airways;
pub mod config;
pub mod db_json;
pub mod stats;
pub mod tables;
pub mod terminal_legs;
pub mod waypoints;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rayon::prelude::*;
use rusqlite::Connection;

use airways::{
    AirwayReferenceData, AirwayReferenceLoadTiming, PreloadedAirwayReferenceJson,
    PreloadedRteSegData, PreloadedWaypointCandidates, export_airway_tables, load_airway_reference,
    preload_airway_reference_json, preload_waypoint_candidates,
};
use config::{
    OutputLocation, detect_start_terminal_id, parse_args, prepare_output_directory,
    prompt_db3_path, prompt_rte_seg_path, validate_required_tables,
};
use db_json::configure_read_connection;
use stats::{AirwayTimingBreakdown, ExportStats, TableExportStats, TerminalLegExportStats};
use tables::{ExistingJsonIndex, export_table_to_json, preload_existing_table_indices};
use terminal_legs::export_terminal_legs;
use waypoints::{
    AirportIdIndex, IlsIdIndex, NavaidIdIndex, RunwayIdIndex, WaypointIdIndex,
    build_airport_id_index, build_ils_id_index, build_navaid_id_index, build_runway_id_index,
    build_waypoint_id_index, export_airport_lookup_table, export_airports_table,
    export_ilses_table, export_navaid_lookup_table, export_navaids_table, export_runways_table,
    export_terminals_table, export_waypoint_lookup_table, export_waypoints_table,
};

const EXPORT_TABLES: &[&str] = &[
    "Waypoints",
    "Runways",
    "AirportLookup",
    "Airports",
    "Ilses",
    "NavaidLookup",
    "Navaids",
    "Terminals",
    "WaypointLookup",
];

#[derive(Clone, Debug)]
struct ResolvedInputPaths {
    rte_seg_path: PathBuf,
    db_path: PathBuf,
    db_validated_during_prompt: bool,
}

#[derive(Clone, Debug, Default)]
struct OutputPrewarmResult {
    start_terminal_id: Option<i64>,
}

#[derive(Debug)]
struct OutputExecutionResult {
    output: OutputLocation,
}

type PreloadedTableIndicesByName = HashMap<String, ExistingJsonIndex>;

struct SharedAirwayReferenceContext {
    runtime_reference_dir: Option<PathBuf>,
    airway_reference: AirwayReferenceData,
    load_timing: AirwayReferenceLoadTiming,
    load_time: Duration,
}

enum AirwayReferenceSource<'a> {
    Shared {
        base_json_dir: Option<&'a Path>,
        airway_reference: &'a AirwayReferenceData,
    },
    PerTarget,
}

struct ExecuteOutputTargetParams<'a> {
    output: OutputLocation,
    prewarmed: Option<&'a OutputPrewarmResult>,
    db_path: &'a Path,
    rte_seg_path: &'a Path,
    preloaded_rte_seg: &'a PreloadedRteSegData,
    preloaded_waypoint_candidates: &'a PreloadedWaypointCandidates,
    preloaded_reference_json: Option<PreloadedAirwayReferenceJson>,
    preloaded_existing_indices: Option<PreloadedTableIndicesByName>,
    reference_source: AirwayReferenceSource<'a>,
}

struct ExportDb3ToJsonParams<'a> {
    db_path: &'a Path,
    start_terminal_id: i64,
    output_dir: &'a Path,
    base_json_dir: Option<&'a Path>,
    airway_reference: &'a airways::AirwayReferenceData,
    rte_seg_path: &'a Path,
    preloaded_rte_seg: &'a PreloadedRteSegData,
    preloaded_waypoint_candidates: &'a PreloadedWaypointCandidates,
    preloaded_existing_indices: Option<PreloadedTableIndicesByName>,
}

struct ExportRuntimeContext<'a> {
    db_path: &'a Path,
    start_terminal_id: i64,
    output_dir: &'a Path,
    base_json_dir: Option<&'a Path>,
    airway_reference: &'a airways::AirwayReferenceData,
    rte_seg_path: &'a Path,
    preloaded_rte_seg: &'a PreloadedRteSegData,
    preloaded_waypoint_candidates: &'a PreloadedWaypointCandidates,
}

struct ReferenceIdIndices {
    airport: AirportIdIndex,
    runway: RunwayIdIndex,
    ils: IlsIdIndex,
    waypoint: WaypointIdIndex,
    navaid: NavaidIdIndex,
}

struct RelatedExportResults {
    table_stats: Vec<TableExportStats>,
    table_export_wall: Duration,
    airway_detail: AirwayTimingBreakdown,
    terminal_leg_stats: TerminalLegExportStats,
    terminal_leg_wall: Duration,
}

impl<'a> ExportDb3ToJsonParams<'a> {
    fn split(self) -> (ExportRuntimeContext<'a>, PreloadedTableIndicesByName) {
        let Self {
            db_path,
            start_terminal_id,
            output_dir,
            base_json_dir,
            airway_reference,
            rte_seg_path,
            preloaded_rte_seg,
            preloaded_waypoint_candidates,
            preloaded_existing_indices,
        } = self;
        (
            ExportRuntimeContext {
                db_path,
                start_terminal_id,
                output_dir,
                base_json_dir,
                airway_reference,
                rte_seg_path,
                preloaded_rte_seg,
                preloaded_waypoint_candidates,
            },
            preloaded_existing_indices.unwrap_or_default(),
        )
    }
}

impl ReferenceIdIndices {
    const fn waypoint_count(&self) -> usize {
        self.airport.db_row_count()
            + self.runway.db_row_count()
            + self.ils.db_row_count()
            + self.waypoint.db_row_count()
            + self.navaid.db_row_count()
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error:#}");
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
fn run() -> Result<()> {
    let config = parse_args()?;
    let prewarm_handle = start_output_prewarm(&config);
    let airway_reference_json_prewarm_handle = start_airway_reference_json_prewarm(&config);
    let table_index_prewarm_handle = start_table_index_prewarm(&config);

    let (resolved_paths, rte_seg_prewarm_handle) = resolve_input_paths(&config)?;
    let total_start = Instant::now();
    let prewarmed_outputs = join_output_prewarm(prewarm_handle)?;
    let mut preloaded_table_indices = join_table_index_prewarm(table_index_prewarm_handle)?;

    let (
        output_results,
        _load_airway_reference_time,
        _airway_reference_load_timing,
        _validate_db_time,
        _export_total,
    ) = if config.output_targets.len() > 1 {
        let (preloaded_airway_result, validate_result) = rayon::join(
            || {
                let (preloaded_rte_seg_result, preloaded_reference_jsons_result) = rayon::join(
                    || join_rte_seg_prewarm(rte_seg_prewarm_handle),
                    || join_airway_reference_json_prewarm(airway_reference_json_prewarm_handle),
                );
                let preloaded_rte_seg = preloaded_rte_seg_result?;
                let preloaded_waypoint_candidates =
                    preload_waypoint_candidates(&resolved_paths.db_path, &preloaded_rte_seg)?;
                Ok::<_, anyhow::Error>((
                    preloaded_rte_seg,
                    preloaded_reference_jsons_result?,
                    preloaded_waypoint_candidates,
                ))
            },
            || {
                validate_db_if_needed(
                    &resolved_paths.db_path,
                    resolved_paths.db_validated_during_prompt,
                )
            },
        );
        let (preloaded_rte_seg, mut preloaded_reference_jsons, preloaded_waypoint_candidates) =
            preloaded_airway_result?;
        let validate_db_time = validate_result?;
        let export_start = Instant::now();
        let per_target_inputs = config
            .output_targets
            .iter()
            .cloned()
            .map(|output| {
                let preloaded_reference_json = take_preloaded_airway_reference_json(
                    &mut preloaded_reference_jsons,
                    Some(output.path.as_path()),
                );
                let preloaded_existing_indices = take_preloaded_table_indices(
                    &mut preloaded_table_indices,
                    output.path.as_path(),
                );
                (output, preloaded_reference_json, preloaded_existing_indices)
            })
            .collect::<Vec<_>>();

        let parallel_results = per_target_inputs
            .into_par_iter()
            .map(
                |(output, preloaded_reference_json, preloaded_existing_indices)| {
                    let prewarmed = prewarmed_outputs.get(&output.path);
                    execute_output_target(ExecuteOutputTargetParams {
                        output,
                        prewarmed,
                        db_path: &resolved_paths.db_path,
                        rte_seg_path: &resolved_paths.rte_seg_path,
                        preloaded_rte_seg: &preloaded_rte_seg,
                        preloaded_waypoint_candidates: &preloaded_waypoint_candidates,
                        preloaded_reference_json,
                        preloaded_existing_indices,
                        reference_source: AirwayReferenceSource::PerTarget,
                    })
                },
            )
            .collect::<Vec<_>>();

        (
            parallel_results
                .into_iter()
                .collect::<Result<Vec<OutputExecutionResult>>>()?,
            Duration::default(),
            AirwayReferenceLoadTiming::default(),
            validate_db_time,
            export_start.elapsed(),
        )
    } else {
        let (shared_reference_result, validate_result) = rayon::join(
            || {
                let (preloaded_rte_seg_result, preloaded_reference_jsons_result) = rayon::join(
                    || join_rte_seg_prewarm(rte_seg_prewarm_handle),
                    || join_airway_reference_json_prewarm(airway_reference_json_prewarm_handle),
                );
                let preloaded_rte_seg = preloaded_rte_seg_result?;
                let mut preloaded_reference_jsons = preloaded_reference_jsons_result?;
                let (shared_reference_result, preloaded_waypoint_candidates_result) = rayon::join(
                    || {
                        prepare_and_load_shared_airway_reference(
                            &config,
                            &resolved_paths.rte_seg_path,
                            &preloaded_rte_seg,
                            take_preloaded_airway_reference_json(
                                &mut preloaded_reference_jsons,
                                config.reference_dir.as_deref(),
                            ),
                        )
                    },
                    || preload_waypoint_candidates(&resolved_paths.db_path, &preloaded_rte_seg),
                );
                Ok::<_, anyhow::Error>((
                    shared_reference_result?,
                    preloaded_rte_seg,
                    preloaded_waypoint_candidates_result?,
                ))
            },
            || {
                validate_db_if_needed(
                    &resolved_paths.db_path,
                    resolved_paths.db_validated_during_prompt,
                )
            },
        );
        let (shared_reference, preloaded_rte_seg, preloaded_waypoint_candidates) =
            shared_reference_result?;
        let preloaded_existing_indices = take_preloaded_table_indices(
            &mut preloaded_table_indices,
            config.output_targets[0].path.as_path(),
        );
        let validate_db_time = validate_result?;
        let export_start = Instant::now();

        (
            vec![execute_output_target(ExecuteOutputTargetParams {
                output: config.output_targets[0].clone(),
                prewarmed: prewarmed_outputs.get(&config.output_targets[0].path),
                db_path: &resolved_paths.db_path,
                rte_seg_path: &resolved_paths.rte_seg_path,
                preloaded_rte_seg: &preloaded_rte_seg,
                preloaded_waypoint_candidates: &preloaded_waypoint_candidates,
                preloaded_reference_json: None,
                preloaded_existing_indices,
                reference_source: AirwayReferenceSource::Shared {
                    base_json_dir: shared_reference.runtime_reference_dir.as_deref(),
                    airway_reference: &shared_reference.airway_reference,
                },
            })?],
            shared_reference.load_time,
            shared_reference.load_timing,
            validate_db_time,
            export_start.elapsed(),
        )
    };

    for result in &output_results {
        println!(
            "Updated {} ({})",
            result.output.label,
            result.output.path.display()
        );
    }

    println!("Total elapsed: {}", format_duration(total_start.elapsed()));

    Ok(())
}

fn start_output_prewarm(
    config: &config::AppConfig,
) -> thread::JoinHandle<Result<HashMap<PathBuf, OutputPrewarmResult>>> {
    let output_targets = config.output_targets.clone();
    let start_terminal_id = config.start_terminal_id;
    let can_detect_start_terminal = true;

    thread::spawn(move || {
        prewarm_output_targets(
            &output_targets,
            start_terminal_id,
            can_detect_start_terminal,
        )
    })
}

fn join_output_prewarm(
    handle: thread::JoinHandle<Result<HashMap<PathBuf, OutputPrewarmResult>>>,
) -> Result<HashMap<PathBuf, OutputPrewarmResult>> {
    match handle.join() {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn start_airway_reference_json_prewarm(
    config: &config::AppConfig,
) -> thread::JoinHandle<Result<HashMap<PathBuf, PreloadedAirwayReferenceJson>>> {
    let mut reference_dirs = Vec::new();
    let mut seen_dirs = HashSet::new();

    for output in &config.output_targets {
        if seen_dirs.insert(output.path.clone()) {
            reference_dirs.push(output.path.clone());
        }
    }

    thread::spawn(move || prewarm_airway_reference_jsons(&reference_dirs))
}

fn join_airway_reference_json_prewarm(
    handle: thread::JoinHandle<Result<HashMap<PathBuf, PreloadedAirwayReferenceJson>>>,
) -> Result<HashMap<PathBuf, PreloadedAirwayReferenceJson>> {
    match handle.join() {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn start_table_index_prewarm(
    config: &config::AppConfig,
) -> thread::JoinHandle<Result<HashMap<PathBuf, PreloadedTableIndicesByName>>> {
    let output_paths: Vec<PathBuf> = config
        .output_targets
        .iter()
        .map(|output| output.path.clone())
        .collect();

    thread::spawn(move || prewarm_table_indices(&output_paths))
}

fn join_table_index_prewarm(
    handle: thread::JoinHandle<Result<HashMap<PathBuf, PreloadedTableIndicesByName>>>,
) -> Result<HashMap<PathBuf, PreloadedTableIndicesByName>> {
    match handle.join() {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn start_rte_seg_prewarm(rte_seg_path: PathBuf) -> thread::JoinHandle<Result<PreloadedRteSegData>> {
    thread::spawn(move || airways::preload_rte_seg(rte_seg_path.as_path()))
}

fn join_rte_seg_prewarm(
    handle: thread::JoinHandle<Result<PreloadedRteSegData>>,
) -> Result<PreloadedRteSegData> {
    match handle.join() {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn prewarm_output_targets(
    output_targets: &[OutputLocation],
    start_terminal_id: Option<i64>,
    can_detect_start_terminal: bool,
) -> Result<HashMap<PathBuf, OutputPrewarmResult>> {
    let prewarm_one = |output: &OutputLocation| -> Result<(PathBuf, OutputPrewarmResult)> {
        prepare_output_directory(&output.path)?;

        let start_terminal_id = if can_detect_start_terminal {
            let start_terminal_id = match start_terminal_id {
                Some(value) => value,
                None => detect_start_terminal_id(&output.path)?,
            };
            Some(start_terminal_id)
        } else {
            start_terminal_id
        };

        Ok((
            output.path.clone(),
            OutputPrewarmResult { start_terminal_id },
        ))
    };

    let prewarmed = if output_targets.len() > 1 {
        output_targets
            .par_iter()
            .map(prewarm_one)
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>>>()?
    } else {
        output_targets
            .iter()
            .map(prewarm_one)
            .collect::<Result<Vec<_>>>()?
    };

    Ok(prewarmed.into_iter().collect())
}

fn prewarm_airway_reference_jsons(
    reference_dirs: &[PathBuf],
) -> Result<HashMap<PathBuf, PreloadedAirwayReferenceJson>> {
    let prewarm_one = |reference_dir: &PathBuf| -> Result<(PathBuf, PreloadedAirwayReferenceJson)> {
        let preloaded =
            preload_airway_reference_json(Some(reference_dir.as_path()))?.unwrap_or_default();
        Ok((reference_dir.clone(), preloaded))
    };

    let prewarmed = if reference_dirs.len() > 1 {
        reference_dirs
            .par_iter()
            .map(prewarm_one)
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>>>()?
    } else {
        reference_dirs
            .iter()
            .map(prewarm_one)
            .collect::<Result<Vec<_>>>()?
    };

    Ok(prewarmed.into_iter().collect())
}

fn prewarm_table_indices(
    output_paths: &[PathBuf],
) -> Result<HashMap<PathBuf, PreloadedTableIndicesByName>> {
    let prewarm_one = |output_path: &PathBuf| {
        let indices = preload_existing_table_indices(Some(output_path.as_path()), EXPORT_TABLES)?;
        Ok::<_, anyhow::Error>((output_path.clone(), indices))
    };

    let prewarmed = if output_paths.len() > 1 {
        output_paths
            .par_iter()
            .map(prewarm_one)
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>>>()?
    } else {
        output_paths
            .iter()
            .map(prewarm_one)
            .collect::<Result<Vec<_>>>()?
    };

    Ok(prewarmed.into_iter().collect())
}

fn take_preloaded_airway_reference_json(
    preloaded_reference_jsons: &mut HashMap<PathBuf, PreloadedAirwayReferenceJson>,
    reference_dir: Option<&Path>,
) -> Option<PreloadedAirwayReferenceJson> {
    reference_dir.and_then(|path| preloaded_reference_jsons.remove(path))
}

fn take_preloaded_table_indices(
    preloaded_table_indices: &mut HashMap<PathBuf, PreloadedTableIndicesByName>,
    output_path: &Path,
) -> Option<PreloadedTableIndicesByName> {
    preloaded_table_indices.remove(output_path)
}

fn resolve_input_paths(
    config: &config::AppConfig,
) -> Result<(
    ResolvedInputPaths,
    thread::JoinHandle<Result<PreloadedRteSegData>>,
)> {
    let rte_seg_path = match &config.rte_seg_path {
        Some(path) => path.clone(),
        None => prompt_rte_seg_path()?,
    };
    let rte_seg_prewarm_handle = start_rte_seg_prewarm(rte_seg_path.clone());

    let (db_path, db_validated_during_prompt) = if let Some(path) = &config.db_path {
        (path.clone(), false)
    } else {
        let path = prompt_db3_path()?;
        (path, true)
    };

    Ok((
        ResolvedInputPaths {
            rte_seg_path,
            db_path,
            db_validated_during_prompt,
        },
        rte_seg_prewarm_handle,
    ))
}

fn validate_db_if_needed(db_path: &Path, already_validated: bool) -> Result<Duration> {
    if already_validated {
        return Ok(Duration::default());
    }

    let validate_db_start = Instant::now();
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    configure_read_connection(&conn);
    validate_required_tables(&conn)?;
    drop(conn);
    Ok(validate_db_start.elapsed())
}

fn prepare_and_load_shared_airway_reference(
    config: &config::AppConfig,
    rte_seg_path: &Path,
    preloaded_rte_seg: &PreloadedRteSegData,
    preloaded_reference_json: Option<PreloadedAirwayReferenceJson>,
) -> Result<SharedAirwayReferenceContext> {
    let runtime_reference_dir = config.reference_dir.clone();

    let load_airway_reference_start = Instant::now();
    let (airway_reference, load_timing) = load_airway_reference(
        runtime_reference_dir.as_deref(),
        rte_seg_path,
        Some(preloaded_rte_seg),
        preloaded_reference_json,
    )?;
    let load_time = load_airway_reference_start.elapsed();

    Ok(SharedAirwayReferenceContext {
        runtime_reference_dir,
        airway_reference,
        load_timing,
        load_time,
    })
}

fn execute_output_target(params: ExecuteOutputTargetParams<'_>) -> Result<OutputExecutionResult> {
    let ExecuteOutputTargetParams {
        output,
        prewarmed,
        db_path,
        rte_seg_path,
        preloaded_rte_seg,
        preloaded_waypoint_candidates,
        preloaded_reference_json,
        preloaded_existing_indices,
        reference_source,
    } = params;

    let start_terminal_id = if let Some(prewarmed) = prewarmed {
        match prewarmed.start_terminal_id {
            Some(start_terminal_id) => start_terminal_id,
            None => detect_start_terminal_id(&output.path)?,
        }
    } else {
        detect_start_terminal_id(&output.path)?
    };

    let (base_json_dir, airway_reference) = match reference_source {
        AirwayReferenceSource::Shared {
            base_json_dir,
            airway_reference,
        } => (base_json_dir, airway_reference),
        AirwayReferenceSource::PerTarget => {
            let (airway_reference, _) = load_airway_reference(
                Some(&output.path),
                rte_seg_path,
                Some(preloaded_rte_seg),
                preloaded_reference_json,
            )?;
            let _ = export_db3_to_json(ExportDb3ToJsonParams {
                db_path,
                start_terminal_id,
                output_dir: &output.path,
                base_json_dir: Some(&output.path),
                airway_reference: &airway_reference,
                rte_seg_path,
                preloaded_rte_seg,
                preloaded_waypoint_candidates,
                preloaded_existing_indices,
            })?;
            return Ok(OutputExecutionResult { output });
        }
    };

    let _ = export_db3_to_json(ExportDb3ToJsonParams {
        db_path,
        start_terminal_id,
        output_dir: &output.path,
        base_json_dir,
        airway_reference,
        rte_seg_path,
        preloaded_rte_seg,
        preloaded_waypoint_candidates,
        preloaded_existing_indices,
    })?;

    Ok(OutputExecutionResult { output })
}

fn export_db3_to_json(params: ExportDb3ToJsonParams<'_>) -> Result<ExportStats> {
    let export_start = Instant::now();
    let waypoint_index_start = Instant::now();
    let (context, preloaded_existing_indices) = params.split();
    let indices = build_reference_id_indices(context.db_path, context.base_json_dir)?;
    let waypoint_read_time = waypoint_index_start.elapsed();
    let waypoint_count = indices.waypoint_count();
    let table_jobs = build_table_jobs(preloaded_existing_indices);
    let export_results = export_related_outputs(&context, &indices, table_jobs)?;

    Ok(ExportStats {
        waypoint_count,
        waypoint_index_wall: waypoint_read_time,
        table_stats: export_results.table_stats,
        table_export_wall: export_results.table_export_wall,
        airway_detail: export_results.airway_detail,
        terminal_leg_stats: export_results.terminal_leg_stats,
        terminal_leg_wall: export_results.terminal_leg_wall,
        total_elapsed: export_start.elapsed(),
    })
}

fn build_reference_id_indices(
    db_path: &Path,
    base_json_dir: Option<&Path>,
) -> Result<ReferenceIdIndices> {
    let airport = build_airport_id_index(db_path, base_json_dir)?;
    let runway = build_runway_id_index(db_path, base_json_dir, &airport)?;
    let ils = build_ils_id_index(db_path, base_json_dir, &runway)?;
    let waypoint = build_waypoint_id_index(db_path, base_json_dir)?;
    let navaid = build_navaid_id_index(db_path, base_json_dir)?;

    Ok(ReferenceIdIndices {
        airport,
        runway,
        ils,
        waypoint,
        navaid,
    })
}

fn build_table_jobs(
    mut preloaded_existing_indices: PreloadedTableIndicesByName,
) -> Vec<(&'static str, Option<ExistingJsonIndex>)> {
    EXPORT_TABLES
        .iter()
        .map(|table_name| (*table_name, preloaded_existing_indices.remove(*table_name)))
        .collect()
}

fn export_related_outputs(
    context: &ExportRuntimeContext<'_>,
    indices: &ReferenceIdIndices,
    table_jobs: Vec<(&'static str, Option<ExistingJsonIndex>)>,
) -> Result<RelatedExportResults> {
    let ((table_export_result, airway_export_result), terminal_legs_result) = rayon::join(
        || {
            rayon::join(
                || export_table_batch(context, indices, table_jobs),
                || {
                    export_airway_tables(
                        context.db_path,
                        context.output_dir,
                        context.airway_reference,
                        context.rte_seg_path,
                        Some(context.preloaded_rte_seg),
                        Some(context.preloaded_waypoint_candidates),
                        Some(&indices.waypoint),
                    )
                },
            )
        },
        || {
            let terminal_leg_start = Instant::now();
            export_terminal_legs(
                context.db_path,
                context.start_terminal_id,
                context.output_dir,
                &indices.waypoint,
                &indices.navaid,
            )
            .map(|stats| (stats, terminal_leg_start.elapsed()))
        },
    );

    let (mut table_stats, table_export_wall) = table_export_result?;
    let (airway_stats, airway_detail) = airway_export_result?;
    table_stats.extend(airway_stats);
    let (terminal_leg_stats, terminal_leg_wall) = terminal_legs_result?;
    table_stats.sort_by(|left, right| left.table_name.cmp(&right.table_name));

    Ok(RelatedExportResults {
        table_stats,
        table_export_wall,
        airway_detail,
        terminal_leg_stats,
        terminal_leg_wall,
    })
}

fn export_table_batch(
    context: &ExportRuntimeContext<'_>,
    indices: &ReferenceIdIndices,
    table_jobs: Vec<(&'static str, Option<ExistingJsonIndex>)>,
) -> Result<(Vec<TableExportStats>, Duration)> {
    let table_export_start = Instant::now();
    table_jobs
        .into_par_iter()
        .map(|(table_name, preloaded_existing_index)| {
            export_table_job(context, indices, table_name, preloaded_existing_index)
        })
        .collect::<Result<Vec<_>>>()
        .map(|stats| (stats, table_export_start.elapsed()))
}

fn export_table_job(
    context: &ExportRuntimeContext<'_>,
    indices: &ReferenceIdIndices,
    table_name: &'static str,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    match table_name {
        "Airports" => export_airports_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.airport,
            preloaded_existing_index,
        ),
        "AirportLookup" => export_airport_lookup_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.airport,
            preloaded_existing_index,
        ),
        "Runways" => export_runways_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.runway,
            &indices.airport,
            preloaded_existing_index,
        ),
        "Ilses" => export_ilses_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.ils,
            &indices.runway,
            preloaded_existing_index,
        ),
        "Waypoints" => export_waypoints_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.waypoint,
            &indices.navaid,
            preloaded_existing_index,
        ),
        "WaypointLookup" => export_waypoint_lookup_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.waypoint,
            preloaded_existing_index,
        ),
        "Navaids" => export_navaids_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.navaid,
            preloaded_existing_index,
        ),
        "NavaidLookup" => export_navaid_lookup_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.navaid,
            preloaded_existing_index,
        ),
        "Terminals" => export_terminals_table(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            &indices.airport,
            &indices.runway,
            preloaded_existing_index,
        ),
        _ => export_table_to_json(
            context.db_path,
            context.output_dir,
            context.base_json_dir,
            table_name,
            None,
            preloaded_existing_index,
        ),
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() >= 60 {
        return format!("{:.2}m", duration.as_secs_f64() / 60.0);
    }
    if duration.as_secs() >= 1 {
        return format!("{:.3}s", duration.as_secs_f64());
    }
    format!("{}ms", duration.as_millis())
}
