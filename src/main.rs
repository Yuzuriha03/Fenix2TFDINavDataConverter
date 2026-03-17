mod airways;
mod config;
mod db_json;
mod stats;
mod tables;
mod terminal_legs;

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
    OutputLocation, debug_sample_dir, detect_start_terminal_id, parse_args,
    prepare_debug_output_from_reference, prepare_output_directory, prompt_db3_path,
    prompt_rte_seg_path, validate_required_tables,
};
use db_json::configure_read_connection;
use stats::{ExportStats, PhaseDurations};
use tables::{ExistingJsonIndex, export_table_to_json, preload_existing_table_indices};
use terminal_legs::export_terminal_legs;

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
    non_debug_total_start: Option<Instant>,
}

#[derive(Clone, Debug, Default)]
struct OutputPrewarmResult {
    start_terminal_id: Option<i64>,
    detect_start_terminal_time: Duration,
    prepare_output_time: Duration,
}

#[derive(Debug)]
struct OutputExecutionResult {
    output: OutputLocation,
    detect_start_terminal_time: Duration,
    prepare_output_time: Duration,
    export_stats: ExportStats,
}

type PreloadedTableIndicesByName = HashMap<String, ExistingJsonIndex>;

struct SharedAirwayReferenceContext {
    runtime_reference_dir: Option<PathBuf>,
    airway_reference: AirwayReferenceData,
    load_timing: AirwayReferenceLoadTiming,
    load_time: Duration,
    seed_copy_time: Duration,
}

enum AirwayReferenceSource<'a> {
    Shared {
        base_json_dir: Option<&'a Path>,
        airway_reference: &'a AirwayReferenceData,
    },
    PerTarget,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = parse_args()?;
    let overall_start = Instant::now();
    let prewarm_handle = start_output_prewarm(&config);
    let airway_reference_json_prewarm_handle = start_airway_reference_json_prewarm(&config);
    let table_index_prewarm_handle = start_table_index_prewarm(&config);

    let resolve_paths_start = Instant::now();
    let (resolved_paths, rte_seg_prewarm_handle) = resolve_input_paths(&config)?;
    let resolve_paths_time = resolve_paths_start.elapsed();
    let total_start = if config.debug {
        overall_start
    } else {
        resolved_paths
            .non_debug_total_start
            .unwrap_or(overall_start)
    };
    let prewarmed_outputs = join_output_prewarm(prewarm_handle)?;
    let mut preloaded_table_indices = join_table_index_prewarm(table_index_prewarm_handle)?;

    let (
        output_results,
        debug_seed_copy_time,
        load_airway_reference_time,
        airway_reference_load_timing,
        validate_db_time,
        export_total,
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
                    execute_output_target(
                        output.clone(),
                        prewarmed_outputs.get(&output.path),
                        &resolved_paths.db_path,
                        &resolved_paths.rte_seg_path,
                        &preloaded_rte_seg,
                        &preloaded_waypoint_candidates,
                        preloaded_reference_json,
                        preloaded_existing_indices,
                        AirwayReferenceSource::PerTarget,
                    )
                },
            )
            .collect::<Vec<_>>();

        (
            parallel_results
                .into_iter()
                .collect::<Result<Vec<OutputExecutionResult>>>()?,
            Duration::default(),
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
            vec![execute_output_target(
                config.output_targets[0].clone(),
                prewarmed_outputs.get(&config.output_targets[0].path),
                &resolved_paths.db_path,
                &resolved_paths.rte_seg_path,
                &preloaded_rte_seg,
                &preloaded_waypoint_candidates,
                None,
                preloaded_existing_indices,
                AirwayReferenceSource::Shared {
                    base_json_dir: shared_reference.runtime_reference_dir.as_deref(),
                    airway_reference: &shared_reference.airway_reference,
                },
            )?],
            shared_reference.seed_copy_time,
            shared_reference.load_time,
            shared_reference.load_timing,
            validate_db_time,
            export_start.elapsed(),
        )
    };

    let detect_start_terminal_total: Duration = output_results
        .iter()
        .map(|result| result.detect_start_terminal_time)
        .sum();
    let prepare_output_total: Duration = output_results
        .iter()
        .map(|result| result.prepare_output_time)
        .sum();

    for result in &output_results {
        println!("--- Export for {} ---", result.output.path.display());
        if config.debug {
            println!(
                "Pre-export: detect TerminalID {} | prepare output {}",
                format_duration(result.detect_start_terminal_time),
                format_duration(result.prepare_output_time),
            );
            print_export_stats(&result.export_stats);
            println!("DEBUG output: {}", result.output.path.display());
            println!("DEBUG sample baseline: {}", debug_sample_dir().display());
            if let Some(reference_dir) = &config.reference_dir {
                println!(
                    "Reference base: {} ({})",
                    config.output_label,
                    reference_dir.display()
                );
            }
            println!("RTE_SEG.csv: {}", resolved_paths.rte_seg_path.display());
        } else {
            println!(
                "Updated {} ({})",
                result.output.label,
                result.output.path.display()
            );
        }
    }

    if config.debug {
        let pre_export_subtotal =
            resolve_paths_time + load_airway_reference_time + validate_db_time;
        let total_elapsed = total_start.elapsed().saturating_sub(debug_seed_copy_time);
        println!("\nRun timing:");
        println!(
            "  Pre-export pipeline: seed output {} | resolve paths {} | load airway reference {} | validate DB {} | subtotal {}",
            format_duration(debug_seed_copy_time),
            format_duration(resolve_paths_time),
            format_duration(load_airway_reference_time),
            format_duration(validate_db_time),
            format_duration(pre_export_subtotal),
        );
        println!(
            "  Airway reference detail: mirror {} | Airways.json {} | AirwayLegs.json {}",
            format_duration(airway_reference_load_timing.mirror_reference),
            format_duration(airway_reference_load_timing.airways_json),
            format_duration(airway_reference_load_timing.airway_legs_json),
        );
        println!(
            "  Per-target pre-export sum: detect TerminalID {} | prepare output {}",
            format_duration(detect_start_terminal_total),
            format_duration(prepare_output_total),
        );
        println!(
            "  Export wall (all targets): {}",
            format_duration(export_total)
        );
        println!("  Total elapsed: {}", format_duration(total_elapsed));
    } else {
        println!("Total elapsed: {}", format_duration(total_start.elapsed()));
    }

    Ok(())
}

fn start_output_prewarm(
    config: &config::AppConfig,
) -> thread::JoinHandle<Result<HashMap<PathBuf, OutputPrewarmResult>>> {
    let output_targets = config.output_targets.clone();
    let start_terminal_id = config.start_terminal_id;
    let can_detect_start_terminal = !config.debug;

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

    if config.debug {
        if let Some(reference_dir) = config.reference_dir.clone() {
            if seen_dirs.insert(reference_dir.clone()) {
                reference_dirs.push(reference_dir);
            }
        }
    } else {
        for output in &config.output_targets {
            if seen_dirs.insert(output.path.clone()) {
                reference_dirs.push(output.path.clone());
            }
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
    if config.debug {
        return thread::spawn(|| Ok(HashMap::new()));
    }

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
        let prepare_output_start = Instant::now();
        prepare_output_directory(&output.path)?;
        let prepare_output_time = prepare_output_start.elapsed();

        let (start_terminal_id, detect_start_terminal_time) = if can_detect_start_terminal {
            let detect_start_terminal_start = Instant::now();
            let start_terminal_id = match start_terminal_id {
                Some(value) => value,
                None => detect_start_terminal_id(&output.path)?,
            };
            (
                Some(start_terminal_id),
                detect_start_terminal_start.elapsed(),
            )
        } else {
            (start_terminal_id, Duration::default())
        };

        Ok((
            output.path.clone(),
            OutputPrewarmResult {
                start_terminal_id,
                detect_start_terminal_time,
                prepare_output_time,
            },
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

    let (db_path, db_validated_during_prompt, non_debug_total_start) = match &config.db_path {
        Some(path) => (path.clone(), false, None),
        None => {
            let (path, prompt_result) = prompt_db3_path()?;
            (path, true, Some(prompt_result.total_elapsed_start))
        }
    };

    Ok((
        ResolvedInputPaths {
            rte_seg_path,
            db_path,
            db_validated_during_prompt,
            non_debug_total_start,
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
    let mut seed_copy_time = Duration::default();
    let runtime_reference_dir = if config.debug {
        if let (Some(reference_dir), Some(output_target)) = (
            config.reference_dir.as_deref(),
            config.output_targets.first(),
        ) {
            let seed_start = Instant::now();
            prepare_debug_output_from_reference(reference_dir, &output_target.path)?;
            seed_copy_time = seed_start.elapsed();
            Some(output_target.path.clone())
        } else {
            config.reference_dir.clone()
        }
    } else {
        config.reference_dir.clone()
    };

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
        seed_copy_time,
    })
}

fn execute_output_target(
    output: OutputLocation,
    prewarmed: Option<&OutputPrewarmResult>,
    db_path: &Path,
    rte_seg_path: &Path,
    preloaded_rte_seg: &PreloadedRteSegData,
    preloaded_waypoint_candidates: &PreloadedWaypointCandidates,
    preloaded_reference_json: Option<PreloadedAirwayReferenceJson>,
    preloaded_existing_indices: Option<PreloadedTableIndicesByName>,
    reference_source: AirwayReferenceSource<'_>,
) -> Result<OutputExecutionResult> {
    let (start_terminal_id, detect_start_terminal_time) = if let Some(prewarmed) = prewarmed {
        match prewarmed.start_terminal_id {
            Some(start_terminal_id) => (start_terminal_id, prewarmed.detect_start_terminal_time),
            None => {
                let detect_start_terminal_start = Instant::now();
                let start_terminal_id = detect_start_terminal_id(&output.path)?;
                (start_terminal_id, detect_start_terminal_start.elapsed())
            }
        }
    } else {
        let detect_start_terminal_start = Instant::now();
        let start_terminal_id = detect_start_terminal_id(&output.path)?;
        (start_terminal_id, detect_start_terminal_start.elapsed())
    };

    let prepare_output_time = prewarmed
        .map(|result| result.prepare_output_time)
        .unwrap_or_default();

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
            let export_stats = export_db3_to_json(
                db_path,
                start_terminal_id,
                &output.path,
                Some(&output.path),
                &airway_reference,
                rte_seg_path,
                preloaded_rte_seg,
                preloaded_waypoint_candidates,
                preloaded_existing_indices,
            )?;
            return Ok(OutputExecutionResult {
                output,
                detect_start_terminal_time,
                prepare_output_time,
                export_stats,
            });
        }
    };

    let export_stats = export_db3_to_json(
        db_path,
        start_terminal_id,
        &output.path,
        base_json_dir,
        airway_reference,
        rte_seg_path,
        preloaded_rte_seg,
        preloaded_waypoint_candidates,
        preloaded_existing_indices,
    )?;

    Ok(OutputExecutionResult {
        output,
        detect_start_terminal_time,
        prepare_output_time,
        export_stats,
    })
}

fn export_db3_to_json(
    db_path: &Path,
    start_terminal_id: i64,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    airway_reference: &airways::AirwayReferenceData,
    rte_seg_path: &Path,
    preloaded_rte_seg: &PreloadedRteSegData,
    preloaded_waypoint_candidates: &PreloadedWaypointCandidates,
    preloaded_existing_indices: Option<PreloadedTableIndicesByName>,
) -> Result<ExportStats> {
    let export_start = Instant::now();
    let waypoint_read_time = Duration::default();
    let waypoint_count = 0usize;
    let mut preloaded_existing_indices = preloaded_existing_indices.unwrap_or_default();
    let table_jobs = EXPORT_TABLES
        .iter()
        .map(|table_name| (*table_name, preloaded_existing_indices.remove(*table_name)))
        .collect::<Vec<_>>();

    let ((table_export_result, airway_export_result), terminal_legs_result) = rayon::join(
        || {
            rayon::join(
                || {
                    let table_export_start = Instant::now();
                    table_jobs
                        .into_par_iter()
                        .map(|(table_name, preloaded_existing_index)| {
                            export_table_to_json(
                                db_path,
                                output_dir,
                                base_json_dir,
                                table_name,
                                None,
                                preloaded_existing_index,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                        .map(|stats| (stats, table_export_start.elapsed()))
                },
                || {
                    export_airway_tables(
                        db_path,
                        output_dir,
                        airway_reference,
                        rte_seg_path,
                        Some(preloaded_rte_seg),
                        Some(preloaded_waypoint_candidates),
                    )
                },
            )
        },
        || {
            let terminal_leg_start = Instant::now();
            export_terminal_legs(db_path, start_terminal_id, output_dir)
                .map(|stats| (stats, terminal_leg_start.elapsed()))
        },
    );

    let (mut table_stats, table_export_wall) = table_export_result?;
    let (airway_stats, airway_detail) = airway_export_result?;
    table_stats.extend(airway_stats);
    let (terminal_leg_stats, terminal_leg_wall) = terminal_legs_result?;
    table_stats.sort_by(|left, right| left.table_name.cmp(&right.table_name));

    Ok(ExportStats {
        waypoint_count,
        waypoint_index_wall: waypoint_read_time,
        table_stats,
        table_export_wall,
        airway_detail,
        terminal_leg_stats,
        terminal_leg_wall,
        total_elapsed: export_start.elapsed(),
    })
}

fn print_export_stats(stats: &ExportStats) {
    let mut aggregate = PhaseDurations::default();
    for table in &stats.table_stats {
        aggregate.add_assign(&table.phase);
    }
    aggregate.add_assign(&stats.terminal_leg_stats.phase);

    println!("\nExport timing:");
    println!(
        "  Total: {} | Waypoint index: {} rows",
        format_duration(stats.total_elapsed),
        stats.waypoint_count
    );
    println!(
        "  Wall clock: Waypoints {} | Base tables {} | ProcedureLegs {}",
        format_duration(stats.waypoint_index_wall),
        format_duration(stats.table_export_wall),
        format_duration(stats.terminal_leg_wall)
    );
    println!(
        "  Task sum: DB {} | JSON build {} | JSON write {} | Total {}",
        format_duration(aggregate.db_read),
        format_duration(aggregate.json_transform),
        format_duration(aggregate.json_write),
        format_duration(aggregate.total())
    );
    println!("  Tables:");
    for table in &stats.table_stats {
        println!(
            "    {:<15} rows {:>7} | DB {} | JSON build {} | JSON write {} | Total {}",
            table.table_name,
            table.row_count,
            format_duration(table.phase.db_read),
            format_duration(table.phase.json_transform),
            format_duration(table.phase.json_write),
            format_duration(table.phase.total())
        );
    }
    println!(
        "  ProcedureLegs: rows {} | files {} | DB {} | JSON build {} | JSON write {} | Total {}",
        stats.terminal_leg_stats.row_count,
        stats.terminal_leg_stats.file_count,
        format_duration(stats.terminal_leg_stats.phase.db_read),
        format_duration(stats.terminal_leg_stats.phase.json_transform),
        format_duration(stats.terminal_leg_stats.phase.json_write),
        format_duration(stats.terminal_leg_stats.phase.total())
    );

    println!(
        "  ProcedureLegs detail: legs-query {} | terminal-meta {} | runway-coords {} | waypoint-coords {} | group {} | cleanup {}",
        format_duration(stats.terminal_leg_stats.detail.db_terminal_legs),
        format_duration(stats.terminal_leg_stats.detail.db_terminal_metadata),
        format_duration(stats.terminal_leg_stats.detail.db_runway_coords),
        format_duration(stats.terminal_leg_stats.detail.db_waypoint_coords),
        format_duration(stats.terminal_leg_stats.detail.group_rows),
        format_duration(stats.terminal_leg_stats.detail.cleanup_files),
    );

    println!(
        "  Airways detail: waypoint-candidates {} | rte-seg-build {} | merge {} | write-airways {} | write-airwaylegs {}",
        format_duration(stats.airway_detail.waypoint_candidates_load),
        format_duration(stats.airway_detail.build_from_rte_seg),
        format_duration(stats.airway_detail.merge_outputs),
        format_duration(stats.airway_detail.write_airways),
        format_duration(stats.airway_detail.write_airway_legs),
    );

    println!("  Table detail:");
    for table in &stats.table_stats {
        if let Some(detail) = &table.detail {
            println!(
                "    {:<15} source {:>7} -> formatted {:>7} | existing-load {} | format {} | merge {}",
                table.table_name,
                detail.source_rows,
                detail.formatted_rows,
                format_duration(detail.existing_load),
                format_duration(detail.format_rows),
                format_duration(detail.merge_rows),
            );
        }
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
