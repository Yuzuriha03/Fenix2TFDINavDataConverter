mod airways;
mod config;
mod db_json;
mod stats;
mod tables;
mod terminal_legs;

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rayon::prelude::*;
use rusqlite::Connection;

use airways::{export_airway_tables, load_airway_reference};
use config::{
    detect_start_terminal_id, parse_args, prepare_output_directory, prompt_db3_path,
    prompt_rte_seg_path, validate_required_tables,
};
use db_json::configure_read_connection;
use stats::{ExportStats, PhaseDurations};
use tables::{export_table_to_json, fetch_waypoints};
use terminal_legs::export_terminal_legs;

const EXPORT_TABLES: &[&str] = &[
    "AirportLookup",
    "Airports",
    "Ilses",
    "NavaidLookup",
    "Navaids",
    "Terminals",
    "WaypointLookup",
];

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = parse_args()?;
    let total_start = Instant::now();
    let rte_seg_path = match &config.rte_seg_path {
        Some(path) => path.clone(),
        None => prompt_rte_seg_path()?,
    };

    let db_path = match &config.db_path {
        Some(path) => path.clone(),
        None => prompt_db3_path()?,
    };

    let airway_reference = load_airway_reference(config.reference_dir.as_deref(), &rte_seg_path)?;

    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    configure_read_connection(&conn);
    validate_required_tables(&conn)?;
    drop(conn);

    for output in &config.output_targets {
        let start_terminal_id = match config.start_terminal_id {
            Some(value) => value,
            None => detect_start_terminal_id(&output.path)?,
        };
        prepare_output_directory(&output.path)?;

        let export_stats = export_db3_to_json(
            &db_path,
            start_terminal_id,
            &output.path,
            config.reference_dir.as_deref(),
            &airway_reference,
            &rte_seg_path,
        )?;

        if config.debug {
            println!("--- Export for {} ---", output.path.display());
            print_export_stats(&export_stats);
            println!("DEBUG output: {}", output.path.display());
            if let Some(reference_dir) = &config.reference_dir {
                println!(
                    "Reference base: {} ({})",
                    config.output_label,
                    reference_dir.display()
                );
            }
            println!("RTE_SEG.csv: {}", rte_seg_path.display());
        } else {
            println!("Updated {} ({})", output.label, output.path.display());
        }
    }

    if !config.debug {
        println!("Total elapsed: {}", format_duration(total_start.elapsed()));
    }

    Ok(())
}

fn export_db3_to_json(
    db_path: &Path,
    start_terminal_id: i64,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    airway_reference: &airways::AirwayReferenceData,
    rte_seg_path: &Path,
) -> Result<ExportStats> {
    let export_start = Instant::now();
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;

    // Export waypoints first so table exports and lookup maps share a single source.
    let waypoint_read_start = Instant::now();
    let waypoints = fetch_waypoints(&conn)?;
    let waypoint_read_time = waypoint_read_start.elapsed();
    let waypoint_table_stats =
        export_table_to_json(db_path, output_dir, base_json_dir, "Waypoints", &waypoints)?;
    let runway_table_stats =
        export_table_to_json(db_path, output_dir, base_json_dir, "Runways", &waypoints)?;

    drop(conn);

    let ((table_export_result, airway_export_result), terminal_legs_result) = rayon::join(
        || {
            rayon::join(
                || {
                    let table_export_start = Instant::now();
                    EXPORT_TABLES
                        .par_iter()
                        .map(|table_name| {
                            export_table_to_json(
                                db_path,
                                output_dir,
                                base_json_dir,
                                table_name,
                                &waypoints,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                        .map(|stats| (stats, table_export_start.elapsed()))
                },
                || export_airway_tables(db_path, output_dir, airway_reference, rte_seg_path),
            )
        },
        || {
            let terminal_leg_start = Instant::now();
            export_terminal_legs(db_path, start_terminal_id, output_dir)
                .map(|stats| (stats, terminal_leg_start.elapsed()))
        },
    );

    let (mut table_stats, table_export_wall) = table_export_result?;
    table_stats.insert(0, waypoint_table_stats);
    table_stats.insert(1, runway_table_stats);
    table_stats.extend(airway_export_result?);
    let (terminal_leg_stats, terminal_leg_wall) = terminal_legs_result?;
    table_stats.sort_by(|left, right| left.table_name.cmp(&right.table_name));

    Ok(ExportStats {
        waypoint_count: waypoints.len(),
        waypoint_index_wall: waypoint_read_time,
        table_stats,
        table_export_wall,
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
