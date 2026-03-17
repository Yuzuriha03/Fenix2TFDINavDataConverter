#![allow(dead_code)]

use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub(crate) struct PhaseDurations {
    pub(crate) db_read: Duration,
    pub(crate) json_transform: Duration,
    pub(crate) json_write: Duration,
}

impl PhaseDurations {
    pub(crate) fn total(&self) -> Duration {
        self.db_read + self.json_transform + self.json_write
    }

    pub(crate) fn add_assign(&mut self, other: &Self) {
        self.db_read += other.db_read;
        self.json_transform += other.json_transform;
        self.json_write += other.json_write;
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TableExportStats {
    pub(crate) table_name: String,
    pub(crate) row_count: usize,
    pub(crate) phase: PhaseDurations,
    pub(crate) detail: Option<TableTimingBreakdown>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TableTimingBreakdown {
    pub(crate) source_rows: usize,
    pub(crate) formatted_rows: usize,
    pub(crate) existing_load: Duration,
    pub(crate) format_rows: Duration,
    pub(crate) merge_rows: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct TerminalLegExportStats {
    pub(crate) row_count: usize,
    pub(crate) file_count: usize,
    pub(crate) phase: PhaseDurations,
    pub(crate) detail: TerminalLegTimingBreakdown,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TerminalLegTimingBreakdown {
    pub(crate) db_terminal_legs: Duration,
    pub(crate) db_terminal_metadata: Duration,
    pub(crate) db_runway_coords: Duration,
    pub(crate) db_waypoint_coords: Duration,
    pub(crate) group_rows: Duration,
    pub(crate) cleanup_files: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct ExportStats {
    pub(crate) waypoint_count: usize,
    pub(crate) waypoint_index_wall: Duration,
    pub(crate) table_stats: Vec<TableExportStats>,
    pub(crate) table_export_wall: Duration,
    pub(crate) airway_detail: AirwayTimingBreakdown,
    pub(crate) terminal_leg_stats: TerminalLegExportStats,
    pub(crate) terminal_leg_wall: Duration,
    pub(crate) total_elapsed: Duration,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AirwayTimingBreakdown {
    pub(crate) waypoint_candidates_load: Duration,
    pub(crate) build_from_rte_seg: Duration,
    pub(crate) merge_outputs: Duration,
    pub(crate) write_airways: Duration,
    pub(crate) write_airway_legs: Duration,
}
