use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use rayon::prelude::*;
use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::db_json::{
    configure_read_connection, fetch_table_rows, fetch_table_rows_after_id, format_row,
    json_to_i64, write_json_objects,
};
use crate::stats::{PhaseDurations, TableExportStats, TableTimingBreakdown};

#[derive(Clone, Debug)]
pub(crate) struct ExistingJsonIndex {
    pub(crate) path: PathBuf,
    pub(crate) ids: HashSet<i64>,
    pub(crate) row_count: usize,
}

pub(crate) fn export_table_to_json(
    db_path: &Path,
    output_dir: &Path,
    base_json_dir: Option<&Path>,
    table_name: &str,
    waypoints: Option<&HashMap<i64, String>>,
    preloaded_existing_index: Option<ExistingJsonIndex>,
) -> Result<TableExportStats> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;
    configure_read_connection(&conn);
    let existing_load_start = Instant::now();
    let existing_merge = match preloaded_existing_index {
        Some(existing) => Some(existing),
        None => load_existing_id_index(base_json_dir, table_name)?,
    };
    let existing_load_time = existing_load_start.elapsed();

    let db_read_start = Instant::now();
    let rows = if let Some(existing) = &existing_merge {
        if let Some(max_id) = existing.ids.iter().max().copied() {
            fetch_table_rows_after_id(&conn, table_name, max_id)?
        } else {
            fetch_table_rows(&conn, table_name)?
        }
    } else {
        fetch_table_rows(&conn, table_name)?
    };
    let db_read_time = db_read_start.elapsed();
    let source_rows = rows.len();

    let format_start = Instant::now();
    let formatted_rows: Vec<Map<String, Value>> = if let Some(existing) = &existing_merge {
        rows.into_par_iter()
            .filter_map(|row| {
                let keep_row = json_to_i64(row.get("ID"))
                    .is_none_or(|id| !existing.ids.contains(&id));
                keep_row.then(|| format_row(row, table_name, waypoints))
            })
            .collect()
    } else {
        rows.into_par_iter()
            .map(|row| format_row(row, table_name, waypoints))
            .collect()
    };
    let format_time = format_start.elapsed();
    let formatted_rows_count = formatted_rows.len();

    let merge_start = Instant::now();
    let merge_time = merge_start.elapsed();
    let json_transform_time = existing_load_time + format_time + merge_time;

    let output_path = output_dir.join(format!("{table_name}.json"));
    let json_write_start = Instant::now();
    let final_row_count = if let Some(existing) = &existing_merge {
        write_json_append_from_base(&existing.path, &output_path, &formatted_rows)?;
        existing.row_count + formatted_rows_count
    } else {
        write_json_objects(&output_path, &formatted_rows)?;
        formatted_rows_count
    };

    Ok(TableExportStats {
        table_name: table_name.to_string(),
        row_count: final_row_count,
        phase: PhaseDurations {
            db_read: db_read_time,
            json_transform: json_transform_time,
            json_write: json_write_start.elapsed(),
        },
        detail: Some(TableTimingBreakdown {
            source_rows,
            formatted_rows: formatted_rows_count,
            existing_load: existing_load_time,
            format_rows: format_time,
            merge_rows: merge_time,
        }),
    })
}

pub(crate) fn preload_existing_table_indices(
    base_json_dir: Option<&Path>,
    table_names: &[&str],
) -> Result<HashMap<String, ExistingJsonIndex>> {
    let mut indices = HashMap::new();
    for &table_name in table_names {
        if let Some(index) = load_existing_id_index(base_json_dir, table_name)? {
            indices.insert(table_name.to_string(), index);
        }
    }
    Ok(indices)
}

fn load_existing_id_index(
    base_json_dir: Option<&Path>,
    table_name: &str,
) -> Result<Option<ExistingJsonIndex>> {
    let Some(base_json_dir) = base_json_dir else {
        return Ok(None);
    };

    let base_json_path = base_json_dir.join(format!("{table_name}.json"));
    if !base_json_path.exists() {
        return Ok(None);
    }

    let bytes = fs::read(&base_json_path).with_context(|| {
        format!(
            "failed to read reference json: {}",
            base_json_path.display()
        )
    })?;
    let (existing_ids, row_count) = scan_json_id_index(&bytes);

    Ok(Some(ExistingJsonIndex {
        path: base_json_path,
        ids: existing_ids,
        row_count,
    }))
}

fn scan_json_id_index(bytes: &[u8]) -> (HashSet<i64>, usize) {
    const ID_KEY: &[u8] = b"\"ID\":";
    let mut ids = HashSet::new();
    let mut row_count = 0usize;

    for byte in bytes {
        if *byte == b'{' {
            row_count += 1;
        }
    }

    let mut idx = 0usize;
    while idx + ID_KEY.len() <= bytes.len() {
        if &bytes[idx..idx + ID_KEY.len()] != ID_KEY {
            idx += 1;
            continue;
        }

        let mut pos = idx + ID_KEY.len();
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        let negative = if pos < bytes.len() && bytes[pos] == b'-' {
            pos += 1;
            true
        } else {
            false
        };

        let start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        if start == pos {
            idx += ID_KEY.len();
            continue;
        }

        if let Ok(text) = std::str::from_utf8(&bytes[start..pos])
            && let Ok(mut value) = text.parse::<i64>()
        {
            if negative {
                value = -value;
            }
            ids.insert(value);
        }

        idx = pos;
    }

    (ids, row_count)
}

#[allow(clippy::too_many_lines)]
fn write_json_append_from_base(
    base_json_path: &Path,
    output_path: &Path,
    appended_rows: &[Map<String, Value>],
) -> Result<()> {
    if appended_rows.is_empty() {
        if base_json_path == output_path {
            return Ok(());
        }
        copy_json_bytes(base_json_path, output_path)?;
        return Ok(());
    }

    let base_bytes = fs::read(base_json_path).with_context(|| {
        format!(
            "failed to read reference json: {}",
            base_json_path.display()
        )
    })?;
    let Some(last_non_ws) = base_bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
    else {
        anyhow::bail!("reference json is empty: {}", base_json_path.display());
    };
    if base_bytes[last_non_ws] != b']' {
        anyhow::bail!(
            "reference json is not an array ending with ']': {}",
            base_json_path.display()
        );
    }

    let Some(first_non_ws) = base_bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
    else {
        anyhow::bail!("reference json is empty: {}", base_json_path.display());
    };
    if base_bytes[first_non_ws] != b'[' {
        anyhow::bail!(
            "reference json is not an array starting with '[': {}",
            base_json_path.display()
        );
    }

    let has_existing_items = base_bytes[first_non_ws + 1..last_non_ws]
        .iter()
        .any(|byte| !byte.is_ascii_whitespace());

    let appended_bytes = serde_json::to_vec(appended_rows).with_context(|| {
        format!(
            "failed to encode appended rows for {}",
            output_path.display()
        )
    })?;
    let appended_last_non_ws = appended_bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(0);
    let appended_first_non_ws = appended_bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(0);
    if appended_bytes.get(appended_first_non_ws) != Some(&b'[')
        || appended_bytes.get(appended_last_non_ws) != Some(&b']')
    {
        anyhow::bail!("appended rows are not a JSON array");
    }
    let appended_inner = &appended_bytes[appended_first_non_ws + 1..appended_last_non_ws];

    let output_file = File::create(output_path)
        .with_context(|| format!("failed to create output json: {}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, output_file);
    writer
        .write_all(&base_bytes[..last_non_ws])
        .with_context(|| {
            format!(
                "failed to write merged json prefix: {}",
                output_path.display()
            )
        })?;
    if !appended_inner.is_empty() {
        if has_existing_items {
            writer.write_all(b",").with_context(|| {
                format!(
                    "failed to write merged json delimiter: {}",
                    output_path.display()
                )
            })?;
        }
        writer.write_all(appended_inner).with_context(|| {
            format!(
                "failed to write merged json body: {}",
                output_path.display()
            )
        })?;
    }
    writer
        .write_all(b"]")
        .with_context(|| format!("failed to finalize merged json: {}", output_path.display()))?;
    writer
        .write_all(&base_bytes[last_non_ws + 1..])
        .with_context(|| {
            format!(
                "failed to write merged json suffix: {}",
                output_path.display()
            )
        })?;
    writer
        .flush()
        .with_context(|| format!("failed to flush merged json: {}", output_path.display()))?;

    Ok(())
}

fn copy_json_bytes(source_path: &Path, output_path: &Path) -> Result<()> {
    let source = File::open(source_path)
        .with_context(|| format!("failed to open reference json: {}", source_path.display()))?;
    let output = File::create(output_path)
        .with_context(|| format!("failed to create output json: {}", output_path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, source);
    let mut writer = BufWriter::with_capacity(1024 * 1024, output);
    std::io::copy(&mut reader, &mut writer).with_context(|| {
        format!(
            "failed to copy reference json from {} to {}",
            source_path.display(),
            output_path.display()
        )
    })?;
    writer
        .flush()
        .with_context(|| format!("failed to flush output json: {}", output_path.display()))?;
    Ok(())
}
