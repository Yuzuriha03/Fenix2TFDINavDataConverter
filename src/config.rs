use std::collections::HashSet;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

const REQUIRED_TABLES: &[&str] = &[
    "AirportCommunication",
    "AirportLookup",
    "Airports",
    "AirwayLegs",
    "Airways",
    "config",
    "Gls",
    "GridMora",
    "Holdings",
    "ILSes",
    "Markers",
    "MarkerTypes",
    "NavaidLookup",
    "Navaids",
    "NavaidTypes",
    "Runways",
    "SurfaceTypes",
    "TerminalLegs",
    "TerminalLegsEx",
    "Terminals",
    "TrmLegTypes",
    "WaypointLookup",
    "Waypoints",
];

const DEBUG_DB_PATH: &str = r"c:\ProgramData\Fenix\Navdata\nd.db3";
const DEBUG_START_TERMINAL_ID: i64 = 96701;
const DEBUG_RTE_SEG_PATH: &str = r"D:\yyz\Documents\NAIP\RTE_SEG.csv";

#[derive(Clone, Debug)]
pub(crate) struct AppConfig {
    pub(crate) debug: bool,
    pub(crate) output_dir: PathBuf,
    pub(crate) output_label: String,
    pub(crate) reference_dir: Option<PathBuf>,
    pub(crate) db_path: Option<PathBuf>,
    pub(crate) start_terminal_id: Option<i64>,
    pub(crate) rte_seg_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct OutputLocation {
    label: String,
    path: PathBuf,
}

pub(crate) fn parse_args() -> Result<AppConfig> {
    let args: Vec<String> = env::args().skip(1).collect();
    let debug = args.iter().any(|arg| arg == "--DEBUG");
    let invalid_args: Vec<&str> = args
        .iter()
        .filter(|arg| arg.as_str() != "--DEBUG")
        .map(String::as_str)
        .collect();
    if !invalid_args.is_empty() {
        bail!("unsupported arguments: {}", invalid_args.join(" "));
    }

    if debug {
        let timestamp = debug_timestamp()?;
        let detected_output = detect_output_directory().ok();
        return Ok(AppConfig {
            debug: true,
            output_dir: Path::new("Nav-Primary_debug").join(timestamp),
            output_label: detected_output
                .as_ref()
                .map(|location| format!("{} reference", location.label))
                .unwrap_or_else(|| "no installed Nav-Primary detected".to_string()),
            reference_dir: detected_output.map(|location| location.path),
            db_path: Some(PathBuf::from(DEBUG_DB_PATH)),
            start_terminal_id: Some(DEBUG_START_TERMINAL_ID),
            rte_seg_path: Some(PathBuf::from(DEBUG_RTE_SEG_PATH)),
        });
    }

    let detected_output = detect_output_directory()?;
    Ok(AppConfig {
        debug: false,
        output_dir: detected_output.path.clone(),
        output_label: detected_output.label,
        reference_dir: Some(detected_output.path),
        db_path: None,
        start_terminal_id: None,
        rte_seg_path: None,
    })
}

pub(crate) fn prepare_output_directory(config: &AppConfig) -> Result<()> {
    fs::create_dir_all(&config.output_dir).with_context(|| {
        format!(
            "failed to create output directory: {}",
            config.output_dir.display()
        )
    })?;
    fs::create_dir_all(config.output_dir.join("ProcedureLegs")).with_context(|| {
        format!(
            "failed to create ProcedureLegs directory: {}",
            config.output_dir.display()
        )
    })?;
    Ok(())
}

pub(crate) fn prompt_db3_path() -> Result<PathBuf> {
    loop {
        let input = prompt("Enter Fenix nd.db3 path: ")?;
        let trimmed = input.trim().trim_matches(['\'', '"']);
        let path = PathBuf::from(trimmed);
        if !path.exists() || path.extension().and_then(|ext| ext.to_str()) != Some("db3") {
            println!("Invalid db3 path. Please enter a valid nd.db3 file.");
            continue;
        }

        let validation_conn = match Connection::open(&path) {
            Ok(conn) => conn,
            Err(error) => {
                println!("Failed to open database: {error}. Please try again.");
                continue;
            }
        };

        match validate_required_tables(&validation_conn) {
            Ok(()) => return Ok(path),
            Err(_) => println!("This file is not a valid Fenix nav database. Please try again."),
        }
    }
}

pub(crate) fn prompt_rte_seg_path() -> Result<PathBuf> {
    loop {
        let input = prompt("Enter RTE_SEG.csv path: ")?;
        let trimmed = input.trim().trim_matches(['\'', '"']);
        let path = PathBuf::from(trimmed);
        if !path.exists() || path.extension().and_then(|ext| ext.to_str()) != Some("csv") {
            println!("Invalid CSV path. Please enter a valid RTE_SEG.csv file.");
            continue;
        }
        return Ok(path);
    }
}

pub(crate) fn detect_start_terminal_id(output_dir: &Path) -> Result<i64> {
    let procedure_legs_dir = output_dir.join("ProcedureLegs");
    if !procedure_legs_dir.exists() {
        return Ok(1);
    }

    let mut max_terminal_id = 0i64;
    for entry in fs::read_dir(&procedure_legs_dir).with_context(|| {
        format!(
            "failed to read ProcedureLegs directory: {}",
            procedure_legs_dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read ProcedureLegs entry: {}",
                procedure_legs_dir.display()
            )
        })?;
        let Some(file_name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(number_text) = file_name
            .strip_prefix("TermID_")
            .and_then(|value| value.strip_suffix(".json"))
        else {
            continue;
        };
        let Ok(value) = number_text.parse::<i64>() else {
            continue;
        };
        max_terminal_id = max_terminal_id.max(value);
    }

    Ok(max_terminal_id + 1)
}

pub(crate) fn validate_required_tables(conn: &Connection) -> Result<()> {
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .context("failed to inspect sqlite schema")?;
    let table_rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .context("failed to iterate sqlite schema")?;
    let tables: HashSet<String> = table_rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to read sqlite schema rows")?
        .into_iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    let missing: Vec<&str> = REQUIRED_TABLES
        .iter()
        .copied()
        .filter(|table| !tables.contains(&table.to_ascii_lowercase()))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        bail!("missing required tables: {}", missing.join(", "));
    }
}

fn debug_timestamp() -> Result<String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("failed to read current time")?
        .as_secs();
    Ok(timestamp.to_string())
}

fn detect_output_directory() -> Result<OutputLocation> {
    for location in nav_primary_candidates() {
        if location.path.exists() {
            return Ok(location);
        }
    }

    let candidates = nav_primary_candidates()
        .into_iter()
        .map(|location| format!("{}: {}", location.label, location.path.display()))
        .collect::<Vec<_>>()
        .join("\n");
    bail!("failed to detect TFDI MD-11 Nav-Primary directory.\n{candidates}");
}

fn nav_primary_candidates() -> Vec<OutputLocation> {
    vec![
        OutputLocation {
            label: "MSFS2020 (Microsoft Store)".to_string(),
            path: expand_env(
                r"%LocalAppData%\Packages\Microsoft.FlightSimulator_8wekyb3d8bbwe\LocalState\packages\tfdidesign-aircraft-md11\work\Nav-Primary",
            ),
        },
        OutputLocation {
            label: "MSFS2020 (Steam)".to_string(),
            path: expand_env(
                r"%AppData%\Microsoft Flight Simulator\Packages\tfdidesign-aircraft-md11\work\Nav-Primary",
            ),
        },
        OutputLocation {
            label: "MSFS2024 (Microsoft Store)".to_string(),
            path: expand_env(
                r"%LocalAppData%\Packages\Microsoft.Limitless_8wekyb3d8bbwe\LocalState\WASM\MSFS2024\tfdidesign-aircraft-md11\work\Nav-Primary",
            ),
        },
        OutputLocation {
            label: "MSFS2024 (Steam)".to_string(),
            path: expand_env(
                r"%AppData%\Microsoft Flight Simulator 2024\WASM\MSFS2024\tfdidesign-aircraft-md11\work\Nav-Primary",
            ),
        },
    ]
}

fn expand_env(path: &str) -> PathBuf {
    let mut expanded = OsString::new();
    let mut chars = path.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            expanded.push(ch.to_string());
            continue;
        }

        let mut variable = String::new();
        while let Some(next) = chars.peek().copied() {
            chars.next();
            if next == '%' {
                break;
            }
            variable.push(next);
        }

        if variable.is_empty() {
            expanded.push("%");
            continue;
        }

        if let Some(value) = env::var_os(&variable) {
            expanded.push(value);
        } else {
            expanded.push(format!("%{variable}%"));
        }
    }
    PathBuf::from(expanded)
}

fn prompt(message: &str) -> Result<String> {
    print!("{message}");
    io::stdout().flush().context("failed to flush stdout")?;
    let mut buffer = String::new();
    io::stdin()
        .read_line(&mut buffer)
        .context("failed to read user input")?;
    Ok(buffer)
}
