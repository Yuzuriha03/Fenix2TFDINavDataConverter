use serde_json::{Map, Value};

const ZULS_ICAO: &str = "ZULS";
const ZULS_SPECIAL_STAR_PREFIXES: &[&str] = &[
    "DUM08A", "DUM09A", "DUM10A", "DUM28A", "IGD10A", "IGD28A", "IKU10A", "IKU28A",
];

pub(crate) fn is_excluded_terminal_row(row: &Map<String, Value>) -> bool {
    is_excluded_terminal(
        json_string(row.get("ICAO")),
        json_string(row.get("Name")),
        json_string(row.get("FullName")),
    )
}

pub(crate) fn is_excluded_terminal(
    icao: Option<&str>,
    name: Option<&str>,
    full_name: Option<&str>,
) -> bool {
    if !icao.is_some_and(|value| value.trim().eq_ignore_ascii_case(ZULS_ICAO)) {
        return false;
    }

    let candidates = [name, full_name]
        .into_iter()
        .flatten()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>();

    candidates.iter().any(|candidate| {
        candidate.starts_with("DEP")
            || candidate.starts_with("EO")
            || candidate.starts_with('R')
            || ZULS_SPECIAL_STAR_PREFIXES
                .iter()
                .any(|prefix| candidate.starts_with(prefix))
    })
}

fn json_string(value: Option<&Value>) -> Option<&str> {
    match value {
        Some(Value::String(text)) => Some(text),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excludes_zuls_sid_names() {
        assert!(is_excluded_terminal(
            Some("ZULS"),
            Some("DEP3A"),
            Some("DEP3A RWY 10"),
        ));
        assert!(is_excluded_terminal(
            Some("zuls"),
            Some("eo2b"),
            Some("EO2B RWY 28"),
        ));
    }

    #[test]
    fn excludes_zuls_star_and_iap_names() {
        assert!(is_excluded_terminal(
            Some("ZULS"),
            Some("DUM08A"),
            Some("DUM08A TRANSITION"),
        ));
        assert!(is_excluded_terminal(
            Some("ZULS"),
            Some("R10-Z"),
            Some("R10-Z"),
        ));
    }

    #[test]
    fn keeps_other_airports_and_non_matching_names() {
        assert!(!is_excluded_terminal(
            Some("ZBAA"),
            Some("DEP3A"),
            Some("DEP3A RWY 10"),
        ));
        assert!(!is_excluded_terminal(
            Some("ZULS"),
            Some("NORM1A"),
            Some("NORM1A"),
        ));
    }
}
