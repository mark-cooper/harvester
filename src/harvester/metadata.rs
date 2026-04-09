use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

use quick_xml::{Reader, escape, events::Event};
use serde_json::{Map, Value};

use crate::{
    harvester::{BatchStats, rules::RuleSet},
    oai::{HarvestEvent, OaiRecord, OaiRecordStatus},
};

use super::Harvester;

pub(super) async fn run(harvester: &Harvester, rules: PathBuf) -> anyhow::Result<()> {
    let rules = RuleSet::load(File::open(rules)?)?;
    harvester
        .batched(
            OaiRecordStatus::Available,
            "Extracted metadata for",
            async |batch| process_batch(harvester, &rules, batch).await,
        )
        .await
}

async fn process_batch(
    harvester: &Harvester,
    rules: &RuleSet,
    records: &[OaiRecord],
) -> BatchStats {
    let mut results = Vec::with_capacity(records.len());
    for record in records {
        results.push(process_record(harvester, rules, record).await);
    }
    BatchStats::from_results(results)
}

async fn process_record(
    harvester: &Harvester,
    rules: &RuleSet,
    record: &OaiRecord,
) -> anyhow::Result<bool> {
    let path = harvester.config.data_dir.join(record.path());
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) => {
            let message = format!("Unable to open metadata file {}: {}", path.display(), error);
            return harvester
                .update(record, &HarvestEvent::MetadataFailed { message: &message })
                .await;
        }
    };

    match extract_metadata(&file, rules) {
        Ok(metadata) => {
            harvester
                .update(record, &HarvestEvent::MetadataExtracted { metadata })
                .await
        }
        Err(error) => {
            let message = error.to_string();
            harvester
                .update(record, &HarvestEvent::MetadataFailed { message: &message })
                .await
        }
    }
}

fn extract_metadata(reader: impl Read, rules: &RuleSet) -> anyhow::Result<Value> {
    let buf_reader = BufReader::new(reader);
    let mut reader = Reader::from_reader(buf_reader);
    let mut result: HashMap<String, Vec<String>> = HashMap::new();
    let mut stack: Vec<String> = Vec::new();
    // Track accumulated text at each depth level to handle nested markup
    let mut text_at_depth: HashMap<usize, String> = HashMap::new();
    let mut buf = Vec::new();

    // Skip these elements entirely (dsc contains container lists, often 90%+ of file)
    const SKIP_ELEMENTS: &[&str] = &["dsc"];

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                if SKIP_ELEMENTS.contains(&name.as_str()) {
                    reader.read_to_end_into(e.name(), &mut Vec::new())?;
                } else {
                    stack.push(name);
                    text_at_depth.entry(stack.len()).or_default();
                }
            }
            Ok(Event::Text(e)) => {
                let decoded = e
                    .decode()
                    .map_err(|err| anyhow::anyhow!("XML decode error: {}", err))?;
                // Append text to current depth and all parent depths
                for depth in 1..=stack.len() {
                    text_at_depth.entry(depth).or_default().push_str(&decoded);
                }
            }
            Ok(Event::GeneralRef(e)) => {
                // Resolve entity references like &amp; -> &, &lt; -> <, etc.
                let entity = e
                    .decode()
                    .map_err(|err| anyhow::anyhow!("XML decode error: {}", err))?;
                let resolved = escape::resolve_predefined_entity(&entity).unwrap_or(" ");
                for depth in 1..=stack.len() {
                    text_at_depth.entry(depth).or_default().push_str(resolved);
                }
            }
            Ok(Event::End(_)) => {
                let depth = stack.len();
                if let Some(text) = text_at_depth.remove(&depth) {
                    let text = text.trim();
                    if !text.is_empty()
                        && let Some(terminal) = stack.last()
                    {
                        for rule in rules.by_terminal(terminal) {
                            if stack_matches_path(&stack, &rule.path) {
                                result
                                    .entry(rule.key.clone())
                                    .or_default()
                                    .push(text.to_string());
                            }
                        }
                    }
                }
                stack.pop();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(anyhow::anyhow!("XML parse error: {}", e)),
            _ => {}
        }
        buf.clear();
    }

    // Check for required fields
    for rule in rules.required() {
        if result.get(&rule.key).is_none_or(|v| v.is_empty()) {
            return Err(anyhow::anyhow!("Required field '{}' is empty", rule.key));
        }
    }

    // Convert to JSON with values as arrays
    let json_map: Map<_, _> = result
        .into_iter()
        .map(|(k, v)| (k, serde_json::json!(v)))
        .collect();

    Ok(Value::Object(json_map))
}

/// Check if element stack ends with the given path
/// e.g., stack ["ead", "archdesc", "repository", "corpname"] matches path ["repository", "corpname"]
fn stack_matches_path(stack: &[String], path: &[String]) -> bool {
    if path.len() > stack.len() {
        return false;
    }
    stack
        .iter()
        .rev()
        .zip(path.iter().rev())
        .all(|(s, p)| s == p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_extract_metadata() {
        let rules_csv = "\
title,unittitle,required
unit_id,unitid,required
creator,origination/persname,
date,unitdate,
repository,repository/corpname,required
extent,extent,
";

        let rules = RuleSet::load(rules_csv.as_bytes()).unwrap();
        let file = File::open("fixtures/ead.xml").unwrap();
        let metadata = extract_metadata(file, &rules).unwrap();

        // Check required fields are extracted
        assert_eq!(metadata["title"], serde_json::json!(["ANW-1805 test"]));
        assert_eq!(
            metadata["repository"],
            serde_json::json!(["Allen Doe Research Center"])
        );

        // unitid appears multiple times in the fixture
        let unit_ids = metadata["unit_id"].as_array().unwrap();
        assert!(unit_ids.contains(&serde_json::json!("MSS54321")));

        // Check optional field
        assert_eq!(metadata["date"], serde_json::json!(["1950-1960"]));

        // extent is in physdesc, not directly under did
        assert_eq!(metadata["extent"], serde_json::json!(["2 Linear Feet"]));

        // creator/origination/persname is not in the fixture
        assert!(metadata.get("creator").is_none());
    }

    #[test]
    fn test_extract_metadata_missing_required() {
        let rules_csv = "\
title,unittitle,required
nonexistent,nonexistent/field,required
";

        let rules = RuleSet::load(rules_csv.as_bytes()).unwrap();
        let file = File::open("fixtures/ead.xml").unwrap();
        let result = extract_metadata(file, &rules);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"));
        assert!(err.contains("empty"));
    }

    #[test]
    fn test_extract_metadata_with_nested_markup() {
        let xml = r#"<?xml version="1.0"?>
<ead xmlns="urn:isbn:1-931666-22-9">
  <archdesc>
    <did>
      <repository><corpname>Test Repo</corpname></repository>
      <unittitle><i>Italicized Title</i></unittitle>
      <unitid>ID-123</unitid>
    </did>
  </archdesc>
</ead>"#;

        let rules_csv = "\
title,unittitle,required
unit_id,unitid,required
repository,repository/corpname,required
";

        let rules = RuleSet::load(rules_csv.as_bytes()).unwrap();
        let metadata = extract_metadata(xml.as_bytes(), &rules).unwrap();

        assert_eq!(metadata["title"], serde_json::json!(["Italicized Title"]));
    }

    #[test]
    fn test_extract_metadata_with_ampersand() {
        let xml = r#"<?xml version="1.0"?>
<ead xmlns="urn:isbn:1-931666-22-9">
  <archdesc>
    <did>
      <repository><corpname>Allen Doe Research &amp; Center</corpname></repository>
      <unittitle>Test Title</unittitle>
      <unitid>ID-123</unitid>
    </did>
  </archdesc>
</ead>"#;

        let rules_csv = "\
title,unittitle,required
unit_id,unitid,required
repository,repository/corpname,required
";

        let rules = RuleSet::load(rules_csv.as_bytes()).unwrap();
        let metadata = extract_metadata(xml.as_bytes(), &rules).unwrap();

        assert_eq!(
            metadata["repository"],
            serde_json::json!(["Allen Doe Research & Center"])
        );
    }
}
