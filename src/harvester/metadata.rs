use std::{collections::HashMap, fs::File, io::Read, path::PathBuf};

use crate::{
    db::{FetchRecordsParams, UpdateStatusParams, do_update_status_query, fetch_records_by_status},
    harvester::{oai::OaiRecordStatus, rules::RuleSet},
};

use super::Harvester;

const BATCH_SIZE: usize = 100;

pub(super) async fn run(harvester: &Harvester, rules: PathBuf) -> anyhow::Result<()> {
    let rules = RuleSet::load(File::open(rules)?)?;
    let mut last_identifier: Option<String> = None;
    let mut total_processed = 0usize;

    loop {
        let params = FetchRecordsParams {
            endpoint: &harvester.config.endpoint,
            metadata_prefix: &harvester.config.metadata_prefix,
            status: OaiRecordStatus::Available.as_str(),
            last_identifier: last_identifier.as_deref(),
        };
        let batch = fetch_records_by_status(&harvester.pool, params).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());

        for record in &batch {
            let path = harvester.config.data_dir.join(record.path());
            let file = std::fs::File::open(path)?;
            match extract_metadata(&file, &rules) {
                Ok(metadata) => {
                    update_record_metadata(harvester, &record.identifier, metadata).await?;
                    total_processed += 1;
                }
                Err(e) => {
                    let params = UpdateStatusParams {
                        endpoint: &harvester.config.endpoint,
                        metadata_prefix: &harvester.config.metadata_prefix,
                        identifier: &record.identifier,
                        status: OaiRecordStatus::Failed.as_str(),
                        message: &e.to_string(),
                    };
                    do_update_status_query(&harvester.pool, params).await?;
                }
            }
        }

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    println!("Extracted metadata for {} records", total_processed);
    Ok(())
}

fn extract_metadata(reader: impl Read, rules: &RuleSet) -> anyhow::Result<serde_json::Value> {
    use quick_xml::Reader;
    use quick_xml::escape::resolve_predefined_entity;
    use quick_xml::events::Event;
    use std::io::BufReader;

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
                let resolved = resolve_predefined_entity(&entity).unwrap_or(" ");
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
    let json_map: serde_json::Map<_, _> = result
        .into_iter()
        .map(|(k, v)| (k, serde_json::json!(v)))
        .collect();

    Ok(serde_json::Value::Object(json_map))
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

async fn update_record_metadata(
    harvester: &Harvester,
    identifier: &str,
    metadata: serde_json::Value,
) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE oai_records
        SET status = $4, metadata = $5, last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
        &harvester.config.endpoint,
        &harvester.config.metadata_prefix,
        identifier,
        OaiRecordStatus::Parsed.as_str(),
        metadata
    )
    .execute(&harvester.pool)
    .await?;
    Ok(())
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
