use std::{collections::HashMap, io::Read};

#[derive(Debug)]
pub struct Rule {
    pub key: String,
    pub path: Vec<String>,
    pub required: bool,
}

#[derive(Debug)]
pub struct RuleSet {
    rules: Vec<Rule>,
    by_terminal: HashMap<String, Vec<usize>>,
}

impl RuleSet {
    pub fn load(reader: impl Read) -> anyhow::Result<Self> {
        let mut rules = Vec::new();
        let mut by_terminal: HashMap<String, Vec<usize>> = HashMap::new();

        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(reader);

        for record in csv_reader.records() {
            let record = record?;
            let key = record[0].to_string();
            let path_str = &record[1];
            let required = record.get(2).map(|s| s == "required").unwrap_or(false);

            let path: Vec<String> = path_str.split('/').map(|s| s.to_string()).collect();

            if let Some(terminal) = path.last() {
                by_terminal
                    .entry(terminal.clone())
                    .or_default()
                    .push(rules.len());
            }

            rules.push(Rule {
                key,
                path,
                required,
            });
        }

        Ok(Self { rules, by_terminal })
    }

    /// Returns rules whose path ends with the given terminal element
    pub fn by_terminal(&self, terminal: &str) -> impl Iterator<Item = &Rule> {
        self.by_terminal
            .get(terminal)
            .into_iter()
            .flatten()
            .map(|&idx| &self.rules[idx])
    }

    /// Returns rules marked as required
    pub fn required(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter().filter(|r| r.required)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_rules() {
        let csv = "\
title,unittitle,required
unit_id,unitid,required
creator,origination/persname,
date,unitdate,
repository,repository/corpname,required
extent,extent,
";

        let ruleset = RuleSet::load(csv.as_bytes()).unwrap();

        // Test by_terminal lookup
        let corpname_rules: Vec<_> = ruleset.by_terminal("corpname").collect();
        assert_eq!(corpname_rules.len(), 1);
        assert_eq!(corpname_rules[0].key, "repository");
        assert_eq!(corpname_rules[0].path, vec!["repository", "corpname"]);

        let persname_rules: Vec<_> = ruleset.by_terminal("persname").collect();
        assert_eq!(persname_rules.len(), 1);
        assert_eq!(persname_rules[0].key, "creator");

        // Test required iterator
        let required: Vec<_> = ruleset.required().collect();
        assert_eq!(required.len(), 3);
        assert!(required.iter().all(|r| r.required));

        // Verify specific required rules
        let required_keys: Vec<_> = required.iter().map(|r| r.key.as_str()).collect();
        assert!(required_keys.contains(&"title"));
        assert!(required_keys.contains(&"unit_id"));
        assert!(required_keys.contains(&"repository"));
    }
}
