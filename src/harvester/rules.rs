use std::io::Read;

#[derive(Debug)]
pub struct Rule {
    pub key: String,
    pub path: Vec<String>,
    pub required: bool,
}

pub fn load_rules(data: impl Read) -> anyhow::Result<Vec<Rule>> {
    let mut rules = Vec::new();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(data);

    for record in reader.records() {
        let record = record?;
        let key = record[0].to_string();
        let path_str = &record[1];
        let required = record.get(2).map(|s| s == "required").unwrap_or(false);

        let path: Vec<String> = path_str.split('/').map(|s| s.to_string()).collect();

        rules.push(Rule {
            key,
            path,
            required,
        });
    }

    Ok(rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_rules() {
        let rules = "\
title,unittitle,required
unit_id,unitid,required
creator,origination/persname,
date,unitdate,
repository,repository/corpname,required
extent,extent,
";

        let rules = load_rules(rules.as_bytes()).unwrap();

        assert_eq!(rules.len(), 6);

        assert_eq!(rules[0].key, "title");
        assert_eq!(rules[0].path, vec!["unittitle"]);
        assert!(rules[0].required);

        assert_eq!(rules[1].key, "unit_id");
        assert_eq!(rules[1].path, vec!["unitid"]);
        assert!(rules[1].required);

        assert_eq!(rules[2].key, "creator");
        assert_eq!(rules[2].path, vec!["origination", "persname"]);
        assert!(!rules[2].required);

        assert_eq!(rules[3].key, "date");
        assert_eq!(rules[3].path, vec!["unitdate"]);
        assert!(!rules[3].required);

        assert_eq!(rules[4].key, "repository");
        assert_eq!(rules[4].path, vec!["repository", "corpname"]);
        assert!(rules[4].required);

        assert_eq!(rules[5].key, "extent");
        assert_eq!(rules[5].path, vec!["extent"]);
        assert!(!rules[5].required);
    }
}
