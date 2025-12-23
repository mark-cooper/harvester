use std::path::PathBuf;

use crate::harvester::rules::load_rules;

use super::Harvester;

pub(super) async fn run(harvester: &Harvester, rules: PathBuf) -> anyhow::Result<()> {
    let rules = load_rules(rules);
    println!("{:?}", rules);

    todo!()
}
