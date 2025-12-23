use std::{fs::File, path::PathBuf};

use crate::harvester::rules::load_rules;

use super::Harvester;

pub(super) async fn run(harvester: &Harvester, rules: PathBuf) -> anyhow::Result<()> {
    let rules = load_rules(File::open(rules)?);
    println!("{:?}", rules);

    todo!()
}
