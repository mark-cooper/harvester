use crate::harvester::oai::OaiRecord;

use super::Harvester;

use oai_pmh::{Client, ListIdentifiersArgs};

pub(super) fn run(harvester: &Harvester) -> anyhow::Result<i32> {
    let client = Client::new(harvester.config.endpoint.as_str())?;

    client.identify()?;

    let mut visited = 0;
    let mut imported = 0;
    let args = ListIdentifiersArgs::new(harvester.config.metadata_prefix.as_str());

    for response in client.list_identifiers(args)? {
        visited += 1;

        match response {
            Ok(response) => {
                if let Some(e) = response.error {
                    return Err(anyhow::anyhow!("OAI-PMH request error: {:?}", e));
                }

                if let Some(payload) = response.payload {
                    for header in payload.header {
                        let mut record = OaiRecord::from(header);
                        record.endpoint(harvester.config.endpoint.clone());
                        println!("{:?}", record);

                        // TODO: check exists (existing)
                        // IF not: import
                        // IF it does exist:
                        // a) if existing.status failed: continue.
                        // b) if existing.datestamp != record.datestamp: update to pending.

                        imported += 1;
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    // TODO: batch update last_checked_at
    println!("Imported {} of {} records", imported, visited);
    Ok(imported)
}
