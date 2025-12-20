use crate::harvester::oai::OaiRecord;

use super::Harvester;

use oai_pmh::{Client, ListIdentifiersArgs};

pub(super) fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let client = Client::new(harvester.config.endpoint.as_str())?;

    client.identify()?;

    let mut processed = 0;
    let args = ListIdentifiersArgs::new(harvester.config.metadata_prefix.as_str());

    for response in client.list_identifiers(args)? {
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

                        processed += 1;
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    println!("Processed {} records", processed);
    Ok(())
}
