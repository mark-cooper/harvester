use anyhow::Ok;

use crate::Indexer;

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    repository: String,
    oai_endpoint: String,
    oai_repository: String,
    solr_url: String,
}

impl IndexerConfig {
    // TODO: constructor supports validation later if necessary
    // (for example: parse endpoint/url as uri etc.)
    pub fn new(
        repository: String,
        oai_endpoint: String,
        oai_repository: String,
        solr_url: String,
    ) -> Self {
        Self {
            repository,
            oai_endpoint,
            oai_repository,
            solr_url,
        }
    }
}

// fetch_records_to_index:
// endpoint = oai_endpoint
// metadata_prefix = "oai_ead"
// metadata[repository] contains oai_repository
// status = "available"
// fetch_records_to_delete:
// same but status = "deleted"
pub async fn run(_: &Indexer) -> anyhow::Result<()> {
    Ok(())
}
