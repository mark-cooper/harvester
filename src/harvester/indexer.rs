#[derive(Debug, Clone)]
pub struct IndexerConfig {
    arclight_repository: String,
    arclight_url: String,
    oai_repository: String,
    oai_url: String,
}

impl IndexerConfig {
    pub fn new(
        arclight_repository: String,
        arclight_url: String,
        oai_repository: String,
        oai_url: String,
    ) -> Self {
        Self {
            arclight_repository,
            arclight_url,
            oai_repository,
            oai_url,
        }
    }
}
