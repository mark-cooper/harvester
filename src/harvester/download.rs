use crate::harvester::oai::OaiRecordStatus;

use super::Harvester;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    todo!()
}

async fn fetch_pending_records(harvester: &Harvester) -> anyhow::Result<Vec<OaiRecordStatus>> {
    let records: Vec<OaiRecordStatus> = sqlx::query_as!(
        OaiRecordStatus,
        r#"
        SELECT endpoint, metadata_prefix, identifier, status
        FROM oai_records
        WHERE endpoint = $1 AND metadata_prefix = $2 AND status = 'pending'
        "#,
        &harvester.config.endpoint,
        &harvester.config.metadata_prefix
    )
    .fetch_all(&harvester.pool)
    .await?;

    Ok(records)
}
