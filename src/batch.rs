use crate::OaiRecordId;

const BATCH_SIZE: usize = 100;

/// Generic batch processing loop over `OaiRecordId` records.
///
/// Handles pagination (via `last_identifier`), shutdown checking, and
/// batch-size termination. The caller provides:
/// - `is_shutdown`: checked before each fetch
/// - `fetch`: returns the next page given `last_identifier`
/// - `process`: called with each non-empty batch
pub async fn run<S>(
    is_shutdown: impl Fn() -> bool,
    fetch: impl AsyncFn(Option<&str>) -> anyhow::Result<Vec<OaiRecordId>>,
    process: impl AsyncFn(&[OaiRecordId]) -> S,
) -> anyhow::Result<Vec<S>> {
    let mut last_identifier: Option<String> = None;
    let mut results = Vec::new();

    loop {
        if is_shutdown() {
            break;
        }

        let batch = fetch(last_identifier.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());
        results.push(process(&batch).await);

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    Ok(results)
}
