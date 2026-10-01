use crate::configuration::GcpConfig;
use crate::gcp::gcp_utils::to_bucket_path;
use anyhow::{Result, anyhow};
use compact_str::CompactString;
use futures_util::StreamExt;
use futures_util::stream;
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use google_cloud_storage::client::Storage;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::time::timeout;
use tracing::{error, info};

pub struct GcsStorageManager {
    client: Arc<Storage>,
    gcp_config: &'static GcpConfig,
    race_id: &'static CompactString,
    upload_timeout: Duration,
}

impl GcsStorageManager {
    pub async fn new(race_id: &'static CompactString, gcp_config: &'static GcpConfig) -> Result<Self> {
        let client = Storage::builder()
            .with_retry_policy(AlwaysRetry.with_attempt_limit(u32::from(gcp_config.max_retries)))
            .build()
            .await?;
        Ok(Self {
            race_id,
            client: Arc::new(client),
            gcp_config,
            upload_timeout: Duration::from_secs(u64::from(gcp_config.upload_timeout_s.get())),
        })
    }

    /// Uploads each local file to `{raceId}/{nested_path_in_bucket}/{file_name}` in the race bucket.
    pub async fn upload_files(
        &self,
        file_paths: Vec<PathBuf>,
        nested_path_in_bucket: &str,
    ) -> Result<()> {
        let bucket_name = self.gcp_config.export_bucket.as_str();
        let bucket_prefix = to_bucket_path(self.race_id, nested_path_in_bucket);
        info!(
            "Uploading {} files to bucket: {} under {}",
            file_paths.len(),
            bucket_name,
            bucket_prefix
        );
        let upload_timeout = self.upload_timeout;
        let results = stream::iter(file_paths)
            .map(|path| {
                let client = Arc::clone(&self.client);
                let bucket = self.gcp_config.export_bucket.clone();
                let bucket_prefix = bucket_prefix.clone();
                async move {
                    let file_name = path.file_name().ok_or_else(|| {
                        anyhow!("Export path has no file name: {}", path.display())
                    })?;
                    let in_bucket_path = format!("{bucket_prefix}/{}", file_name.to_string_lossy());
                    Self::upload_file(&client, &bucket, &path, &in_bucket_path, upload_timeout).await
                }
            })
            .buffer_unordered(self.gcp_config.max_gcs_concurrent_uploads.get() as usize)
            .collect::<Vec<Result<String>>>()
            .await;

        let mut failed_uploads = 0;
        for result in &results {
            match result {
                Ok(bucket_name) => info!("Uploaded file: {}", bucket_name),
                Err(e) => {
                    failed_uploads += 1;
                    error!(%e, "Error uploading file");
                }
            }
        }
        info!(
            "Uploaded {} out of {} files",
            results.len() - failed_uploads,
            &results.len()
        );
        if failed_uploads > 0 {
            Err(anyhow!("Failed to upload {failed_uploads} files"))
        } else {
            Ok(())
        }
    }

    async fn upload_file(
        client: &Storage,
        bucket: &str,
        file_path: &Path,
        in_bucket_path: &str,
        upload_timeout: Duration,
    ) -> Result<String> {
        let upload = async {
            let file = File::open(file_path).await?;
            Box::pin(
                client
                    .write_object(format!("projects/_/buckets/{bucket}"), in_bucket_path, file)
                    .set_if_generation_match(0)
                    .send_unbuffered(),
            )
            .await
            .map(|o| o.name)
            .map_err(Into::into)
        };

        match timeout(upload_timeout, upload).await {
            Ok(result) => result,
            Err(_) => Err(anyhow!(
                "GCS upload of {} timed out after {:?}",
                file_path.display(),
                upload_timeout
            )),
        }
    }
}
