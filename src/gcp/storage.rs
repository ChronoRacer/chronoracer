use crate::configuration::GcpConfig;
use crate::gcp::gcp_utils::to_bucket_path;
use compact_str::CompactString;
use futures_util::stream;
use futures_util::StreamExt;
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use google_cloud_storage::client::Storage;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tracing::{error, info};

pub enum Bucket {
    ExportBucket,
}

pub struct GcsStorageManager<'a> {
    client: Arc<Storage>,
    gcp_config: &'a GcpConfig,
    race_id: &'a CompactString,
}

impl<'a> GcsStorageManager<'a> {
    pub async fn new(
        race_id: &'a CompactString,
        gcp_config: &'a GcpConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let client = Storage::builder()
            .with_retry_policy(AlwaysRetry.with_attempt_limit(u32::from(gcp_config.max_retries)))
            .build()
            .await?;
        Ok(Self {
            race_id,
            client: Arc::new(client),
            gcp_config,
        })
    }

    pub async fn upload_files(
        &self,
        file_paths: Vec<PathBuf>,
        bucket: Bucket,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bucket_name = Arc::new(match bucket {
            Bucket::ExportBucket => self.gcp_config.export_bucket.clone(),
        });
        info!(
            "Uploading {} files to bucket: {}",
            file_paths.len(),
            bucket_name.clone()
        );
        let results = stream::iter(file_paths)
            .map(|path| {
                let client = Arc::clone(&self.client);
                let bucket = Arc::clone(&bucket_name);
                let in_bucket_path = to_bucket_path(self.race_id, &path.to_string_lossy());
                async move { Self::upload_file(&client, &bucket, &path, &in_bucket_path).await }
            })
            .buffer_unordered(self.gcp_config.max_gcs_concurrent_uploads as usize)
            .collect::<Vec<Result<String, String>>>()
            .await;

        let mut failed_uploads = 0;
        for result in &results {
            match result {
                Ok(bucket_name) => info!("Uploaded file: {}", bucket_name),
                Err(e) => {
                    failed_uploads += 1;
                    error!("Error uploading file: {}", e);
                }
            }
        }
        info!(
            "Uploaded {} out of {} files",
            &results.len() - failed_uploads,
            &results.len()
        );
        if failed_uploads > 0 {
            Err(format!("Failed to upload {} files", failed_uploads).into())
        } else {
            Ok(())
        }
    }

    async fn upload_file(
        client: &Storage,
        bucket: &str,
        file_path: &Path,
        in_bucket_path: &str,
    ) -> Result<String, String> {
        let file = File::open(file_path).await.map_err(|e| e.to_string())?;
        client
            .write_object(format!("projects/_/buckets/{bucket}"), in_bucket_path, file)
            .set_if_generation_match(0)
            .send_unbuffered()
            .await
            .map(|o| o.name)
            .map_err(|e| e.to_string())
    }
}
