use std::path::PathBuf;

use clap::Parser;

/// Storge - chunked upload server
#[derive(Parser, Debug)]
#[command(name = "storge", version, about)]
pub struct Config {
    /// Listen address (host:port)
    #[arg(long, env = "APP_LISTEN_ADDR", default_value = "0.0.0.0:9000")]
    pub listen_addr: String,

    /// Directory for temporary upload staging (`uploads/<upload_id>/data.part` +
    /// `done.bin` + `meta.json`, removed after merge)
    #[arg(long, env = "APP_UPLOADS_DIR", default_value = "./uploads")]
    pub uploads_dir: PathBuf,

    /// Directory for merged / final files
    #[arg(long, env = "APP_FILES_DIR", default_value = "./files")]
    pub files_dir: PathBuf,

    /// Max concurrent disk-write upload operations (chunk writes + merges, across all files)
    #[arg(long, env = "APP_MAX_CONCURRENT_UPLOADS", default_value_t = 16)]
    pub max_concurrent_uploads: usize,

    /// Always keep this many bytes free on the files_dir volume; uploads that would
    /// eat into it are rejected with 507 (0 = disable the floor)
    #[arg(long, env = "APP_MIN_FREE_BYTES", default_value_t = 1024 * 1024 * 1024)]
    pub min_free_bytes: u64,

    /// Hard cap on total *volume* bytes used (used + new writes must stay under it);
    /// 0 = unlimited. Useful when several services share one disk
    #[arg(long, env = "APP_STORAGE_QUOTA", default_value_t = 0)]
    pub storage_quota: u64,

    /// Cache lifetime of a statvfs sample in ms (shared by API requests and upload checks)
    #[arg(long, env = "APP_DISK_CHECK_TTL_MS", default_value_t = 1000)]
    pub disk_check_ttl_ms: u64,

    /// Re-sample the disk and re-check capacity every N uploaded chunks (0 = never)
    #[arg(long, env = "APP_DISK_CHECK_EVERY_CHUNKS", default_value_t = 32)]
    pub disk_check_every_chunks: u64,

    /// Delete abandoned upload sessions (`uploads/<id>/`) idle for longer than this
    /// many seconds; 0 = disable cleanup
    #[arg(long, env = "APP_UPLOAD_TTL_SECS", default_value_t = 7 * 24 * 3600)]
    pub upload_ttl_secs: u64,

    /// Interval in seconds between upload-session cleanup passes
    #[arg(long, env = "APP_UPLOAD_CLEANUP_INTERVAL_SECS", default_value_t = 3600)]
    pub upload_cleanup_interval_secs: u64,
}

impl Config {
    /// CLI / env → 磁盘守卫参数
    pub fn disk_limits(&self) -> crate::disk::DiskLimits {
        crate::disk::DiskLimits {
            min_free: self.min_free_bytes,
            quota: self.storage_quota,
            sample_ttl: std::time::Duration::from_millis(self.disk_check_ttl_ms),
            check_every_chunks: self.disk_check_every_chunks,
            session_ttl: std::time::Duration::from_secs(self.upload_ttl_secs),
            cleanup_interval: std::time::Duration::from_secs(
                self.upload_cleanup_interval_secs.max(1),
            ),
        }
    }
}
