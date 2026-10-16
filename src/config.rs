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
}
