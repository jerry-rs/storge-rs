mod config;
mod disk;
mod error;
mod handlers;
mod openapi;
mod routers;
mod state;
mod utils;

use clap::Parser;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_ansi_sanitization(true)
        .with_line_number(true)
        .pretty()
        .init();

    let cfg = config::Config::parse();
    let limits = cfg.disk_limits();
    tracing::info!(
        listen_addr = %cfg.listen_addr,
        uploads_dir = %cfg.uploads_dir.display(),
        files_dir = %cfg.files_dir.display(),
        max_concurrent_uploads = cfg.max_concurrent_uploads,
        min_free_bytes = limits.min_free,
        storage_quota = limits.quota,
        disk_check_ttl_ms = limits.sample_ttl.as_millis(),
        disk_check_every_chunks = limits.check_every_chunks,
        upload_ttl_secs = limits.session_ttl.as_secs(),
        "configuration loaded"
    );

    let state = state::AppState::with_limits(
        cfg.uploads_dir,
        cfg.files_dir,
        cfg.max_concurrent_uploads,
        limits,
    )
    .expect("failed to init storage directories");

    // 重启前未完成的上传：稀疏 staging 在 statvfs 里只显已落盘部分，先把“声明但未写入”
    // 的体积重新记账，否则重启后第一批 init 会把磁盘超卖
    handlers::upload::restore_reservations(&state);
    // 后台回收闲置超时的上传会话（传一半关浏览器是磁盘被慢慢撑爆的主因）
    handlers::upload::spawn_upload_janitor(state.clone());

    let app = routers::build_global_routers(state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .expect("failed to bind address");
    tracing::info!(addr = %cfg.listen_addr, "server started");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server exited unexpectedly");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
