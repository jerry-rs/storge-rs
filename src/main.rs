mod config;
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
    tracing::info!(
        listen_addr = %cfg.listen_addr,
        uploads_dir = %cfg.uploads_dir.display(),
        files_dir = %cfg.files_dir.display(),
        max_concurrent_uploads = cfg.max_concurrent_uploads,
        "configuration loaded"
    );

    let state = state::AppState::new(cfg.uploads_dir, cfg.files_dir, cfg.max_concurrent_uploads)
        .expect("failed to init storage directories");

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
