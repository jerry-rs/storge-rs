use axum::Router;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Request};
use axum::routing::{get, post};
use std::net::SocketAddr;
use tower_http::trace::{DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tower_http::validate_request::ValidateRequestHeaderLayer;
use tracing::Level;

use crate::handlers;
use crate::state::AppState;
use crate::utils::MAX_CHUNK_SIZE;

const AUTH_TOKEN: &str = "storge";

/// 公开路由：无需鉴权
pub fn public_global_routers() -> Router<AppState> {
    Router::new()
        .route("/", get(handlers::index::index))
        .route("/favicon.svg", get(handlers::index::favicon))
        .merge(crate::openapi::swagger_ui())
}

/// 受保护路由：JSON API，要求 `Authorization: Bearer storge`
#[allow(deprecated)]
pub fn protected_global_routers() -> Router<AppState> {
    Router::new()
        .route(
            "/api/entries/upload/init",
            post(handlers::upload::init_upload),
        )
        .route(
            "/api/entries/upload/chunk",
            post(handlers::upload::upload_chunk),
        )
        .route(
            "/api/entries/upload/merge",
            post(handlers::upload::merge_upload),
        )
        .route(
            "/api/entries/upload/status",
            get(handlers::upload::upload_status),
        )
        .route("/api/entries", get(handlers::list::list_entry))
        .route("/api/entries/disk", get(handlers::disk::disk_info))
        .route("/api/entries/info", get(handlers::info::entry_info))
        .route("/api/entries/mkdir", post(handlers::mkdir::make_dir_entry))
        .route("/api/entries/rename", post(handlers::rename::rename_entry))
        .route(
            "/api/entries/download",
            get(handlers::download::download_entry),
        )
        .route("/api/entries/delete", post(handlers::delete::delete_entry))
        .layer(DefaultBodyLimit::max(MAX_CHUNK_SIZE))
        .layer(ValidateRequestHeaderLayer::bearer(AUTH_TOKEN))
}

/// 合并公开 + 受保护路由，并挂载请求 tracing（含客户端 IP）
pub fn build_global_routers(state: AppState) -> Router {
    Router::new()
        .merge(public_global_routers())
        .merge(protected_global_routers())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &Request<_>| {
                    let ip = req
                        .extensions()
                        .get::<ConnectInfo<SocketAddr>>()
                        .map(|ConnectInfo(addr)| addr.ip().to_string())
                        .unwrap_or_else(|| "-".into());
                    tracing::info_span!(
                        "request",
                        ip = %ip,
                        method = %req.method(),
                        uri = %req.uri()
                    )
                })
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO))
                .on_failure(DefaultOnFailure::new().level(Level::ERROR)),
        )
        .with_state(state)
}
