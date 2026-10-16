use axum::http::header;
use axum::response::IntoResponse;

/// 前端页面，编译时嵌入二进制
const INDEX_HTML: &str = include_str!("../../web/index.html");
/// 站点图标，编译时嵌入二进制
const FAVICON_SVG: &str = include_str!("../../web/favicon.svg");

pub async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX_HTML,
    )
}

pub async fn favicon() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        FAVICON_SVG,
    )
}
