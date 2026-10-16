//! GET /api/entries/download —— 下载文件（目录打包为 .tar 流式下载）
use async_tar::{Builder, Header};
use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode, header};
use axum::response::Response;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tower::Service;
use tower_http::services::ServeFile;
use utoipa::{IntoParams, ToSchema};

use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils;

#[derive(Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct DownloadQuery {
    /// 相对 files/ 的文件路径（可含子目录，含文件名）
    path: String,
}

const COPY_BUF: usize = 1 << 20; // 1MiB 写缓冲
const CHAN_DEPTH: usize = 16; // 通道深度：吸收磁盘/网络抖动（有界，天然背压）

/// 下载入口：根据路径参数分发 —— 文件走 ServeFile（支持 Range 续传），目录 tar 流式打包下载
#[utoipa::path(
    get,
    path = "/api/entries/download",
    tag = "entries",
    params(DownloadQuery),
    responses(
        (status = 200, description = "File download (octet-stream) or directory as .tar"),
        (status = 400, description = "Invalid path", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody)
    )
)]
pub async fn download_entry(
    State(state): State<AppState>,
    Query(q): Query<DownloadQuery>,
    req_headers: HeaderMap,
) -> Result<Response, AppError> {
    let files_dir = state.files_dir.clone();
    let rel = q.path.clone();
    let target = utils::blocking(move || utils::resolve_file(&files_dir, &rel)).await?;

    let md = tokio::fs::symlink_metadata(&target)
        .await
        .map_err(|_| AppError::not_found("file not found"))?;
    if md.is_symlink() {
        return Err(AppError::not_found("file not found"));
    }
    if md.is_dir() {
        return download_dir(target).await;
    }
    if !md.is_file() {
        return Err(AppError::not_found("file not found"));
    }
    let name = target
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "download".into());
    download_file(target, name, req_headers).await
}

/// 下载文件：tower-http `ServeFile` 流式发送（原生支持 Range 断点续传 / 条件请求）
/// + 覆写响应头强制下载
async fn download_file(
    target: PathBuf,
    name: String,
    req_headers: HeaderMap,
) -> Result<Response, AppError> {
    let mut builder = Request::builder().uri("/");
    for (key, value) in &req_headers {
        if matches!(
            key,
            &header::RANGE
                | &header::IF_RANGE
                | &header::IF_MODIFIED_SINCE
                | &header::IF_UNMODIFIED_SINCE
                | &header::IF_NONE_MATCH
                | &header::IF_MATCH
        ) {
            builder = builder.header(key, value);
        }
    }
    let req = builder
        .body(Body::empty())
        .map_err(|_| AppError::bad_request("invalid request headers"))?;

    let mut serve = ServeFile::new(target);
    let response = serve.call(req).await.unwrap_or_else(|never| match never {});
    let (parts, body) = response.into_parts();
    let mut resp = Response::from_parts(parts, Body::new(body));

    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    resp.headers_mut()
        .insert(header::CONTENT_DISPOSITION, content_disposition(&name)?);
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    resp.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    Ok(resp)
}

// ---------- tar 流式打包（目录下载，基于 async-tar） ----------

/// 下载目录：tar 流式打包
async fn download_dir(target: PathBuf) -> Result<Response, AppError> {
    let dir_name = target
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "download".into());
    let archive_name = format!("{dir_name}.tar");
    let root_entry = format!("{dir_name}/");

    let (pipe, mut pipe_reader) = tokio::io::duplex(COPY_BUF);
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(CHAN_DEPTH);

    tokio::spawn(async move {
        let mut buf = vec![0u8; COPY_BUF];
        while let Ok(n) = pipe_reader.read(&mut buf).await {
            if n == 0 {
                break;
            }
            if tx.send(Bytes::copy_from_slice(&buf[..n])).await.is_err() {
                break;
            }
        }
    });

    tokio::spawn(async move {
        let mut builder = Builder::new(BufWriter::with_capacity(COPY_BUF, pipe));
        builder.follow_symlinks(false);
        let pack: Result<(), String> = async {
            builder
                .append_path_with_name(&target, &root_entry)
                .await
                .map_err(|e| e.to_string())?;
            append_dir(&mut builder, &target, &root_entry)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        }
        .await;
        if let Err(m) = pack {
            let content = format!("tar packaging failed: {m}\n");
            let mut header = Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            let _ = builder
                .append_data(
                    &mut header,
                    format!("{root_entry}_TAR_ERROR.txt"),
                    content.as_bytes(),
                )
                .await;
        }
        match builder.into_inner().await {
            Ok(mut w) => {
                let _ = w.flush().await;
            }
            Err(e) => {
                tracing::debug!(error = %e, "tar archive finalize failed (client likely disconnected)");
            }
        }
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-tar")
        .header(
            header::CONTENT_DISPOSITION,
            content_disposition(&archive_name)?,
        )
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from_stream(
            ReceiverStream::new(rx).map(Ok::<Bytes, AppError>),
        ))
        .map_err(|_| AppError::internal("failed to build response"))
}

/// 递归把目录树加入 tar；条目名按名称排序保证确定性顺序。跳过隐藏文件（与 list 一致）。
async fn append_dir<W: tokio::io::AsyncWrite + Unpin + Send + Sync>(
    builder: &mut Builder<W>,
    root: &Path,
    rel: &str,
) -> Result<(), AppError> {
    let mut entries = tokio::fs::read_dir(root).await?;
    let mut names: Vec<String> = Vec::new();
    while let Some(e) = entries.next_entry().await? {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        names.push(name);
    }
    names.sort();
    for name in names {
        let path = root.join(&name);
        let entry_name = format!("{rel}{name}");
        let md = tokio::fs::symlink_metadata(&path).await?;
        if md.is_symlink() {
            continue;
        }
        if md.is_dir() {
            builder
                .append_path_with_name(&path, format!("{entry_name}/"))
                .await?;
            Box::pin(append_dir(builder, &path, &format!("{entry_name}/"))).await?;
        } else if md.is_file() {
            builder.append_path_with_name(&path, &entry_name).await?;
        }
    }
    Ok(())
}

/// UTF-8 percent-encoding（用于 Content-Disposition 的 filename*）
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn content_disposition(name: &str) -> Result<HeaderValue, AppError> {
    let ascii = name.replace(['"', '\\'], "");
    let cd = format!(
        "attachment; filename=\"{ascii}\"; filename*=UTF-8''{}",
        percent_encode(name)
    );
    HeaderValue::from_str(&cd).map_err(|_| AppError::internal("invalid content-disposition"))
}
