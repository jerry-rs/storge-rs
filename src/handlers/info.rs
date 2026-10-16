//! GET /api/entries/info —— 文件 / 目录详细信息（含 SHA-256）
use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils;

#[derive(Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct DetailInfoQuery {
    /// 相对 files/ 的路径（可含子目录，含文件名或目录名）
    path: String,
}

#[derive(Serialize, ToSchema)]
pub struct DetailInfo {
    pub name: String,
    /// "file" | "dir"
    pub kind: String,
    /// 相对 files/ 的公开路径
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    pub created: u64,
    pub modified: u64,
    pub accessed: u64,
    /// 文件 SHA-256（hex）；目录无此字段
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// 查询单个文件或目录的详细信息。文件会返回 SHA-256（有 sidecar 则直接读，否则计算并缓存）。
#[utoipa::path(
    get,
    path = "/api/entries/info",
    tag = "entries",
    params(DetailInfoQuery),
    responses(
        (status = 200, description = "Entry details", body = DetailInfo),
        (status = 400, description = "Invalid path", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody)
    )
)]
pub async fn entry_info(
    State(state): State<AppState>,
    Query(q): Query<DetailInfoQuery>,
) -> Result<Json<DetailInfo>, AppError> {
    let files_dir = state.files_dir.clone();
    let rel = q.path.clone();
    let info = utils::blocking(move || info_sync(&files_dir, &rel)).await?;
    Ok(Json(info))
}

fn info_sync(files_dir: &std::path::Path, rel: &str) -> Result<DetailInfo, AppError> {
    let target = utils::resolve_file(files_dir, rel)?;
    let md = std::fs::symlink_metadata(&target).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::not_found("not found")
        } else {
            e.into()
        }
    })?;
    if md.is_symlink() {
        return Err(AppError::not_found("not found"));
    }

    let name = target
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let path = rel.trim().trim_matches('/').to_string();
    let (created, modified, accessed) = utils::entry_times(&md);

    if md.is_dir() {
        return Ok(DetailInfo {
            name,
            kind: "dir".into(),
            path,
            size: None,
            created,
            modified,
            accessed,
            sha256: None,
        });
    }
    if !md.is_file() {
        return Err(AppError::not_found("not found"));
    }

    let sha256 = utils::file_sha256_cached(&target)?;
    Ok(DetailInfo {
        name,
        kind: "file".into(),
        path,
        size: Some(md.len()),
        created,
        modified,
        accessed,
        sha256: Some(sha256),
    })
}
