//! GET /api/entries —— 列出目录内容
use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};
use std::path::Path;
use utoipa::{IntoParams, ToSchema};

use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils;

/// GET /api/entries 的单条记录（文件或子目录），字段与前端表格列一一对应
#[derive(Serialize, ToSchema)]
pub struct FileEntry {
    pub name: String,
    /// "file" | "dir"（Type 列）
    pub kind: String,
    /// 文件大小（Size 列；目录无）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    pub created: u64,  // unix 秒（Created 列）
    pub modified: u64, // unix 秒（Modified 列）
    pub accessed: u64, // unix 秒（Accessed 列）
}

#[derive(Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct ListQuery {
    /// 相对 files/ 的子目录路径（省略或 "/" 表示根目录）
    #[serde(default)]
    path: Option<String>,
}

/// 列出指定目录下的文件与子目录（支持递归浏览）
#[utoipa::path(
    get,
    path = "/api/entries",
    tag = "entries",
    params(ListQuery),
    responses(
        (status = 200, description = "Directory listing", body = Vec<FileEntry>),
        (status = 400, description = "Not a directory", body = ErrorBody),
        (status = 404, description = "Directory not found", body = ErrorBody)
    )
)]
pub async fn list_entry(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<FileEntry>>, AppError> {
    let path = q.path.unwrap_or_default();
    let entries = utils::blocking(move || list_dir(&state.files_dir, &path)).await?;
    Ok(Json(entries))
}

/// 列出指定目录下的文件与子目录（递归浏览），跳过隐藏文件、sidecar 与符号链接
fn list_dir(files_dir: &Path, rel: &str) -> Result<Vec<FileEntry>, AppError> {
    let target = utils::resolve_subdir(files_dir, rel)?;
    if !target.exists() {
        return Err(AppError::not_found("directory not found"));
    }
    let md = std::fs::symlink_metadata(&target)?;
    if md.is_symlink() || !md.is_dir() {
        return Err(AppError::bad_request("not a directory"));
    }
    let mut v: Vec<FileEntry> = Vec::new();
    for e in std::fs::read_dir(&target)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        let ft = e.file_type()?;
        if ft.is_symlink() {
            continue;
        }
        let md = e.metadata()?;
        let (created, modified, accessed) = utils::entry_times(&md);
        if md.is_dir() {
            v.push(FileEntry {
                name,
                kind: "dir".into(),
                size: None,
                created,
                modified,
                accessed,
            });
        } else if md.is_file() {
            v.push(FileEntry {
                name,
                kind: "file".into(),
                size: Some(md.len()),
                created,
                modified,
                accessed,
            });
        }
    }
    v.sort_by(|a, b| {
        (a.kind != "dir", std::cmp::Reverse(a.modified), a.name.to_lowercase()).cmp(&(
            b.kind != "dir",
            std::cmp::Reverse(b.modified),
            b.name.to_lowercase(),
        ))
    });
    Ok(v)
}
