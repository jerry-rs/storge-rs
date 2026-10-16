//! POST /api/entries/delete —— 删除文件或文件夹
use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils;

#[derive(Serialize, ToSchema)]
pub struct OkResp {
    pub ok: bool,
}

#[derive(Deserialize, ToSchema)]
pub struct DeleteReq {
    /// 父目录（相对 files/，省略或 "/" 表示根目录）
    #[serde(default)]
    path: Option<String>,
    /// 要删除的文件或文件夹名（单段，不允许 / 和 ..）
    name: String,
}

/// 删除指定目录下的文件或文件夹（目录递归删除；禁止删除隐藏文件）
#[utoipa::path(
    post,
    path = "/api/entries/delete",
    tag = "entries",
    request_body = DeleteReq,
    responses(
        (status = 200, description = "Deleted", body = OkResp),
        (status = 400, description = "Invalid path or unsupported type", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody)
    )
)]
pub async fn delete_entry(
    State(state): State<AppState>,
    Json(req): Json<DeleteReq>,
) -> Result<Json<OkResp>, AppError> {
    let name = req.name.trim().to_string();
    utils::validate_entry_name(&name)?;
    let parent_rel = utils::normalize_rel_dir(req.path.as_deref().unwrap_or(""))?;

    let value = utils::blocking(move || {
        let (_parent, target) =
            utils::resolve_parent_entry(&state.files_dir, &parent_rel, &name)?;
        let canon_target =
            std::fs::canonicalize(&target).map_err(|_| AppError::not_found("not found"))?;
        if !canon_target.starts_with(&state.files_dir) {
            return Err(AppError::bad_request("invalid path"));
        }
        let md = std::fs::symlink_metadata(&target)?;
        if md.is_symlink() || md.is_file() {
            std::fs::remove_file(&target)?;
            utils::remove_file_sha256(&target);
        } else if md.is_dir() {
            std::fs::remove_dir_all(&target)?;
        } else {
            return Err(AppError::bad_request("unsupported file type"));
        }
        tracing::info!(path = %canon_target.display(), "entry deleted");
        Ok(OkResp { ok: true })
    })
    .await?;
    Ok(Json(value))
}
