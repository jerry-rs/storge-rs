//! POST /api/entries/rename —— 重命名文件或文件夹
use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils;

#[derive(Serialize, ToSchema)]
pub struct RenameResp {
    pub ok: bool,
    pub name: String,
}

#[derive(Deserialize, ToSchema)]
pub struct RenameReq {
    /// 父目录（相对 files/，省略或 "/" 表示根目录）
    #[serde(default)]
    path: Option<String>,
    /// 原文件名或文件夹名（单段，不允许 / 和 ..）
    name: String,
    /// 新名称（单段，不允许 / 和 ..）
    new_name: String,
}

/// 重命名指定目录下的文件或文件夹（同目录内改名，不允许跨目录移动）
#[utoipa::path(
    post,
    path = "/api/entries/rename",
    tag = "entries",
    request_body = RenameReq,
    responses(
        (status = 200, description = "Renamed", body = RenameResp),
        (status = 400, description = "Invalid name", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
        (status = 409, description = "Name already exists", body = ErrorBody)
    )
)]
pub async fn rename_entry(
    State(state): State<AppState>,
    Json(req): Json<RenameReq>,
) -> Result<Json<RenameResp>, AppError> {
    let name = req.name.trim().to_string();
    let new_name = req.new_name.trim().to_string();
    utils::validate_entry_name(&name)?;
    utils::validate_entry_name(&new_name)?;
    let parent_rel = utils::normalize_rel_dir(req.path.as_deref().unwrap_or(""))?;

    let value = utils::blocking(move || {
        let src = utils::resolve_parent_entry(&state.files_dir, &parent_rel, &name)?.1;
        if name == new_name {
            return Ok(RenameResp {
                ok: true,
                name: new_name,
            });
        }
        let dst = utils::resolve_parent_entry(&state.files_dir, &parent_rel, &new_name)?.1;
        let src_md = std::fs::symlink_metadata(&src).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                AppError::not_found("not found")
            } else {
                e.into()
            }
        })?;
        if src_md.is_symlink() {
            return Err(AppError::not_found("not found"));
        }
        if std::fs::symlink_metadata(&dst).is_ok() {
            return Err(AppError::conflict("name already exists"));
        }
        std::fs::rename(&src, &dst)?;
        utils::rename_file_sha256(&src, &dst);
        tracing::info!(from = %src.display(), to = %dst.display(), "entry renamed");
        Ok(RenameResp {
            ok: true,
            name: new_name,
        })
    })
    .await?;
    Ok(Json(value))
}
