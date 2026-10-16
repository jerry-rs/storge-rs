//! POST /api/entries/mkdir —— 新建文件夹
use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils;

#[derive(Serialize, ToSchema)]
pub struct PathResp {
    pub path: String,
}

#[derive(Deserialize, ToSchema)]
pub struct MkdirReq {
    /// 父目录（相对 files/，省略或 "/" 表示根目录）
    #[serde(default)]
    path: Option<String>,
    /// 新文件夹名（单段，不允许 / 和 ..）
    name: String,
}

/// 在指定目录下新建文件夹
#[utoipa::path(
    post,
    path = "/api/entries/mkdir",
    tag = "entries",
    request_body = MkdirReq,
    responses(
        (status = 200, description = "Folder created", body = PathResp),
        (status = 400, description = "Invalid name", body = ErrorBody),
        (status = 404, description = "Parent directory not found", body = ErrorBody),
        (status = 409, description = "Folder already exists", body = ErrorBody)
    )
)]
pub async fn make_dir_entry(
    State(state): State<AppState>,
    Json(req): Json<MkdirReq>,
) -> Result<Json<PathResp>, AppError> {
    let name = req.name.trim().to_string();
    utils::validate_entry_name(&name)?;
    let parent_rel = utils::normalize_rel_dir(req.path.as_deref().unwrap_or(""))?;

    let value = utils::blocking(move || {
        let (_parent, target) =
            utils::resolve_parent_entry(&state.files_dir, &parent_rel, &name)?;
        match std::fs::create_dir(&target) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(AppError::conflict("folder already exists"));
            }
            Err(e) => return Err(e.into()),
        }
        tracing::info!(path = %target.display(), "folder created");
        let path = utils::public_file_path(&parent_rel, &name);
        Ok(PathResp { path })
    })
    .await?;
    Ok(Json(value))
}
