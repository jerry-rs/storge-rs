//! GET /api/entries/disk —— 本磁盘（files_dir 所在卷）占用与余量
//!
//! 前端据此展示容量条、在点「Start Upload」前做余量预估。
//! **注意**：这个接口只用于展示与提示，真正的容量强制在
//! `init` / `chunk` / `merge` 里（见 `src/disk.rs`）——从展示到点击之间磁盘可能已被
//! 其他进程写满，服务端必须自己把关。
use axum::Json;
use axum::extract::State;
use serde::Serialize;
use std::time::Duration;
use utoipa::ToSchema;

use crate::disk::DiskSnapshot;
use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils;

/// statvfs 在 NFS / SMB / FUSE 上可能很慢甚至卡住：超时就用缓存值降级返回，
/// 不把一个只读展示接口变成前端的时间黑洞。
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Serialize, ToSchema)]
pub struct DiskResp {
    /// 卷总容量（字节）
    pub total: u64,
    /// 已用（total - free）
    pub used: u64,
    /// 非特权进程可用（POSIX f_bavail，已扣除 root 预留块）
    pub free: u64,
    /// 服务端判定的「还能安全接收」字节：已扣 min_free / 配额 / 在途上传预留
    pub available: u64,
    /// 已声明但尚未落盘的字节（稀疏 staging 在 `df` 里看不见的部分）
    pub reserved: u64,
    /// 有预留记录的上传会话数
    pub sessions: u64,
    /// 服务端强制保留的磁盘余量
    pub min_free: u64,
    /// 整盘已用量上限，0 = 不限制
    pub quota: u64,
    /// uploads_dir 与 files_dir 是否同一文件系统
    pub staging_same_volume: bool,
    /// 仅当临时目录跨卷时给出
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_free: Option<u64>,
    /// 最后一次成功采样的 unix 秒
    pub sampled_at: u64,
    /// true = 本次采样失败，返回的是过期缓存
    pub stale: bool,
}

impl From<DiskSnapshot> for DiskResp {
    fn from(s: DiskSnapshot) -> Self {
        Self {
            total: s.total,
            used: s.used,
            free: s.free,
            available: s.available,
            reserved: s.reserved,
            sessions: s.sessions,
            min_free: s.min_free,
            quota: s.quota,
            staging_same_volume: s.staging_same_volume,
            staging_total: s.staging_total,
            staging_free: s.staging_free,
            sampled_at: s.sampled_at,
            stale: s.stale,
        }
    }
}

/// 查询 files_dir 所在卷的容量余量
#[utoipa::path(
    get,
    path = "/api/entries/disk",
    tag = "entries",
    responses(
        (status = 200, description = "Disk capacity of the files_dir volume", body = DiskResp),
        (status = 503, description = "Disk capacity unavailable (statvfs failed on this filesystem)", body = ErrorBody)
    )
)]
pub async fn disk_info(State(state): State<AppState>) -> Result<Json<DiskResp>, AppError> {
    let meter = state.disk.clone();
    let task = {
        let meter = meter.clone();
        utils::blocking(move || Ok(meter.snapshot(true)))
    };
    let snap = match tokio::time::timeout(SAMPLE_TIMEOUT, task).await {
        Ok(res) => res?,
        Err(_) => {
            tracing::warn!("disk sampling timed out, serving cached snapshot");
            meter.stale_snapshot()
        }
    };
    match snap {
        Some(s) => Ok(Json(DiskResp::from(s))),
        None => Err(AppError::unavailable("disk capacity unavailable")),
    }
}
