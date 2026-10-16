use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::fmt;

/// 统一错误响应 `{"error": "..."}`；磁盘容量错误额外携带字节数供前端精确提示
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ErrorBody {
    pub error: String,
    /// 服务端当前可安全接收的字节数（仅 507）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub free: Option<u64>,
    /// 本次操作需要的字节数（仅 507）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<u64>,
}

#[derive(Debug)]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
    /// 磁盘容量类错误的上下文（字节）
    pub free: Option<u64>,
    /// 磁盘容量类错误的上下文（字节）
    pub required: Option<u64>,
}

impl AppError {
    fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self {
            status,
            message: msg.into(),
            free: None,
            required: None,
        }
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, msg)
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, msg)
    }

    /// 文件完整性校验失败
    pub fn integrity(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, msg)
    }

    /// 服务能力暂不可用（如 statvfs 报错）
    pub fn unavailable(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, msg)
    }

    /// 磁盘空间不足（容量守卫主动拒绝）
    pub fn disk_full(msg: impl Into<String>, free: u64, required: u64) -> Self {
        Self {
            status: StatusCode::INSUFFICIENT_STORAGE,
            message: msg.into(),
            free: Some(free),
            required: Some(required),
        }
    }

    /// 磁盘写满 / 配额耗尽（来自文件系统的 ENOSPC）
    pub fn storage_full(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INSUFFICIENT_STORAGE,
            message: msg.into(),
            free: Some(0),
            required: None,
        }
    }

    pub fn io(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::NotFound => Self::not_found("not found"),
            std::io::ErrorKind::AlreadyExists => Self::conflict("already exists"),
            std::io::ErrorKind::PermissionDenied => Self::new(StatusCode::FORBIDDEN, "permission denied"),
            std::io::ErrorKind::IsADirectory => Self::bad_request("is a directory"),
            std::io::ErrorKind::NotADirectory => Self::bad_request("not a directory"),
            std::io::ErrorKind::InvalidInput => Self::bad_request(format!("invalid input: {e}")),
            // 磁盘写满：必须与一般 500 区分开，否则前端会按“临时错误”重试 3 次，
            // 每次都白写一堆数据还反复弄脏 staging。
            std::io::ErrorKind::StorageFull => Self::storage_full("no space left on device"),
            _ => {
                // 兼容 kind 未映射的平台：ENOSPC(28) / ERROR_DISK_FULL(112)
                if matches!(e.raw_os_error(), Some(28) | Some(112)) {
                    Self::storage_full("no space left on device")
                } else {
                    Self::internal(format!("io error: {e}"))
                }
            }
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status.as_u16(), self.message)
    }
}

impl std::error::Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        Self::io(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::warn!(
            status = self.status.as_u16(),
            error = %self.message,
            "request failed"
        );
        let body = ErrorBody {
            error: self.message,
            free: self.free,
            required: self.required,
        };
        // 旧客户端只读 `error` 字段，新增字段均为可选，保持向后兼容
        (self.status, Json(json!(body))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enospc_maps_to_insufficient_storage() {
        // ENOSPC(28) / ERROR_DISK_FULL(112)：必须是 507，不能落到 500 让前端白重试
        for errno in [28, 112] {
            let e = AppError::io(std::io::Error::from_raw_os_error(errno));
            assert_eq!(e.status, StatusCode::INSUFFICIENT_STORAGE, "errno {errno}");
            assert_eq!(e.free, Some(0));
        }
        let other = AppError::io(std::io::Error::other("boom"));
        assert_eq!(other.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn error_body_keeps_legacy_shape_when_no_disk_context() {
        let v = serde_json::to_value(ErrorBody {
            error: "bad".into(),
            free: None,
            required: None,
        })
        .unwrap();
        assert_eq!(v, json!({ "error": "bad" }));

        let v = serde_json::to_value(ErrorBody {
            error: "full".into(),
            free: Some(10),
            required: Some(20),
        })
        .unwrap();
        assert_eq!(v, json!({ "error": "full", "free": 10, "required": 20 }));
    }
}
