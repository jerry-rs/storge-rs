use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::fmt;

/// 统一错误响应 `{"error": "..."}`
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ErrorBody {
    pub error: String,
}
#[derive(Debug)]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
}

impl AppError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: msg.into(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }

    /// 文件完整性校验失败
    pub fn integrity(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: msg.into(),
        }
    }

    pub fn io(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::NotFound => Self::not_found("not found"),
            std::io::ErrorKind::AlreadyExists => Self::conflict("already exists"),
            std::io::ErrorKind::PermissionDenied => Self {
                status: StatusCode::FORBIDDEN,
                message: "permission denied".into(),
            },
            std::io::ErrorKind::IsADirectory => Self::bad_request("is a directory"),
            std::io::ErrorKind::NotADirectory => Self::bad_request("not a directory"),
            std::io::ErrorKind::InvalidInput => Self::bad_request(format!("invalid input: {e}")),
            _ => Self::internal(format!("io error: {e}")),
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
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}
