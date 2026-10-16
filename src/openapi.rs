use axum::Router;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::error::ErrorBody;
use crate::handlers;
use crate::handlers::delete::OkResp;
use crate::handlers::list::FileEntry;
use crate::handlers::mkdir::PathResp;
use crate::handlers::rename::RenameResp;
use crate::handlers::upload::{
    ChunkQuery, ChunkResp, InitReq, InitResp, MergeReq, MergeResp, StatusQuery, StatusResp,
};
use crate::state::AppState;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Storge",
        description = "Chunked upload and file/directory management API. Send `Authorization: Bearer storge`.",
        version = "0.1.0"
    ),
    paths(
        handlers::upload::init_upload,
        handlers::upload::upload_chunk,
        handlers::upload::merge_upload,
        handlers::upload::upload_status,
        handlers::list::list_entry,
        handlers::disk::disk_info,
        handlers::info::entry_info,
        handlers::mkdir::make_dir_entry,
        handlers::rename::rename_entry,
        handlers::download::download_entry,
        handlers::delete::delete_entry,
    ),
    components(schemas(
        InitReq,
        InitResp,
        ChunkQuery,
        ChunkResp,
        MergeReq,
        MergeResp,
        StatusQuery,
        StatusResp,
        FileEntry,
        handlers::disk::DiskResp,
        handlers::info::DetailInfo,
        PathResp,
        RenameResp,
        OkResp,
        ErrorBody,
        handlers::list::ListQuery,
        handlers::info::DetailInfoQuery,
        handlers::mkdir::MkdirReq,
        handlers::rename::RenameReq,
        handlers::delete::DeleteReq,
        handlers::download::DownloadQuery,
    )),
    tags(
        (name = "upload", description = "Chunked upload (init / chunk / merge / status)"),
        (name = "entries", description = "List, info, mkdir, rename, download, delete")
    )
)]
pub struct ApiDoc;

pub fn swagger_ui() -> Router<AppState> {
    SwaggerUi::new("/swagger-ui")
        .url("/api-docs/openapi.json", ApiDoc::openapi())
        .into()
}
