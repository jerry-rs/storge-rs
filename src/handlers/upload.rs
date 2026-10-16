use axum::Json;
use axum::extract::{Query, State};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use utoipa::{IntoParams, ToSchema};

use crate::error::{AppError, ErrorBody};
use crate::state::AppState;
use crate::utils::{self, validate_upload_sizes};

#[derive(Deserialize, ToSchema)]
pub struct InitReq {
    pub upload_id: String,
    pub filename: String,
    pub file_size: u64,
    pub chunk_size: usize,
    /// 合并后保存到的目标子目录（相对 files/，省略或 "/" 表示根目录）
    #[serde(default)]
    pub target_dir: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct InitResp {
    pub upload_id: String,
    /// 已上传的分片下标（用于断点续传）
    pub uploaded: Vec<usize>,
    pub total_chunks: u64,
    pub merged: bool,
}

#[derive(Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct ChunkQuery {
    pub upload_id: String,
    pub index: usize,
}

#[derive(Serialize, ToSchema)]
pub struct ChunkResp {
    pub received: usize,
    pub total_chunks: u64,
    pub complete: bool,
}

#[derive(Deserialize, ToSchema)]
pub struct MergeReq {
    pub upload_id: String,
    /// 客户端对原文件计算的 SHA-256（hex），用于合并后完整性校验
    #[serde(default)]
    pub sha256: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct MergeResp {
    pub filename: String,
    pub file_size: u64,
    pub path: String,
    /// 服务端对合并后文件计算的 SHA-256
    pub sha256: String,
    /// 是否与客户端提供的哈希一致（客户端未提供时为 false）
    pub verified: bool,
}

#[derive(Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct StatusQuery {
    pub upload_id: String,
}

#[derive(Serialize, ToSchema)]
pub struct StatusResp {
    pub uploaded: Vec<usize>,
    pub total_chunks: u64,
    pub filename: Option<String>,
    pub merged: bool,
}

impl InitReq {
    /// 校验请求参数，防止路径穿越 / 非法大小
    pub fn validate(&self) -> Result<(), String> {
        if !valid_id(&self.upload_id) {
            return Err("invalid upload_id".into());
        }
        if utils::validate_entry_name(&self.filename).is_err() {
            return Err("invalid filename".into());
        }
        validate_upload_sizes(self.file_size, self.chunk_size).map(|_| ())
    }

    pub fn total_chunks(&self) -> u64 {
        crate::utils::total_chunks(self.file_size, self.chunk_size)
    }
}

#[derive(Serialize, Deserialize)]
struct Meta {
    filename: String,
    file_size: u64,
    chunk_size: usize,
    total_chunks: u64,
    /// 目标子目录（相对 files/，已规范化，空串 = 根目录）
    #[serde(default)]
    target_dir: String,
}

impl Meta {
    fn validate(&self) -> Result<(), AppError> {
        utils::validate_entry_name(&self.filename)
            .map_err(|_| AppError::bad_request("invalid filename in metadata"))?;
        let expected = validate_upload_sizes(self.file_size, self.chunk_size)
            .map_err(AppError::bad_request)?;
        if self.total_chunks != expected {
            return Err(AppError::bad_request("corrupt upload metadata"));
        }
        utils::normalize_rel_dir(&self.target_dir)?;
        Ok(())
    }

    /// 第 index 片（0 起）的期望字节数
    fn expected_chunk_len(&self, index: u64) -> u64 {
        if self.total_chunks == 0 || self.chunk_size == 0 || index >= self.total_chunks {
            return 0;
        }
        if index + 1 == self.total_chunks {
            let r = self.file_size % self.chunk_size as u64;
            return if r == 0 { self.chunk_size as u64 } else { r };
        }
        self.chunk_size as u64
    }

    fn chunk_offset(&self, index: u64) -> u64 {
        index.saturating_mul(self.chunk_size as u64)
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn ensure_valid_upload_id(upload_id: &str) -> Result<(), AppError> {
    if !valid_id(upload_id) {
        return Err(AppError::bad_request("invalid upload_id"));
    }
    Ok(())
}

fn meta_path(uploads_dir: &Path, upload_id: &str) -> PathBuf {
    uploads_dir.join(upload_id).join("meta.json")
}

fn data_path(uploads_dir: &Path, upload_id: &str) -> PathBuf {
    uploads_dir.join(upload_id).join("data.part")
}

fn done_path(uploads_dir: &Path, upload_id: &str) -> PathBuf {
    uploads_dir.join(upload_id).join("done.bin")
}

fn load_meta(uploads_dir: &Path, upload_id: &str) -> Result<Option<Meta>, AppError> {
    let p = meta_path(uploads_dir, upload_id);
    match std::fs::read_to_string(&p) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
        Ok(s) => {
            let meta: Meta = serde_json::from_str(&s)
                .map_err(|_| AppError::bad_request("corrupt upload metadata"))?;
            meta.validate()?;
            Ok(Some(meta))
        }
    }
}

fn save_meta(uploads_dir: &Path, upload_id: &str, meta: &Meta) -> std::io::Result<()> {
    let json = serde_json::to_string(meta)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    utils::atomic_write(&meta_path(uploads_dir, upload_id), json)
}

/// 完成位图：bit i 表示第 i 片已完整写入 `data.part`。
struct DoneBits(Vec<u8>);

impl DoneBits {
    fn byte_len(total: u64) -> usize {
        total.div_ceil(8) as usize
    }

    fn new(total: u64) -> Self {
        Self(vec![0u8; Self::byte_len(total)])
    }

    fn load(uploads_dir: &Path, upload_id: &str, total: u64) -> std::io::Result<Self> {
        let expected = Self::byte_len(total);
        match std::fs::read(done_path(uploads_dir, upload_id)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new(total)),
            Err(e) => Err(e),
            Ok(mut v) => {
                v.resize(expected, 0);
                Ok(Self(v))
            }
        }
    }

    fn save(&self, uploads_dir: &Path, upload_id: &str) -> std::io::Result<()> {
        utils::atomic_write(&done_path(uploads_dir, upload_id), &self.0)
    }

    fn get(&self, index: u64) -> bool {
        let i = index as usize;
        let b = i / 8;
        b < self.0.len() && (self.0[b] & (1 << (i % 8))) != 0
    }

    fn set(&mut self, index: u64) {
        let i = index as usize;
        let b = i / 8;
        if b < self.0.len() {
            self.0[b] |= 1 << (i % 8);
        }
    }

    fn filled_count(&self, total: u64) -> u64 {
        if total == 0 || self.0.is_empty() {
            return 0;
        }
        let full_bytes = (total / 8) as usize;
        let mut n = self.0[..full_bytes.min(self.0.len())]
            .iter()
            .map(|b| b.count_ones() as u64)
            .sum();
        let rem = total % 8;
        if rem > 0 && full_bytes < self.0.len() {
            let mask = (1u8 << rem) - 1;
            n += (self.0[full_bytes] & mask).count_ones() as u64;
        }
        n
    }

    fn indices(&self, total: u64) -> Vec<usize> {
        let mut v = Vec::with_capacity(self.filled_count(total) as usize);
        for i in 0..total {
            if self.get(i) {
                v.push(i as usize);
            }
        }
        v
    }
}

fn scan_uploaded(uploads_dir: &Path, upload_id: &str, meta: &Meta) -> Vec<usize> {
    DoneBits::load(uploads_dir, upload_id, meta.total_chunks)
        .map(|b| b.indices(meta.total_chunks))
        .unwrap_or_default()
}

fn open_staging(path: &Path, file_size: u64) -> std::io::Result<std::fs::File> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    if f.metadata()?.len() != file_size {
        f.set_len(file_size)?;
    }
    Ok(f)
}

fn write_chunk(
    uploads_dir: &Path,
    upload_id: &str,
    meta: &Meta,
    index: u64,
    data: &[u8],
    bit_lock: &std::sync::Mutex<()>,
) -> Result<(), AppError> {
    let expected = meta.expected_chunk_len(index);
    if data.len() as u64 != expected {
        return Err(AppError::bad_request(format!(
            "chunk size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }

    {
        let _g = bit_lock.lock().unwrap_or_else(|p| p.into_inner());
        let bits = DoneBits::load(uploads_dir, upload_id, meta.total_chunks)?;
        if bits.get(index) {
            return Ok(());
        }
    }

    let mut f = open_staging(&data_path(uploads_dir, upload_id), meta.file_size)?;
    f.seek(SeekFrom::Start(meta.chunk_offset(index)))?;
    f.write_all(data)?;
    f.sync_data()?;
    drop(f);

    let _g = bit_lock.lock().unwrap_or_else(|p| p.into_inner());
    let mut bits = DoneBits::load(uploads_dir, upload_id, meta.total_chunks)?;
    bits.set(index);
    bits.save(uploads_dir, upload_id)?;
    Ok(())
}

fn range_fully_covered(old: &Meta, bits: &DoneBits, start: u64, len: u64) -> bool {
    if len == 0 || old.chunk_size == 0 {
        return false;
    }
    let end = start + len;
    let mut off = start;
    while off < end {
        let i = off / old.chunk_size as u64;
        if i >= old.total_chunks || !bits.get(i) {
            return false;
        }
        off = (i + 1).saturating_mul(old.chunk_size as u64);
    }
    true
}

fn merged_file_path(state: &AppState, meta: &Meta) -> Result<PathBuf, AppError> {
    let rel = utils::public_file_path(&meta.target_dir, &meta.filename);
    utils::resolve_file(&state.files_dir, &rel)
}

/// 分片大小变化时只重映射完成位图：`data.part` 的字节布局不变。
fn rechunk(
    uploads_dir: &Path,
    upload_id: &str,
    old_meta: &Meta,
    new_chunk_size: usize,
) -> std::io::Result<Vec<usize>> {
    if new_chunk_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "chunk_size must be > 0",
        ));
    }
    let old_bits = DoneBits::load(uploads_dir, upload_id, old_meta.total_chunks)?;
    if new_chunk_size == old_meta.chunk_size {
        return Ok(old_bits.indices(old_meta.total_chunks));
    }

    let new_total = crate::utils::total_chunks(old_meta.file_size, new_chunk_size);
    let new_meta = Meta {
        chunk_size: new_chunk_size,
        total_chunks: new_total,
        filename: old_meta.filename.clone(),
        file_size: old_meta.file_size,
        target_dir: old_meta.target_dir.clone(),
    };
    let mut new_bits = DoneBits::new(new_total);
    for j in 0..new_total {
        let start = new_meta.chunk_offset(j);
        let len = new_meta.expected_chunk_len(j);
        if range_fully_covered(old_meta, &old_bits, start, len) {
            new_bits.set(j);
        }
    }
    new_bits.save(uploads_dir, upload_id)?;
    save_meta(uploads_dir, upload_id, &new_meta)?;
    Ok(new_bits.indices(new_total))
}

fn dest_exists_as_file(state: &AppState, meta: &Meta) -> bool {
    merged_file_path(state, meta)
        .ok()
        .is_some_and(|p| utils::is_regular_file(&p))
}

fn hash_staging(path: &Path, expected_size: u64) -> Result<(u64, String), AppError> {
    let md = std::fs::symlink_metadata(path)?;
    if !md.is_file() {
        return Err(AppError::internal("staging file missing"));
    }
    if md.len() != expected_size {
        return Err(AppError::internal(format!(
            "merged size mismatch: expected {expected_size}, got {}",
            md.len()
        )));
    }
    let f = OpenOptions::new().read(true).write(true).open(path)?;
    f.sync_all()?;
    drop(f);
    Ok((md.len(), utils::sha256_file(path)?))
}

fn split_ext(name: &str) -> (String, String) {
    match name.rfind('.') {
        Some(i) if i > 0 && i + 1 < name.len() => (name[..i].to_string(), name[i..].to_string()),
        _ => (name.to_string(), String::new()),
    }
}

fn reserve_unique_path(dir: &Path, filename: &str) -> Result<PathBuf, AppError> {
    let (stem, ext) = split_ext(filename);
    let mut n = 0u32;
    loop {
        let candidate = if n == 0 {
            dir.join(filename)
        } else {
            dir.join(format!("{stem} ({n}){ext}"))
        };
        match std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) if candidate.exists() => {}
            Err(e) => return Err(e.into()),
        }
        n += 1;
        if n > 100_000 {
            return Err(AppError::internal("too many filename collisions"));
        }
    }
}

/// 初始化上传：返回已上传分片列表（断点续传依据）
#[utoipa::path(
    post,
    path = "/api/entries/upload/init",
    tag = "upload",
    request_body = InitReq,
    responses(
        (status = 200, description = "Upload session initialized or resumed", body = InitResp),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 409, description = "upload_id bound to another file", body = ErrorBody)
    )
)]
pub async fn init_upload(
    State(state): State<AppState>,
    Json(req): Json<InitReq>,
) -> Result<Json<InitResp>, AppError> {
    req.validate().map_err(AppError::bad_request)?;
    let target_dir = utils::normalize_rel_dir(req.target_dir.as_deref().unwrap_or(""))?;

    let _upload_guard = state.lock_upload_write(&req.upload_id).await;
    let _permit = state
        .io_slots
        .acquire()
        .await
        .map_err(|_| AppError::internal("server shutting down"))?;

    let state = state.clone();
    let resp = utils::blocking(move || init_sync(state, req, target_dir)).await?;
    Ok(Json(resp))
}

fn init_sync(state: AppState, req: InitReq, target_dir: String) -> Result<InitResp, AppError> {
    let dir = state.uploads_dir.join(&req.upload_id);
    std::fs::create_dir_all(&dir)?;

    let (uploaded, merged) =
        if let Some(mut meta) = load_meta(&state.uploads_dir, &req.upload_id)? {
            if meta.file_size != req.file_size || meta.filename != req.filename {
                tracing::warn!(
                    upload_id = %req.upload_id,
                    "upload_id conflicts with existing metadata, resume rejected"
                );
                return Err(AppError::conflict(
                    "upload_id is bound to another file, please re-initialize",
                ));
            }
            if meta.target_dir != target_dir {
                meta.target_dir = target_dir.clone();
                save_meta(&state.uploads_dir, &req.upload_id, &meta)?;
            }
            let uploaded = if meta.chunk_size != req.chunk_size {
                tracing::info!(
                    upload_id = %req.upload_id,
                    old_chunk_size = meta.chunk_size,
                    new_chunk_size = req.chunk_size,
                    "chunk size changed, re-chunking uploaded data"
                );
                rechunk(&state.uploads_dir, &req.upload_id, &meta, req.chunk_size)?
            } else {
                scan_uploaded(&state.uploads_dir, &req.upload_id, &meta)
            };
            let merged = dest_exists_as_file(&state, &meta);
            (uploaded, merged)
        } else {
            let meta = Meta {
                filename: req.filename.clone(),
                file_size: req.file_size,
                chunk_size: req.chunk_size,
                total_chunks: req.total_chunks(),
                target_dir: target_dir.clone(),
            };
            open_staging(&data_path(&state.uploads_dir, &req.upload_id), req.file_size)?;
            DoneBits::new(meta.total_chunks).save(&state.uploads_dir, &req.upload_id)?;
            save_meta(&state.uploads_dir, &req.upload_id, &meta)?;
            (Vec::new(), false)
        };

    let total_chunks = req.total_chunks();
    if uploaded.is_empty() {
        tracing::info!(
            upload_id = %req.upload_id,
            filename = %req.filename,
            target_dir = %if target_dir.is_empty() { "/" } else { target_dir.as_str() },
            file_size = req.file_size,
            chunk_size = req.chunk_size,
            total_chunks,
            "upload initialized"
        );
    } else {
        tracing::info!(
            upload_id = %req.upload_id,
            filename = %req.filename,
            resumed_chunks = uploaded.len(),
            total_chunks,
            "resuming upload, skipping already-uploaded chunks"
        );
    }
    Ok(InitResp {
        upload_id: req.upload_id,
        uploaded,
        total_chunks,
        merged,
    })
}

/// 上传单个分片：body 为原始二进制，upload_id / index 通过 query 传递
#[utoipa::path(
    post,
    path = "/api/entries/upload/chunk",
    tag = "upload",
    params(ChunkQuery),
    request_body(
        content = Vec<u8>,
        content_type = "application/octet-stream",
        description = "Raw chunk bytes; length must match the expected size for this index"
    ),
    responses(
        (status = 200, description = "Chunk accepted", body = ChunkResp),
        (status = 400, description = "Invalid chunk or upload not found", body = ErrorBody)
    )
)]
pub async fn upload_chunk(
    State(state): State<AppState>,
    Query(q): Query<ChunkQuery>,
    body: Bytes,
) -> Result<Json<ChunkResp>, AppError> {
    ensure_valid_upload_id(&q.upload_id)?;

    let _upload_guard = state.lock_upload_read(&q.upload_id).await;
    let _permit = state
        .io_slots
        .acquire()
        .await
        .map_err(|_| AppError::internal("server shutting down"))?;

    let state = state.clone();
    let bit_lock = state.lock_done_bits(&q.upload_id);
    let upload_id = q.upload_id;
    let index = q.index;
    let total_chunks = utils::blocking(move || {
        let meta = load_meta(&state.uploads_dir, &upload_id)?
            .ok_or_else(|| AppError::bad_request("upload not found, please call init first"))?;

        if (index as u64) >= meta.total_chunks {
            return Err(AppError::bad_request("index out of range"));
        }
        write_chunk(
            &state.uploads_dir,
            &upload_id,
            &meta,
            index as u64,
            &body,
            &bit_lock,
        )?;

        tracing::debug!(
            upload_id = %upload_id,
            index,
            bytes = body.len(),
            "chunk written"
        );
        Ok(meta.total_chunks)
    })
    .await?;

    Ok(Json(ChunkResp {
        received: index,
        total_chunks,
        complete: false,
    }))
}

/// 合并分片为最终文件
#[utoipa::path(
    post,
    path = "/api/entries/upload/merge",
    tag = "upload",
    request_body = MergeReq,
    responses(
        (status = 200, description = "Chunks merged", body = MergeResp),
        (status = 400, description = "Incomplete chunks or invalid request", body = ErrorBody),
        (status = 422, description = "Integrity check failed", body = ErrorBody)
    )
)]
pub async fn merge_upload(
    State(state): State<AppState>,
    Json(req): Json<MergeReq>,
) -> Result<Json<MergeResp>, AppError> {
    ensure_valid_upload_id(&req.upload_id)?;
    let expected_sha = match req.sha256.as_deref() {
        Some(s) => Some(parse_sha256(s)?),
        None => None,
    };

    let _upload_guard = state.lock_upload_write(&req.upload_id).await;
    let _permit = state
        .io_slots
        .acquire()
        .await
        .map_err(|_| AppError::internal("server shutting down"))?;

    let state = state.clone();
    let resp = utils::blocking(move || merge_sync(state, req.upload_id, expected_sha)).await?;
    Ok(Json(resp))
}

fn merge_sync(
    state: AppState,
    upload_id: String,
    expected_sha: Option<String>,
) -> Result<MergeResp, AppError> {
    let meta = load_meta(&state.uploads_dir, &upload_id)?
        .ok_or_else(|| AppError::bad_request("upload not found, please call init first"))?;

    let dest_path = merged_file_path(&state, &meta)?;
    if utils::is_regular_file(&dest_path) {
        let actual = utils::sha256_file(&dest_path)?;
        let idempotent = expected_sha
            .as_ref()
            .is_some_and(|c| c.eq_ignore_ascii_case(&actual));
        if idempotent {
            std::fs::remove_dir_all(state.uploads_dir.join(&upload_id)).ok();
            if let Err(e) = utils::write_file_sha256(&dest_path, &actual, meta.file_size) {
                tracing::warn!(error = %e, path = %dest_path.display(), "failed to write sha256 sidecar");
            }
            return Ok(MergeResp {
                filename: meta.filename.clone(),
                file_size: meta.file_size,
                path: utils::public_file_path(&meta.target_dir, &meta.filename),
                sha256: actual,
                verified: true,
            });
        }
        tracing::info!(
            upload_id = %upload_id,
            filename = %meta.filename,
            "filename collision, will merge under a unique name"
        );
    }

    let bits = DoneBits::load(&state.uploads_dir, &upload_id, meta.total_chunks)?;
    if bits.filled_count(meta.total_chunks) != meta.total_chunks {
        let mut missing = Vec::new();
        for i in 0..meta.total_chunks {
            if !bits.get(i) {
                missing.push(i.to_string());
                if missing.len() >= 5 {
                    break;
                }
            }
        }
        tracing::warn!(
            upload_id = %upload_id,
            missing = %missing.join(", "),
            "incomplete chunks, merge rejected"
        );
        return Err(AppError::bad_request(format!(
            "incomplete chunks, missing: {}",
            missing.join(", ")
        )));
    }

    let target = dest_path
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| AppError::internal("invalid destination path"))?;
    std::fs::create_dir_all(&target)?;

    let staging = data_path(&state.uploads_dir, &upload_id);
    let (written, actual) = hash_staging(&staging, meta.file_size)?;
    if written != meta.file_size {
        return Err(AppError::internal(format!(
            "merged size mismatch: expected {}, got {written}",
            meta.file_size
        )));
    }

    // 必须在挪入 files/ 之前比对：失败则清空完成位图，保留 staging 供重传覆盖。
    // 若先 replace 再失败，data.part 已消失但 done.bin 仍全 1，续传会跳过所有分片并再次合并出错误内容。
    let verified = match &expected_sha {
        Some(expected) if expected == &actual => true,
        Some(expected) => {
            let _ = DoneBits::new(meta.total_chunks).save(&state.uploads_dir, &upload_id);
            tracing::error!(
                upload_id = %upload_id,
                expected = %expected,
                actual = %actual,
                "integrity check failed, staging kept for re-upload"
            );
            return Err(AppError::integrity(format!(
                "integrity check failed: expected {expected}, got {actual}"
            )));
        }
        None => false,
    };

    let reserved = reserve_unique_path(&target, &meta.filename)?;
    if let Err(e) = utils::atomic_replace(&staging, &reserved) {
        let _ = std::fs::remove_file(&reserved);
        return Err(e.into());
    }
    let final_path = reserved;

    std::fs::remove_dir_all(state.uploads_dir.join(&upload_id)).ok();

    let filename = final_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| meta.filename.clone());

    tracing::info!(
        upload_id = %upload_id,
        filename = %filename,
        file_size = meta.file_size,
        sha256 = %actual,
        verified,
        path = %final_path.display(),
        "chunks merged"
    );

    let path = utils::public_file_path(&meta.target_dir, &filename);

    if let Err(e) = utils::write_file_sha256(&final_path, &actual, meta.file_size) {
        tracing::warn!(error = %e, path = %final_path.display(), "failed to write sha256 sidecar");
    }

    Ok(MergeResp {
        filename,
        file_size: meta.file_size,
        path,
        sha256: actual,
        verified,
    })
}

fn parse_sha256(s: &str) -> Result<String, AppError> {
    let s = s.to_ascii_lowercase();
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(AppError::bad_request("sha256 must be a 64-char hex string"));
    }
    Ok(s)
}

/// 查询上传进度
#[utoipa::path(
    get,
    path = "/api/entries/upload/status",
    tag = "upload",
    params(StatusQuery),
    responses(
        (status = 200, description = "Upload progress", body = StatusResp),
        (status = 400, description = "Invalid upload_id", body = ErrorBody)
    )
)]
pub async fn upload_status(
    State(state): State<AppState>,
    Query(q): Query<StatusQuery>,
) -> Result<Json<StatusResp>, AppError> {
    ensure_valid_upload_id(&q.upload_id)?;
    let _upload_guard = state.lock_upload_read(&q.upload_id).await;
    let state = state.clone();
    let upload_id = q.upload_id;
    let resp = utils::blocking(move || {
        let Some(meta) = load_meta(&state.uploads_dir, &upload_id)? else {
            return Ok(StatusResp {
                uploaded: Vec::new(),
                total_chunks: 0,
                filename: None,
                merged: false,
            });
        };

        let uploaded = scan_uploaded(&state.uploads_dir, &upload_id, &meta);
        let merged = dest_exists_as_file(&state, &meta);

        Ok(StatusResp {
            uploaded,
            total_chunks: meta.total_chunks,
            filename: Some(meta.filename),
            merged,
        })
    })
    .await?;
    Ok(Json(resp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{MAX_CHUNK_SIZE, MAX_FILE_SIZE, MIN_CHUNK_SIZE, total_chunks};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn init(file_size: u64, chunk_size: usize) -> InitReq {
        InitReq {
            upload_id: "abc".into(),
            filename: "a.bin".into(),
            file_size,
            chunk_size,
            target_dir: None,
        }
    }

    fn tmp_dir() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "storge-upload-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn meta(file_size: u64, chunk_size: usize) -> Meta {
        Meta {
            filename: "a.bin".into(),
            file_size,
            chunk_size,
            total_chunks: total_chunks(file_size, chunk_size),
            target_dir: String::new(),
        }
    }

    #[test]
    fn size_and_chunk_count_limits() {
        assert!(init(1, MIN_CHUNK_SIZE).validate().is_ok());
        assert!(init(MAX_FILE_SIZE, MAX_CHUNK_SIZE).validate().is_ok());
        assert!(init(0, MIN_CHUNK_SIZE).validate().is_err());
        assert!(init(MAX_FILE_SIZE + 1, MAX_CHUNK_SIZE).validate().is_err());
        assert!(init(1, MIN_CHUNK_SIZE - 1).validate().is_err());
        assert!(init(1, MAX_CHUNK_SIZE + 1).validate().is_err());
        let over_500g = 500u64 * 1024 * 1024 * 1024 + 1;
        assert!(init(over_500g, MAX_CHUNK_SIZE).validate().is_ok());
        assert!(init(MAX_FILE_SIZE, MIN_CHUNK_SIZE).validate().is_err());
    }

    #[test]
    fn valid_id_rejects_path_chars() {
        assert!(valid_id("abc-123"));
        assert!(!valid_id(""));
        assert!(!valid_id("../x"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id(&"a".repeat(65)));
    }

    fn mark_done(root: &Path, id: &str, m: &Meta, indices: &[u64]) {
        let mut bits = DoneBits::new(m.total_chunks);
        for &i in indices {
            bits.set(i);
        }
        bits.save(root, id).unwrap();
    }

    #[test]
    fn rechunk_ignores_holes() {
        let root = tmp_dir();
        let id = "u1";
        std::fs::create_dir_all(root.join(id)).unwrap();
        let old = meta(1000, 100);
        mark_done(&root, id, &old, &[0, 1, 5]);
        let uploaded = rechunk(&root, id, &old, 250).unwrap();
        assert!(uploaded.is_empty());
        let raw = std::fs::read_to_string(meta_path(&root, id)).unwrap();
        let new_meta: Meta = serde_json::from_str(&raw).unwrap();
        assert_eq!(new_meta.chunk_size, 250);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rechunk_remaps_contiguous_prefix() {
        let root = tmp_dir();
        let id = "u2";
        std::fs::create_dir_all(root.join(id)).unwrap();
        let old = meta(1000, 100);
        mark_done(&root, id, &old, &[0, 1, 2, 3]);
        let uploaded = rechunk(&root, id, &old, 250).unwrap();
        assert_eq!(uploaded, vec![0]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn expected_chunk_len_last_chunk() {
        let m = meta(250, 100);
        assert_eq!(m.expected_chunk_len(0), 100);
        assert_eq!(m.expected_chunk_len(1), 100);
        assert_eq!(m.expected_chunk_len(2), 50);
        let exact = meta(200, 100);
        assert_eq!(exact.expected_chunk_len(1), 100);
    }

    #[test]
    fn done_bits_roundtrip() {
        let root = tmp_dir();
        let id = "bits";
        std::fs::create_dir_all(root.join(id)).unwrap();
        let mut bits = DoneBits::new(10);
        bits.set(0);
        bits.set(9);
        bits.save(&root, id).unwrap();
        let loaded = DoneBits::load(&root, id, 10).unwrap();
        assert!(loaded.get(0));
        assert!(!loaded.get(1));
        assert!(loaded.get(9));
        assert_eq!(loaded.filled_count(10), 2);
        assert_eq!(loaded.indices(10), vec![0, 9]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rechunk_does_not_rewrite_staging_bytes() {
        let root = tmp_dir();
        let id = "u3";
        std::fs::create_dir_all(root.join(id)).unwrap();
        let old = meta(1000, 100);
        let lock = std::sync::Mutex::new(());
        for i in 0..4u64 {
            write_chunk(
                &root,
                id,
                &old,
                i,
                &vec![i as u8; old.expected_chunk_len(i) as usize],
                &lock,
            )
            .unwrap();
        }
        let before = std::fs::read(data_path(&root, id)).unwrap();
        let uploaded = rechunk(&root, id, &old, 250).unwrap();
        assert_eq!(uploaded, vec![0]);
        assert_eq!(std::fs::read(data_path(&root, id)).unwrap(), before);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn reserve_skips_existing_directory() {
        let root = tmp_dir();
        std::fs::create_dir(root.join("a.bin")).unwrap();
        let p = reserve_unique_path(&root, "a.bin").unwrap();
        assert_eq!(p.file_name().unwrap(), "a (1).bin");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn staging_write_and_hash() {
        let root = tmp_dir();
        let id = "u4";
        std::fs::create_dir_all(root.join(id)).unwrap();
        let m = meta(6, 4);
        let lock = std::sync::Mutex::new(());
        write_chunk(&root, id, &m, 0, b"abcd", &lock).unwrap();
        write_chunk(&root, id, &m, 1, b"ef", &lock).unwrap();
        let p = data_path(&root, id);
        let (n, hash) = hash_staging(&p, 6).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&std::fs::read(&p).unwrap()[..6], b"abcdef");
        assert_eq!(hash, utils::sha256_file(&p).unwrap());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn integrity_mismatch_keeps_staging_and_clears_bits() {
        let root = tmp_dir();
        let state = crate::state::AppState::new(root.join("uploads"), root.join("files"), 1).unwrap();
        let id = "u-int".to_string();
        std::fs::create_dir_all(state.uploads_dir.join(&id)).unwrap();
        let chunk = MIN_CHUNK_SIZE;
        let m = meta(chunk as u64 + 10, chunk);
        save_meta(&state.uploads_dir, &id, &m).unwrap();
        let lock = std::sync::Mutex::new(());
        write_chunk(
            &state.uploads_dir,
            &id,
            &m,
            0,
            &vec![1u8; chunk],
            &lock,
        )
        .unwrap();
        write_chunk(&state.uploads_dir, &id, &m, 1, &[2u8; 10], &lock).unwrap();

        let err = match merge_sync(state.clone(), id.clone(), Some("0".repeat(64))) {
            Err(e) => e,
            Ok(_) => panic!("integrity mismatch should fail"),
        };
        assert_eq!(err.status, axum::http::StatusCode::UNPROCESSABLE_ENTITY);
        assert!(data_path(&state.uploads_dir, &id).exists());
        assert!(!state.files_dir.join("a.bin").exists());
        let bits = DoneBits::load(&state.uploads_dir, &id, m.total_chunks).unwrap();
        assert_eq!(bits.filled_count(m.total_chunks), 0);

        write_chunk(
            &state.uploads_dir,
            &id,
            &m,
            0,
            &vec![1u8; chunk],
            &lock,
        )
        .unwrap();
        write_chunk(&state.uploads_dir, &id, &m, 1, &[2u8; 10], &lock).unwrap();
        let ok = merge_sync(state.clone(), id.clone(), None).unwrap();
        assert!(!ok.verified);
        assert!(state.files_dir.join("a.bin").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn expected_chunk_len_out_of_range() {
        let m = meta(250, 100);
        assert_eq!(m.expected_chunk_len(3), 0);
    }

    #[test]
    fn meta_uses_the_same_size_rules_as_init() {
        let n = total_chunks(MIN_CHUNK_SIZE as u64, MIN_CHUNK_SIZE);
        let ok = Meta {
            filename: "a.bin".into(),
            file_size: MIN_CHUNK_SIZE as u64,
            chunk_size: MIN_CHUNK_SIZE,
            total_chunks: n,
            target_dir: String::new(),
        };
        assert!(ok.validate().is_ok());
        assert!(
            Meta {
                filename: "a.bin".into(),
                file_size: 100,
                chunk_size: 1,
                total_chunks: 100,
                target_dir: String::new(),
            }
            .validate()
            .is_err()
        );
        assert!(
            Meta {
                filename: "a.bin".into(),
                file_size: MIN_CHUNK_SIZE as u64,
                chunk_size: MIN_CHUNK_SIZE,
                total_chunks: n + 1,
                target_dir: String::new(),
            }
            .validate()
            .is_err()
        );
        assert!(
            Meta {
                filename: "a.bin".into(),
                file_size: MAX_FILE_SIZE + 1,
                chunk_size: MIN_CHUNK_SIZE,
                total_chunks: 1,
                target_dir: String::new(),
            }
            .validate()
            .is_err()
        );
        assert!(
            Meta {
                filename: "a.bin".into(),
                file_size: MIN_CHUNK_SIZE as u64,
                chunk_size: MAX_CHUNK_SIZE + 1,
                total_chunks: 1,
                target_dir: String::new(),
            }
            .validate()
            .is_err()
        );
    }
}
