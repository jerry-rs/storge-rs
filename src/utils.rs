use crate::error::AppError;
use std::io::Read;
use std::path::{Path, PathBuf};

/// 单文件上限 16 TiB（覆盖 500GB+）。
pub const MAX_FILE_SIZE: u64 = 16u64 << 40;
/// 分片下限：过小会导致分片数爆炸（位图 / 续传列表）。
pub const MIN_CHUNK_SIZE: usize = 256 * 1024;
/// 分片上限，同时是 `/api/entries/upload/chunk` 的 HTTP body 上限。
pub const MAX_CHUNK_SIZE: usize = 64 * 1024 * 1024;
/// 单文件最多分片数；超限须增大 `chunk_size`。
pub const MAX_CHUNKS: u64 = 1_000_000;

/// `file_size` 与 `chunk_size` 是独立约束；分片数由它们推导，且不得超过 `MAX_CHUNKS`。
pub fn total_chunks(file_size: u64, chunk_size: usize) -> u64 {
    if chunk_size == 0 {
        return 0;
    }
    file_size.div_ceil(chunk_size as u64)
}

pub(crate) fn validate_upload_sizes(file_size: u64, chunk_size: usize) -> Result<u64, String> {
    if file_size == 0 || file_size > MAX_FILE_SIZE {
        return Err("invalid file size (0 < size <= 16TiB)".into());
    }
    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
        return Err("chunk_size must be between 256KB and 64MB".into());
    }
    let n = total_chunks(file_size, chunk_size);
    if n > MAX_CHUNKS {
        return Err("too many chunks (max 1000000); increase chunk_size".into());
    }
    Ok(n)
}

/// Run filesystem / CPU work off the async runtime.
pub async fn blocking<F, T>(f: F) -> Result<T, AppError>
where
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|_| Err(AppError::internal("background task panicked")))
}

/// 校验目录下的单段条目名（文件 / 文件夹通用）：
/// 非空、非 . / ..、非隐藏、不含路径分隔符 / NUL / 控制字符，长度 ≤ 255
pub fn validate_entry_name(name: &str) -> Result<(), AppError> {
    if name.is_empty()
        || name.len() > 255
        || name == "."
        || name == ".."
        || name.starts_with('.')
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
        || name.contains('"')
        || name.chars().any(|c| c.is_control())
        || name.ends_with(' ')
        || name.ends_with('.')
    {
        return Err(AppError::bad_request("invalid name"));
    }
    Ok(())
}

/// 将相对子目录路径规范化（去除首尾 '/'，空串表示根目录），并做安全校验
pub fn normalize_rel_dir(rel: &str) -> Result<String, AppError> {
    let rel = rel.trim().trim_matches('/');
    if rel.is_empty() {
        return Ok(String::new());
    }
    for seg in rel.split('/') {
        validate_entry_name(seg).map_err(|_| AppError::bad_request("invalid target_dir"))?;
    }
    Ok(rel.to_string())
}

/// 确认 `path` 落在已 canonicalize 的 `canon_base` 内（防符号链接逃逸）
fn ensure_within(canon_base: &Path, path: &Path) -> Result<PathBuf, AppError> {
    let mut ancestor = path.to_path_buf();
    while !ancestor.exists() {
        match ancestor.parent() {
            Some(p) if p != ancestor.as_path() => ancestor = p.to_path_buf(),
            _ => {
                return Err(AppError::bad_request("invalid path"));
            }
        }
    }
    let canon_ancestor =
        std::fs::canonicalize(&ancestor).map_err(|_| AppError::bad_request("invalid path"))?;
    if canon_ancestor.starts_with(canon_base) {
        Ok(path.to_path_buf())
    } else {
        Err(AppError::bad_request("invalid path"))
    }
}

/// 将用户传入的相对路径安全解析到 files_dir 内（防目录穿越）
pub fn resolve_subdir(files_dir: &Path, rel: &str) -> Result<PathBuf, AppError> {
    let rel = normalize_rel_dir(rel)?;
    if rel.is_empty() {
        return Ok(files_dir.to_path_buf());
    }
    let mut out = files_dir.to_path_buf();
    for seg in rel.split('/') {
        out = out.join(seg);
    }
    ensure_within(files_dir, &out)
}

/// 将用户传入的相对文件路径安全解析到 files_dir 内（含子目录 + 文件名，防目录穿越）
pub fn resolve_file(files_dir: &Path, rel: &str) -> Result<PathBuf, AppError> {
    let rel = rel.trim().trim_matches('/');
    if rel.is_empty() {
        return Err(AppError::bad_request("invalid path"));
    }
    let (parent, name) = match rel.rfind('/') {
        Some(i) => (&rel[..i], &rel[i + 1..]),
        None => ("", rel),
    };
    validate_entry_name(name)?;
    let dir = resolve_subdir(files_dir, parent)?;
    ensure_within(files_dir, &dir.join(name))
}

/// 解析父目录 + 单段条目名，确认落在 files_dir 内且父目录存在
pub fn resolve_parent_entry(
    files_dir: &Path,
    parent_rel: &str,
    name: &str,
) -> Result<(PathBuf, PathBuf), AppError> {
    let parent = resolve_subdir(files_dir, parent_rel)?;
    if !parent.is_dir() {
        return Err(AppError::not_found("parent directory not found"));
    }
    let target = ensure_within(files_dir, &parent.join(name))?;
    Ok((parent, target))
}

/// API 对外暴露的相对路径（相对 files/），避免泄漏服务器绝对路径
pub fn public_file_path(target_dir: &str, filename: &str) -> String {
    if target_dir.is_empty() {
        filename.to_string()
    } else {
        format!("{target_dir}/{filename}")
    }
}

fn sys_secs(t: std::io::Result<std::time::SystemTime>) -> u64 {
    t.ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `(created, modified, accessed)` unix 秒。`created` 在 Linux 上可能不可用，回退到 modified。
pub fn entry_times(md: &std::fs::Metadata) -> (u64, u64, u64) {
    let modified = sys_secs(md.modified());
    let accessed = sys_secs(md.accessed());
    let created = md
        .created()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(modified);
    (created, modified, accessed)
}

/// 隐藏 sidecar：`foo.bin` → `.foo.bin.sha256`（list / tar 已跳过 `.` 开头）
pub fn sha256_sidecar_path(file: &Path) -> Option<PathBuf> {
    let name = file.file_name()?.to_str()?;
    if name.is_empty() || name.starts_with('.') {
        return None;
    }
    Some(file.with_file_name(format!(".{name}.sha256")))
}

pub fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// 写入 SHA-256 sidecar（`hex\nsize\n`）。size 用于文件被改写后失效缓存。
pub fn write_file_sha256(file: &Path, hash: &str, size: u64) -> std::io::Result<()> {
    let Some(p) = sha256_sidecar_path(file) else {
        return Ok(());
    };
    atomic_write(&p, format!("{}\n{size}\n", hash.to_ascii_lowercase()))
}

pub fn remove_file_sha256(file: &Path) {
    if let Some(p) = sha256_sidecar_path(file) {
        let _ = std::fs::remove_file(p);
    }
}

pub fn rename_file_sha256(src: &Path, dst: &Path) {
    let Some(src_side) = sha256_sidecar_path(src) else {
        return;
    };
    let Some(dst_side) = sha256_sidecar_path(dst) else {
        return;
    };
    if !src_side.exists() {
        return;
    }
    let _ = std::fs::remove_file(&dst_side);
    let _ = std::fs::rename(&src_side, &dst_side);
}

fn parse_sha256_sidecar(text: &str) -> Option<(String, u64)> {
    let mut lines = text.lines();
    let hash = lines.next()?.trim().to_ascii_lowercase();
    if !is_sha256_hex(&hash) {
        return None;
    }
    let size = lines.next()?.trim().parse().ok()?;
    Some((hash, size))
}

/// 读取 sidecar；缺失或 size 对不上时现场计算并写回。
pub fn file_sha256_cached(path: &Path) -> std::io::Result<String> {
    let md = std::fs::symlink_metadata(path)?;
    if !md.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    let size = md.len();
    if let Some(side) = sha256_sidecar_path(path)
        && let Ok(text) = std::fs::read_to_string(&side)
        && let Some((hash, cached_size)) = parse_sha256_sidecar(&text)
        && cached_size == size
    {
        return Ok(hash);
    }
    let hash = sha256_file(path)?;
    let _ = write_file_sha256(path, &hash, size);
    Ok(hash)
}

fn tmp_sidecar(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

fn is_cross_device(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::CrossesDevices || e.raw_os_error() == Some(18)
}

/// 将临时文件原子落到 `dest`。Windows 上 rename 不能覆盖已存在文件，需先删占位。
/// 跨文件系统时 fallback 为 copy + 删除源。
pub fn atomic_replace(tmp: &Path, dest: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        let _ = std::fs::remove_file(dest);
    }
    match std::fs::rename(tmp, dest) {
        Ok(()) => Ok(()),
        Err(e) if is_cross_device(&e) => {
            std::fs::copy(tmp, dest)?;
            let _ = std::fs::remove_file(tmp);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// 原子写入：先写 sidecar `.tmp` 再 rename，避免半截文件。
/// 通过 `atomic_replace` 覆盖已有目标（Windows 上 rename 不能替换）。
pub fn atomic_write(path: &Path, data: impl AsRef<[u8]>) -> std::io::Result<()> {
    let tmp = tmp_sidecar(path);
    std::fs::write(&tmp, data)?;
    match atomic_replace(&tmp, path) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// 流式计算文件 SHA-256（ring 实现，1MB 缓冲，大文件不占内存）
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    use ring::digest;

    let md = std::fs::symlink_metadata(path)?;
    if !md.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }

    let mut f = std::fs::File::open(path)?;
    let mut hasher = digest::Context::new(&digest::SHA256);
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_encode(hasher.finish().as_ref()))
}

pub fn is_regular_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|md| md.is_file())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp_dir() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "storge-util-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn entry_name_rejects_traversal() {
        assert!(validate_entry_name("ok.txt").is_ok());
        assert!(validate_entry_name("..").is_err());
        assert!(validate_entry_name(".").is_err());
        assert!(validate_entry_name(".hidden").is_err());
        assert!(validate_entry_name("a/b").is_err());
        assert!(validate_entry_name("a\\b").is_err());
        assert!(validate_entry_name("trailing.").is_err());
        assert!(validate_entry_name("quote\"name").is_err());
    }

    #[test]
    fn normalize_rel_dir_ok() {
        assert_eq!(normalize_rel_dir("/a/b/").unwrap(), "a/b");
        assert_eq!(normalize_rel_dir("").unwrap(), "");
        assert!(normalize_rel_dir("../etc").is_err());
        assert!(normalize_rel_dir("a/../b").is_err());
    }

    #[test]
    fn public_file_path_joins_dir() {
        assert_eq!(public_file_path("", "a.bin"), "a.bin");
        assert_eq!(public_file_path("videos", "a.bin"), "videos/a.bin");
    }

    #[test]
    fn atomic_write_overwrites_existing() {
        let root = tmp_dir();
        let p = root.join("f.bin");
        atomic_write(&p, b"aaa").unwrap();
        atomic_write(&p, b"bb").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"bb");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn sha256_file_rejects_directory() {
        let root = tmp_dir();
        let err = sha256_file(&root).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn sha256_sidecar_roundtrip_and_invalidate_on_size_change() {
        let root = tmp_dir();
        let p = root.join("a.bin");
        std::fs::write(&p, b"hello").unwrap();
        let hash = sha256_file(&p).unwrap();
        write_file_sha256(&p, &hash, 5).unwrap();
        assert_eq!(file_sha256_cached(&p).unwrap(), hash);

        let side = sha256_sidecar_path(&p).unwrap();
        assert_eq!(side.file_name().unwrap(), ".a.bin.sha256");

        std::fs::write(&p, b"hello!").unwrap();
        let hash2 = file_sha256_cached(&p).unwrap();
        assert_ne!(hash2, hash);
        assert_eq!(hash2, sha256_file(&p).unwrap());

        let dst = root.join("b.bin");
        std::fs::rename(&p, &dst).unwrap();
        rename_file_sha256(&p, &dst);
        assert!(!side.exists());
        assert_eq!(file_sha256_cached(&dst).unwrap(), hash2);

        remove_file_sha256(&dst);
        assert!(!sha256_sidecar_path(&dst).unwrap().exists());
        let _ = std::fs::remove_dir_all(&root);
    }
}
