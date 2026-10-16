//! 磁盘容量：statvfs 采样 + TTL 缓存 + 进程内预留量记账
//!
//! ## 为什么需要"预留量"
//! 分片 staging 文件 `data.part` 由 `set_len` 预分配为**稀疏文件**，`statvfs` 看不到
//! "客户端声明但尚未写入"的体积；并发 `init` 之间也互相看不到对方的需求。
//! 因此进程内为每个上传会话记一笔预留，判定用
//! `budget = avail - min_free - Σ预留`（再叠加可选的整盘配额上限）。
//!
//! ## 判定链（服务端是唯一真相）
//! 1. `init`   —— 预检：`remaining + merge_extra`，通过则记预留
//! 2. `chunk`  —— 每 `check_every_chunks` 片复检一次，顺手把预留刷成“尚未落盘”的真实值
//!    （statvfs 成本被摊销到每 N 片一次，且走 TTL 缓存）
//! 3. `merge`  —— 终检（跨卷 merge 会退化成 copy，需要额外 1x 空间）
//! 4. ENOSPC   —— 一律映射为 507（见 [`crate::error::AppError`]）
//!
//! 前端展示只是体验优化，任何情况下都不能替代以上校验。
//!
//! ## 采样失败时的策略
//! 取不到 statvfs（奇异文件系统 / 权限 / NFS 卡死）时**放行并 warn**：
//! 容量保护是兜底功能，不应该因为自己坏掉而让所有上传失败。
//!
//! ## 平台支持
//! 只在 **unix（Linux / macOS / \*BSD）** 上采样容量（`nix::sys::statvfs`）。
//! 非 unix（如 Windows）不引 `windows-sys`：`query_space` 返回 `Unsupported`，
//! [`DiskMeter`] 按上面的策略退化（不拦截上传、不记预留，`GET /api/entries/disk`
//! 返回 503，前端自动隐藏容量条）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::AppError;

/// 一次 `statvfs` 采样结果（字节）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeSpace {
    /// 卷总容量
    pub total: u64,
    /// **非特权进程**实际可用容量（POSIX `f_bavail`，已扣除 root 预留块）
    pub available: u64,
}

/// 采样 + 预留 + 清理策略参数（由 CLI / env 构造）
#[derive(Debug, Clone)]
pub struct DiskLimits {
    /// 任何写入都必须留给磁盘的余量（防止把系统 / 其他服务挤死）
    pub min_free: u64,
    /// 整盘"已用 + 本次写入"的上限，0 = 不限制。用于同一磁盘上多服务共存
    pub quota: u64,
    /// 采样缓存 TTL：多标签页 / 多请求共享一次 statvfs
    pub sample_ttl: Duration,
    /// 每写入多少分片强制重采样并复检一次
    pub check_every_chunks: u64,
    /// 上传会话（`uploads/<id>/`）无活动多久后清理，0 = 关闭清理
    pub session_ttl: Duration,
    /// 清理任务轮询间隔
    pub cleanup_interval: Duration,
}

impl Default for DiskLimits {
    fn default() -> Self {
        Self {
            min_free: 1024 * 1024 * 1024,
            quota: 0,
            sample_ttl: Duration::from_millis(1000),
            check_every_chunks: 32,
            session_ttl: Duration::from_secs(7 * 24 * 3600),
            cleanup_interval: Duration::from_secs(3600),
        }
    }
}

impl DiskLimits {
    /// 关闭全部容量保护（min_free / 配额 / 周期复检 / 会话清理），测试专用
    #[cfg(test)]
    pub fn unlimited() -> Self {
        Self {
            min_free: 0,
            quota: 0,
            check_every_chunks: 0,
            session_ttl: Duration::ZERO,
            ..Self::default()
        }
    }
}

/// 采样成功的结果 + 采样时刻
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub space: VolumeSpace,
    /// 单调时钟（TTL 判定）
    pub at: Instant,
    /// wall clock unix 秒（给前端展示"最后一次成功采样时间"）
    pub at_unix: u64,
}

impl Sample {
    fn stale(&self, ttl: Duration) -> bool {
        self.at.elapsed() > ttl
    }
}

/// `/api/entries/disk` 的数据面（handler 只做字段映射）
#[derive(Debug, Clone)]
pub struct DiskSnapshot {
    pub total: u64,
    pub used: u64,
    pub free: u64,
    /// 服务端判定的"还能安全接收"字节：已扣 min_free / 配额 / 预留
    pub available: u64,
    /// 已声明但尚未落盘的字节（稀疏 staging 在 df 里看不见的那部分）
    pub reserved: u64,
    /// 有预留记录的上传会话数
    pub sessions: u64,
    pub min_free: u64,
    pub quota: u64,
    /// uploads_dir 与 files_dir 是否同一文件系统
    pub staging_same_volume: bool,
    /// 仅当两目录跨卷时给出 staging 所在卷的信息
    pub staging_total: Option<u64>,
    pub staging_free: Option<u64>,
    pub sampled_at: u64,
    /// true = 本次采样失败，返回的是过期缓存
    pub stale: bool,
}

/// 容量守卫：`AppState` 持有 `Arc<DiskMeter>`，各 handler 共享同一份缓存与预留账本
pub struct DiskMeter {
    files_dir: PathBuf,
    uploads_dir: PathBuf,
    limits: DiskLimits,
    staging_same_volume: bool,
    /// 最近一次**成功**的采样（失败时保留旧值，标记 stale）
    sample: Mutex<Option<Sample>>,
    /// staging 卷（仅当 uploads_dir 跳卷时用到）的采样缓存
    staging_sample: Mutex<Option<Sample>>,
    /// upload_id → 预留字节
    reserved: Mutex<HashMap<String, u64>>,
    /// 自上次复检以来写入的分片数
    chunks_since_check: AtomicU64,
}

impl DiskMeter {
    pub fn new(files_dir: PathBuf, uploads_dir: PathBuf, limits: DiskLimits) -> Arc<Self> {
        let staging_same_volume = same_volume(&files_dir, &uploads_dir);
        if !staging_same_volume {
            tracing::warn!(
                files_dir = %files_dir.display(),
                uploads_dir = %uploads_dir.display(),
                "uploads_dir and files_dir are on different volumes: merge needs ~2x space (rename falls back to copy)"
            );
        }
        Arc::new(Self {
            files_dir,
            uploads_dir,
            limits,
            staging_same_volume,
            sample: Mutex::new(None),
            staging_sample: Mutex::new(None),
            reserved: Mutex::new(HashMap::new()),
            chunks_since_check: AtomicU64::new(0),
        })
    }

    pub fn limits(&self) -> &DiskLimits {
        &self.limits
    }

    pub fn staging_same_volume(&self) -> bool {
        self.staging_same_volume
    }

    /// files_dir 所在卷的采样：TTL 内命中缓存；`force = true` 忽略 TTL 重新 statvfs。
    /// 采样失败时返回旧缓存（可能 `None`），绝不返回错误 —— 容量保护不阻塞主流程。
    pub fn sample(&self, force: bool) -> Option<Sample> {
        self.sample_of(&self.files_dir, &self.sample, force, "files")
    }

    /// staging（uploads_dir）所在卷的采样；同卷时复用 files 卷的采样，不额外 statvfs
    pub fn sample_staging(&self, force: bool) -> Option<Sample> {
        if self.staging_same_volume {
            return self.sample(force);
        }
        self.sample_of(&self.uploads_dir, &self.staging_sample, force, "staging")
    }

    fn sample_of(
        &self,
        path: &Path,
        cache: &Mutex<Option<Sample>>,
        force: bool,
        label: &str,
    ) -> Option<Sample> {
        if !force {
            let cached = locked(cache);
            if let Some(s) = cached.as_ref()
                && !s.stale(self.limits.sample_ttl)
            {
                return Some(*s);
            }
        }
        if !SUPPORTED {
            warn_unsupported_once();
            return None;
        }
        match query_space(path) {
            Ok(space) => {
                let s = Sample {
                    space,
                    at: Instant::now(),
                    at_unix: unix_now(),
                };
                *locked_mut(cache) = Some(s);
                Some(s)
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    volume = label,
                    path = %path.display(),
                    "disk space query failed, capacity guard degraded"
                );
                locked(cache).as_ref().copied()
            }
        }
    }

    /// 只读缓存，不做 statvfs（超时降级路径用）
    pub fn cached_sample(&self) -> Option<Sample> {
        locked(&self.sample).as_ref().copied()
    }

    /// 当前可安全接收的字节数；采样不可用时返回 `None`（前端据此隐藏提示）
    pub fn available(&self, force: bool) -> Option<u64> {
        let s = self.sample(force)?;
        Some(safe_budget(
            s.space,
            self.limits.min_free,
            self.limits.quota,
            self.pending(),
        ))
    }

    /// 容量判定：`budget >= need` 才放行。
    /// `exclude` 指定"本次操作自己的会话"——它已有的预留不应算成别人的占用。
    /// `need = 0` 依然会走一遍判定：不需要额外空间，但要确认 min_free 没被其他会话击穿。
    ///
    /// 走 TTL 缓存而不强制 statvfs：本方法的调用频率（init / 每 N 片）远高于缓存周期，
    /// 1s 的新鲜度对"防写满"足够；merge / delete 回带余量时才用 `available(true)` 强制刷新。
    ///
    /// `need_staging` 只在 `uploads_dir` 与 `files_dir` **跨卷**时参与判定（同卷时它已含在
    /// `need_files` 里）：跨卷时分片先占 staging 卷、merge 后才占 files 卷，两块盘分开算。
    pub fn check(
        &self,
        exclude: Option<&str>,
        need_files: u64,
        need_staging: u64,
    ) -> Result<(), AppError> {
        let pending = match exclude {
            Some(id) => self.pending_excluding(id),
            None => self.pending(),
        };
        if let Some(s) = self.sample(false) {
            let budget = safe_budget(s.space, self.limits.min_free, self.limits.quota, pending);
            if budget < need_files {
                return Err(self.reject("files", &s, budget, need_files));
            }
        }
        if need_staging > 0
            && !self.staging_same_volume
            && let Some(st) = self.sample_staging(false)
        {
            // staging 卷只保证 min_free（配额面向的是长期存放的 files 卷）
            let budget = st.space.available.saturating_sub(self.limits.min_free);
            if budget < need_staging {
                return Err(self.reject("staging", &st, budget, need_staging));
            }
        }
        Ok(())
    }

    fn reject(&self, volume: &str, s: &Sample, budget: u64, need: u64) -> AppError {
        tracing::warn!(
            volume,
            need,
            budget,
            free = s.space.available,
            total = s.space.total,
            min_free = self.limits.min_free,
            quota = self.limits.quota,
            pending = self.pending(),
            "upload rejected: insufficient disk space"
        );
        AppError::disk_full(
            format!(
                "insufficient disk space on {volume} volume: need {need}, server can accept \
                 about {budget} (free {}, reserved min_free {})",
                s.space.available, self.limits.min_free
            ),
            budget,
            need,
        )
    }

    /// 记账：覆盖该会话的预留量（重复 init / 周期性续期都走这里）
    pub fn reserve(&self, upload_id: &str, bytes: u64) {
        locked_mut(&self.reserved).insert(upload_id.to_string(), bytes);
    }

    /// 释放（merge 成功 / 会话目录被清理）
    pub fn release(&self, upload_id: &str) {
        locked_mut(&self.reserved).remove(upload_id);
    }

    /// 丢弃已不存在会话目录的预留（janitor 顺带做，防账本无界增长）
    pub fn prune_reserved(&self) -> usize {
        let dead: Vec<String> = {
            let map = locked(&self.reserved);
            map.keys()
                .filter(|id| !self.uploads_dir.join(id).exists())
                .cloned()
                .collect()
        };
        if !dead.is_empty() {
            let mut map = locked_mut(&self.reserved);
            for id in &dead {
                map.remove(id);
            }
        }
        dead.len()
    }

    pub fn pending(&self) -> u64 {
        locked(&self.reserved).values().sum()
    }

    pub fn pending_excluding(&self, upload_id: &str) -> u64 {
        locked(&self.reserved)
            .iter()
            .filter(|(k, _)| k.as_str() != upload_id)
            .map(|(_, v)| *v)
            .sum()
    }

    pub fn sessions(&self) -> u64 {
        locked(&self.reserved).len() as u64
    }

    /// 分片写入计数：返回 true 表示"该做周期性复检了"（并清零）
    pub fn tick_chunk_check(&self) -> bool {
        if self.limits.check_every_chunks == 0 {
            return false;
        }
        let n = self.chunks_since_check.fetch_add(1, Ordering::Relaxed);
        if n + 1 >= self.limits.check_every_chunks {
            self.chunks_since_check.store(0, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// 组装接口响应；`force = false` 时只用 TTL 内数据
    pub fn snapshot(&self, force: bool) -> Option<DiskSnapshot> {
        self.build_snapshot(self.sample(force))
    }

    /// 采样超时 / 失败后的降级：直接吐缓存并标记 stale
    pub fn stale_snapshot(&self) -> Option<DiskSnapshot> {
        self.build_snapshot(self.cached_sample())
    }

    fn build_snapshot(&self, s: Option<Sample>) -> Option<DiskSnapshot> {
        let s = s?;
        let sp = s.space;
        let reserved = self.pending();
        let available = safe_budget(sp, self.limits.min_free, self.limits.quota, reserved);
        let (staging_total, staging_free) = if self.staging_same_volume {
            (None, None)
        } else {
            match self.sample_staging(true) {
                Some(st) => (Some(st.space.total), Some(st.space.available)),
                None => (None, None),
            }
        };
        Some(DiskSnapshot {
            total: sp.total,
            used: sp.total.saturating_sub(sp.available),
            free: sp.available,
            available,
            reserved,
            sessions: self.sessions(),
            min_free: self.limits.min_free,
            quota: self.limits.quota,
            staging_same_volume: self.staging_same_volume,
            staging_total,
            staging_free,
            sampled_at: s.at_unix,
            stale: s.stale(self.limits.sample_ttl),
        })
    }
}

/// 纯函数：给定采样结果与策略，算出"还能安全接收"的字节。
/// 单独抽出来是为了可测试（不依赖真实文件系统）。
pub(crate) fn safe_budget(
    sp: VolumeSpace,
    min_free: u64,
    quota: u64,
    pending: u64,
) -> u64 {
    let mut budget = sp
        .available
        .saturating_sub(min_free)
        .saturating_sub(pending);
    if quota > 0 {
        // 配额语义：整盘已用量不得越过 quota
        let used = sp.total.saturating_sub(sp.available);
        budget = budget.min(quota.saturating_sub(used));
    }
    budget
}

/// 纯函数：一个上传会话还需要多少"尚未记账"的空间。
/// - `remaining`：还没写进 staging 的字节（稀疏文件让 df 看不到的部分由预留账本承担）
/// - `merge_extra`：跨卷 merge 时 rename 退化为 copy，需要额外 1x 文件大小的空间
pub(crate) fn need_for_upload(file_size: u64, allocated: u64, same_volume: bool) -> u64 {
    let remaining = file_size.saturating_sub(allocated);
    let merge_extra = if same_volume { 0 } else { file_size };
    remaining.saturating_add(merge_extra)
}

/// 平台不支持采样时只提醒一次，避免每次请求刷日志
fn warn_unsupported_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        tracing::warn!(
            "disk capacity sampling is unavailable on this platform;              capacity guard degraded and GET /api/entries/disk returns 503"
        );
    });
}

fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn locked_mut<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 文件**实际占用**的字节数（稀疏文件按已分配的块计算，不是 `len()`）。
/// 用于把"已落盘"的部分从预留里扣掉，避免同一批字节被计入两次。
pub fn allocated_bytes(path: &Path) -> u64 {
    let Ok(md) = std::fs::metadata(path) else {
        return 0;
    };
    if !md.is_file() {
        return 0;
    }
    allocated_of(&md)
}

#[cfg(unix)]
fn allocated_of(md: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    // st_blocks 单位恒为 512 字节（POSIX）
    md.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_of(md: &std::fs::Metadata) -> u64 {
    // 非 unix 拿不到稀疏文件的真实分配，保守用 len()：宁可多留余量，不可少留。
    md.len()
}

/// `a` 与 `b` 是否位于同一文件系统（两者都必须是已存在的目录 / 文件）
pub fn same_volume(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (std::fs::metadata(a), std::fs::metadata(b)) {
            (Ok(x), Ok(y)) => x.dev() == y.dev(),
            _ => true, // 查不了就当作同卷（不因噎废食）
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        true
    }
}

/// 本平台能否采样磁盘容量。`false` 时压根不去 statvfs，避免逐次 warn 刷屏
pub const SUPPORTED: bool = cfg!(unix);

/// 查询路径所在卷的容量
#[cfg(unix)]
pub fn query_space(path: &Path) -> std::io::Result<VolumeSpace> {
    let st = nix::sys::statvfs::statvfs(path).map_err(std::io::Error::from)?;
    // f_blocks / f_bavail 以 f_frsize（基本块）为单位；个别文件系统报 0，退化到 f_bsize
    let fr = st.fragment_size() as u64;
    let bs = st.block_size() as u64;
    let block = if fr > 0 { fr } else { bs };
    if block == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "statvfs reported zero block size",
        ));
    }
    Ok(VolumeSpace {
        total: (st.blocks() as u64).saturating_mul(block),
        available: (st.blocks_available() as u64).saturating_mul(block),
    })
}

/// 非 unix：`std` 不提供获取空闲空间的能力，也不为此引入 `windows-sys`。
/// 返回 `Unsupported`，[`DiskMeter`] 据此退化放行（见模块文档「平台支持」）。
#[cfg(not(unix))]
pub fn query_space(path: &Path) -> std::io::Result<VolumeSpace> {
    let _ = path;
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "disk capacity sampling is only implemented on unix platforms",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    fn sp(total: u64, avail: u64) -> VolumeSpace {
        VolumeSpace { total, available: avail }
    }

    /// 注入固定采样的 meter：缓存 TTL 拉长到 1 小时，判定完全由注入值决定（不依赖真实盘）
    fn meter_with(total: u64, avail: u64, mut limits: DiskLimits) -> Arc<DiskMeter> {
        limits.sample_ttl = Duration::from_secs(3600);
        let root = std::env::temp_dir();
        let m = DiskMeter::new(root.clone(), root, limits);
        *locked_mut(&m.sample) = Some(Sample {
            space: sp(total, avail),
            at: Instant::now(),
            at_unix: unix_now(),
        });
        m
    }

    #[test]
    fn budget_subtracts_min_free_and_pending() {
        // 100G 总量 / 30G 可用，留 1G，已预留 5G
        assert_eq!(safe_budget(sp(100 * GB, 30 * GB), GB, 0, 5 * GB), 24 * GB);
        // 可用不足 min_free → 0（不允许写）
        assert_eq!(safe_budget(sp(100 * GB, 512 * MB), GB, 0, 0), 0);
        // saturating：预留超过可用不会回绕成天文数字
        assert_eq!(safe_budget(sp(100 * GB, 10 * GB), GB, 0, 100 * GB), 0);
    }

    #[test]
    fn quota_caps_budget() {
        // 配额 60G，已用 55G → 还能写 5G（即使卷上还有 45G 可用）
        assert_eq!(safe_budget(sp(100 * GB, 45 * GB), GB, 60 * GB, 0), 5 * GB);
        // 已越过配额 → 0
        assert_eq!(safe_budget(sp(100 * GB, 30 * GB), GB, 50 * GB, 0), 0);
        // quota = 0 表示不限制
        assert_eq!(safe_budget(sp(100 * GB, 30 * GB), GB, 0, 0), 29 * GB);
    }

    #[test]
    fn need_accounts_for_remaining_and_cross_volume_merge() {
        // 全新上传，同卷：只需 file_size
        assert_eq!(need_for_upload(10 * GB, 0, true), 10 * GB);
        // 断点续传：已落盘 4G，只补剩下的
        assert_eq!(need_for_upload(10 * GB, 4 * GB, true), 6 * GB);
        // 跨卷：额外 1x（merge 的 rename 退化为 copy）
        assert_eq!(need_for_upload(10 * GB, 0, false), 20 * GB);
        assert_eq!(need_for_upload(10 * GB, 4 * GB, false), 16 * GB);
        // 已写完：同卷不再需要空间
        assert_eq!(need_for_upload(10 * GB, 10 * GB, true), 0);
        // 不溢出
        assert_eq!(need_for_upload(u64::MAX, 0, false), u64::MAX);
    }

    #[test]
    fn check_rejects_when_over_budget() {
        let m = meter_with(100 * GB, 10 * GB, DiskLimits::default());
        // budget = 10G - 1G(min_free) = 9G
        assert!(m.check(None, 8 * GB, 0).is_ok());
        let err = m.check(None, 12 * GB, 0).unwrap_err();
        assert_eq!(
            err.status,
            axum::http::StatusCode::INSUFFICIENT_STORAGE
        );
        assert_eq!(err.required, Some(12 * GB));
        assert_eq!(err.free, Some(9 * GB));
        assert!(err.message.contains("insufficient disk space"));
    }

    #[test]
    fn reservations_reduce_budget_and_exclude_own_session() {
        let m = meter_with(100 * GB, 10 * GB, DiskLimits::default());
        m.reserve("u1", 5 * GB);
        assert_eq!(m.pending(), 5 * GB);
        assert_eq!(m.pending_excluding("u1"), 0);
        // 别人的预留要算进去：预算只剩 10 - 1(min_free) - 5 = 4G
        assert!(m.check(None, 4 * GB, 0).is_ok());
        assert!(m.check(None, 5 * GB, 0).is_err());
        // 自己重新 init：旧预留不算别人的（预算回到 9G）
        assert!(m.check(Some("u1"), 9 * GB, 0).is_ok());
        assert!(m.check(Some("u1"), 10 * GB, 0).is_err());
        // 覆盖式记账 + 释放
        m.reserve("u1", GB);
        assert_eq!(m.pending(), GB);
        m.release("u1");
        assert_eq!(m.pending(), 0);
        assert_eq!(m.sessions(), 0);
    }

    #[test]
    fn sample_honours_ttl_and_force() {
        let root = std::env::temp_dir();
        let m = DiskMeter::new(
            root.clone(),
            root,
            DiskLimits {
                sample_ttl: Duration::from_millis(40),
                ..DiskLimits::default()
            },
        );
        if !SUPPORTED {
            // 无采样能力的平台：始终 None（守卫退化，不拦截）
            assert!(m.sample(false).is_none());
            assert!(m.sample(true).is_none());
            return;
        }
        // TTL 内命中缓存：不重新 statvfs，时间戳不变
        let a = m.sample(false).unwrap();
        let b = m.sample(false).unwrap();
        assert_eq!(a.at, b.at);
        // TTL 过期后重新 statvfs（取到真实盘的数值）
        std::thread::sleep(Duration::from_millis(60));
        let c = m.sample(false).unwrap();
        assert!(c.at > a.at);
        assert!(c.space.total > 0 && c.space.available <= c.space.total);
        // force 总是重新 statvfs
        let d = m.sample(false).unwrap();
        let e = m.sample(true).unwrap();
        assert!(e.at >= d.at);
    }

    #[test]
    #[cfg(unix)] // 结尾要验证主动刷新后不再 stale，需要 statvfs 可用
    fn stale_snapshot_flagged() {
        // 注意：不用 meter_with（它会把 TTL 拉长到 1 小时，正好避开本测试要验证的过期分支）
        let root = std::env::temp_dir();
        let m = DiskMeter::new(
            root.clone(),
            root,
            DiskLimits {
                sample_ttl: Duration::from_millis(1),
                ..DiskLimits::default()
            },
        );
        *locked_mut(&m.sample) = Some(Sample {
            space: sp(100 * GB, 10 * GB),
            at: Instant::now(),
            at_unix: unix_now(),
        });
        std::thread::sleep(Duration::from_millis(5));
        // 只读缓存：不 statvfs，但标记 stale
        let s = m.stale_snapshot().unwrap();
        assert!(s.stale);
        assert_eq!(s.total, 100 * GB);
        assert_eq!(s.sessions, 0);
        // stale_snapshot 只读缓存，不依赖平台采样能力；
        // 主动刷新只有采样可用时才能摆脱 stale
        let refreshed = m.snapshot(true);
        if SUPPORTED {
            assert!(!refreshed.unwrap().stale);
        } else {
            assert!(refreshed.is_none());
        }
    }

    /// 直接构造跨卷 meter（同一模块内可以直接填字段），两块卷的余量分开注入
    #[cfg(test)]
    fn cross_volume_meter(files: VolumeSpace, staging: VolumeSpace) -> Arc<DiskMeter> {
        let root = std::env::temp_dir();
        let now = Instant::now();
        Arc::new(DiskMeter {
            files_dir: root.clone(),
            uploads_dir: root.clone(),
            limits: DiskLimits {
                sample_ttl: Duration::from_secs(3600),
                ..DiskLimits::default()
            },
            staging_same_volume: false,
            sample: Mutex::new(Some(Sample {
                space: files,
                at: now,
                at_unix: unix_now(),
            })),
            staging_sample: Mutex::new(Some(Sample {
                space: staging,
                at: now,
                at_unix: unix_now(),
            })),
            reserved: Mutex::new(HashMap::new()),
            chunks_since_check: AtomicU64::new(0),
        })
    }

    #[test]
    fn cross_volume_rejects_when_staging_volume_too_small() {
        // files 卷宽裕，staging 卷只剩 2G（且要留 1G min_free）
        let m = cross_volume_meter(sp(100 * GB, 90 * GB), sp(20 * GB, 2 * GB));
        // staging 只需 512M → 通过
        assert!(m.check(None, GB, 512 * MB).is_ok());
        // staging 要 2G → 超出（budget = 2G - 1G）
        let err = m.check(None, GB, 2 * GB).unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::INSUFFICIENT_STORAGE);
        assert!(err.message.contains("staging volume"), "{}", err.message);
        // staging 充足但 files 卷不够 → 报 files 卷
        let err = m.check(None, 200 * GB, GB).unwrap_err();
        assert!(err.message.contains("files volume"), "{}", err.message);
    }

    #[test]
    fn staging_need_is_ignored_on_same_volume() {
        // 同卷（测试用同一个 temp_dir）：need_staging 已含在 need_files 里，不应重复判定
        let m = meter_with(100 * GB, 10 * GB, DiskLimits::default());
        assert!(m.staging_same_volume());
        assert!(m.check(None, GB, 9 * GB).is_ok());
        assert!(m.check(None, 12 * GB, GB).is_err());
    }

    #[test]
    fn chunk_check_ticks_every_n() {
        let m = meter_with(
            GB,
            GB / 2,
            DiskLimits {
                check_every_chunks: 3,
                ..DiskLimits::default()
            },
        );
        assert!(!m.tick_chunk_check());
        assert!(!m.tick_chunk_check());
        assert!(m.tick_chunk_check());
        assert!(!m.tick_chunk_check());
        // 关闭复检
        let limits2 = DiskLimits {
            check_every_chunks: 0,
            ..DiskLimits::default()
        };
        let m2 = meter_with(GB, GB / 2, limits2);
        assert!(!m2.tick_chunk_check());
    }

    #[test]
    fn volume_query_matches_platform_support() {
        // 同一路径必然同卷（两分支都适用）
        assert!(same_volume(Path::new("."), Path::new(".")));
        let r = query_space(Path::new("."));
        if SUPPORTED {
            let sp = r.expect("支持采样的平台 statvfs 应当成功");
            assert!(sp.total > 0);
            assert!(sp.available <= sp.total);
        } else {
            assert_eq!(r.unwrap_err().kind(), std::io::ErrorKind::Unsupported);
        }
    }

    #[test]
    fn allocated_bytes_missing_file_is_zero() {
        assert_eq!(allocated_bytes(Path::new("/definitely/not/here")), 0);
    }
}
