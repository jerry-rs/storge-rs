use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, Semaphore};

use crate::disk::DiskMeter;

/// 全局应用状态
#[derive(Clone)]
pub struct AppState {
    /// 临时上传目录：uploads/<upload_id>/ 下存放稀疏 staging 文件 data.part、
    /// 完成位图 done.bin 与元数据 meta.json（上传完成后整个目录被删除）
    pub uploads_dir: PathBuf,
    /// 合并后的最终文件目录
    pub files_dir: PathBuf,
    /// 全局并发闸门：所有文件共享的磁盘写入操作（分片写入 + 合并）上限，
    /// 多文件并行上传时防止 IO 拥塞
    pub io_slots: Arc<Semaphore>,
    /// 磁盘容量守卫：采样缓存 + 预留量账本（所有会话共享一份）
    pub disk: Arc<DiskMeter>,
    /// 按 upload_id 的读写锁：分片上传可并行（读锁），init / 重切 / 合并互斥（写锁）。
    /// 必须先拿本锁，再拿 io_slots，避免交叉等待死锁。
    upload_locks: Arc<Mutex<HashMap<String, Arc<RwLock<()>>>>>,
    /// 完成位图 `done.bin` 的进程内互斥：分片可并行写入 data.part 的不同偏移，
    /// 但置位必须串行，避免读-改-写丢失 bit。
    done_bit_locks: Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::Mutex<()>>>>>,
}

impl AppState {
    /// 测试专用：关掉磁盘容量保护，否则用例会受跑测试那块盘的真实余量影响
    #[cfg(test)]
    pub fn unlimited(
        uploads_dir: PathBuf,
        files_dir: PathBuf,
        max_concurrent_uploads: usize,
    ) -> std::io::Result<Self> {
        Self::with_limits(
            uploads_dir,
            files_dir,
            max_concurrent_uploads,
            crate::disk::DiskLimits::unlimited(),
        )
    }

    pub fn with_limits(
        uploads_dir: PathBuf,
        files_dir: PathBuf,
        max_concurrent_uploads: usize,
        limits: crate::disk::DiskLimits,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&uploads_dir)?;
        std::fs::create_dir_all(&files_dir)?;
        // 启动时 canonicalize，避免每次请求都处理 /var → /private/var 这类符号链接
        let uploads_dir = std::fs::canonicalize(&uploads_dir)?;
        let files_dir = std::fs::canonicalize(&files_dir)?;
        Ok(Self {
            uploads_dir: uploads_dir.clone(),
            files_dir: files_dir.clone(),
            disk: DiskMeter::new(files_dir, uploads_dir, limits),
            io_slots: Arc::new(Semaphore::new(max_concurrent_uploads.max(1))),
            upload_locks: Arc::new(Mutex::new(HashMap::new())),
            done_bit_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    async fn upload_lock(&self, upload_id: &str) -> Arc<RwLock<()>> {
        let mut map = self.upload_locks.lock().await;
        map.retain(|_, lock| Arc::strong_count(lock) > 1);
        map.entry(upload_id.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// 分片写入 / 状态查询：同一 upload 可并行。先拿本锁，再拿 `io_slots`。
    pub async fn lock_upload_read(&self, upload_id: &str) -> OwnedRwLockReadGuard<()> {
        self.upload_lock(upload_id).await.read_owned().await
    }

    /// init / 重切 / 合并：与其它操作互斥。先拿本锁，再拿 `io_slots`。
    pub async fn lock_upload_write(&self, upload_id: &str) -> OwnedRwLockWriteGuard<()> {
        self.upload_lock(upload_id).await.write_owned().await
    }

    /// 完成位图互斥。在已持有对应 `upload_id` 的读写锁之后调用（可在 `spawn_blocking` 内）。
    pub fn lock_done_bits(&self, upload_id: &str) -> Arc<std::sync::Mutex<()>> {
        let mut map = self
            .done_bit_locks
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        map.retain(|_, lock| Arc::strong_count(lock) > 1);
        map.entry(upload_id.to_string())
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone()
    }
}
