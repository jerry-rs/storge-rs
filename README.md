# Storge

基于 **Rust + Axum** 的高性能分片上传 / 文件管理服务端，附带一个零依赖的单页 Web 前端（`web/index.html`）。

支持大文件（≤ 16TiB）分片上传、断点续传、动态调整分片大小、SHA-256 完整性校验、文件/目录的浏览、详情（含 SHA-256）、重命名、删除，以及流式下载（目录自动打包为 `.tar`）。

## 特性

- **分片上传 + 断点续传**：初始化时返回已上传分片列表，客户端跳过即可续传
- **稀疏 staging**：分片按偏移写入同一 `data.part`（稀疏文件，磁盘占用 ≈ 已上传字节），完成状态记在 `done.bin` 位图；500GB+ 文件无需再为每个分片建 inode，合并也无需二次拷贝
- **分片大小动态调整**：重新 init 时更换 `chunk_size`，服务端只重映射完成位图，不改写已上传字节
- **完整性校验**：合并后服务端用 `ring` 流式计算 SHA-256 并与客户端比对；校验失败的文件改名为 `.corrupt` 保留现场
- **幂等合并与防覆盖**：同名文件哈希一致则幂等返回；不同文件同名冲突自动改名为 `name (n)`；`O_CREAT|O_EXCL` 保证并发下不会两次占用同一目标
- **全局 IO 并发闸门**：分片写入与合并共享 `Semaphore`，多文件并行上传时防止磁盘 IO 拥塞
- **流式下载**：文件 1MiB 分块流式输出；目录用 `async-tar` 流式打包为 `.tar`（纯 tar 无压缩、两级背压、内存占用与目录大小无关），打包失败时 tar 尾部追加 `_TAR_ERROR.txt` 说明原因，tar 始终合法
- **安全**：所有路径参数经过规范化 + 逐段校验 + 祖先目录 canonicalize 比对，防止路径穿越与符号链接逃逸；`/api/*` 要求 `Authorization: Bearer storge`
- **零数据库**：上传元数据存于临时目录的 `meta.json`，上传完成后整个临时目录被删除
- **单页前端**：shadcn 风格深色/浅色主题的文件管理界面，无需构建工具

## 技术栈

| 组件 | 说明 |
| --- | --- |
| [axum](https://github.com/tokio-rs/axum) 0.8 | HTTP 框架 |
| [tokio](https://tokio.rs) | 异步运行时 |
| [async-tar](https://github.com/est31/AsyncTar) | 目录流式 tar 打包（`runtime-tokio`） |
| [ring](https://github.com/briansmith/ring) | SHA-256 计算 |
| [clap](https://github.com/clap-rs/clap) | CLI 参数（兼容环境变量） |
| [tower-http](https://github.com/tokio-rs/tower-http) | Bearer 鉴权、请求 tracing、静态文件 |
| [tracing](https://github.com/tokio-rs/tracing) | 结构化日志 |

## 快速开始

```bash
# 开发运行
cargo run

# 生产构建（已针对速度优化：LTO fat / codegen-units=1 / panic=abort）
cargo build --release
./target/release/storge-rs
```

启动后访问 `http://localhost:9000/` 即可使用 Web 界面。

## 配置

CLI 参数与环境变量（`APP_*`）二选一：

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--listen-addr` | `APP_LISTEN_ADDR` | `0.0.0.0:9000` | 监听地址 |
| `--uploads-dir` | `APP_UPLOADS_DIR` | `./uploads` | 临时 staging 目录（`<upload_id>/data.part` + `done.bin` + `meta.json`，合并后删除） |
| `--files-dir` | `APP_FILES_DIR` | `./files` | 最终文件目录 |
| `--max-concurrent-uploads` | `APP_MAX_CONCURRENT_UPLOADS` | `16` | 全局磁盘写并发上限（分片写入 + 合并） |

日志级别通过 `RUST_LOG` 控制（默认 `info`）：

```bash
RUST_LOG=debug cargo run
```

## API

所有 `/api/*` 请求须带：

```http
Authorization: Bearer storge
```

未带或错误时返回 `401 Unauthorized`。Web 界面会自动附带该头。页面、favicon 与 `/swagger-ui` 仍公开。

### 上传流程

`init → chunk* → merge`，`upload_id` 由客户端生成（≤64 位字母数字/`-`），推荐用文件内容的哈希以保证续传时不冲突。

#### `POST /api/entries/upload/init` — 初始化 / 断点续传探测

```json
{
  "upload_id": "abc123",
  "filename": "video.mp4",
  "file_size": 1073741824,
  "chunk_size": 10485760,
  "target_dir": "videos/2024"
}
```

约束：`0 < file_size ≤ 16TiB`，`256KB ≤ chunk_size ≤ 64MB`，分片数 ≤ 1,000,000（超限须增大 `chunk_size`），`filename` 不含路径分隔符。

响应（`uploaded` 为已上传分片下标，客户端据此跳过）：

```json
{ "upload_id": "abc123", "uploaded": [0, 1, 5], "total_chunks": 103, "merged": false }
```

#### `POST /api/entries/upload/chunk?upload_id=abc123&index=2` — 上传单个分片

Body 为原始二进制，长度必须等于该片期望字节数（末片可能较短）。按偏移写入 staging 文件后 fsync，再置位完成位图。

```json
{ "received": 2, "total_chunks": 103, "complete": false }
```

#### `POST /api/entries/upload/merge` — 合并

```json
{ "upload_id": "abc123", "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" }
```

`sha256` 可选（客户端对原文件计算的 hex）。响应包含实际落盘文件名（同名冲突时可能已被改名为 `name (n)`）、服务端计算的实际哈希与 `verified` 标志：

```json
{ "filename": "video.mp4", "file_size": 1073741824, "path": "...", "sha256": "...", "verified": true }
```

超大文件合并时服务端会流式计算 SHA-256（500GB 量级可能需要数十分钟）。若前面有反向代理，需调高读超时；`uploads-dir` 与 `files-dir` 建议放在同一文件系统，以便 merge 是 rename 而不是整文件拷贝。

#### `GET /api/entries/upload/status?upload_id=abc123` — 查询进度

```json
{ "uploaded": [0, 1, 2], "total_chunks": 103, "filename": "video.mp4", "merged": false }
```

### 文件管理

#### `GET /api/entries?path=videos/2024` — 列出目录

`path` 为相对 `files/` 的子目录（省略或 `/` 为根）。目录在前、文件在后，各自按 Modified 新→旧，同秒再按名称。时间戳均为 unix 秒。隐藏文件（含 SHA-256 sidecar `.name.sha256`）不出现在列表中。

```json
[
  { "name": "sub", "kind": "dir", "created": 1710000000, "modified": 1710000000, "accessed": 1710000000 },
  { "name": "a.mp4", "kind": "file", "size": 1024, "created": 1710000000, "modified": 1710000000, "accessed": 1710000000 }
]
```

#### `GET /api/entries/info?path=videos/a.mp4` — 详细信息

文件返回 SHA-256（合并时写入隐藏 sidecar `.a.mp4.sha256`；若缺失则现场计算并缓存）。目录无 `sha256` / `size`。

```json
{
  "name": "a.mp4",
  "kind": "file",
  "path": "videos/a.mp4",
  "size": 1024,
  "created": 1710000000,
  "modified": 1710000000,
  "accessed": 1710000000,
  "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}
```

#### `POST /api/entries/mkdir` — 新建目录

`path` 为父目录（省略或 `/` 为根），`name` 为单段目录名（不允许 `/` 和 `..`）：

```json
{ "path": "videos", "name": "2024" }
```

#### `POST /api/entries/rename` — 重命名文件 / 目录

同目录内改名（不允许跨目录移动）：

```json
{ "path": "videos", "name": "a.mp4", "new_name": "b.mp4" }
```

#### `GET /api/entries/download?path=videos/a.mp4` — 下载

- 文件：流式下载（1MiB 分块，`Content-Disposition` 携带 UTF-8 文件名）
- 目录：`async-tar` 流式打包为 `<dir>.tar`（chunked 传输，无 `Content-Length`）

#### `POST /api/entries/delete` — 删除文件 / 目录

```json
{ "path": "videos", "name": "a.mp4" }
```

### 公开路由

- `GET /` — Web 管理界面（`web/index.html`）
- `GET /favicon.svg` — 站点图标
- `GET /swagger-ui` — OpenAPI Swagger UI（规范：`/api-docs/openapi.json`）

## 项目结构

```
├── src/
│   ├── main.rs            # 入口：日志初始化、配置解析、启动服务
│   ├── config.rs          # CLI / 环境变量配置（clap）
│   ├── state.rs           # 全局状态：目录 + IO 并发闸门 + 位图锁
│   ├── routers.rs         # 路由与全局中间件（Bearer 鉴权、BodyLimit 64MB）
│   ├── openapi.rs         # OpenAPI 规范 + Swagger UI
│   ├── utils.rs           # 路径安全解析、原子写入、SHA-256、上传大小约束
│   ├── error.rs           # 统一错误类型（AppError → HTTP 状态码）
│   ├── handlers/
│   │   ├── upload.rs      # init / chunk / merge / status，稀疏 staging 与位图
│   │   ├── download.rs    # 文件流式下载 + 目录 tar 流式打包
│   │   ├── list.rs        # 目录浏览
│   │   ├── info.rs        # 文件 / 目录详情（SHA-256）
│   │   ├── mkdir.rs       # 新建目录
│   │   ├── rename.rs      # 重命名
│   │   ├── delete.rs      # 删除
│   │   └── index.rs       # Web 页面 / favicon
└── web/
    ├── index.html         # 单页前端（无构建依赖）
    └── favicon.svg
```

## 设计要点

- **全程流式**：分片按偏移写入稀疏 `data.part`、合并为 rename + SHA-256、下载均不整块载入内存；tar 打包链路为 `磁盘 → Builder → BufWriter(1MiB) → duplex(有界) → 有界通道 → HTTP 响应`，两级背压保证内存恒定
- **原子性**：分片 fsync 后再置位 `done.bin`；合并在同一文件系统上是 rename（`uploads-dir` 与 `files-dir` 请放在同一盘，避免 500GB+ 跨设备拷贝）
- **性能**：release profile 开启 `lto = "fat"`、`codegen-units = 1`、`panic = "abort"`（见 `Cargo.toml`）
- **BodyLimit**：JSON API 与分片共用 BodyLimit 64MB（分片上限）
- **鉴权**：`ValidateRequestHeaderLayer::bearer("storge")` 挂在受保护路由上
