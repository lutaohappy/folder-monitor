# folder-monitor

一个中等规模可生产化的文件目录监听服务。任务启动时传入 `taskId`、`C0` 目录、`C1` 目录；服务会同时监听两个目录，支持文件来自前端上传、其他服务 copy、FTP/SFTP 传输或外部程序直接写入。

## 架构

```text
外部服务 / FTP / 前端上传
        ↓
任务目录 C0 / C1
        ↓
fs.watch 快速发现 + 定时扫描补偿
        ↓
文件稳定检测 size/mtime 连续不变
        ↓
去重 claim
        ↓
Redis Stream 队列（可选）/ 本地队列
        ↓
SSE 按 taskId 推送到前端
```

## 关键能力

- `start`：传入 `taskId`、`C0`、`C1`，启动任务监听。
- `end`：结束任务监听，关闭该任务的 C0/C1 watcher。
- 支持外部写文件：copy、FTP、其他服务写目录都能被扫描补偿发现。
- 文件稳定检测：避免文件还在上传/复制时就推送半成品。
- 流式上传：测试上传接口不再把整个文件读进内存。
- Redis 可选：配置 `REDIS_URL` 后启用 Redis Stream 队列、事件去重、任务定义恢复。
- SSE 推送：`/events?taskId=<id>` 可只订阅指定任务。

## 运行

```bash
npm install
npm start
```

打开：

```text
http://127.0.0.1:3000
```

## Redis 模式

```bash
REDIS_URL=redis://127.0.0.1:6379 npm start
```

Redis 会用于：

- `folder-monitor:file-events`：文件事件 Stream。
- `folder-monitor:processed-events`：事件去重集合。
- `folder-monitor:tasks`：任务定义保存，服务重启后恢复 watcher。

## 环境变量

```bash
HOST=127.0.0.1
PORT=3000
BASE_PATH=/folder-monitor
REDIS_URL=redis://127.0.0.1:6379
SCAN_INTERVAL_MS=3000
STABILITY_CHECKS=2
```

## API

### 启动任务

```http
POST /api/tasks/start
Content-Type: application/json

{
  "taskId": "task-001",
  "C0": "/data/inbox/task-001/C0",
  "C1": "/data/inbox/task-001/C1"
}
```

兼容旧接口：

```http
POST /api/start
```

如果不传 `C0`、`C1`，服务会默认创建：

```text
data/tasks/<taskId>/C0
data/tasks/<taskId>/C1
```

### 结束任务

```http
POST /api/tasks/:taskId/end
```

兼容旧接口：

```http
POST /api/tasks/:taskId/stop
```

### 查询任务

```http
GET /api/tasks
```

### 测试上传文件

```http
POST /api/tasks/:taskId/upload
Content-Type: application/octet-stream
X-File-Name: demo.txt
X-Subdir: C0
```

### 前端推送

订阅全部任务：

```http
GET /events
```

订阅单个任务：

```http
GET /events?taskId=task-001
```

## 写入方建议

为了让外部 copy/FTP/服务写入更可靠，推荐写入方使用临时文件再 rename：

```text
demo.txt.tmp  ->  demo.txt
```

当前服务也会通过稳定检测兜底：只有文件 size 和 mtime 连续多次扫描不变，才会推送事件。
