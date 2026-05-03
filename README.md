# 文件夹监控 Demo

这个小项目包含三个部分：

- 监控服务：`POST /api/start` 后按 `taskId` 创建 `data/tasks/<taskId>/C1` 和 `data/tasks/<taskId>/C0`，并同时监听两个子目录的新文件。
- 接收服务：监控服务发现新文件后，会内部 `POST /receiver/file-event`。
- 测试前端：可同时启动多个任务，选择任务和 `C1`/`C0` 子目录上传文件，并按任务窗口与子目录分别显示接收服务收到的文件消息。
- 停止任务：关闭该任务目录 watcher，并通过 SSE 通知前端关闭对应接收窗口。

## 运行

```bash
npm start
```

打开：

```text
http://127.0.0.1:3000
```

如果需要部署在 Nginx 子路径下，可以设置：

```bash
BASE_PATH=/folder-monitor PORT=5601 HOST=127.0.0.1 node server.js
```

## API

### 启动任务

```http
POST /api/start
Content-Type: application/json

{
  "taskId": "1714636800000"
}
```

### 上传文件到任务目录

```http
POST /api/tasks/:taskId/upload
Content-Type: application/octet-stream
X-File-Name: demo.txt
X-Subdir: C1
```

### 停止任务

```http
POST /api/tasks/:taskId/stop
```

### 接收服务

```http
POST /receiver/file-event
Content-Type: application/json
```

### 前端推送

```http
GET /events
```
