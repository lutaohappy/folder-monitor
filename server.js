const http = require("http");
const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { pipeline } = require("stream/promises");
const { URL } = require("url");

const HOST = process.env.HOST || "127.0.0.1";
const PORT = Number(process.env.PORT || 3000);
const BASE_PATH = normalizeBasePath(process.env.BASE_PATH || "");
const REDIS_URL = process.env.REDIS_URL || "";
const SCAN_INTERVAL_MS = Number(process.env.SCAN_INTERVAL_MS || 3000);
const STABILITY_CHECKS = Number(process.env.STABILITY_CHECKS || 2);
const EVENT_HISTORY_LIMIT = Number(process.env.EVENT_HISTORY_LIMIT || 200);

const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, "public");
const TASKS_DIR = path.join(ROOT, "data", "tasks");
const SUBDIRS = ["C0", "C1"];
const TMP_FILE_PATTERN = /\.(tmp|part|crdownload|swp)$/i;
const REDIS_STREAM = "folder-monitor:file-events";
const REDIS_GROUP = "folder-monitor-service";
const REDIS_TASKS_KEY = "folder-monitor:tasks";
const REDIS_DEDUPE_KEY = "folder-monitor:processed-events";

const tasks = new Map();
const clients = new Set();
const memoryProcessed = new Set();
const eventHistoryByTask = new Map();

let redisClient = null;
let consumerRunning = false;

function normalizeBasePath(basePath) {
  if (!basePath || basePath === "/") return "";
  return `/${basePath.replace(/^\/+|\/+$/g, "")}`;
}

function routePath(pathname) {
  if (!BASE_PATH) return pathname;
  if (pathname === BASE_PATH) return "/";
  if (pathname.startsWith(`${BASE_PATH}/`)) return pathname.slice(BASE_PATH.length);
  return null;
}

function sendJson(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body)
  });
  res.end(body);
}

function sendText(res, status, text, contentType = "text/plain; charset=utf-8") {
  res.writeHead(status, { "content-type": contentType });
  res.end(text);
}

function isSafeTaskId(taskId) {
  return /^[a-zA-Z0-9_-]+$/.test(taskId);
}

function isSafeFileName(fileName) {
  return Boolean(fileName) && path.basename(fileName) === fileName && !fileName.includes("\0");
}

function isValidSubdir(subdir) {
  return SUBDIRS.includes(subdir);
}

function defaultTaskDirs(taskId) {
  const dir = path.join(TASKS_DIR, taskId);
  return {
    C0: path.join(dir, "C0"),
    C1: path.join(dir, "C1")
  };
}

function normalizeTaskDirs(taskId, body) {
  const defaults = defaultTaskDirs(taskId);
  return {
    C0: path.resolve(String(body.C0 || body.c0 || defaults.C0)),
    C1: path.resolve(String(body.C1 || body.c1 || defaults.C1))
  };
}

async function readJson(req) {
  const body = await readBody(req);
  if (!body.length) return {};
  return JSON.parse(body.toString("utf8"));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

function sendSse(res, event, payload) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
}

function broadcast(event, payload) {
  for (const client of clients) {
    if (payload.taskId && client.taskId && client.taskId !== payload.taskId) continue;
    sendSse(client.res, event, payload);
  }
}

function fileFingerprint(taskId, subdir, filePath, stat) {
  return `${taskId}|${subdir}|${filePath}|${stat.size}|${Math.trunc(stat.mtimeMs)}`;
}

function shouldIgnoreFile(filePath) {
  const fileName = path.basename(filePath);
  return fileName.startsWith(".") || TMP_FILE_PATTERN.test(fileName);
}

async function walkFiles(dir) {
  const files = [];
  let entries;
  try {
    entries = await fsp.readdir(dir, { withFileTypes: true });
  } catch (error) {
    if (error.code === "ENOENT") return files;
    throw error;
  }

  for (const entry of entries) {
    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await walkFiles(entryPath));
    } else if (entry.isFile() && !shouldIgnoreFile(entryPath)) {
      files.push(entryPath);
    }
  }
  return files;
}

async function markProcessed(key) {
  if (redisClient) {
    await redisClient.sAdd(REDIS_DEDUPE_KEY, key);
    return;
  }
  memoryProcessed.add(key);
}

async function claimEvent(key) {
  if (redisClient) {
    return (await redisClient.sAdd(REDIS_DEDUPE_KEY, key)) === 1;
  }
  if (memoryProcessed.has(key)) return false;
  memoryProcessed.add(key);
  return true;
}

async function initRedis() {
  if (!REDIS_URL) return;

  const { createClient } = require("redis");
  redisClient = createClient({ url: REDIS_URL });
  redisClient.on("error", (error) => console.error("redis error:", error.message));
  await redisClient.connect();
  try {
    await redisClient.sendCommand(["XGROUP", "CREATE", REDIS_STREAM, REDIS_GROUP, "0", "MKSTREAM"]);
  } catch (error) {
    if (!String(error.message).includes("BUSYGROUP")) throw error;
  }
  console.log(`Redis enabled: ${REDIS_URL}`);
}

async function restoreTasksFromRedis() {
  if (!redisClient) return;
  const saved = await redisClient.hGetAll(REDIS_TASKS_KEY);
  for (const taskJson of Object.values(saved)) {
    try {
      const task = JSON.parse(taskJson);
      await startTask(task.taskId, { C0: task.dirs.C0, C1: task.dirs.C1 }, { restored: true });
    } catch (error) {
      console.error("failed to restore task:", error);
    }
  }
}

async function saveTask(task) {
  if (!redisClient) return;
  await redisClient.hSet(REDIS_TASKS_KEY, task.taskId, JSON.stringify(publicTask(task)));
}

async function deleteSavedTask(taskId) {
  if (!redisClient) return;
  await redisClient.hDel(REDIS_TASKS_KEY, taskId);
}

async function enqueueFileEvent(payload) {
  if (redisClient) {
    await redisClient.xAdd(REDIS_STREAM, "*", { payload: JSON.stringify(payload) });
    return;
  }
  handleFileEvent(payload);
}

async function startRedisConsumer() {
  if (!redisClient || consumerRunning) return;
  consumerRunning = true;
  const consumerName = `consumer-${process.pid}`;

  while (consumerRunning) {
    try {
      const response = await redisClient.sendCommand([
        "XREADGROUP",
        "GROUP",
        REDIS_GROUP,
        consumerName,
        "COUNT",
        "50",
        "BLOCK",
        "2000",
        "STREAMS",
        REDIS_STREAM,
        ">"
      ]);
      if (!response) continue;

      for (const [, messages] of response) {
        for (const [id, fields] of messages) {
          const payloadIndex = fields.indexOf("payload");
          if (payloadIndex >= 0) {
            handleFileEvent(JSON.parse(fields[payloadIndex + 1]));
          }
          await redisClient.xAck(REDIS_STREAM, REDIS_GROUP, id);
        }
      }
    } catch (error) {
      if (consumerRunning) {
        console.error("redis consumer error:", error.message);
        await sleep(1000);
      }
    }
  }
}

function handleFileEvent(payload) {
  const history = eventHistoryByTask.get(payload.taskId) || [];
  history.unshift(payload);
  eventHistoryByTask.set(payload.taskId, history.slice(0, EVENT_HISTORY_LIMIT));
  broadcast("file-received", payload);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function primeExistingFiles(task, lane) {
  const files = await walkFiles(lane.dir);
  for (const filePath of files) {
    try {
      const stat = await fsp.stat(filePath);
      if (!stat.isFile()) continue;
      await markProcessed(fileFingerprint(task.taskId, lane.name, filePath, stat));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
}

function scheduleScan(task, lane, delay = 150) {
  if (lane.scanTimeout) return;
  lane.scanTimeout = setTimeout(() => {
    lane.scanTimeout = null;
    scanLane(task, lane).catch((error) => {
      console.error(`scan error for ${task.taskId}/${lane.name}:`, error);
    });
  }, delay);
}

async function scanLane(task, lane) {
  const files = await walkFiles(lane.dir);
  const seenThisScan = new Set(files);

  for (const filePath of files) {
    let stat;
    try {
      stat = await fsp.stat(filePath);
    } catch (error) {
      if (error.code === "ENOENT") continue;
      throw error;
    }
    if (!stat.isFile()) continue;

    const previous = lane.candidates.get(filePath);
    const sameFile = previous && previous.size === stat.size && previous.mtimeMs === stat.mtimeMs;
    const stableCount = sameFile ? previous.stableCount + 1 : 1;
    lane.candidates.set(filePath, {
      size: stat.size,
      mtimeMs: stat.mtimeMs,
      stableCount
    });

    if (stableCount < STABILITY_CHECKS) continue;

    const key = fileFingerprint(task.taskId, lane.name, filePath, stat);
    lane.candidates.delete(filePath);
    if (!await claimEvent(key)) continue;

    await enqueueFileEvent({
      taskId: task.taskId,
      subdir: lane.name,
      fileName: path.basename(filePath),
      size: stat.size,
      mtimeMs: Math.trunc(stat.mtimeMs),
      path: filePath,
      source: "directory",
      detectedAt: new Date().toISOString()
    });
  }

  for (const filePath of lane.candidates.keys()) {
    if (!seenThisScan.has(filePath)) lane.candidates.delete(filePath);
  }
}

async function startTask(taskId, dirs, options = {}) {
  if (!isSafeTaskId(taskId)) {
    const error = new Error("taskId only supports letters, numbers, underscore, and hyphen");
    error.statusCode = 400;
    throw error;
  }

  if (tasks.has(taskId)) {
    return tasks.get(taskId);
  }

  await fsp.mkdir(TASKS_DIR, { recursive: true });
  for (const subdir of SUBDIRS) {
    await fsp.mkdir(dirs[subdir], { recursive: true });
  }

  const task = {
    taskId,
    dirs,
    lanes: [],
    startedAt: new Date().toISOString()
  };

  for (const subdir of SUBDIRS) {
    const lane = {
      name: subdir,
      dir: dirs[subdir],
      candidates: new Map(),
      watcher: null,
      scanTimeout: null,
      scanInterval: null
    };

    await primeExistingFiles(task, lane);
    lane.watcher = fs.watch(lane.dir, { persistent: true }, () => scheduleScan(task, lane));
    lane.scanInterval = setInterval(() => scheduleScan(task, lane, 0), SCAN_INTERVAL_MS);
    task.lanes.push(lane);
    scheduleScan(task, lane, 0);
  }

  tasks.set(taskId, task);
  await saveTask(task);
  if (!options.restored) broadcast("task-started", publicTask(task));
  return task;
}

async function endTask(taskId) {
  const task = tasks.get(taskId);
  if (!task) return null;

  closeTaskRuntime(task);
  tasks.delete(taskId);
  await deleteSavedTask(taskId);

  const payload = {
    ...publicTask(task),
    endedAt: new Date().toISOString()
  };
  broadcast("task-ended", payload);
  broadcast("task-stopped", payload);
  return payload;
}

function closeTaskRuntime(task) {
  for (const lane of task.lanes) {
    if (lane.watcher) lane.watcher.close();
    if (lane.scanTimeout) clearTimeout(lane.scanTimeout);
    if (lane.scanInterval) clearInterval(lane.scanInterval);
  }
}

function publicTask(task) {
  return {
    taskId: task.taskId,
    dirs: task.dirs,
    subdirs: task.lanes.map((lane) => ({
      name: lane.name,
      dir: lane.dir
    })),
    startedAt: task.startedAt
  };
}

async function writeUploadToTask(req, task, subdir, fileName) {
  const lane = task.lanes.find((item) => item.name === subdir);
  if (!lane) {
    const error = new Error("subdir not found for task");
    error.statusCode = 400;
    throw error;
  }

  const tempPath = path.join(lane.dir, `.${fileName}.${process.pid}.${Date.now()}.tmp`);
  const finalPath = path.join(lane.dir, fileName);
  await pipeline(req, fs.createWriteStream(tempPath));
  await fsp.rename(tempPath, finalPath);
  const stat = await fsp.stat(finalPath);
  scheduleScan(task, lane, 0);
  return { taskId: task.taskId, subdir, fileName, size: stat.size, path: finalPath };
}

async function serveStatic(req, res, pathname) {
  const requestPath = pathname === "/" ? "/index.html" : pathname;
  const filePath = path.normalize(path.join(PUBLIC_DIR, requestPath));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    sendText(res, 403, "Forbidden");
    return;
  }

  try {
    let content = await fsp.readFile(filePath);
    const ext = path.extname(filePath);
    const contentType = ext === ".html" ? "text/html; charset=utf-8" : "application/octet-stream";
    if (ext === ".html" && BASE_PATH) {
      content = Buffer.from(
        content.toString("utf8").replace(
          "</head>",
          `<script>window.APP_BASE_PATH = ${JSON.stringify(BASE_PATH)};</script></head>`
        )
      );
    }
    res.writeHead(200, { "content-type": contentType });
    res.end(content);
  } catch (error) {
    if (error.code === "ENOENT") {
      sendText(res, 404, "Not found");
      return;
    }
    throw error;
  }
}

async function handleRequest(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = routePath(url.pathname);
  if (pathname === null) {
    sendText(res, 404, "Not found");
    return;
  }

  if (req.method === "POST" && (pathname === "/api/start" || pathname === "/api/tasks/start")) {
    const body = await readJson(req);
    const taskId = String(body.taskId || body.taskID || Date.now());
    const task = await startTask(taskId, normalizeTaskDirs(taskId, body));
    sendJson(res, 200, publicTask(task));
    return;
  }

  if (req.method === "GET" && pathname === "/api/tasks") {
    sendJson(res, 200, Array.from(tasks.values()).map(publicTask));
    return;
  }

  const endMatch = pathname.match(/^\/api\/tasks\/([^/]+)\/(end|stop)$/);
  if (req.method === "POST" && endMatch) {
    const taskId = decodeURIComponent(endMatch[1]);
    const task = await endTask(taskId);
    if (!task) {
      sendJson(res, 404, { error: "task not found or already ended" });
      return;
    }
    sendJson(res, 200, task);
    return;
  }

  const uploadMatch = pathname.match(/^\/api\/tasks\/([^/]+)\/upload$/);
  if (req.method === "POST" && uploadMatch) {
    const taskId = decodeURIComponent(uploadMatch[1]);
    const task = tasks.get(taskId);
    if (!task) {
      sendJson(res, 404, { error: "task not found; call /api/tasks/start first" });
      return;
    }

    const fileName = decodeURIComponent(req.headers["x-file-name"] || "");
    if (!isSafeFileName(fileName)) {
      sendJson(res, 400, { error: "invalid x-file-name header" });
      return;
    }

    const subdir = String(req.headers["x-subdir"] || "");
    if (!isValidSubdir(subdir)) {
      sendJson(res, 400, { error: "invalid x-subdir header; expected C0 or C1" });
      return;
    }

    sendJson(res, 200, await writeUploadToTask(req, task, subdir, fileName));
    return;
  }

  if (req.method === "POST" && pathname === "/receiver/file-event") {
    const event = await readJson(req);
    handleFileEvent({
      ...event,
      receivedAt: new Date().toISOString()
    });
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "GET" && pathname === "/events") {
    const taskId = url.searchParams.get("taskId") || "";
    res.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive"
    });
    res.write(": connected\n\n");
    const client = { res, taskId };
    clients.add(client);

    if (taskId && eventHistoryByTask.has(taskId)) {
      for (const payload of eventHistoryByTask.get(taskId).slice().reverse()) {
        sendSse(res, "file-received", payload);
      }
    }

    req.on("close", () => clients.delete(client));
    return;
  }

  await serveStatic(req, res, pathname);
}

async function shutdown() {
  consumerRunning = false;
  for (const task of tasks.values()) {
    closeTaskRuntime(task);
  }
  if (redisClient) await redisClient.quit();
}

async function main() {
  await initRedis();
  await restoreTasksFromRedis();
  startRedisConsumer().catch((error) => console.error("redis consumer stopped:", error));
  await fsp.mkdir(TASKS_DIR, { recursive: true });

  const server = http.createServer((req, res) => {
    handleRequest(req, res).catch((error) => {
      console.error(error);
      sendJson(res, error.statusCode || 500, { error: error.message || "internal server error" });
    });
  });

  setInterval(() => {
    for (const client of clients) {
      client.res.write(": heartbeat\n\n");
    }
  }, 25000).unref();

  server.listen(PORT, HOST, () => {
    console.log(`Server running at http://${HOST}:${PORT}${BASE_PATH || "/"}`);
    console.log(`Redis: ${REDIS_URL ? "enabled" : "disabled"}`);
    console.log(`Scan interval: ${SCAN_INTERVAL_MS}ms; stability checks: ${STABILITY_CHECKS}`);
  });
}

process.on("SIGINT", () => {
  shutdown().finally(() => process.exit(0));
});

process.on("SIGTERM", () => {
  shutdown().finally(() => process.exit(0));
});

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
