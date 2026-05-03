const http = require("http");
const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { URL } = require("url");

const HOST = process.env.HOST || "127.0.0.1";
const PORT = Number(process.env.PORT || 3000);
const BASE_PATH = normalizeBasePath(process.env.BASE_PATH || "");
const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, "public");
const TASKS_DIR = path.join(ROOT, "data", "tasks");
const SUBDIRS = ["C1", "C0"];

const tasks = new Map();
const clients = new Set();

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

function broadcast(event, payload) {
  const data = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  for (const client of clients) {
    client.write(data);
  }
}

async function postToReceiver(payload) {
  const response = await fetch(`http://${HOST}:${PORT}${BASE_PATH}/receiver/file-event`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    throw new Error(`receiver responded with ${response.status}`);
  }
}

async function startTask(taskId) {
  if (!isSafeTaskId(taskId)) {
    const error = new Error("taskId only supports letters, numbers, underscore, and hyphen");
    error.statusCode = 400;
    throw error;
  }

  await fsp.mkdir(TASKS_DIR, { recursive: true });
  const dir = path.join(TASKS_DIR, taskId);
  await fsp.mkdir(dir, { recursive: true });
  for (const subdir of SUBDIRS) {
    await fsp.mkdir(path.join(dir, subdir), { recursive: true });
  }

  if (tasks.has(taskId)) {
    return tasks.get(taskId);
  }

  const watchers = [];
  for (const subdir of SUBDIRS) {
    const subdirPath = path.join(dir, subdir);
    const seen = new Set(await listCurrentFiles(subdirPath));
    const watcher = fs.watch(subdirPath, async (eventType, fileName) => {
      if (!fileName || fileName.startsWith(".") || seen.has(fileName)) return;

      const filePath = path.join(subdirPath, fileName);
      try {
        const stat = await fsp.stat(filePath);
        if (!stat.isFile()) return;
        seen.add(fileName);
        await postToReceiver({
          taskId,
          subdir,
          fileName,
          size: stat.size,
          path: filePath,
          eventType,
          detectedAt: new Date().toISOString()
        });
      } catch (error) {
        if (error.code !== "ENOENT") {
          console.error(`monitor error for ${filePath}:`, error);
        }
      }
    });
    watchers.push(watcher);
  }

  const task = {
    taskId,
    dir,
    subdirs: SUBDIRS.map((subdir) => ({
      name: subdir,
      dir: path.join(dir, subdir)
    })),
    startedAt: new Date().toISOString(),
    watchers
  };
  tasks.set(taskId, task);
  broadcast("task-started", publicTask(task));
  return task;
}

function stopTask(taskId) {
  const task = tasks.get(taskId);
  if (!task) return null;

  for (const watcher of task.watchers) {
    watcher.close();
  }
  tasks.delete(taskId);

  const payload = {
    ...publicTask(task),
    stoppedAt: new Date().toISOString()
  };
  broadcast("task-stopped", payload);
  return payload;
}

async function listCurrentFiles(dir) {
  try {
    const entries = await fsp.readdir(dir, { withFileTypes: true });
    return entries.filter((entry) => entry.isFile()).map((entry) => entry.name);
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

function publicTask(task) {
  return {
    taskId: task.taskId,
    dir: task.dir,
    subdirs: task.subdirs,
    startedAt: task.startedAt
  };
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

  if (req.method === "POST" && pathname === "/api/start") {
    const body = await readJson(req);
    const taskId = String(body.taskId || Date.now());
    const task = await startTask(taskId);
    sendJson(res, 200, publicTask(task));
    return;
  }

  if (req.method === "GET" && pathname === "/api/tasks") {
    sendJson(res, 200, Array.from(tasks.values()).map(publicTask));
    return;
  }

  const stopMatch = pathname.match(/^\/api\/tasks\/([^/]+)\/stop$/);
  if (req.method === "POST" && stopMatch) {
    const taskId = decodeURIComponent(stopMatch[1]);
    const task = stopTask(taskId);
    if (!task) {
      sendJson(res, 404, { error: "task not found or already stopped" });
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
      sendJson(res, 404, { error: "task not found; call /api/start first" });
      return;
    }

    const fileName = decodeURIComponent(req.headers["x-file-name"] || "");
    if (!isSafeFileName(fileName)) {
      sendJson(res, 400, { error: "invalid x-file-name header" });
      return;
    }

    const subdir = String(req.headers["x-subdir"] || "");
    if (!isValidSubdir(subdir)) {
      sendJson(res, 400, { error: "invalid x-subdir header; expected C1 or C0" });
      return;
    }

    const body = await readBody(req);
    const targetDir = path.join(task.dir, subdir);
    const tempPath = path.join(targetDir, `.${fileName}.${process.pid}.tmp`);
    const finalPath = path.join(targetDir, fileName);
    await fsp.writeFile(tempPath, body);
    await fsp.rename(tempPath, finalPath);
    sendJson(res, 200, { taskId, subdir, fileName, size: body.length, path: finalPath });
    return;
  }

  if (req.method === "POST" && pathname === "/receiver/file-event") {
    const event = await readJson(req);
    const payload = {
      ...event,
      receivedAt: new Date().toISOString()
    };
    broadcast("file-received", payload);
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "GET" && pathname === "/events") {
    res.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive"
    });
    res.write(": connected\n\n");
    clients.add(res);
    req.on("close", () => clients.delete(res));
    return;
  }

  await serveStatic(req, res, pathname);
}

async function main() {
  await fsp.mkdir(TASKS_DIR, { recursive: true });
  const server = http.createServer((req, res) => {
    handleRequest(req, res).catch((error) => {
      console.error(error);
      sendJson(res, error.statusCode || 500, { error: error.message || "internal server error" });
    });
  });

  server.listen(PORT, HOST, () => {
    console.log(`Server running at http://${HOST}:${PORT}${BASE_PATH || "/"}`);
    console.log(`Task folders: ${TASKS_DIR}`);
  });
}

process.on("SIGINT", () => {
  for (const taskId of tasks.keys()) {
    stopTask(taskId);
  }
  process.exit(0);
});

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
