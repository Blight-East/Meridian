const fs = require("fs");
const path = require("path");

const ROOT_DIR = process.env.AGENT_FLUX_ROOT || __dirname;
const ENV_PATH = path.join(ROOT_DIR, ".env");

function parseDotEnv(filePath) {
  const values = {};
  if (!fs.existsSync(filePath)) {
    return values;
  }
  for (const rawLine of fs.readFileSync(filePath, "utf8").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }
    const idx = line.indexOf("=");
    if (idx === -1) {
      continue;
    }
    const key = line.slice(0, idx).trim();
    let value = line.slice(idx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    values[key] = value;
  }
  return values;
}

function resolveLogDir(rootDir, envValues) {
  const preferred = process.env.AGENT_FLUX_LOG_DIR || envValues.AGENT_FLUX_LOG_DIR;
  const candidates = [
    preferred,
    rootDir.startsWith("/opt/agent-flux") ? path.join(rootDir, "logs") : null,
    "/tmp/agent-flux/logs",
  ].filter(Boolean);

  for (const candidate of candidates) {
    try {
      fs.mkdirSync(candidate, { recursive: true });
      return candidate;
    } catch (_err) {
      continue;
    }
  }
  throw new Error("Unable to create a writable Agent Flux log directory");
}

const dotEnv = parseDotEnv(ENV_PATH);
const LOG_DIR = resolveLogDir(ROOT_DIR, dotEnv);
const PYTHONPATH = [ROOT_DIR, path.join(ROOT_DIR, "runtime"), path.join(ROOT_DIR, "deploy")].join(":");
const MCP_PYTHONPATH = [ROOT_DIR, path.join(ROOT_DIR, "deploy")].join(":");
const ENABLE_LOCAL_MCP = ["1", "true", "yes", "on"].includes(
  String(process.env.AGENT_FLUX_ENABLE_LOCAL_MCP || dotEnv.AGENT_FLUX_ENABLE_LOCAL_MCP || "").trim().toLowerCase()
);
const SHARED_ENV = {
  ...dotEnv,
  ...process.env,
  PYTHONPATH,
  AGENT_FLUX_LOG_DIR: LOG_DIR,
};

function logFile(name, suffix) {
  return path.join(LOG_DIR, `${name}-${suffix}.log`);
}

const apps = [
    {
      name: "agent-flux-api",
      script: "python3",
      args: "-m uvicorn runtime.main:app --host 0.0.0.0 --port 8000 --log-level info",
      cwd: ROOT_DIR,
      interpreter: "none",
      env: SHARED_ENV,
      max_restarts: 10,
      restart_delay: 5000,
      autorestart: true,
      watch: false,
      log_file: logFile("api", "pm2"),
      error_file: logFile("api", "error"),
      out_file: logFile("api", "out"),
    },
    {
      name: "agent-flux-worker",
      script: "runtime/worker.py",
      cwd: ROOT_DIR,
      interpreter: "python3",
      env: SHARED_ENV,
      max_restarts: 10,
      restart_delay: 5000,
      autorestart: true,
      watch: false,
      log_file: logFile("worker", "pm2"),
      error_file: logFile("worker", "error"),
      out_file: logFile("worker", "out"),
    },
    {
      name: "agent-flux-scheduler",
      script: "runtime/scheduler/cron_tasks.py",
      cwd: ROOT_DIR,
      interpreter: "python3",
      env: SHARED_ENV,
      max_restarts: 10,
      restart_delay: 5000,
      autorestart: true,
      watch: false,
      // Safety net: scheduler currently boots at ~800MB (SentenceTransformer
      // loaded twice via duplicate import path). Cap at 1.5GB so a true leak
      // can't take down the box; PM2 will restart with backoff.
      max_memory_restart: "1500M",
      log_file: logFile("scheduler", "pm2"),
      error_file: logFile("scheduler", "error"),
      out_file: logFile("scheduler", "out"),
    },
    {
      name: "agentflux-telegram",
      script: "runtime/telegram_bot.py",
      cwd: ROOT_DIR,
      interpreter: "python3",
      env: SHARED_ENV,
      max_restarts: 10,
      restart_delay: 5000,
      autorestart: true,
      watch: false,
      log_file: logFile("telegram", "pm2"),
      error_file: logFile("telegram", "error"),
      out_file: logFile("telegram", "out"),
    },
];

if (ROOT_DIR.startsWith("/opt/agent-flux") || ENABLE_LOCAL_MCP) {
  apps.push({
      name: "meridian-mcp",
      script: "python3",
      args: "-m runtime.mcp.server",
      cwd: ROOT_DIR,
      interpreter: "none",
      env: {
        ...SHARED_ENV,
        // Exclude the raw runtime path so fastmcp resolves the real `mcp` package.
        PYTHONPATH: MCP_PYTHONPATH,
      },
      max_restarts: 10,
      restart_delay: 5000,
      autorestart: true,
      watch: false,
      log_file: logFile("mcp", "pm2"),
      error_file: logFile("mcp", "error"),
      out_file: logFile("mcp", "out"),
    });
}

module.exports = {
  apps,
};
