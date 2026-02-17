const fs = require("fs");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const ejs = require("ejs");
const multer = require("multer");
const XLSX = require("xlsx");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { createClient } = require("@supabase/supabase-js");
const { monitorEventLoopDelay, performance } = require("perf_hooks");
const { OntologyService } = require("./src/ontology/service");
const { createRepository } = require("./src/data/repository");
const { IngestionJobQueue } = require("./src/pipeline/job-queue");
const { WorkflowScheduler } = require("./src/workflow/workflow-scheduler");
const {
  buildModelInputSummary,
  buildTablePreview,
  buildRowsDiffPreview,
  collectColumns,
  deriveActionFromCommand,
  parseModelTransformPlan,
  applyModelTransformPlan,
  rowsToCsv,
  sanitizeExecutionRows,
} = require("./src/data/ai-processing");
const { runPythonDataTransform } = require("./src/data/python-transformer");
const {
  runLocalCommands,
  normalizeCommandList,
  normalizePermissionProfile,
} = require("./src/workflow/local-command-runner");
const {
  appendMailboxMessage,
  readUnreadMailboxMessages,
  markMailboxMessagesRead,
} = require("./src/workflow/team-mailbox");

const app = express();
const port = Number(process.env.PORT) || 3000;
const MICROCACHE_TTL_MS = Math.max(0, Number(process.env.MICROCACHE_TTL_MS || 0));
const isProduction = process.env.NODE_ENV === "production";
const runtimeMode = String(process.env.RUNTIME_MODE || "").trim().toLowerCase();
const isServerlessRuntime =
  runtimeMode === "serverless" ||
  process.env.VERCEL === "1" ||
  process.env.VERCEL === "true";
const LATENCY_SAMPLE_SIZE = Math.max(1, Number(process.env.LATENCY_SAMPLE_SIZE || 2048));
const MAX_UPLOAD_SIZE_MB = Math.max(1, Number(process.env.MAX_UPLOAD_SIZE_MB || 20));
const JOB_POLL_INTERVAL_MS = Math.max(100, Number(process.env.JOB_POLL_INTERVAL_MS || 750));
const JOB_BATCH_SIZE = Math.max(1, Number(process.env.JOB_BATCH_SIZE || 5));
const WORKFLOW_POLL_INTERVAL_MS = Math.max(
  100,
  Number(process.env.WORKFLOW_POLL_INTERVAL_MS || 900)
);
const WORKFLOW_BATCH_SIZE = Math.max(
  1,
  Number(process.env.WORKFLOW_BATCH_SIZE || 3)
);
const WORKFLOW_TASK_DELAY_MS = Math.max(
  0,
  Number(process.env.WORKFLOW_TASK_DELAY_MS || 100)
);
const DEFAULT_ACCESS_SECRET = "dev-access-secret-change-this";
const DEFAULT_REFRESH_SECRET = "dev-refresh-secret-change-this";
const ACCESS_TOKEN_TTL_SEC = Math.max(
  60,
  Number(process.env.ACCESS_TOKEN_TTL_SEC || 15 * 60)
);
const REFRESH_TOKEN_TTL_SEC = Math.max(
  300,
  Number(process.env.REFRESH_TOKEN_TTL_SEC || 14 * 24 * 60 * 60)
);
const JWT_ACCESS_SECRET = String(
  process.env.JWT_ACCESS_SECRET || DEFAULT_ACCESS_SECRET
);
const JWT_REFRESH_SECRET = String(
  process.env.JWT_REFRESH_SECRET || DEFAULT_REFRESH_SECRET
);
const AUTH_COOKIE_DOMAIN = String(process.env.AUTH_COOKIE_DOMAIN || "").trim() || undefined;
const AUTH_BOOTSTRAP_EMAIL = String(
  process.env.AUTH_BOOTSTRAP_EMAIL || "admin@agent.local"
).trim();
const AUTH_BOOTSTRAP_PASSWORD = String(
  process.env.AUTH_BOOTSTRAP_PASSWORD || (isProduction ? "" : "admin1234!")
).trim();
const AUTH_BOOTSTRAP_NAME = String(process.env.AUTH_BOOTSTRAP_NAME || "Owner").trim();
const AUTH_BOOTSTRAP_ROLE = String(process.env.AUTH_BOOTSTRAP_ROLE || "owner").trim();
const AUTH_DISABLED =
  String(process.env.AUTH_DISABLED || "true")
    .trim()
    .toLowerCase() !== "false";
const LOGIN_WINDOW_MS = Math.max(
  5000,
  Number(process.env.LOGIN_WINDOW_MS || 60 * 1000)
);
const LOGIN_MAX_ATTEMPTS = Math.max(
  1,
  Number(process.env.LOGIN_MAX_ATTEMPTS || 5)
);
const ACCESS_COOKIE_NAME = "aw_access_token";
const REFRESH_COOKIE_NAME = "aw_refresh_token";
const SUPABASE_URL = String(process.env.SUPABASE_URL || "").trim();
const SUPABASE_SERVICE_ROLE_KEY = String(
  process.env.SUPABASE_SERVICE_ROLE_KEY || ""
).trim();
const SUPABASE_AVATAR_BUCKET = String(
  process.env.SUPABASE_AVATAR_BUCKET || "avatars"
).trim();
const SUPABASE_AVATAR_PREFIX = String(
  process.env.SUPABASE_AVATAR_PREFIX || ""
).trim();
const SUPABASE_AVATAR_PUBLIC = String(
  process.env.SUPABASE_AVATAR_PUBLIC || "true"
).trim().toLowerCase() !== "false";
const SUPABASE_AVATAR_SIGNED_URL_EXPIRES_SEC = Math.max(
  60,
  Number(process.env.SUPABASE_AVATAR_SIGNED_URL_EXPIRES_SEC || 60 * 60)
);
const SUPABASE_AVATAR_MAX_FILES = Math.max(
  100,
  Number(process.env.SUPABASE_AVATAR_MAX_FILES || 5000)
);
const PROVIDER_AUTH_FETCH_TIMEOUT_MS = Math.max(
  2000,
  Number(process.env.PROVIDER_AUTH_FETCH_TIMEOUT_MS || 12000)
);
const PROVIDER_AUTH_MODELS_LIMIT = Math.max(
  5,
  Number(process.env.PROVIDER_AUTH_MODELS_LIMIT || 120)
);
const DATA_AI_MAX_ROWS = Math.max(
  100,
  Number(process.env.DATA_AI_MAX_ROWS || 5000)
);
const DATA_AI_MAX_COLUMNS = Math.max(
  5,
  Number(process.env.DATA_AI_MAX_COLUMNS || 120)
);
const DATA_AI_CELL_MAX_LENGTH = Math.max(
  64,
  Number(process.env.DATA_AI_CELL_MAX_LENGTH || 2048)
);
const DATA_AI_SAMPLE_ROWS = Math.max(
  8,
  Number(process.env.DATA_AI_SAMPLE_ROWS || 40)
);
const DATA_AI_RUN_TTL_SEC = Math.max(
  300,
  Number(process.env.DATA_AI_RUN_TTL_SEC || 30 * 60)
);
const DATA_AI_MODEL_TIMEOUT_MS = Math.max(
  5000,
  Number(process.env.DATA_AI_MODEL_TIMEOUT_MS || 45000)
);
const DATA_AI_PYTHON_TOOL_ENABLED =
  String(
    process.env.DATA_AI_PYTHON_TOOL_ENABLED ||
      (isServerlessRuntime ? "false" : "true")
  )
    .trim()
    .toLowerCase() !== "false";
const DATA_AI_PYTHON_BIN =
  String(process.env.DATA_AI_PYTHON_BIN || "python3").trim() || "python3";
const DATA_AI_PYTHON_TIMEOUT_MS = Math.max(
  1000,
  Number(process.env.DATA_AI_PYTHON_TIMEOUT_MS || 20000)
);
const DATA_AI_PYTHON_SCRIPT =
  String(
    process.env.DATA_AI_PYTHON_SCRIPT ||
      path.join(process.cwd(), "scripts", "data_transformer.py")
  ).trim() || path.join(process.cwd(), "scripts", "data_transformer.py");
const WORKFLOW_LOCAL_EXEC_ENABLED =
  String(process.env.WORKFLOW_LOCAL_EXEC_ENABLED || "false")
    .trim()
    .toLowerCase() === "true";
const WORKFLOW_LOCAL_EXEC_MAX_STEPS = Math.max(
  1,
  Number(process.env.WORKFLOW_LOCAL_EXEC_MAX_STEPS || 3)
);
const WORKFLOW_LOCAL_EXEC_MAX_COMMANDS = Math.max(
  1,
  Number(process.env.WORKFLOW_LOCAL_EXEC_MAX_COMMANDS || 3)
);
const WORKFLOW_LOCAL_EXEC_TIMEOUT_MS = Math.max(
  1000,
  Number(process.env.WORKFLOW_LOCAL_EXEC_TIMEOUT_MS || 45000)
);
const WORKFLOW_LOCAL_EXEC_OUTPUT_MAX_CHARS = Math.max(
  512,
  Number(process.env.WORKFLOW_LOCAL_EXEC_OUTPUT_MAX_CHARS || 12000)
);
const WORKFLOW_LOCAL_EXEC_CWD =
  String(process.env.WORKFLOW_LOCAL_EXEC_CWD || process.cwd()).trim() ||
  process.cwd();
const WORKFLOW_LOCAL_EXEC_SHELL =
  String(process.env.WORKFLOW_LOCAL_EXEC_SHELL || process.env.SHELL || "zsh").trim() ||
  "zsh";
const WORKFLOW_NODE_SESSION_ROOT =
  String(
    process.env.WORKFLOW_NODE_SESSION_ROOT ||
      path.join(process.cwd(), ".cache", "workflow-node-sessions")
  ).trim() || path.join(process.cwd(), ".cache", "workflow-node-sessions");
const WORKFLOW_NODE_MEMORY_TAIL_MAX = Math.max(
  4,
  Number(process.env.WORKFLOW_NODE_MEMORY_TAIL_MAX || 20)
);
const WORKFLOW_NODE_MEMORY_SUMMARY_MAX = Math.max(
  200,
  Number(process.env.WORKFLOW_NODE_MEMORY_SUMMARY_MAX || 4000)
);
const WORKFLOW_AGENT_TEAMS_ENABLED =
  String(process.env.WORKFLOW_AGENT_TEAMS_ENABLED || "true")
    .trim()
    .toLowerCase() !== "false";
const WORKFLOW_AGENT_INBOX_MAX_MESSAGES = Math.max(
  1,
  Number(process.env.WORKFLOW_AGENT_INBOX_MAX_MESSAGES || 20)
);
const WORKFLOW_AGENT_OUTBOX_MAX_MESSAGES = Math.max(
  1,
  Number(process.env.WORKFLOW_AGENT_OUTBOX_MAX_MESSAGES || 8)
);
const PROVIDER_OAUTH_CHALLENGE_TTL_SEC = Math.max(
  120,
  Number(process.env.PROVIDER_OAUTH_CHALLENGE_TTL_SEC || 10 * 60)
);
const PROVIDER_AUTH_ENCRYPTION_SECRET = String(
  process.env.PROVIDER_AUTH_ENCRYPTION_SECRET ||
    process.env.JWT_REFRESH_SECRET ||
    process.env.JWT_ACCESS_SECRET ||
    ""
).trim();
const OPENAI_CODEX_OAUTH_CLIENT_ID = String(
  process.env.OPENAI_CODEX_OAUTH_CLIENT_ID || "app_EMoamEEZ73f0CkXaXp7hrann"
).trim();
const OPENAI_CODEX_OAUTH_AUTHORIZE_URL = String(
  process.env.OPENAI_CODEX_OAUTH_AUTHORIZE_URL ||
    "https://auth.openai.com/oauth/authorize"
).trim();
const OPENAI_CODEX_OAUTH_TOKEN_URL = String(
  process.env.OPENAI_CODEX_OAUTH_TOKEN_URL ||
    "https://auth.openai.com/oauth/token"
).trim();
const OPENAI_CODEX_OAUTH_REDIRECT_URI = String(
  process.env.OPENAI_CODEX_OAUTH_REDIRECT_URI ||
    "http://localhost:1455/auth/callback"
).trim();
const OPENAI_CODEX_OAUTH_SCOPE = String(
  process.env.OPENAI_CODEX_OAUTH_SCOPE ||
    "openid profile email offline_access"
).trim();
const OPENAI_CODEX_ACCOUNT_CLAIM_PATH = "https://api.openai.com/auth";
const OPENAI_API_BASE_URL = String(
  process.env.OPENAI_API_BASE_URL || "https://api.openai.com/v1"
)
  .trim()
  .replace(/\/+$/, "");
const OPENAI_CODEX_RESPONSES_URL = String(
  process.env.OPENAI_CODEX_RESPONSES_URL ||
    "https://chatgpt.com/backend-api/codex/responses"
).trim();
const OPENAI_CODEX_ORIGINATOR = String(
  process.env.OPENAI_CODEX_ORIGINATOR || "pi"
).trim() || "pi";
const OAUTH_CALLBACK_BRIDGE_ENABLED = String(
  process.env.OAUTH_CALLBACK_BRIDGE_ENABLED || "true"
).trim().toLowerCase() !== "false";
const MB = 1024 * 1024;

const repository = createRepository();
const ontologyService = new OntologyService();
const jobQueue = new IngestionJobQueue({
  repository,
  ontologyService,
  pollIntervalMs: JOB_POLL_INTERVAL_MS,
  maxJobsPerTick: JOB_BATCH_SIZE,
});
const workflowScheduler = new WorkflowScheduler({
  repository,
  taskExecutor: executeWorkflowTaskWithAgent,
  pollIntervalMs: WORKFLOW_POLL_INTERVAL_MS,
  maxTasksPerTick: WORKFLOW_BATCH_SIZE,
  taskDelayMs: WORKFLOW_TASK_DELAY_MS,
});

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_UPLOAD_SIZE_MB * MB },
});

const viewsDir = path.join(__dirname, "views");
const dashboardTemplatePath = path.join(viewsDir, "dashboard.ejs");
const cssPath = path.join(__dirname, "public/css/tailwind.css");
const getCssVersion = () =>
  fs.existsSync(cssPath)
  ? String(Math.floor(fs.statSync(cssPath).mtimeMs))
  : String(Date.now());
const initialDashboardData = Object.freeze({
  pageTitle: "SaaS Control Center",
  assetVersion: getCssVersion(),
  user: Object.freeze({
    name: "Choi",
    role: "Founder",
  }),
});
const compiledDashboardTemplate = isProduction
  ? ejs.compile(fs.readFileSync(dashboardTemplatePath, "utf8"), {
      filename: dashboardTemplatePath,
      rmWhitespace: true,
    })
  : null;
const localAvatarDirs = [
  path.join(__dirname, "public/avatars"),
  path.join(__dirname, "avatars"),
];
const avatarStaticDir =
  localAvatarDirs.find((item) => fs.existsSync(item)) ||
  localAvatarDirs[0];
const avatarCatalogCache = {
  source: "none",
  files: [],
  loadedAt: 0,
};
const AVATAR_CATALOG_TTL_MS = 5 * 60 * 1000;
let supabaseAdminClient = null;
const skillSourceRoots = [
  path.join(process.env.HOME || "", ".codex/skills"),
  path.join(process.env.HOME || "", ".agents/skills"),
];
const providerAuthTemplates = Object.freeze([
  Object.freeze({
    provider: "openai",
    label: "OPEN AI",
    description:
      "OpenAI Codex OAuth(Auth)로 연결해 ChatGPT/Codex 계열 모델을 사용합니다.",
    authMode: "oauth",
  }),
  Object.freeze({
    provider: "google",
    label: "GOOGLE",
    description: "Google Gemini API key를 연결해 Gemini 모델을 사용합니다.",
    authMode: "api_key",
  }),
]);
const openaiCodexModelCatalog = Object.freeze([
  Object.freeze({ provider: "openai-codex", id: "gpt-5.3-codex", label: "gpt-5.3-codex" }),
  Object.freeze({
    provider: "openai-codex",
    id: "gpt-5.3-codex-spark",
    label: "gpt-5.3-codex-spark",
  }),
  Object.freeze({ provider: "openai-codex", id: "gpt-5.2-codex", label: "gpt-5.2-codex" }),
  Object.freeze({ provider: "openai-codex", id: "gpt-5.2", label: "gpt-5.2" }),
  Object.freeze({
    provider: "openai-codex",
    id: "gpt-5.1-codex-max",
    label: "gpt-5.1-codex-max",
  }),
  Object.freeze({
    provider: "openai-codex",
    id: "gpt-5.1-codex-mini",
    label: "gpt-5.1-codex-mini",
  }),
  Object.freeze({ provider: "openai-codex", id: "gpt-5.1", label: "gpt-5.1" }),
]);
const WORKFLOW_TEAM_COLOR_PALETTE = Object.freeze([
  "red",
  "blue",
  "green",
  "yellow",
  "purple",
  "orange",
  "pink",
  "cyan",
]);
const providerAuthTemplateById = new Map(
  providerAuthTemplates.map((item) => [item.provider, item])
);

const routeCache = new Map();
const useRouteCache = isProduction && MICROCACHE_TTL_MS > 0;
const eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
const latencySamples = new Float64Array(LATENCY_SAMPLE_SIZE);
let latencyIndex = 0;
let latencyCount = 0;
let requestsTotal = 0;
let inflightRequests = 0;
let initialized = false;
let initPromise = null;
let queueStarted = false;
let workflowSchedulerStarted = false;
let httpServer = null;
let oauthCallbackBridgeServer = null;
let oauthCallbackBridgeAttempted = false;
let shutdownHooksRegistered = false;
const loginAttempts = new Map();
const oauthChallengesByState = new Map();

eventLoopDelay.enable();
app.disable("x-powered-by");
app.set("view engine", "ejs");
app.set("views", viewsDir);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

function round(value, precision = 3) {
  return Number(value.toFixed(precision));
}

function trackLatency(latencyMs) {
  latencySamples[latencyIndex] = latencyMs;
  latencyIndex = (latencyIndex + 1) % LATENCY_SAMPLE_SIZE;
  if (latencyCount < LATENCY_SAMPLE_SIZE) {
    latencyCount += 1;
  }
}

function getLatencySnapshot() {
  if (latencyCount === 0) {
    return { samples: 0 };
  }

  const values = Array.from(latencySamples.slice(0, latencyCount));
  values.sort((a, b) => a - b);

  const percentile = (p) => {
    const index = Math.floor((p / 100) * (values.length - 1));
    return round(values[Math.max(0, Math.min(values.length - 1, index))]);
  };
  const total = values.reduce((sum, value) => sum + value, 0);

  return {
    samples: values.length,
    avg_ms: round(total / values.length),
    p50_ms: percentile(50),
    p90_ms: percentile(90),
    p99_ms: percentile(99),
    max_ms: round(values[values.length - 1]),
  };
}

function buildDashboardData() {
  return {
    ...initialDashboardData,
    assetVersion: isProduction
      ? initialDashboardData.assetVersion
      : getCssVersion(),
  };
}

function renderDashboardView() {
  if (isProduction) {
    return compiledDashboardTemplate(buildDashboardData());
  }

  const dashboardTemplateSource = fs.readFileSync(dashboardTemplatePath, "utf8");
  const renderDashboard = ejs.compile(dashboardTemplateSource, {
    filename: dashboardTemplatePath,
    rmWhitespace: true,
  });

  return renderDashboard(buildDashboardData());
}

function sanitizeTableRows(rows) {
  return rows.map((row) => {
    const cleanRow = {};

    for (const [key, value] of Object.entries(row || {})) {
      const normalizedKey = String(key || "").replace(/\s+/g, " ").trim();
      if (!normalizedKey) {
        continue;
      }
      cleanRow[normalizedKey] = value;
    }

    return cleanRow;
  });
}

function parseWorkbookRows(fileBuffer) {
  const workbook = XLSX.read(fileBuffer, { type: "buffer", raw: false });
  const firstSheetName = workbook.SheetNames[0];

  if (!firstSheetName) {
    throw new Error("Uploaded workbook has no sheets");
  }

  const sheet = workbook.Sheets[firstSheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: null, raw: false });

  return {
    sheetName: firstSheetName,
    rows: sanitizeTableRows(rows),
  };
}

function toPositiveInt(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function summarizeJob(job, includePayload = false) {
  if (!job) {
    return null;
  }

  const rows = Array.isArray(job.payload?.rows) ? job.payload.rows : [];
  const payloadSummary = {
    companyName: job.payload?.companyName || null,
    sourceName: job.payload?.sourceName || null,
    rowCount: rows.length,
    metadata: job.payload?.metadata || {},
  };

  return {
    id: job.id,
    jobType: job.jobType,
    status: job.status,
    attempts: job.attempts,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    completedAt: job.completedAt,
    error: job.error,
    result: job.result,
    payload: includePayload ? job.payload : payloadSummary,
  };
}

function serializeDataset(dataset, previewRows = 20) {
  if (!dataset) {
    return null;
  }

  const maxRows = Math.max(1, Number(previewRows) || 20);
  const rows = Array.isArray(dataset.normalizedRows) ? dataset.normalizedRows : [];

  return {
    id: dataset.id,
    companyName: dataset.companyName,
    sourceName: dataset.sourceName,
    createdAt: dataset.createdAt,
    rowCount: dataset.rowCount,
    columnMapping: dataset.columnMapping || [],
    sampleRows: (dataset.sampleRows || rows).slice(0, 10),
    previewRows: rows.slice(0, maxRows),
  };
}

function normalizeToolSelection(input) {
  if (!Array.isArray(input)) {
    return [];
  }
  return [...new Set(input.map((item) => String(item || "").trim()).filter(Boolean))];
}

function parseCommaSeparated(value) {
  return String(value || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function normalizeStringArray(input) {
  if (!Array.isArray(input)) {
    return [];
  }
  return [...new Set(input.map((item) => String(item || "").trim()).filter(Boolean))];
}

function summarizeWorkflowTaskCounts(tasks) {
  const counts = {
    total: 0,
    pending: 0,
    running: 0,
    completed: 0,
    failed: 0,
  };

  for (const task of Array.isArray(tasks) ? tasks : []) {
    counts.total += 1;
    const status = String(task?.status || "").trim().toLowerCase();
    if (status === "completed") {
      counts.completed += 1;
      continue;
    }
    if (status === "failed") {
      counts.failed += 1;
      continue;
    }
    if (status === "running") {
      counts.running += 1;
      continue;
    }
    counts.pending += 1;
  }

  return counts;
}

function buildWorkflowTasksFromRequest({
  tasksInput,
  nodesInput,
  edgesInput,
  agents,
  goal,
}) {
  const agentsById = new Map(
    (Array.isArray(agents) ? agents : [])
      .filter((agent) => agent?.id)
      .map((agent) => [String(agent.id), agent])
  );
  const agentIdsByLowerName = new Map();
  for (const agent of Array.isArray(agents) ? agents : []) {
    const key = String(agent?.name || "").trim().toLowerCase();
    if (!key || agentIdsByLowerName.has(key)) {
      continue;
    }
    agentIdsByLowerName.set(key, String(agent.id));
  }

  const normalizeKind = (value) => {
    const safe = String(value || "general").trim().toLowerCase();
    return safe || "general";
  };

  const normalizeTaskBase = (item, index) => {
    const taskKey = String(item?.taskKey || item?.id || `task-${index + 1}`).trim();
    const title = String(item?.title || item?.name || `Task ${index + 1}`).trim() || `Task ${index + 1}`;
    const dependsOnTaskKeys = normalizeStringArray(
      item?.dependsOnTaskKeys || item?.dependsOn
    );

    let agentId = String(item?.agentId || "").trim();
    if (!agentId) {
      const candidateName = String(item?.agentName || item?.name || "").trim().toLowerCase();
      if (candidateName && agentIdsByLowerName.has(candidateName)) {
        agentId = agentIdsByLowerName.get(candidateName);
      }
    }
    if (agentId && !agentsById.has(agentId)) {
      throw new Error(`agent not found for task '${taskKey}': ${agentId}`);
    }

    return {
      taskKey,
      title,
      kind: normalizeKind(item?.kind),
      agentId: agentId || null,
      dependsOnTaskKeys,
      input: item?.input && typeof item.input === "object" ? item.input : {},
    };
  };

  if (Array.isArray(tasksInput) && tasksInput.length > 0) {
    const tasks = tasksInput.map((item, index) => normalizeTaskBase(item, index));
    const keySet = new Set(tasks.map((task) => task.taskKey));
    if (keySet.size !== tasks.length) {
      throw new Error("duplicate taskKey in tasks");
    }
    for (const task of tasks) {
      for (const depKey of task.dependsOnTaskKeys) {
        if (!keySet.has(depKey)) {
          throw new Error(`dependsOn taskKey not found: ${depKey}`);
        }
        if (depKey === task.taskKey) {
          throw new Error(`task cannot depend on itself: ${task.taskKey}`);
        }
      }
    }
    assertWorkflowTasksAcyclic(tasks);
    return tasks;
  }

  if (Array.isArray(nodesInput) && nodesInput.length > 0) {
    const nodes = nodesInput.map((item, index) => {
      const nodeId = String(item?.id || `node-${index + 1}`).trim();
      const base = normalizeTaskBase(item, index);
      const input = {
        ...(base.input || {}),
        nodeId,
      };
      if (
        !Object.prototype.hasOwnProperty.call(input, "permissionProfile") &&
        item &&
        Object.prototype.hasOwnProperty.call(item, "permissionProfile")
      ) {
        input.permissionProfile = item.permissionProfile;
      }
      if (
        !Object.prototype.hasOwnProperty.call(input, "permissions") &&
        item &&
        item.permissions &&
        typeof item.permissions === "object"
      ) {
        input.permissions = item.permissions;
      }
      if (
        !Object.prototype.hasOwnProperty.call(input, "allowCommands") &&
        Array.isArray(item?.allowCommands)
      ) {
        input.allowCommands = item.allowCommands;
      }
      if (
        !Object.prototype.hasOwnProperty.call(input, "denyPatterns") &&
        Array.isArray(item?.denyPatterns)
      ) {
        input.denyPatterns = item.denyPatterns;
      }
      return {
        ...base,
        nodeId,
        input,
      };
    });

    const taskByNodeId = new Map(nodes.map((node) => [node.nodeId, node.taskKey]));
    const dependsOnByTaskKey = new Map(nodes.map((node) => [node.taskKey, new Set()]));
    for (const edge of Array.isArray(edgesInput) ? edgesInput : []) {
      const fromId = String(edge?.from || edge?.source || "").trim();
      const toId = String(edge?.to || edge?.target || "").trim();
      if (!fromId || !toId) {
        continue;
      }
      const fromTaskKey = taskByNodeId.get(fromId);
      const toTaskKey = taskByNodeId.get(toId);
      if (!fromTaskKey || !toTaskKey || fromTaskKey === toTaskKey) {
        continue;
      }
      dependsOnByTaskKey.get(toTaskKey)?.add(fromTaskKey);
    }

    const tasks = nodes.map((node) => ({
      taskKey: node.taskKey,
      title: node.title,
      kind: node.kind,
      agentId: node.agentId,
      dependsOnTaskKeys: [
        ...new Set([
          ...node.dependsOnTaskKeys,
          ...Array.from(dependsOnByTaskKey.get(node.taskKey) || []),
        ]),
      ],
      input: node.input,
    }));
    assertWorkflowTasksAcyclic(tasks);
    return tasks;
  }

  return [
    {
      taskKey: "task-1",
      title: "Goal kickoff",
      kind: "planning",
      agentId: null,
      dependsOnTaskKeys: [],
      input: {
        goal: String(goal || "").trim(),
      },
    },
  ];
}

function assertWorkflowTasksAcyclic(tasks) {
  const list = Array.isArray(tasks) ? tasks : [];
  const graph = new Map();
  for (const task of list) {
    const key = String(task?.taskKey || "").trim();
    if (!key) {
      continue;
    }
    graph.set(key, normalizeStringArray(task?.dependsOnTaskKeys));
  }

  const visiting = new Set();
  const visited = new Set();

  const visit = (key) => {
    if (visited.has(key)) {
      return;
    }
    if (visiting.has(key)) {
      throw new Error(`workflow dependency cycle detected: ${key}`);
    }
    visiting.add(key);
    const deps = graph.get(key) || [];
    for (const dep of deps) {
      if (!graph.has(dep)) {
        continue;
      }
      visit(dep);
    }
    visiting.delete(key);
    visited.add(key);
  };

  for (const key of graph.keys()) {
    visit(key);
  }
}

function toPosixPath(value) {
  return String(value || "").split(path.sep).join("/");
}

function encodePathSegments(value) {
  return String(value || "")
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function listFilesRecursive(rootDir, predicate) {
  if (!rootDir || !fs.existsSync(rootDir)) {
    return [];
  }

  const files = [];
  const directories = [""];

  while (directories.length > 0) {
    const nextDir = directories.pop();
    const absoluteDir = path.join(rootDir, nextDir);

    let entries = [];
    try {
      entries = fs.readdirSync(absoluteDir, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      const relativePath = nextDir ? path.join(nextDir, entry.name) : entry.name;
      if (entry.isDirectory()) {
        directories.push(relativePath);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }

      const normalized = toPosixPath(relativePath);
      if (typeof predicate === "function" && !predicate(normalized)) {
        continue;
      }
      files.push(normalized);
    }
  }

  return files;
}

function normalizeStoragePrefix(value) {
  return String(value || "").trim().replace(/^\/+|\/+$/g, "");
}

function hasSupabaseAvatarStorage() {
  return Boolean(
    SUPABASE_URL &&
      SUPABASE_SERVICE_ROLE_KEY &&
      SUPABASE_AVATAR_BUCKET
  );
}

function getSupabaseAdminClient() {
  if (!hasSupabaseAvatarStorage()) {
    return null;
  }
  if (supabaseAdminClient) {
    return supabaseAdminClient;
  }
  supabaseAdminClient = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });
  return supabaseAdminClient;
}

function loadLocalAvatarCatalog() {
  const files = [];
  const seen = new Set();
  for (const rootDir of localAvatarDirs) {
    const localFiles = listFilesRecursive(rootDir, (item) =>
      item.toLowerCase().endsWith(".svg")
    );
    for (const file of localFiles) {
      const key = file.toLowerCase();
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      files.push(file);
    }
  }
  return files;
}

async function listSupabaseFilesRecursive(
  client,
  bucket,
  prefix = "",
  maxFiles = SUPABASE_AVATAR_MAX_FILES
) {
  const files = [];
  const directories = [normalizeStoragePrefix(prefix)];
  const limit = 1000;

  while (directories.length > 0 && files.length < maxFiles) {
    const currentPrefix = directories.pop();
    let offset = 0;

    while (files.length < maxFiles) {
      const { data, error } = await client.storage.from(bucket).list(currentPrefix, {
        limit,
        offset,
        sortBy: {
          column: "name",
          order: "asc",
        },
      });

      if (error) {
        throw new Error(error.message || "failed to list storage objects");
      }

      const entries = Array.isArray(data) ? data : [];
      for (const entry of entries) {
        const name = String(entry?.name || "").trim();
        if (!name) {
          continue;
        }

        const fullPath = currentPrefix ? `${currentPrefix}/${name}` : name;
        const metadata = entry?.metadata;
        const isDirectory = !entry?.id && (metadata === null || metadata === undefined);

        if (isDirectory) {
          directories.push(fullPath);
          continue;
        }

        if (name.toLowerCase().endsWith(".svg")) {
          files.push(fullPath);
          if (files.length >= maxFiles) {
            break;
          }
        }
      }

      if (entries.length < limit) {
        break;
      }

      offset += limit;
    }
  }

  return files;
}

async function loadAvatarCatalog() {
  const now = Date.now();
  const supabaseEnabled = hasSupabaseAvatarStorage();
  const preferredSource = supabaseEnabled ? "supabase" : "local";

  if (
    avatarCatalogCache.files.length > 0 &&
    now - avatarCatalogCache.loadedAt < AVATAR_CATALOG_TTL_MS &&
    avatarCatalogCache.source === preferredSource
  ) {
    return {
      files: avatarCatalogCache.files,
      source: avatarCatalogCache.source,
      client: supabaseEnabled ? getSupabaseAdminClient() : null,
    };
  }

  if (supabaseEnabled) {
    const client = getSupabaseAdminClient();
    try {
      const files = await listSupabaseFilesRecursive(
        client,
        SUPABASE_AVATAR_BUCKET,
        SUPABASE_AVATAR_PREFIX
      );
      if (files.length > 0) {
        avatarCatalogCache.files = files;
        avatarCatalogCache.loadedAt = now;
        avatarCatalogCache.source = "supabase";
        return {
          files,
          source: "supabase",
          client,
        };
      }
    } catch (error) {
      console.error("[avatar] supabase listing failed:", error.message);
    }
  }

  const localFiles = loadLocalAvatarCatalog();
  avatarCatalogCache.files = localFiles;
  avatarCatalogCache.loadedAt = now;
  avatarCatalogCache.source = localFiles.length > 0 ? "local" : "none";
  return {
    files: localFiles,
    source: avatarCatalogCache.source,
    client: null,
  };
}

async function resolveSupabaseAvatarUrl(client, filePath) {
  if (!client) {
    return "";
  }

  if (SUPABASE_AVATAR_PUBLIC) {
    const { data } = client.storage.from(SUPABASE_AVATAR_BUCKET).getPublicUrl(filePath);
    return String(data?.publicUrl || "");
  }

  const { data, error } = await client.storage
    .from(SUPABASE_AVATAR_BUCKET)
    .createSignedUrl(filePath, SUPABASE_AVATAR_SIGNED_URL_EXPIRES_SEC);
  if (error) {
    throw new Error(error.message || "failed to create signed avatar url");
  }
  return String(data?.signedUrl || "");
}

function createGeneratedAvatar() {
  const palettes = [
    { bg: "#eff6ff", skin: "#fde68a", shirt: "#1f2937", hair: "#111827" },
    { bg: "#f0fdf4", skin: "#fed7aa", shirt: "#0f766e", hair: "#1f2937" },
    { bg: "#fff7ed", skin: "#fcd34d", shirt: "#7c2d12", hair: "#111827" },
    { bg: "#f5f3ff", skin: "#f5d0fe", shirt: "#312e81", hair: "#27272a" },
    { bg: "#fdf2f8", skin: "#fde68a", shirt: "#be185d", hair: "#111827" },
  ];

  const pick = palettes[Math.floor(Math.random() * palettes.length)];
  const seed = crypto.randomBytes(4).toString("hex");
  const eyeStyle = Math.floor(Math.random() * 3);
  const eyes =
    eyeStyle === 0
      ? '<circle cx="104" cy="104" r="4.5" fill="#111827"/><circle cx="152" cy="104" r="4.5" fill="#111827"/>'
      : eyeStyle === 1
        ? '<rect x="99.5" y="100.5" width="9" height="6" rx="3" fill="#111827"/><rect x="147.5" y="100.5" width="9" height="6" rx="3" fill="#111827"/>'
        : '<path d="M98 105q6-6 12 0" stroke="#111827" stroke-width="3" fill="none" stroke-linecap="round"/><path d="M146 105q6-6 12 0" stroke="#111827" stroke-width="3" fill="none" stroke-linecap="round"/>';

  const svg = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" width="256" height="256">
  <rect width="256" height="256" fill="${pick.bg}" />
  <circle cx="128" cy="112" r="52" fill="${pick.skin}" />
  <path d="M76 104c0-36 24-60 52-60s52 24 52 60c-12-14-32-24-52-24s-40 10-52 24z" fill="${pick.hair}" />
  ${eyes}
  <path d="M110 132q18 14 36 0" stroke="#92400e" stroke-width="4" fill="none" stroke-linecap="round" />
  <path d="M54 232c0-44 33-74 74-74s74 30 74 74" fill="${pick.shirt}" />
</svg>
  `.trim();

  return {
    path: `generated/${seed}.svg`,
    name: `generated-${seed}.svg`,
    url: `data:image/svg+xml;base64,${Buffer.from(svg, "utf8").toString("base64")}`,
    generated: true,
  };
}

function listAvailableSkills() {
  const skills = [];
  const seen = new Set();

  for (const root of skillSourceRoots) {
    if (!root || !fs.existsSync(root)) {
      continue;
    }

    const skillFiles = listFilesRecursive(root, (item) =>
      item.toLowerCase().endsWith("skill.md")
    );

    for (const skillFile of skillFiles) {
      const relativeDir = toPosixPath(path.posix.dirname(skillFile));
      const id = relativeDir === "." ? "root" : relativeDir;
      const dedupeKey = id.toLowerCase();
      if (seen.has(dedupeKey)) {
        continue;
      }
      seen.add(dedupeKey);
      skills.push({
        id,
        label: id.replace(/^\.system\//, "").replaceAll("/", " / "),
      });
    }
  }

  return skills.sort((a, b) => a.id.localeCompare(b.id));
}

function normalizeProviderId(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-");
}

function getProviderTemplate(provider) {
  return providerAuthTemplateById.get(normalizeProviderId(provider)) || null;
}

function resolveProviderLabel(provider) {
  const template = getProviderTemplate(provider);
  if (template?.label) {
    return template.label;
  }
  const normalized = normalizeProviderId(provider);
  if (!normalized) {
    return "Unknown";
  }
  return normalized.toUpperCase();
}

function normalizeProviderModelEntry(provider, model) {
  const providerOverride =
    typeof model === "object" && model !== null ? model.provider : null;
  const providerId = normalizeProviderId(providerOverride || provider);
  if (!providerId) {
    return null;
  }

  const modelIdRaw =
    typeof model === "string"
      ? model
      : String(model?.id || model?.modelId || model?.name || "").trim();
  const modelId = String(modelIdRaw || "")
    .replace(/^models\//i, "")
    .trim();
  if (!modelId) {
    return null;
  }

  const labelRaw =
    typeof model === "object" && model !== null
      ? String(model.label || model.displayName || model.name || modelId)
      : modelId;
  const label = labelRaw.trim() || modelId;
  const value = `${providerId}/${modelId}`;

  return {
    provider: providerId,
    modelId,
    value,
    label,
  };
}

function normalizeProviderModels(provider, models) {
  if (!Array.isArray(models) || models.length === 0) {
    return [];
  }

  const dedupe = new Set();
  const normalized = [];

  for (const item of models) {
    const entry = normalizeProviderModelEntry(provider, item);
    if (!entry || dedupe.has(entry.value)) {
      continue;
    }
    dedupe.add(entry.value);
    normalized.push(entry);
    if (normalized.length >= PROVIDER_AUTH_MODELS_LIMIT) {
      break;
    }
  }

  return normalized;
}

function toPublicProviderAuthConnection(connection) {
  const provider = normalizeProviderId(connection?.provider);
  const models = normalizeProviderModels(provider, connection?.models);
  const statusRaw = String(connection?.status || "pending")
    .trim()
    .toLowerCase();
  const status = ["connected", "pending", "error"].includes(statusRaw)
    ? statusRaw
    : "pending";

  return {
    id: connection?.id || null,
    provider,
    displayName: String(connection?.displayName || resolveProviderLabel(provider)).trim(),
    authMode: String(connection?.authMode || "api_key").trim().toLowerCase() || "api_key",
    status,
    isAuthenticated: status === "connected",
    modelCount: models.length,
    models,
    errorMessage: String(connection?.errorMessage || "").trim() || null,
    lastCheckedAt: connection?.lastCheckedAt || null,
    updatedAt: connection?.updatedAt || null,
  };
}

function getModelCatalogFromConnections(connections) {
  const dedupe = new Set();
  const models = [
    {
      value: "Balanced (default)",
      label: "Balanced (default)",
      provider: "system",
      modelId: "Balanced (default)",
      source: "builtin",
    },
  ];
  dedupe.add("Balanced (default)");

  for (const connection of connections) {
    if (!connection || connection.status !== "connected") {
      continue;
    }
    const normalizedModels = normalizeProviderModels(
      connection.provider,
      connection.models
    );
    for (const model of normalizedModels) {
      if (dedupe.has(model.value)) {
        continue;
      }
      dedupe.add(model.value);
      models.push({
        ...model,
        label: `${connection.displayName} / ${model.label}`,
        source: "provider",
      });
    }
  }

  return models;
}

function getProviderAuthEncryptionKey() {
  const seed =
    PROVIDER_AUTH_ENCRYPTION_SECRET ||
    `${JWT_ACCESS_SECRET}:${JWT_REFRESH_SECRET}`;
  return crypto.createHash("sha256").update(seed).digest();
}

function encryptProviderSecret(secret) {
  const normalized = String(secret || "").trim();
  if (!normalized) {
    return null;
  }

  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(
    "aes-256-gcm",
    getProviderAuthEncryptionKey(),
    iv
  );
  const encrypted = Buffer.concat([
    cipher.update(normalized, "utf8"),
    cipher.final(),
  ]);
  const authTag = cipher.getAuthTag();
  return `v1:${iv.toString("base64")}:${authTag.toString(
    "base64"
  )}:${encrypted.toString("base64")}`;
}

function decryptProviderSecret(secretEncrypted) {
  const encoded = String(secretEncrypted || "").trim();
  if (!encoded) {
    return null;
  }

  const [version, ivB64, authTagB64, encryptedB64] = encoded.split(":");
  if (version !== "v1" || !ivB64 || !authTagB64 || !encryptedB64) {
    throw new Error("invalid encrypted provider secret format");
  }

  const iv = Buffer.from(ivB64, "base64");
  const authTag = Buffer.from(authTagB64, "base64");
  const encrypted = Buffer.from(encryptedB64, "base64");

  const decipher = crypto.createDecipheriv(
    "aes-256-gcm",
    getProviderAuthEncryptionKey(),
    iv
  );
  decipher.setAuthTag(authTag);
  const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
  return decrypted.toString("utf8");
}

function toBase64Url(input) {
  if (input === null || input === undefined) {
    return "";
  }
  const source = Buffer.isBuffer(input)
    ? input
    : Buffer.from(String(input), "utf8");
  return source.toString("base64url");
}

function fromBase64Url(value) {
  return Buffer.from(String(value || ""), "base64url");
}

function signOAuthChallengePayload(payloadEncoded) {
  return toBase64Url(
    crypto
      .createHmac("sha256", getProviderAuthEncryptionKey())
      .update(String(payloadEncoded || ""))
      .digest()
  );
}

function createOAuthChallengeToken(payload) {
  const body = toBase64Url(JSON.stringify(payload || {}));
  const signature = signOAuthChallengePayload(body);
  return `${body}.${signature}`;
}

function parseOAuthChallengeToken(token) {
  const raw = String(token || "").trim();
  if (!raw) {
    throw new Error("challengeToken is required");
  }

  const [body, signature] = raw.split(".");
  if (!body || !signature) {
    throw new Error("invalid challenge token");
  }

  const expected = signOAuthChallengePayload(body);
  const providedBuffer = fromBase64Url(signature);
  const expectedBuffer = fromBase64Url(expected);
  if (
    providedBuffer.length !== expectedBuffer.length ||
    !crypto.timingSafeEqual(providedBuffer, expectedBuffer)
  ) {
    throw new Error("invalid challenge token signature");
  }

  let payload = {};
  try {
    payload = JSON.parse(fromBase64Url(body).toString("utf8"));
  } catch {
    throw new Error("invalid challenge token payload");
  }

  const expiresAt = Number(payload?.exp || 0);
  if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
    throw new Error("challenge token expired");
  }

  return payload;
}

function generatePkcePair() {
  const verifier = toBase64Url(crypto.randomBytes(32));
  const challenge = toBase64Url(
    crypto.createHash("sha256").update(verifier).digest()
  );
  return { verifier, challenge };
}

function parseAuthorizationInput(input) {
  const value = String(input || "").trim();
  if (!value) {
    return {};
  }

  try {
    const url = new URL(value);
    return {
      code: url.searchParams.get("code") || undefined,
      state: url.searchParams.get("state") || undefined,
    };
  } catch {
    // not a URL
  }

  if (value.includes("#")) {
    const [code, state] = value.split("#", 2);
    return {
      code: String(code || "").trim() || undefined,
      state: String(state || "").trim() || undefined,
    };
  }

  if (value.includes("code=")) {
    const params = new URLSearchParams(value);
    return {
      code: params.get("code") || undefined,
      state: params.get("state") || undefined,
    };
  }

  return { code: value };
}

function decodeJwtPayload(accessToken) {
  const token = String(accessToken || "").trim();
  if (!token) {
    return null;
  }
  const parts = token.split(".");
  if (parts.length !== 3) {
    return null;
  }

  try {
    const payloadRaw = fromBase64Url(parts[1]).toString("utf8");
    return JSON.parse(payloadRaw);
  } catch {
    return null;
  }
}

function getOpenAICodexAccountId(accessToken) {
  const payload = decodeJwtPayload(accessToken);
  const authClaims = payload?.[OPENAI_CODEX_ACCOUNT_CLAIM_PATH];
  const accountId = authClaims?.chatgpt_account_id;
  return typeof accountId === "string" && accountId.trim() ? accountId.trim() : null;
}

function getOpenAICodexModels() {
  return openaiCodexModelCatalog.map((item) => ({
    provider: item.provider,
    id: item.id,
    label: item.label,
  }));
}

function createOpenAICodexOAuthChallenge({ connectionId, userId }) {
  const { verifier, challenge } = generatePkcePair();
  const state = crypto.randomBytes(16).toString("hex");
  const issuedAt = Date.now();
  const expiresAt = issuedAt + PROVIDER_OAUTH_CHALLENGE_TTL_SEC * 1000;

  const url = new URL(OPENAI_CODEX_OAUTH_AUTHORIZE_URL);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", OPENAI_CODEX_OAUTH_CLIENT_ID);
  url.searchParams.set("redirect_uri", OPENAI_CODEX_OAUTH_REDIRECT_URI);
  url.searchParams.set("scope", OPENAI_CODEX_OAUTH_SCOPE);
  url.searchParams.set("code_challenge", challenge);
  url.searchParams.set("code_challenge_method", "S256");
  url.searchParams.set("state", state);
  url.searchParams.set("id_token_add_organizations", "true");
  url.searchParams.set("codex_cli_simplified_flow", "true");
  url.searchParams.set("originator", "pi");

  const challengeToken = createOAuthChallengeToken({
    v: 1,
    provider: "openai",
    connectionId: String(connectionId || ""),
    userId: String(userId || ""),
    redirectUri: OPENAI_CODEX_OAUTH_REDIRECT_URI,
    verifier,
    state,
    iat: issuedAt,
    exp: expiresAt,
  });

  return {
    challengeToken,
    authorizeUrl: url.toString(),
    expiresAt: new Date(expiresAt).toISOString(),
    state,
    instructions: [
      "브라우저에서 OpenAI 로그인을 완료한 뒤",
      "리다이렉트된 URL 전체를 복사해 붙여넣으세요.",
      `redirect_uri: ${OPENAI_CODEX_OAUTH_REDIRECT_URI}`,
    ].join(" "),
  };
}

function trackOAuthChallengeState({ state, challengeToken, connectionId, userId, expiresAt }) {
  const stateKey = String(state || "").trim();
  if (!stateKey) {
    return;
  }
  const expiresAtMs = new Date(expiresAt).getTime();
  oauthChallengesByState.set(stateKey, {
    challengeToken: String(challengeToken || "").trim(),
    connectionId: String(connectionId || "").trim(),
    userId: String(userId || "").trim(),
    expiresAtMs: Number.isFinite(expiresAtMs) ? expiresAtMs : Date.now() + 60 * 1000,
  });
}

function consumeOAuthChallengeState(state) {
  const stateKey = String(state || "").trim();
  if (!stateKey) {
    return null;
  }
  const item = oauthChallengesByState.get(stateKey);
  if (!item) {
    return null;
  }
  oauthChallengesByState.delete(stateKey);
  if (Date.now() > Number(item.expiresAtMs || 0)) {
    return null;
  }
  return item;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function isLoopbackHost(hostname) {
  const value = String(hostname || "").trim().toLowerCase();
  return value === "localhost" || value === "127.0.0.1" || value === "::1";
}

function resolveOpenAICallbackTarget() {
  try {
    const url = new URL(OPENAI_CODEX_OAUTH_REDIRECT_URI);
    const port = Number(url.port || (url.protocol === "http:" ? 80 : 443));
    if (!Number.isFinite(port) || port <= 0) {
      return null;
    }
    return {
      protocol: url.protocol,
      hostname: url.hostname,
      port,
      pathname: url.pathname || "/",
    };
  } catch {
    return null;
  }
}

function shouldStartOAuthCallbackBridge() {
  if (!OAUTH_CALLBACK_BRIDGE_ENABLED || isServerlessRuntime) {
    return false;
  }
  const target = resolveOpenAICallbackTarget();
  if (!target) {
    return false;
  }
  if (target.protocol !== "http:") {
    return false;
  }
  if (!isLoopbackHost(target.hostname)) {
    return false;
  }
  return true;
}

function getOAuthCallbackBridgeHtml({
  title = "OAuth 인증 완료",
  message = "원래 창으로 돌아가 자동 완료를 기다리세요. 창이 자동으로 닫히지 않으면 직접 닫아도 됩니다.",
  callbackUrl = "",
  payload = {},
  closeDelayMs = 500,
} = {}) {
  const safePayload = JSON.stringify(payload || {});
  const safeTitle = escapeHtml(title);
  const safeMessage = escapeHtml(message);
  const safeCallbackUrl = escapeHtml(callbackUrl);
  return `<!doctype html>
<html lang="ko">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${safeTitle}</title>
    <style>
      body {
        margin: 0;
        padding: 24px;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #f8fafc;
        color: #0f172a;
      }
      .wrap {
        max-width: 640px;
        margin: 0 auto;
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 16px;
      }
      h1 {
        margin: 0 0 8px;
        font-size: 18px;
      }
      p {
        margin: 0;
        font-size: 14px;
        color: #475569;
      }
      code {
        display: block;
        margin-top: 12px;
        padding: 10px;
        border-radius: 10px;
        background: #0f172a;
        color: #e2e8f0;
        font-size: 12px;
        word-break: break-all;
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <h1>${safeTitle}</h1>
      <p>${safeMessage}</p>
      <code id="callback"></code>
    </div>
    <script>
      (function () {
        var callbackUrl = ${JSON.stringify(String(callbackUrl || ""))} || window.location.href;
        var payload = ${safePayload};
        if (!payload || typeof payload !== "object") {
          payload = {};
        }
        payload.callbackUrl = callbackUrl;
        var callbackNode = document.getElementById("callback");
        if (callbackNode) {
          callbackNode.textContent = callbackUrl;
        }
        try {
          if (window.opener && !window.opener.closed) {
            window.opener.postMessage(payload, "*");
          }
        } catch (error) {
          // ignore postMessage errors
        }
        setTimeout(function () {
          try {
            window.close();
          } catch (error) {
            // ignore close errors
          }
        }, ${Math.max(0, Number(closeDelayMs) || 0)});
      })();
    </script>
  </body>
</html>`;
}

function ensureOAuthCallbackBridgeStarted() {
  if (oauthCallbackBridgeServer || oauthCallbackBridgeAttempted) {
    return;
  }
  oauthCallbackBridgeAttempted = true;
  if (!shouldStartOAuthCallbackBridge()) {
    return;
  }

  const target = resolveOpenAICallbackTarget();
  if (!target) {
    return;
  }

  const bridge = http.createServer(async (req, res) => {
    const method = String(req.method || "GET").toUpperCase();
    if (method !== "GET") {
      res.statusCode = 405;
      res.setHeader("content-type", "text/plain; charset=utf-8");
      res.end("Method Not Allowed");
      return;
    }

    let url;
    try {
      url = new URL(req.url || "/", "http://localhost");
    } catch {
      res.statusCode = 400;
      res.setHeader("content-type", "text/plain; charset=utf-8");
      res.end("Bad Request");
      return;
    }

    if (url.pathname !== target.pathname) {
      res.statusCode = 404;
      res.setHeader("content-type", "text/plain; charset=utf-8");
      res.end("Not Found");
      return;
    }

    const callbackUrlObject = new URL(`${target.protocol}//localhost`);
    callbackUrlObject.hostname = target.hostname;
    callbackUrlObject.port = String(target.port);
    callbackUrlObject.pathname = url.pathname;
    callbackUrlObject.search = url.search;
    const callbackUrl = callbackUrlObject.toString();
    const callbackState = String(url.searchParams.get("state") || "").trim();
    const oauthError = String(url.searchParams.get("error") || "").trim();
    const oauthErrorDescription = String(
      url.searchParams.get("error_description") || ""
    ).trim();
    const code = String(url.searchParams.get("code") || "").trim();

    if (oauthError) {
      const message = oauthErrorDescription || oauthError;
      res.statusCode = 200;
      res.setHeader("content-type", "text/html; charset=utf-8");
      res.end(
        getOAuthCallbackBridgeHtml({
          title: "OAuth 인증 실패",
          message,
          callbackUrl,
          payload: {
            type: "provider-oauth-complete",
            ok: false,
            errorMessage: message,
          },
          closeDelayMs: 0,
        })
      );
      return;
    }

    if (code && callbackState) {
      const tracked = consumeOAuthChallengeState(callbackState);
      if (tracked?.challengeToken && tracked?.connectionId) {
        const now = new Date().toISOString();
        try {
          const verified = await verifyProviderOAuth({
            provider: "openai",
            challengeToken: tracked.challengeToken,
            callbackInput: callbackUrl,
            expectedConnectionId: tracked.connectionId,
            expectedUserId: tracked.userId,
          });
          const updated = await repository.updateProviderAuthConnection(
            tracked.connectionId,
            {
              status: "connected",
              secretEncrypted: encryptProviderSecret(
                JSON.stringify(verified.secret || {})
              ),
              models: normalizeProviderModels("openai", verified.models),
              meta: verified.meta || {},
              lastCheckedAt: now,
              errorMessage: null,
            }
          );
          const provider = toPublicProviderAuthConnection(updated || {});
          const verifiedModels = Array.isArray(verified.models)
            ? verified.models.length
            : 0;

          res.statusCode = 200;
          res.setHeader("content-type", "text/html; charset=utf-8");
          res.end(
            getOAuthCallbackBridgeHtml({
              title: "OAuth 인증 완료",
              message: "원래 창으로 돌아가 주세요. 인증 상태를 갱신합니다.",
              callbackUrl,
              payload: {
                type: "provider-oauth-complete",
                ok: true,
                providerId: tracked.connectionId,
                providerName: provider.displayName || "OPEN AI",
                verifiedModels,
              },
            })
          );
          return;
        } catch (error) {
          const message = sanitizeProviderAuthError(error);
          try {
            await repository.updateProviderAuthConnection(tracked.connectionId, {
              status: "error",
              errorMessage: message,
              lastCheckedAt: now,
            });
          } catch {
            // ignore secondary update errors
          }

          res.statusCode = 200;
          res.setHeader("content-type", "text/html; charset=utf-8");
          res.end(
            getOAuthCallbackBridgeHtml({
              title: "OAuth 인증 실패",
              message,
              callbackUrl,
              payload: {
                type: "provider-oauth-complete",
                ok: false,
                providerId: tracked.connectionId,
                errorMessage: message,
              },
              closeDelayMs: 0,
            })
          );
          return;
        }
      }
    }

    res.statusCode = 200;
    res.setHeader("content-type", "text/html; charset=utf-8");
    res.end(
      getOAuthCallbackBridgeHtml({
        title: "OAuth 콜백 수신",
        message:
          "자동 인증 매칭에 실패했습니다. 앱에서 callback URL을 직접 붙여넣어 완료하세요.",
        callbackUrl,
        payload: {
          type: "provider-oauth-callback",
          callbackUrl,
        },
        closeDelayMs: 0,
      })
    );
  });

  bridge.on("error", (error) => {
    const code = String(error?.code || "").trim();
    if (code === "EADDRINUSE") {
      console.warn(
        `[oauth] callback bridge port already in use: ${target.hostname}:${target.port} (manual callback paste fallback)`
      );
    } else {
      console.warn(
        `[oauth] callback bridge failed: ${error?.message || "unknown error"} (manual callback paste fallback)`
      );
    }
  });

  bridge.listen(target.port, () => {
    oauthCallbackBridgeServer = bridge;
    console.log(
      `[oauth] callback bridge ready at http://${target.hostname}:${target.port}${target.pathname} (listener=:${target.port})`
    );
  });
}

async function exchangeOpenAICodexAuthorizationCode({ code, verifier, redirectUri }) {
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    client_id: OPENAI_CODEX_OAUTH_CLIENT_ID,
    code: String(code || "").trim(),
    code_verifier: String(verifier || "").trim(),
    redirect_uri: String(redirectUri || OPENAI_CODEX_OAUTH_REDIRECT_URI).trim(),
  });

  const { response, payload } = await fetchJsonWithTimeout(
    OPENAI_CODEX_OAUTH_TOKEN_URL,
    {
      method: "POST",
      headers: {
        "content-type": "application/x-www-form-urlencoded",
      },
      body,
    }
  );

  if (!response.ok) {
    const detail =
      payload?.error_description ||
      payload?.error ||
      payload?.message ||
      `OpenAI OAuth token exchange failed (${response.status})`;
    throw new Error(detail);
  }

  const access = String(payload?.access_token || "").trim();
  const refresh = String(payload?.refresh_token || "").trim();
  const expiresIn = Number(payload?.expires_in || 0);

  if (!access || !refresh || !Number.isFinite(expiresIn) || expiresIn <= 0) {
    throw new Error("OpenAI OAuth token response is invalid");
  }

  return {
    access,
    refresh,
    expires: Date.now() + expiresIn * 1000,
    expiresIn,
    tokenType: String(payload?.token_type || "").trim() || null,
    scope: String(payload?.scope || "").trim() || null,
  };
}

function sanitizeProviderAuthError(error) {
  const raw = String(error?.message || "provider authentication failed")
    .replace(/\s+/g, " ")
    .trim();
  if (!raw) {
    return "provider authentication failed";
  }
  return raw.slice(0, 240);
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = PROVIDER_AUTH_FETCH_TIMEOUT_MS) {
  if (typeof fetch !== "function") {
    throw new Error("runtime fetch is unavailable");
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = {};
    try {
      payload = text ? JSON.parse(text) : {};
    } catch {
      payload = {};
    }
    return { response, payload };
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("provider auth request timed out");
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function verifyOpenAIProvider(apiKey) {
  const { response, payload } = await fetchJsonWithTimeout(
    `${OPENAI_API_BASE_URL}/models`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${apiKey}`,
      },
    }
  );

  if (!response.ok) {
    const message =
      payload?.error?.message ||
      payload?.message ||
      `OpenAI request failed (${response.status})`;
    throw new Error(message);
  }

  const rows = Array.isArray(payload?.data) ? payload.data : [];
  const models = rows
    .map((item) => String(item?.id || "").trim())
    .filter(Boolean)
    .filter((id) => {
      const lower = id.toLowerCase();
      return (
        lower.startsWith("gpt-") ||
        lower.startsWith("o1") ||
        lower.startsWith("o3") ||
        lower.startsWith("o4") ||
        lower.includes("codex")
      );
    })
    .sort((a, b) => a.localeCompare(b))
    .slice(0, PROVIDER_AUTH_MODELS_LIMIT)
    .map((id) => ({ id, label: id }));

  return {
    models,
    meta: {
      fetchedAt: new Date().toISOString(),
      totalModels: rows.length,
    },
  };
}

async function verifyGoogleProvider(apiKey) {
  const { response, payload } = await fetchJsonWithTimeout(
    `https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(
      apiKey
    )}`,
    {
      method: "GET",
    }
  );

  if (!response.ok) {
    const message =
      payload?.error?.message ||
      payload?.message ||
      `Google request failed (${response.status})`;
    throw new Error(message);
  }

  const rows = Array.isArray(payload?.models) ? payload.models : [];
  const models = rows
    .filter((item) =>
      Array.isArray(item?.supportedGenerationMethods)
        ? item.supportedGenerationMethods.includes("generateContent")
        : true
    )
    .map((item) => String(item?.name || "").replace(/^models\//i, "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b))
    .slice(0, PROVIDER_AUTH_MODELS_LIMIT)
    .map((id) => ({ id, label: id }));

  return {
    models,
    meta: {
      fetchedAt: new Date().toISOString(),
      totalModels: rows.length,
    },
  };
}

async function verifyProviderApiKey({ provider, apiKey }) {
  const normalizedProvider = normalizeProviderId(provider);
  const secret = String(apiKey || "").trim();
  if (!secret) {
    throw new Error("apiKey is required");
  }

  if (normalizedProvider === "openai") {
    return verifyOpenAIProvider(secret);
  }
  if (normalizedProvider === "google") {
    return verifyGoogleProvider(secret);
  }

  throw new Error(`unsupported provider: ${normalizedProvider}`);
}

async function verifyProviderOAuth({
  provider,
  challengeToken,
  callbackInput,
  expectedConnectionId,
  expectedUserId,
}) {
  const normalizedProvider = normalizeProviderId(provider);
  if (!challengeToken) {
    throw new Error("challengeToken is required");
  }

  if (normalizedProvider !== "openai") {
    throw new Error(`unsupported oauth provider: ${normalizedProvider}`);
  }

  const challenge = parseOAuthChallengeToken(challengeToken);
  if (normalizeProviderId(challenge.provider) !== normalizedProvider) {
    throw new Error("challenge provider mismatch");
  }
  if (String(challenge.connectionId || "") !== String(expectedConnectionId || "")) {
    throw new Error("challenge connection mismatch");
  }
  if (String(challenge.userId || "") !== String(expectedUserId || "")) {
    throw new Error("challenge user mismatch");
  }

  const parsedInput = parseAuthorizationInput(callbackInput);
  const code = String(parsedInput.code || "").trim();
  if (!code) {
    throw new Error("authorization code is required");
  }
  if (parsedInput.state && String(parsedInput.state) !== String(challenge.state || "")) {
    throw new Error("state mismatch");
  }

  const token = await exchangeOpenAICodexAuthorizationCode({
    code,
    verifier: challenge.verifier,
    redirectUri: challenge.redirectUri || OPENAI_CODEX_OAUTH_REDIRECT_URI,
  });
  const accountId = getOpenAICodexAccountId(token.access);
  let discoveredModels = [];
  try {
    const verified = await verifyOpenAIProvider(token.access);
    discoveredModels = Array.isArray(verified?.models)
      ? verified.models
          .map((item) => String(item?.id || "").trim())
          .filter(Boolean)
          .map((id) => ({ provider: "openai", id, label: id }))
      : [];
  } catch {
    discoveredModels = [];
  }

  const modelMap = new Map();
  for (const model of [...getOpenAICodexModels(), ...discoveredModels]) {
    const entry = normalizeProviderModelEntry("openai", model);
    if (!entry) {
      continue;
    }
    modelMap.set(entry.value, {
      provider: entry.provider,
      id: entry.modelId,
      label: entry.label,
    });
  }

  return {
    secret: {
      type: "oauth",
      provider: normalizedProvider,
      access: token.access,
      refresh: token.refresh,
      expires: token.expires,
      accountId,
    },
    models: Array.from(modelMap.values()),
    meta: {
      mode: "oauth",
      flow: "openai-codex-pkce",
      connectedAt: new Date().toISOString(),
      accountId,
      tokenType: token.tokenType,
      scope: token.scope,
      expiresAt: new Date(token.expires).toISOString(),
      expiresIn: token.expiresIn,
      discoveredModelCount: discoveredModels.length,
    },
  };
}

function parseModelSelectionValue(modelRaw) {
  const value = String(modelRaw || "").trim();
  if (!value) {
    return {
      provider: "",
      modelId: "",
      value: "",
    };
  }

  if (!value.includes("/")) {
    return {
      provider: normalizeProviderId(value),
      modelId: value,
      value,
    };
  }

  const [providerRaw, ...rest] = value.split("/");
  return {
    provider: normalizeProviderId(providerRaw),
    modelId: rest.join("/").trim(),
    value,
  };
}

function buildOpenAIModelCandidateIds(modelId) {
  const source = String(modelId || "").trim();
  if (!source) {
    return [];
  }

  const candidates = [];
  const push = (value) => {
    const item = String(value || "").trim();
    if (!item || candidates.includes(item)) {
      return;
    }
    candidates.push(item);
  };

  push(source);
  push(source.replace(/-spark$/i, ""));
  push(
    source
      .replace(/-codex-(mini|max)$/i, "")
      .replace(/-codex$/i, "")
  );
  push(
    source
      .replace(/-codex$/i, "")
      .replace(/-spark$/i, "")
  );

  const withoutCodex = source
    .replace(/-codex-(mini|max)$/i, "")
    .replace(/-codex$/i, "");
  const versionMatch = withoutCodex.match(/^(gpt-\d+(?:\.\d+)?)/i);
  if (versionMatch) {
    push(versionMatch[1]);
    const majorMatch = versionMatch[1].match(/^(gpt-\d+)/i);
    if (majorMatch) {
      push(majorMatch[1]);
    }
  }

  return candidates;
}

function pickPreferredOpenAIModel(modelIds) {
  const list = (Array.isArray(modelIds) ? modelIds : [])
    .map((item) => String(item || "").trim())
    .filter(Boolean);
  if (list.length === 0) {
    return "";
  }

  const startsWith = (prefix) =>
    list.find((item) => item.toLowerCase().startsWith(prefix));

  return (
    startsWith("gpt-5") ||
    startsWith("o3") ||
    startsWith("o4") ||
    startsWith("gpt-4.1") ||
    startsWith("gpt-4o") ||
    list[0]
  );
}

function buildOpenAICodexFallbackModelIds(requestedModelId, discoveredModelIds = []) {
  const list = [];
  const push = (value) => {
    const item = String(value || "").trim();
    if (!item || list.includes(item)) {
      return;
    }
    list.push(item);
  };

  const requested = String(requestedModelId || "").trim();
  if (requested) {
    push(requested);
    push(requested.replace(/-spark$/i, ""));
  }

  for (const modelId of Array.isArray(discoveredModelIds) ? discoveredModelIds : []) {
    push(modelId);
  }

  for (const model of getOpenAICodexModels()) {
    push(model.id);
  }

  push("gpt-5.3-codex");
  push("gpt-5.2-codex");
  push("gpt-5.1-codex-max");
  push("gpt-5.1-codex-mini");
  push("gpt-5.1");

  return list;
}

async function fetchOpenAIAvailableModelIds(accessToken) {
  const secret = String(accessToken || "").trim();
  if (!secret) {
    return [];
  }

  const { response, payload } = await fetchJsonWithTimeout(
    `${OPENAI_API_BASE_URL}/models`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${secret}`,
      },
    },
    DATA_AI_MODEL_TIMEOUT_MS
  );

  if (!response.ok) {
    throw new Error(
      sanitizeModelErrorMessage(
        payload,
        response.status,
        `OpenAI model list request failed (${response.status})`
      )
    );
  }

  const rows = Array.isArray(payload?.data) ? payload.data : [];
  return rows
    .map((item) => String(item?.id || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function resolveRequestedOpenAIModel({
  requestedModelId,
  availableModelIds,
}) {
  const requested = String(requestedModelId || "").trim();
  const available = new Set(
    (Array.isArray(availableModelIds) ? availableModelIds : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  );

  if (!requested) {
    const fallback = pickPreferredOpenAIModel(Array.from(available));
    return {
      modelId: fallback,
      changed: Boolean(fallback),
      reason: fallback ? "default_from_available_models" : "requested_model_empty",
    };
  }

  if (available.size === 0) {
    let heuristicFallback = requested;
    if (requested.toLowerCase().includes("codex")) {
      const candidates = buildOpenAIModelCandidateIds(requested);
      const nonCodexCandidates = candidates.filter(
        (candidate) =>
          candidate !== requested && !candidate.toLowerCase().includes("codex")
      );
      heuristicFallback =
        [
          "gpt-4o-mini",
          "gpt-4.1-mini",
          "gpt-4o",
          ...nonCodexCandidates,
        ].find(Boolean) || "gpt-4o-mini";
    }
    return {
      modelId: heuristicFallback || requested,
      changed: Boolean(heuristicFallback && heuristicFallback !== requested),
      reason: "available_models_unknown_use_heuristic",
    };
  }

  const candidates = buildOpenAIModelCandidateIds(requested);
  for (const candidate of candidates) {
    if (available.has(candidate)) {
      return {
        modelId: candidate,
        changed: candidate !== requested,
        reason: candidate !== requested ? "fallback_candidate_match" : "requested_model_available",
      };
    }
  }

  const fallback = pickPreferredOpenAIModel(Array.from(available));
  return {
    modelId: fallback || requested,
    changed: Boolean(fallback && fallback !== requested),
    reason: fallback ? "fallback_preferred_available_model" : "requested_model_not_available",
  };
}

function parseProviderSecretPayload(connection) {
  if (!connection?.secretEncrypted) {
    throw new Error("provider credential is missing");
  }
  const decrypted = decryptProviderSecret(connection.secretEncrypted);
  if (!decrypted) {
    throw new Error("provider credential is empty");
  }
  try {
    return JSON.parse(decrypted);
  } catch {
    throw new Error("provider credential format is invalid");
  }
}

function parseOpenAIOAuthSecret(connection) {
  const payload = parseProviderSecretPayload(connection);
  if (!payload || payload.type !== "oauth") {
    throw new Error("openai oauth credential is invalid");
  }

  const access = String(payload.access || "").trim();
  const refresh = String(payload.refresh || "").trim();
  const expires = Number(payload.expires || 0);
  const accountId = payload.accountId ? String(payload.accountId) : null;

  if (!access || !refresh || !Number.isFinite(expires) || expires <= 0) {
    throw new Error("openai oauth credential is incomplete");
  }

  return {
    access,
    refresh,
    expires,
    accountId,
  };
}

async function refreshOpenAIOAuthAccessToken(refreshToken) {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: OPENAI_CODEX_OAUTH_CLIENT_ID,
    refresh_token: String(refreshToken || "").trim(),
  });

  const { response, payload } = await fetchJsonWithTimeout(
    OPENAI_CODEX_OAUTH_TOKEN_URL,
    {
      method: "POST",
      headers: {
        "content-type": "application/x-www-form-urlencoded",
      },
      body,
    },
    DATA_AI_MODEL_TIMEOUT_MS
  );

  if (!response.ok) {
    const detail =
      payload?.error_description ||
      payload?.error ||
      payload?.message ||
      `OpenAI OAuth refresh failed (${response.status})`;
    throw new Error(detail);
  }

  const access = String(payload?.access_token || "").trim();
  const refresh = String(payload?.refresh_token || "").trim();
  const expiresIn = Number(payload?.expires_in || 0);

  if (!access || !Number.isFinite(expiresIn) || expiresIn <= 0) {
    throw new Error("OpenAI OAuth refresh response is invalid");
  }

  return {
    access,
    refresh,
    expires: Date.now() + expiresIn * 1000,
    expiresIn,
    tokenType: String(payload?.token_type || "").trim() || null,
    scope: String(payload?.scope || "").trim() || null,
    accountId: getOpenAICodexAccountId(access),
  };
}

async function ensureOpenAIOAuthAccessToken(connection) {
  const secret = parseOpenAIOAuthSecret(connection);
  const now = Date.now();
  const renewThresholdMs = 90 * 1000;
  if (secret.expires - now > renewThresholdMs) {
    return {
      accessToken: secret.access,
      credential: secret,
      refreshed: false,
    };
  }

  const refreshed = await refreshOpenAIOAuthAccessToken(secret.refresh);
  const nextSecret = {
    type: "oauth",
    provider: "openai",
    access: refreshed.access,
    refresh: refreshed.refresh || secret.refresh,
    expires: refreshed.expires,
    accountId: refreshed.accountId || secret.accountId || null,
  };

  await repository.updateProviderAuthConnection(connection.id, {
    status: "connected",
    secretEncrypted: encryptProviderSecret(JSON.stringify(nextSecret)),
    lastCheckedAt: new Date().toISOString(),
    errorMessage: null,
    meta: {
      ...(connection.meta && typeof connection.meta === "object"
        ? connection.meta
        : {}),
      mode: "oauth",
      refreshedAt: new Date().toISOString(),
      tokenType: refreshed.tokenType,
      scope: refreshed.scope,
      expiresAt: new Date(refreshed.expires).toISOString(),
      expiresIn: refreshed.expiresIn,
      accountId: nextSecret.accountId,
    },
  });

  return {
    accessToken: nextSecret.access,
    credential: nextSecret,
    refreshed: true,
  };
}

function extractModelTextFromResponsePayload(payload) {
  if (!payload || typeof payload !== "object") {
    return "";
  }

  if (typeof payload.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  if (Array.isArray(payload.output)) {
    for (const item of payload.output) {
      if (!item || typeof item !== "object") {
        continue;
      }
      if (Array.isArray(item.content)) {
        const textParts = item.content
          .map((entry) => {
            if (typeof entry?.text === "string") {
              return entry.text;
            }
            if (
              entry?.type === "output_text" &&
              typeof entry?.text === "string"
            ) {
              return entry.text;
            }
            return "";
          })
          .filter(Boolean);
        if (textParts.length > 0) {
          return textParts.join("\n").trim();
        }
      }
    }
  }

  if (Array.isArray(payload.choices) && payload.choices.length > 0) {
    const first = payload.choices[0];
    const content = first?.message?.content;
    if (typeof content === "string" && content.trim()) {
      return content.trim();
    }
    if (Array.isArray(content)) {
      const text = content
        .map((entry) =>
          typeof entry?.text === "string" ? entry.text : ""
        )
        .filter(Boolean)
        .join("\n")
        .trim();
      if (text) {
        return text;
      }
    }
  }

  return "";
}

function sanitizeModelErrorMessage(payload, statusCode, fallback) {
  const payloadMessage =
    payload?.error?.message ||
    payload?.error_description ||
    payload?.message ||
    "";
  const text = String(payloadMessage || fallback || "").trim();
  const lower = text.toLowerCase();
  if (lower.includes("missing scopes") || lower.includes("insufficient permissions")) {
    if (lower.includes("api.responses.write")) {
      return "OpenAI 권한 부족: Responses API 쓰기 권한(api.responses.write)이 없습니다. OpenAI 프로젝트 권한(Member/Owner, Writer 이상) 확인 후 OAuth를 다시 인증하거나 openai-codex 모델을 선택하세요.";
    }
    if (lower.includes("api.chat.completions.write")) {
      return "OpenAI 권한 부족: Chat Completions 쓰기 권한(api.chat.completions.write)이 없습니다. OpenAI 프로젝트 권한(Member/Owner, Writer 이상) 확인 후 OAuth를 다시 인증하거나 openai-codex 모델을 선택하세요.";
    }
    return "OpenAI 권한 부족: 현재 계정/프로젝트 역할이 쓰기 권한을 갖지 않아 요청이 거부되었습니다. OpenAI 프로젝트 권한과 OAuth 인증을 다시 확인하거나 openai-codex 모델을 사용하세요.";
  }
  if (text) {
    return text.slice(0, 240);
  }
  return `model request failed (${statusCode})`;
}

function buildDataModelPrompts({ command, action, summary }) {
  const normalizedAction = ["analyze", "clean", "format"].includes(
    String(action || "").trim().toLowerCase()
  )
    ? String(action || "").trim().toLowerCase()
    : "analyze";
  const mutationRequested = normalizedAction !== "analyze";
  const systemPrompt = [
    "You are a data engineering assistant.",
    "Return ONLY a JSON object.",
    "Decide data transformations from the user command and sample rows.",
    "Write report in Korean.",
    "In report, answer the user's question directly in the first sentence.",
    "In report, include what data you inspected (rows/columns/samples) and key findings.",
    "If no transformation is applied, clearly state that no data was modified and why.",
    "If the user asks about column values, list observed values (and counts when possible).",
    "Allowed operation types:",
    "rename_column, trim_whitespace, normalize_empty_to_null, value_map, parse_number, normalize_date, drop_columns.",
    "Do not invent operation types.",
    mutationRequested
      ? "The user requested data modification. Return action as clean or format and include concrete operations."
      : "If the command is analysis-only, return empty operations and a concise report.",
  ].join(" ");

  const userPrompt = JSON.stringify(
    {
      task: command,
      action: normalizedAction,
      mutationRequested,
      requiredOutput: {
        action: "analyze|clean|format",
        report:
          "Korean report. first sentence must directly answer user's question. include inspected scope, key findings, and changed/not-changed reason. if asking column values, include observed values and counts.",
        operations: [
          {
            type: "rename_column|trim_whitespace|normalize_empty_to_null|value_map|parse_number|normalize_date|drop_columns",
          },
        ],
      },
      inputSummary: summary,
    },
    null,
    2
  );

  return { systemPrompt, userPrompt };
}

function normalizeSearchText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[`"'’“”]/g, "")
    .replace(/[\s_\-./\\]+/g, "");
}

function findColumnsMentionedInCommand(command, columns) {
  const source = normalizeSearchText(command);
  if (!source) {
    return [];
  }

  const matches = [];
  for (const columnRaw of Array.isArray(columns) ? columns : []) {
    const column = String(columnRaw || "").trim();
    if (!column) {
      continue;
    }

    const compact = normalizeSearchText(column);
    if (!compact) {
      continue;
    }

    if (source.includes(compact)) {
      matches.push(column);
      continue;
    }

    const words = column
      .toLowerCase()
      .split(/[\s_\-./\\]+/)
      .map((item) => item.trim())
      .filter(Boolean);
    if (words.length > 0 && words.every((word) => source.includes(word))) {
      matches.push(column);
    }
  }

  return [...new Set(matches)];
}

function escapeRegexPattern(text) {
  return String(text || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function inferRenameOperationFromCommand(command, columns) {
  const rawCommand = String(command || "").trim();
  if (!rawCommand) {
    return null;
  }

  const lower = rawCommand.toLowerCase();
  const hasRenameIntent =
    lower.includes("변경") ||
    lower.includes("바꿔") ||
    lower.includes("rename") ||
    lower.includes("change");
  if (!hasRenameIntent) {
    return null;
  }

  const mentioned = findColumnsMentionedInCommand(rawCommand, columns || []);
  if (!Array.isArray(mentioned) || mentioned.length === 0) {
    return null;
  }
  const from = String(mentioned[0] || "").trim();
  if (!from) {
    return null;
  }

  let to = "";
  const fromPattern = new RegExp(escapeRegexPattern(from), "i");
  const fromMatch = rawCommand.match(fromPattern);
  const renameAnchorMatch = rawCommand.match(/(?:으로|로)\s*(?:변경|바꿔|rename|change)/i);

  if (fromMatch && renameAnchorMatch && fromMatch.index <= renameAnchorMatch.index) {
    const fromEnd = Number(fromMatch.index) + fromMatch[0].length;
    const targetSlice = rawCommand
      .slice(fromEnd, renameAnchorMatch.index)
      .replace(/^(?:\s*(?:을|를|to|into|as)\s*)/i, "")
      .replace(/^(?:\s*그냥\s*)/i, "")
      .trim();
    to = targetSlice;
  }

  if (!to) {
    const quoted =
      rawCommand.match(/["'`]\s*([^"'`]{1,80}?)\s*["'`]\s*(?:으로|로)\s*(?:변경|바꿔|rename|change)/i) ||
      rawCommand.match(/(?:to|as)\s*["'`]\s*([^"'`]{1,80}?)\s*["'`]/i);
    if (quoted && quoted[1]) {
      to = String(quoted[1]).trim();
    }
  }

  to = String(to || "")
    .replace(/^[\s"'`]+|[\s"'`]+$/g, "")
    .replace(/\s+/g, " ")
    .trim();

  if (!to || to.toLowerCase() === from.toLowerCase()) {
    return null;
  }

  return {
    type: "rename_column",
    from,
    to,
  };
}

function inferFallbackOperationsFromCommand(command, action, columns) {
  const safeAction = String(action || "analyze").trim().toLowerCase();
  if (safeAction === "analyze") {
    return [];
  }

  const renameOp = inferRenameOperationFromCommand(command, columns);
  if (renameOp) {
    return [renameOp];
  }

  return [];
}

function normalizeValueForDistribution(value) {
  if (value === null || value === undefined) {
    return "(empty)";
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  const text =
    typeof value === "string"
      ? value
      : (() => {
          try {
            return JSON.stringify(value);
          } catch {
            return String(value);
          }
        })();

  const normalized = String(text || "").trim();
  return normalized ? normalized.slice(0, 80) : "(empty)";
}

function buildColumnValueDistribution(rows, column, maxDistinct = 12) {
  const counts = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const normalized = normalizeValueForDistribution(row?.[column]);
    counts.set(normalized, (counts.get(normalized) || 0) + 1);
  }

  return Array.from(counts.entries())
    .sort((a, b) => {
      if (b[1] !== a[1]) {
        return b[1] - a[1];
      }
      return String(a[0]).localeCompare(String(b[0]));
    })
    .slice(0, Math.max(1, Number(maxDistinct) || 12))
    .map(([value, count]) => ({ value, count }));
}

function buildFallbackDataReport({
  command,
  action,
  rows,
  stats,
  operations,
}) {
  const safeAction = String(action || "analyze").trim() || "analyze";
  const allColumns = collectColumns(rows);
  const inputRows = Number(stats?.inputRows) || (Array.isArray(rows) ? rows.length : 0);
  const outputRows = Number(stats?.outputRows) || (Array.isArray(rows) ? rows.length : 0);
  const modifiedCells = Number(stats?.modifiedCells) || 0;
  const droppedRows = Number(stats?.droppedRows) || 0;
  const opCount = Array.isArray(operations) ? operations.length : 0;

  const lines = [];
  lines.push(
    `요청 기준으로 데이터를 확인했습니다. 입력 ${inputRows}행, 출력 ${outputRows}행, 컬럼 ${allColumns.length}개입니다.`
  );

  if (safeAction === "analyze") {
    const mentionedColumns = findColumnsMentionedInCommand(command, allColumns);
    if (mentionedColumns.length > 0) {
      for (const column of mentionedColumns.slice(0, 2)) {
        const distribution = buildColumnValueDistribution(rows, column, 10);
        if (distribution.length === 0) {
          lines.push(`\`${column}\` 컬럼은 확인 가능한 값이 없습니다.`);
          continue;
        }
        const detail = distribution
          .map((item) => `${item.value}(${item.count}건)`)
          .join(", ");
        lines.push(`\`${column}\` 값 분포: ${detail}`);
      }
    } else {
      const previewColumns = allColumns.slice(0, 8).join(", ");
      lines.push(
        `질문과 정확히 매칭되는 컬럼명을 찾지 못해 전체 기준으로 분석했습니다. 주요 컬럼: ${previewColumns || "-"}.`
      );
    }
  }

  if (opCount > 0) {
    lines.push(
      `적용 액션 ${opCount}개, 변경 셀 ${modifiedCells}개, 제거 행 ${droppedRows}개가 반영되었습니다.`
    );
  } else {
    lines.push("분석 전용 요청으로 데이터 값 수정은 수행하지 않았습니다.");
  }

  return lines.join("\n").trim();
}

function sanitizeOpenAICodexErrorMessage(payload, statusCode, rawText = "") {
  const code = String(payload?.error?.code || payload?.code || "").trim();
  if (
    statusCode === 429 ||
    /usage_limit_reached|usage_not_included|rate_limit_exceeded/i.test(code)
  ) {
    return "OpenAI Codex 사용량 한도에 도달했습니다. ChatGPT 요금제/한도를 확인한 뒤 다시 시도하세요.";
  }

  const message = String(
    payload?.error?.message ||
      payload?.error_description ||
      payload?.message ||
      rawText ||
      ""
  )
    .replace(/\s+/g, " ")
    .trim();
  if (message) {
    return message.slice(0, 240);
  }
  return `OpenAI Codex request failed (${statusCode})`;
}

function resolveOpenAICodexResponsesUrl() {
  const raw = String(OPENAI_CODEX_RESPONSES_URL || "").trim();
  if (!raw) {
    return "https://chatgpt.com/backend-api/codex/responses";
  }
  if (/\/codex\/responses\/?$/i.test(raw)) {
    return raw.replace(/\/+$/, "");
  }
  const normalized = raw.replace(/\/+$/, "");
  if (/\/codex$/i.test(normalized)) {
    return `${normalized}/responses`;
  }
  return `${normalized}/codex/responses`;
}

function buildOpenAICodexHeaders({ accessToken, accountId }) {
  const token = String(accessToken || "").trim();
  const chatgptAccountId = String(accountId || "").trim();
  if (!token) {
    throw new Error("openai oauth access token is required");
  }
  if (!chatgptAccountId) {
    throw new Error("OpenAI Codex account id is missing. OAuth를 다시 인증하세요.");
  }

  return {
    Authorization: `Bearer ${token}`,
    "chatgpt-account-id": chatgptAccountId,
    "OpenAI-Beta": "responses=experimental",
    originator: OPENAI_CODEX_ORIGINATOR,
    accept: "text/event-stream",
    "content-type": "application/json",
  };
}

async function readOpenAICodexSseOutputText(response) {
  const reader =
    response?.body && typeof response.body.getReader === "function"
      ? response.body.getReader()
      : null;
  if (!reader) {
    return "";
  }

  const decoder = new TextDecoder();
  let buffer = "";
  let deltaText = "";
  let completedText = "";

  const consumeChunk = (chunk) => {
    const dataLines = String(chunk || "")
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trim());
    if (dataLines.length === 0) {
      return;
    }

    const data = dataLines.join("\n").trim();
    if (!data || data === "[DONE]") {
      return;
    }

    let eventPayload = null;
    try {
      eventPayload = JSON.parse(data);
    } catch {
      return;
    }
    if (!eventPayload || typeof eventPayload !== "object") {
      return;
    }

    const eventType = String(eventPayload?.type || "").trim().toLowerCase();
    if (eventType === "error") {
      const message = String(
        eventPayload?.message ||
          eventPayload?.error?.message ||
          "OpenAI Codex stream error"
      ).trim();
      throw new Error(message || "OpenAI Codex stream error");
    }
    if (eventType === "response.failed") {
      const message = String(
        eventPayload?.response?.error?.message ||
          eventPayload?.error?.message ||
          "OpenAI Codex response failed"
      ).trim();
      throw new Error(message || "OpenAI Codex response failed");
    }

    if (
      eventType.includes("output_text.delta") &&
      typeof eventPayload?.delta === "string"
    ) {
      deltaText += eventPayload.delta;
    }
    if (
      eventType.includes("output_text.done") &&
      typeof eventPayload?.text === "string"
    ) {
      deltaText += eventPayload.text;
    }

    if (
      (eventType === "response.completed" || eventType === "response.done") &&
      eventPayload?.response
    ) {
      const parsed = extractModelTextFromResponsePayload(eventPayload.response);
      if (parsed) {
        completedText = parsed;
      }
    }
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
    let index = buffer.indexOf("\n\n");
    while (index !== -1) {
      const chunk = buffer.slice(0, index);
      buffer = buffer.slice(index + 2);
      consumeChunk(chunk);
      index = buffer.indexOf("\n\n");
    }
  }

  if (buffer.trim()) {
    consumeChunk(buffer);
  }

  return String(completedText || deltaText || "").trim();
}

function extractTextFromCodexSseTranscript(rawText) {
  const text = String(rawText || "").replace(/\r\n/g, "\n");
  if (!text || !/(?:^|\n)\s*(?:event|data)\s*:/i.test(text)) {
    return "";
  }

  let deltaText = "";
  let completedText = "";
  const chunks = text.split(/\n\n+/);

  for (const chunk of chunks) {
    const dataLines = String(chunk || "")
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trim());
    if (dataLines.length === 0) {
      continue;
    }

    const data = dataLines.join("\n").trim();
    if (!data || data === "[DONE]") {
      continue;
    }

    let eventPayload = null;
    try {
      eventPayload = JSON.parse(data);
    } catch {
      continue;
    }
    if (!eventPayload || typeof eventPayload !== "object") {
      continue;
    }

    const eventType = String(eventPayload?.type || "").trim().toLowerCase();
    if (
      eventType.includes("output_text.delta") &&
      typeof eventPayload?.delta === "string"
    ) {
      deltaText += eventPayload.delta;
    }
    if (
      eventType.includes("output_text.done") &&
      typeof eventPayload?.text === "string"
    ) {
      deltaText += eventPayload.text;
    }
    if (
      (eventType === "response.completed" || eventType === "response.done") &&
      eventPayload?.response
    ) {
      const parsed = extractModelTextFromResponsePayload(eventPayload.response);
      if (parsed) {
        completedText = parsed;
      }
    }
  }

  return String(completedText || deltaText || "").trim();
}

async function requestDataTransformPlanFromOpenAICodex({
  accessToken,
  accountId,
  modelId,
  command,
  action,
  summary,
}) {
  const { systemPrompt, userPrompt } = buildDataModelPrompts({
    command,
    action,
    summary,
  });

  const headers = buildOpenAICodexHeaders({
    accessToken,
    accountId,
  });
  const codexPayload = {
    model: modelId,
    store: false,
    stream: true,
    instructions: systemPrompt,
    input: [
      {
        role: "user",
        content: [{ type: "input_text", text: userPrompt }],
      },
    ],
    text: { verbosity: "high" },
    include: ["reasoning.encrypted_content"],
    tool_choice: "auto",
    parallel_tool_calls: true,
  };

  if (typeof fetch !== "function") {
    throw new Error("runtime fetch is unavailable");
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DATA_AI_MODEL_TIMEOUT_MS);
  let response;
  try {
    response = await fetch(resolveOpenAICodexResponsesUrl(), {
      method: "POST",
      headers,
      body: JSON.stringify(codexPayload),
      signal: controller.signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("OpenAI Codex request timed out");
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }

  if (!response.ok) {
    const rawText = await response.text().catch(() => "");
    let payload = {};
    try {
      payload = rawText ? JSON.parse(rawText) : {};
    } catch {
      payload = {};
    }
    throw new Error(
      sanitizeOpenAICodexErrorMessage(payload, response.status, rawText)
    );
  }

  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (!contentType.includes("text/event-stream")) {
    const rawText = await response.text().catch(() => "");
    let payload = {};
    try {
      payload = rawText ? JSON.parse(rawText) : {};
    } catch {
      payload = {};
    }
    const text =
      extractModelTextFromResponsePayload(payload) ||
      extractTextFromCodexSseTranscript(rawText) ||
      rawText.trim();
    if (!text) {
      throw new Error("model returned an empty response");
    }
    return text;
  }

  const text = await readOpenAICodexSseOutputText(response);
  if (!text) {
    throw new Error("model returned an empty response");
  }
  return text;
}

async function requestDataTransformPlanFromOpenAIResponsesApi({
  accessToken,
  modelId,
  command,
  action,
  summary,
}) {
  const { systemPrompt, userPrompt } = buildDataModelPrompts({
    command,
    action,
    summary,
  });
  const headers = {
    Authorization: `Bearer ${accessToken}`,
    "content-type": "application/json",
  };

  const responsesPayload = {
    model: modelId,
    input: [
      {
        role: "system",
        content: [{ type: "input_text", text: systemPrompt }],
      },
      {
        role: "user",
        content: [{ type: "input_text", text: userPrompt }],
      },
    ],
    temperature: 0,
    max_output_tokens: 1800,
  };

  const responsesResult = await fetchJsonWithTimeout(
    `${OPENAI_API_BASE_URL}/responses`,
    {
      method: "POST",
      headers,
      body: JSON.stringify(responsesPayload),
    },
    DATA_AI_MODEL_TIMEOUT_MS
  );

  if (responsesResult.response.ok) {
    const text = extractModelTextFromResponsePayload(responsesResult.payload);
    if (!text) {
      throw new Error("model returned an empty response");
    }
    return text;
  }

  const responsesErrorText = String(
    responsesResult.payload?.error?.message ||
      responsesResult.payload?.message ||
      ""
  )
    .trim()
    .toLowerCase();
  const shouldFallbackToChatCompletions =
    responsesResult.response.status === 400 ||
    responsesResult.response.status === 403 ||
    responsesResult.response.status === 404 ||
    responsesResult.response.status === 422 ||
    responsesErrorText.includes("api.responses.write");

  if (!shouldFallbackToChatCompletions) {
    throw new Error(
      sanitizeModelErrorMessage(
        responsesResult.payload,
        responsesResult.response.status,
        "OpenAI responses API request failed"
      )
    );
  }

  const chatPayload = {
    model: modelId,
    response_format: { type: "json_object" },
    temperature: 0,
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: userPrompt },
    ],
  };

  const chatResult = await fetchJsonWithTimeout(
    `${OPENAI_API_BASE_URL}/chat/completions`,
    {
      method: "POST",
      headers,
      body: JSON.stringify(chatPayload),
    },
    DATA_AI_MODEL_TIMEOUT_MS
  );

  if (!chatResult.response.ok) {
    throw new Error(
      sanitizeModelErrorMessage(
        chatResult.payload,
        chatResult.response.status,
        "OpenAI chat completion request failed"
      )
    );
  }

  const text = extractModelTextFromResponsePayload(chatResult.payload);
  if (!text) {
    throw new Error("model returned an empty response");
  }
  return text;
}

async function requestDataTransformPlanFromOpenAI({
  provider,
  accessToken,
  accountId,
  modelId,
  command,
  action,
  summary,
}) {
  const normalizedProvider = normalizeProviderId(provider);
  if (normalizedProvider === "openai-codex") {
    return requestDataTransformPlanFromOpenAICodex({
      accessToken,
      accountId,
      modelId,
      command,
      action,
      summary,
    });
  }

  return requestDataTransformPlanFromOpenAIResponsesApi({
    accessToken,
    modelId,
    command,
    action,
    summary,
  });
}

function truncateSingleLine(value, maxLength = 320) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, Math.max(16, Number(maxLength) || 320));
}

function extractJsonObjectTextLoose(text) {
  const raw = String(text || "").trim();
  if (!raw) {
    return "";
  }

  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced && fenced[1]) {
    return String(fenced[1]).trim();
  }

  const start = raw.indexOf("{");
  const end = raw.lastIndexOf("}");
  if (start >= 0 && end > start) {
    return raw.slice(start, end + 1);
  }

  return raw;
}

function parseJsonObjectLoose(text) {
  const candidate = extractJsonObjectTextLoose(text);
  if (!candidate) {
    return {};
  }

  try {
    const parsed = JSON.parse(candidate);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed;
    }
  } catch {
    // ignore parse failures and fallback to plain text handling
  }

  return {};
}

function normalizeShortTextArray(value, { maxItems = 8, maxLength = 220 } = {}) {
  const limit = Math.max(1, Number(maxItems) || 8);
  const textLimit = Math.max(24, Number(maxLength) || 220);
  const list = [];

  if (Array.isArray(value)) {
    for (const item of value) {
      const text = truncateSingleLine(item, textLimit);
      if (!text) {
        continue;
      }
      list.push(text);
      if (list.length >= limit) {
        break;
      }
    }
    return list;
  }

  if (typeof value === "string") {
    const parts = value
      .split(/\n|•|- |\u2022|;/g)
      .map((item) => truncateSingleLine(item, textLimit))
      .filter(Boolean);
    return parts.slice(0, limit);
  }

  return [];
}

function normalizeCommandArray(value, maxItems = 3) {
  const fromArray = Array.isArray(value) ? value : [];
  const fromString =
    typeof value === "string"
      ? value
          .split(/\n|;/g)
          .map((item) => String(item || "").trim())
          .filter(Boolean)
      : [];
  return normalizeCommandList(
    fromArray.length > 0 ? fromArray : fromString,
    Math.max(1, Number(maxItems) || 3)
  );
}

function normalizeTeamMessageArray(value, maxItems = 8) {
  if (!Array.isArray(value)) {
    return [];
  }
  const limit = Math.max(1, Number(maxItems) || 8);
  const out = [];
  for (const item of value) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const typeRaw = String(item.type || "message").trim().toLowerCase();
    const type = typeRaw === "broadcast" ? "broadcast" : "message";
    const recipientTaskKey = String(
      item.recipientTaskKey || item.recipient || item.toTaskKey || ""
    ).trim();
    const content = String(item.content || item.text || "").trim();
    const summary = truncateSingleLine(item.summary || "", 180);
    if (!content) {
      continue;
    }
    if (type === "message" && !recipientTaskKey) {
      continue;
    }
    out.push({
      type,
      recipientTaskKey,
      content: content.slice(0, 4000),
      summary,
    });
    if (out.length >= limit) {
      break;
    }
  }
  return out;
}

function normalizeLongText(value, maxLength = 16000) {
  const limit = Math.max(200, Number(maxLength) || 16000);
  if (typeof value === "string") {
    return String(value || "").trim().slice(0, limit);
  }
  if (Array.isArray(value)) {
    return value
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .join("\n")
      .slice(0, limit);
  }
  if (value && typeof value === "object") {
    return JSON.stringify(value, null, 2).slice(0, limit);
  }
  return "";
}

function parseWorkflowTaskModelOutput(text) {
  const rawText = String(text || "").trim();
  const parsed = parseJsonObjectLoose(rawText);
  const deliverable = normalizeLongText(
    parsed.deliverable ||
      parsed.report ||
      parsed.analysis ||
      parsed.body ||
      parsed.article ||
      parsed.answerDetailed ||
      "",
    20000
  );

  const summaryCandidate =
    parsed.summary ||
    deliverable ||
    parsed.answer ||
    parsed.message ||
    parsed.result ||
    rawText;
  const summary =
    truncateSingleLine(summaryCandidate, 360) ||
    "작업 결과 요약이 비어 있습니다.";

  const insights = normalizeShortTextArray(
    parsed.insights || parsed.findings || parsed.highlights,
    {
      maxItems: 12,
      maxLength: 240,
    }
  );
  const nextActions = normalizeShortTextArray(
    parsed.nextActions || parsed.next_steps || parsed.actions,
    {
      maxItems: 10,
      maxLength: 220,
    }
  );

  const commands = normalizeCommandArray(
    parsed.commands || parsed.shellCommands || parsed.toolCommands,
    WORKFLOW_LOCAL_EXEC_MAX_COMMANDS
  );
  const messages = normalizeTeamMessageArray(
    parsed.messages || parsed.teamMessages || parsed.mailboxMessages,
    WORKFLOW_AGENT_OUTBOX_MAX_MESSAGES
  );
  const statusRaw = String(parsed.status || "").trim().toLowerCase();
  const status = ["blocked", "needs_commands", "completed"].includes(statusRaw)
    ? statusRaw
    : commands.length > 0
      ? "needs_commands"
      : "completed";

  return {
    summary,
    deliverable,
    insights,
    nextActions,
    commands,
    messages,
    status,
    rawText: rawText.slice(0, 12000),
  };
}

async function buildWorkflowTaskDatasetContext(workflow, task) {
  const datasetId = String(workflow?.datasetId || "").trim();
  if (!datasetId) {
    return null;
  }

  const dataset = await repository.getDataset(datasetId);
  if (!dataset) {
    return null;
  }

  const normalizedRows = Array.isArray(dataset.normalizedRows)
    ? dataset.normalizedRows
    : [];
  const sanitized = sanitizeExecutionRows(normalizedRows, {
    maxRows: Math.min(DATA_AI_MAX_ROWS, 1200),
    maxColumns: Math.min(DATA_AI_MAX_COLUMNS, 64),
    maxCellLength: 180,
  });
  const allColumns = collectColumns(sanitized.rows);

  const workflowFeatures = normalizeStringArray(workflow?.selectedFeatures || []);
  const taskFeatures = normalizeStringArray(task?.input?.features || []);
  const preferredColumns = normalizeStringArray([...taskFeatures, ...workflowFeatures]).filter(
    (column) => allColumns.includes(column)
  );
  const usedColumns = (preferredColumns.length > 0 ? preferredColumns : allColumns).slice(
    0,
    24
  );

  const scopedRows = sanitized.rows.slice(0, 80).map((row) => {
    const next = {};
    for (const column of usedColumns) {
      next[column] = row?.[column] ?? null;
    }
    return next;
  });

  const summary = buildModelInputSummary(scopedRows, {
    sampleRows: 30,
    maxColumns: Math.max(1, Math.min(24, usedColumns.length || 24)),
    maxRowsInPrompt: 20,
    maxCellLength: 120,
  });

  return {
    datasetId: dataset.id,
    sourceName: dataset.sourceName || null,
    rowCount: Number(dataset.rowCount) || normalizedRows.length,
    totalColumns: allColumns.length,
    workflowFeatures: workflowFeatures.slice(0, 120),
    taskFeatures: taskFeatures.slice(0, 120),
    usedColumns,
    summary,
  };
}

function buildWorkflowTaskPromptPayload({
  workflow,
  task,
  agent,
  agentLabel,
  taskTitle,
  taskKind,
  dependencyOutputs,
  datasetContext,
  localExec = null,
  commandHistory = [],
  nodeSession = null,
  teamContext = null,
}) {
  const agentInstruction = String(agent?.systemPrompt || "").trim().slice(0, 5000);
  const localExecEnabled = Boolean(localExec?.enabled);
  const teamEnabled = Boolean(teamContext?.enabled);
  const maxCommands = Math.max(
    1,
    Number(localExec?.maxCommands || WORKFLOW_LOCAL_EXEC_MAX_COMMANDS)
  );
  const permissionProfile = normalizePermissionProfile(
    localExec?.permissionProfile && typeof localExec.permissionProfile === "object"
      ? localExec.permissionProfile
      : {}
  );
  const systemPrompt = [
    "You are an autonomous specialist agent inside a multi-agent workflow.",
    `Agent name: ${agentLabel}.`,
    agentInstruction
      ? `Follow this agent system instruction strictly: ${agentInstruction}`
      : "No custom system instruction is provided.",
    "Respond in Korean.",
    "Return ONLY one valid JSON object.",
    "Required JSON keys: summary, deliverable, insights, nextActions, status, commands, messages.",
    "summary: one concise sentence describing the task result.",
    "deliverable: the full work artifact (detailed analysis/report/draft) in Korean plain text.",
    "deliverable must be concrete and usable immediately, not a placeholder sentence.",
    "insights: array of key findings for this task.",
    "nextActions: array of concrete handoff items for downstream tasks.",
    "status: completed, blocked, or needs_commands.",
    localExecEnabled
      ? `commands: optional array of shell commands (max ${maxCommands}) when local execution is required.`
      : "commands: always return empty array because local execution is disabled.",
    localExecEnabled
      ? "If you need command execution before finalizing, set status to needs_commands and provide commands."
      : "Do not request command execution.",
    teamEnabled
      ? "messages: optional array for teammate communication. message item schema: {type:'message'|'broadcast', recipientTaskKey?:string, content:string, summary?:string}. Use 'message' for specific teammate and 'broadcast' sparingly."
      : "messages: always return empty array when team messaging is disabled.",
    teamEnabled
      ? "팀원에게 전달할 정보는 반드시 messages를 사용하세요. 일반 텍스트만 작성하면 팀원에게 전달되지 않습니다."
      : "팀 메시징 비활성화 상태입니다.",
    "Do not include markdown code fences.",
  ].join(" ");

  const payload = {
    workflow: {
      id: workflow?.id || null,
      goal: String(workflow?.goal || "").trim(),
      selectedFeatures: normalizeStringArray(workflow?.selectedFeatures || []).slice(
        0,
        120
      ),
      meta: workflow?.meta && typeof workflow.meta === "object" ? workflow.meta : {},
    },
    task: {
      id: task?.id || null,
      taskKey: task?.taskKey || null,
      title: taskTitle,
      kind: taskKind,
      input: task?.input && typeof task.input === "object" ? task.input : {},
    },
    dependencies: Array.isArray(dependencyOutputs)
      ? dependencyOutputs.map((item) => ({
          taskId: item.taskId,
          taskKey: item.taskKey,
          title: item.title,
          status: item.status,
          summary: item.summary,
          output: item.output,
        }))
      : [],
    dataset: datasetContext
      ? {
          datasetId: datasetContext.datasetId,
          sourceName: datasetContext.sourceName,
          rowCount: datasetContext.rowCount,
          totalColumns: datasetContext.totalColumns,
          workflowFeatures: datasetContext.workflowFeatures,
          taskFeatures: datasetContext.taskFeatures,
          usedColumns: datasetContext.usedColumns,
          summary: datasetContext.summary,
        }
      : null,
    session: nodeSession
      ? {
          sessionKey: String(nodeSession.sessionKey || "").trim(),
          nodeId: String(nodeSession.nodeId || "").trim(),
          workingDir: String(nodeSession.workingDir || "").trim(),
          permissionProfile:
            nodeSession.permissionProfile &&
            typeof nodeSession.permissionProfile === "object"
              ? nodeSession.permissionProfile
              : {},
          memorySummary: truncateSingleLine(
            nodeSession.memorySummary || "",
            WORKFLOW_NODE_MEMORY_SUMMARY_MAX
          ),
          memoryTail: toPromptMemoryTail(nodeSession.memoryTail, 8),
        }
      : null,
    team: teamEnabled
      ? {
          teamName: String(teamContext?.teamName || "").trim(),
          self:
            teamContext?.self && typeof teamContext.self === "object"
              ? teamContext.self
              : null,
          leader:
            teamContext?.leader && typeof teamContext.leader === "object"
              ? teamContext.leader
              : null,
          inboxMessages: Array.isArray(teamContext?.inboxMessages)
            ? teamContext.inboxMessages.slice(0, WORKFLOW_AGENT_INBOX_MAX_MESSAGES)
            : [],
        }
      : null,
    localExecution: {
      enabled: localExecEnabled,
      maxCommands,
      cwd: localExecEnabled ? String(localExec?.cwd || "") : "",
      shell: localExecEnabled ? String(localExec?.shell || "") : "",
      permissionProfile: localExecEnabled
        ? {
            mode: permissionProfile.mode,
            allowCommands: normalizeStringArray(permissionProfile.allowCommands).slice(
              0,
              80
            ),
            denyPatterns: normalizeStringArray(permissionProfile.denyPatterns).slice(
              0,
              40
            ),
          }
        : null,
    },
    commandHistory: Array.isArray(commandHistory)
      ? commandHistory.map((item) => ({
          step: item.step,
          commands: item.commands,
          results: item.results,
        }))
      : [],
    request:
      "현재 task를 수행한 결과를 작성하고, 다음 task가 바로 실행할 수 있도록 핵심 인사이트와 후속 액션을 정리하세요.",
  };

  return {
    systemPrompt,
    userPrompt: JSON.stringify(payload, null, 2),
  };
}

async function requestWorkflowTaskResponseFromOpenAICodex({
  accessToken,
  accountId,
  modelId,
  systemPrompt,
  userPrompt,
}) {
  const headers = buildOpenAICodexHeaders({
    accessToken,
    accountId,
  });
  const payload = {
    model: modelId,
    store: false,
    stream: true,
    instructions: systemPrompt,
    input: [
      {
        role: "user",
        content: [{ type: "input_text", text: userPrompt }],
      },
    ],
    text: { verbosity: "medium" },
    tool_choice: "auto",
    parallel_tool_calls: false,
  };

  if (typeof fetch !== "function") {
    throw new Error("runtime fetch is unavailable");
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DATA_AI_MODEL_TIMEOUT_MS);
  let response;
  try {
    response = await fetch(resolveOpenAICodexResponsesUrl(), {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("OpenAI Codex request timed out");
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }

  if (!response.ok) {
    const rawText = await response.text().catch(() => "");
    let payloadJson = {};
    try {
      payloadJson = rawText ? JSON.parse(rawText) : {};
    } catch {
      payloadJson = {};
    }
    throw new Error(
      sanitizeOpenAICodexErrorMessage(payloadJson, response.status, rawText)
    );
  }

  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (!contentType.includes("text/event-stream")) {
    const rawText = await response.text().catch(() => "");
    let payloadJson = {};
    try {
      payloadJson = rawText ? JSON.parse(rawText) : {};
    } catch {
      payloadJson = {};
    }
    const text =
      extractModelTextFromResponsePayload(payloadJson) ||
      extractTextFromCodexSseTranscript(rawText) ||
      rawText.trim();
    if (!text) {
      throw new Error("model returned an empty response");
    }
    return text;
  }

  const text = await readOpenAICodexSseOutputText(response);
  if (!text) {
    throw new Error("model returned an empty response");
  }
  return text;
}

async function requestWorkflowTaskResponseFromOpenAIResponsesApi({
  accessToken,
  modelId,
  systemPrompt,
  userPrompt,
}) {
  const headers = {
    Authorization: `Bearer ${accessToken}`,
    "content-type": "application/json",
  };

  const responsesPayload = {
    model: modelId,
    input: [
      {
        role: "system",
        content: [{ type: "input_text", text: systemPrompt }],
      },
      {
        role: "user",
        content: [{ type: "input_text", text: userPrompt }],
      },
    ],
    max_output_tokens: 1800,
  };

  const responsesResult = await fetchJsonWithTimeout(
    `${OPENAI_API_BASE_URL}/responses`,
    {
      method: "POST",
      headers,
      body: JSON.stringify(responsesPayload),
    },
    DATA_AI_MODEL_TIMEOUT_MS
  );

  if (responsesResult.response.ok) {
    const text = extractModelTextFromResponsePayload(responsesResult.payload);
    if (!text) {
      throw new Error("model returned an empty response");
    }
    return text;
  }

  const responsesErrorText = String(
    responsesResult.payload?.error?.message ||
      responsesResult.payload?.message ||
      ""
  )
    .trim()
    .toLowerCase();
  const shouldFallbackToChatCompletions =
    responsesResult.response.status === 400 ||
    responsesResult.response.status === 403 ||
    responsesResult.response.status === 404 ||
    responsesResult.response.status === 422 ||
    responsesErrorText.includes("api.responses.write");

  if (!shouldFallbackToChatCompletions) {
    throw new Error(
      sanitizeModelErrorMessage(
        responsesResult.payload,
        responsesResult.response.status,
        "OpenAI responses API request failed"
      )
    );
  }

  const chatPayload = {
    model: modelId,
    response_format: { type: "json_object" },
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: userPrompt },
    ],
  };

  const chatResult = await fetchJsonWithTimeout(
    `${OPENAI_API_BASE_URL}/chat/completions`,
    {
      method: "POST",
      headers,
      body: JSON.stringify(chatPayload),
    },
    DATA_AI_MODEL_TIMEOUT_MS
  );

  if (!chatResult.response.ok) {
    throw new Error(
      sanitizeModelErrorMessage(
        chatResult.payload,
        chatResult.response.status,
        "OpenAI chat completion request failed"
      )
    );
  }

  const text = extractModelTextFromResponsePayload(chatResult.payload);
  if (!text) {
    throw new Error("model returned an empty response");
  }
  return text;
}

async function requestWorkflowTaskResponseFromOpenAI({
  provider,
  accessToken,
  accountId,
  modelId,
  systemPrompt,
  userPrompt,
}) {
  const normalizedProvider = normalizeProviderId(provider);
  if (normalizedProvider === "openai-codex") {
    return requestWorkflowTaskResponseFromOpenAICodex({
      accessToken,
      accountId,
      modelId,
      systemPrompt,
      userPrompt,
    });
  }

  return requestWorkflowTaskResponseFromOpenAIResponsesApi({
    accessToken,
    modelId,
    systemPrompt,
    userPrompt,
  });
}

function isTaskLocalExecutionEnabled(task, agent) {
  if (!WORKFLOW_LOCAL_EXEC_ENABLED || isServerlessRuntime) {
    return false;
  }

  const input = task?.input && typeof task.input === "object" ? task.input : {};
  if (typeof input.localExec === "boolean") {
    return input.localExec;
  }
  if (typeof input.allowLocalExec === "boolean") {
    return input.allowLocalExec;
  }

  const tools = normalizeStringArray(agent?.tools || []);
  if (tools.length === 0) {
    return true;
  }

  return tools.some((item) => {
    const lower = String(item || "").trim().toLowerCase();
    return (
      lower === "shell" ||
      lower === "terminal" ||
      lower === "bash" ||
      lower === "zsh" ||
      lower === "local-exec" ||
      lower === "computer-use" ||
      lower.includes("shell") ||
      lower.includes("terminal")
    );
  });
}

function compactCommandResultForModel(result) {
  return {
    command: truncateSingleLine(result?.command || "", 240),
    ok: Boolean(result?.ok),
    exitCode:
      Number.isFinite(Number(result?.exitCode)) && result?.exitCode !== null
        ? Number(result.exitCode)
        : null,
    timedOut: Boolean(result?.timedOut),
    durationMs: Math.max(0, Number(result?.durationMs) || 0),
    stdout: truncateSingleLine(result?.stdout || "", 1000),
    stderr: truncateSingleLine(result?.stderr || "", 1000),
  };
}

function summarizeCommandResultForEvent(result) {
  const command = truncateSingleLine(result?.command || "", 120);
  const ok = Boolean(result?.ok);
  const exitCode =
    Number.isFinite(Number(result?.exitCode)) && result?.exitCode !== null
      ? Number(result.exitCode)
      : null;
  const durationMs = Math.max(0, Number(result?.durationMs) || 0);
  const tail = ok
    ? `exit=${exitCode === null ? "-" : exitCode}, ${durationMs}ms`
    : `실패(exit=${exitCode === null ? "-" : exitCode}, ${durationMs}ms)`;
  return `${command} -> ${tail}`;
}

function toSafePathSegment(value, fallback = "node") {
  const normalized = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || fallback;
}

function resolveTaskNodeId(task, agentId = "") {
  const input = task?.input && typeof task.input === "object" ? task.input : {};
  const inputNodeId = String(input?.nodeId || input?.sessionNodeId || "").trim();
  if (inputNodeId) {
    return inputNodeId;
  }
  const taskKey = String(task?.taskKey || "").trim();
  if (taskKey) {
    return taskKey;
  }
  const fallbackAgentId = String(agentId || "").trim();
  if (fallbackAgentId) {
    return `agent-${fallbackAgentId.slice(0, 8)}`;
  }
  return "node";
}

function resolveTaskPermissionProfile(task) {
  const input = task?.input && typeof task.input === "object" ? task.input : {};
  const permissions =
    input?.permissions && typeof input.permissions === "object"
      ? input.permissions
      : {};
  const profileNameRaw = String(
    input?.permissionProfile || permissions?.profile || permissions?.mode || ""
  )
    .trim()
    .toLowerCase();
  const profileName = ["read_only", "standard", "full", "custom"].includes(
    profileNameRaw
  )
    ? profileNameRaw
    : "standard";

  const allowCommands = normalizeStringArray(
    permissions?.allowCommands || input?.allowCommands || []
  );
  const denyPatterns = normalizeStringArray(
    permissions?.denyPatterns || input?.denyPatterns || []
  );

  const profile = {
    mode: profileName,
    allowCommands,
    denyPatterns,
  };
  return normalizePermissionProfile(profile);
}

function buildNodeSessionWorkDir(workflowId, nodeId) {
  return path.join(
    WORKFLOW_NODE_SESSION_ROOT,
    toSafePathSegment(workflowId, "workflow"),
    toSafePathSegment(nodeId, "node")
  );
}

async function ensureWorkflowNodeSession({
  workflowId,
  task,
  agentId,
  teamIdentity = null,
}) {
  const workflowKey = String(workflowId || "").trim();
  const nodeId = resolveTaskNodeId(task, agentId);
  const workingDir = buildNodeSessionWorkDir(workflowKey, nodeId);
  fs.mkdirSync(workingDir, { recursive: true });

  const permissionProfile = resolveTaskPermissionProfile(task);
  const existing =
    typeof repository.getWorkflowNodeSession === "function"
      ? await repository.getWorkflowNodeSession(workflowKey, nodeId)
      : null;

  const sessionPayload = {
    workflowId: workflowKey,
    nodeId,
    agentId: agentId ? String(agentId).trim() : null,
    sessionKey:
      String(existing?.sessionKey || "").trim() || `${workflowKey}:${nodeId}`,
    workingDir,
    permissionProfile:
      permissionProfile && typeof permissionProfile === "object"
        ? permissionProfile
        : existing?.permissionProfile && typeof existing.permissionProfile === "object"
          ? existing.permissionProfile
          : {},
    memorySummary: String(existing?.memorySummary || "").trim(),
    memoryTail: Array.isArray(existing?.memoryTail) ? existing.memoryTail : [],
    meta:
      existing?.meta && typeof existing.meta === "object"
        ? {
            ...existing.meta,
            nodeId,
            ...(teamIdentity && typeof teamIdentity === "object"
              ? {
                  teamName: String(teamIdentity.teamName || "").trim(),
                  agentIdentity: {
                    agentId: String(teamIdentity.agentId || "").trim(),
                    agentName: String(teamIdentity.agentName || "").trim(),
                    color: String(teamIdentity.color || "").trim(),
                    role: String(teamIdentity.role || "").trim(),
                  },
                  color: String(teamIdentity.color || "").trim(),
                  role: String(teamIdentity.role || "").trim(),
                }
              : {}),
          }
        : {
            nodeId,
            ...(teamIdentity && typeof teamIdentity === "object"
              ? {
                  teamName: String(teamIdentity.teamName || "").trim(),
                  agentIdentity: {
                    agentId: String(teamIdentity.agentId || "").trim(),
                    agentName: String(teamIdentity.agentName || "").trim(),
                    color: String(teamIdentity.color || "").trim(),
                    role: String(teamIdentity.role || "").trim(),
                  },
                  color: String(teamIdentity.color || "").trim(),
                  role: String(teamIdentity.role || "").trim(),
                }
              : {}),
          },
    lastUsedAt: new Date().toISOString(),
  };

  if (typeof repository.upsertWorkflowNodeSession === "function") {
    const stored = await repository.upsertWorkflowNodeSession(sessionPayload);
    if (stored) {
      return stored;
    }
  }

  return {
    id: "",
    workflowId: workflowKey,
    nodeId,
    agentId: agentId ? String(agentId).trim() : null,
    sessionKey: sessionPayload.sessionKey,
    workingDir,
    permissionProfile: sessionPayload.permissionProfile,
    memorySummary: sessionPayload.memorySummary,
    memoryTail: sessionPayload.memoryTail,
    meta: sessionPayload.meta,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    lastUsedAt: new Date().toISOString(),
  };
}

function toPromptMemoryTail(memoryTail, maxItems = 8) {
  const source = Array.isArray(memoryTail) ? memoryTail : [];
  const safeMaxItems = Math.max(1, Number(maxItems) || 8);
  return source
    .slice(Math.max(0, source.length - safeMaxItems))
    .map((item) => ({
      at: String(item?.at || "").trim(),
      taskKey: String(item?.taskKey || "").trim(),
      title: String(item?.title || "").trim(),
      model: String(item?.model || "").trim(),
      status: String(item?.status || "").trim(),
      summary: truncateSingleLine(item?.summary || "", 280),
      insights: normalizeShortTextArray(item?.insights || [], {
        maxItems: 4,
        maxLength: 180,
      }),
      nextActions: normalizeShortTextArray(item?.nextActions || [], {
        maxItems: 4,
        maxLength: 180,
      }),
      commandStepCount: Math.max(0, Number(item?.commandStepCount) || 0),
    }));
}

async function updateWorkflowNodeSessionMemory({
  session,
  task,
  model,
  parsed,
  commandHistory,
}) {
  if (!session || typeof repository.upsertWorkflowNodeSession !== "function") {
    return session;
  }

  const now = new Date().toISOString();
  const existingTail = Array.isArray(session.memoryTail) ? session.memoryTail : [];
  const memoryEntry = {
    at: now,
    taskId: String(task?.id || "").trim(),
    taskKey: String(task?.taskKey || "").trim(),
    title: String(task?.title || "").trim(),
    model: String(model || "").trim(),
    status: String(parsed?.status || "completed").trim(),
    summary: truncateSingleLine(parsed?.summary || "", 320),
    insights: normalizeShortTextArray(parsed?.insights || [], {
      maxItems: 6,
      maxLength: 200,
    }),
    nextActions: normalizeShortTextArray(parsed?.nextActions || [], {
      maxItems: 6,
      maxLength: 200,
    }),
    commandStepCount: Array.isArray(commandHistory) ? commandHistory.length : 0,
  };
  const memoryTail = [...existingTail, memoryEntry].slice(
    Math.max(0, existingTail.length + 1 - WORKFLOW_NODE_MEMORY_TAIL_MAX)
  );

  const previousSummary = String(session.memorySummary || "").trim();
  const nextSummaryLine = `${now.slice(0, 19)} ${memoryEntry.summary}`;
  const mergedSummaryRaw = [previousSummary, nextSummaryLine]
    .filter(Boolean)
    .join("\n");
  const mergedSummary =
    mergedSummaryRaw.length > WORKFLOW_NODE_MEMORY_SUMMARY_MAX
      ? mergedSummaryRaw.slice(
          mergedSummaryRaw.length - WORKFLOW_NODE_MEMORY_SUMMARY_MAX
        )
      : mergedSummaryRaw;

  const updated = await repository.upsertWorkflowNodeSession({
    workflowId: session.workflowId,
    nodeId: session.nodeId,
    agentId: session.agentId,
    sessionKey: session.sessionKey,
    workingDir: session.workingDir,
    permissionProfile:
      session.permissionProfile && typeof session.permissionProfile === "object"
        ? session.permissionProfile
        : {},
    memorySummary: mergedSummary,
    memoryTail,
    meta: session.meta && typeof session.meta === "object" ? session.meta : {},
    lastUsedAt: now,
  });
  return updated || session;
}

function pickWorkflowTeamColor(index) {
  const safe = Math.max(0, Number(index) || 0);
  const palette = Array.isArray(WORKFLOW_TEAM_COLOR_PALETTE)
    ? WORKFLOW_TEAM_COLOR_PALETTE
    : [];
  if (palette.length === 0) {
    return "blue";
  }
  return String(palette[safe % palette.length] || "blue").trim() || "blue";
}

function resolveTaskNodeIdFromStoredTask(task) {
  const input = task?.input && typeof task.input === "object" ? task.input : {};
  const nodeId = String(input?.nodeId || task?.taskKey || "").trim();
  return nodeId || String(task?.id || "").trim();
}

function buildWorkflowTaskKeyByNodeId(workflowTasks) {
  const map = new Map();
  for (const item of Array.isArray(workflowTasks) ? workflowTasks : []) {
    const nodeId = resolveTaskNodeIdFromStoredTask(item);
    const taskKey = String(item?.taskKey || "").trim();
    if (!nodeId || !taskKey) {
      continue;
    }
    if (!map.has(nodeId)) {
      map.set(nodeId, taskKey);
    }
  }
  return map;
}

function toPromptInboxMessages(messages = [], taskKeyByNodeId = new Map()) {
  return (Array.isArray(messages) ? messages : [])
    .slice(0, WORKFLOW_AGENT_INBOX_MAX_MESSAGES)
    .map((message) => {
      const fromNodeId = String(message?.fromNodeId || "").trim();
      return {
        id: String(message?.id || "").trim(),
        fromNodeId,
        fromTaskKey: String(
          message?.fromTaskKey || taskKeyByNodeId.get(fromNodeId) || ""
        ).trim(),
        fromAgent: String(message?.fromAgent || "").trim(),
        color: String(message?.color || "").trim(),
        summary: truncateSingleLine(message?.summary || "", 200),
        text: String(message?.text || "").trim().slice(0, 4000),
        timestamp: String(message?.timestamp || "").trim(),
        type: String(message?.type || "message").trim().toLowerCase() || "message",
      };
    });
}

function buildTeamIdentityForTask({
  workflowId,
  workflowTasks,
  task,
  nodeId,
  agentLabel,
}) {
  const teamName = `workflow-${toSafePathSegment(String(workflowId || "").slice(0, 8), "team")}`;
  const orderedTasks = Array.isArray(workflowTasks) ? workflowTasks : [];
  const taskIndex = orderedTasks.findIndex((item) => String(item?.id || "").trim() === String(task?.id || "").trim());
  const color = pickWorkflowTeamColor(taskIndex >= 0 ? taskIndex : 0);
  const roots = orderedTasks.filter((item) => {
    const deps = Array.isArray(item?.dependsOn) ? item.dependsOn : [];
    return deps.length === 0;
  });
  const leaderTask = roots[0] || orderedTasks[0] || null;
  const isLead =
    leaderTask &&
    String(leaderTask.id || "").trim() === String(task?.id || "").trim();
  return {
    teamName,
    agentId: `${toSafePathSegment(nodeId, "node")}@${teamName}`,
    agentName: String(agentLabel || nodeId || "agent").trim(),
    color,
    role: isLead ? "team-lead" : "teammate",
    leaderTaskKey: String(leaderTask?.taskKey || "").trim(),
    leaderNodeId: leaderTask ? resolveTaskNodeIdFromStoredTask(leaderTask) : "",
  };
}

function resolveRecipientTaskKeys({
  message,
  workflowTasks,
  senderTaskKey,
}) {
  const type = String(message?.type || "message").trim().toLowerCase();
  if (type === "broadcast") {
    return (Array.isArray(workflowTasks) ? workflowTasks : [])
      .map((item) => String(item?.taskKey || "").trim())
      .filter((item) => item && item !== senderTaskKey);
  }
  const recipientTaskKey = String(message?.recipientTaskKey || "").trim();
  return recipientTaskKey ? [recipientTaskKey] : [];
}

async function dispatchWorkflowTeamMessages({
  workflowId,
  workflowTasks,
  senderTask,
  senderSession,
  teamIdentity,
  parsedMessages,
}) {
  if (!WORKFLOW_AGENT_TEAMS_ENABLED) {
    return {
      dispatchCount: 0,
      deliveries: [],
    };
  }

  const messages = normalizeTeamMessageArray(
    parsedMessages,
    WORKFLOW_AGENT_OUTBOX_MAX_MESSAGES
  );
  if (messages.length === 0) {
    return {
      dispatchCount: 0,
      deliveries: [],
    };
  }

  const taskByKey = new Map(
    (Array.isArray(workflowTasks) ? workflowTasks : [])
      .map((item) => [String(item?.taskKey || "").trim(), item])
      .filter((entry) => entry[0])
  );

  const deliveries = [];
  const senderTaskKey = String(senderTask?.taskKey || "").trim();
  const senderNodeId = String(senderSession?.nodeId || "").trim();

  for (const message of messages) {
    const recipients = resolveRecipientTaskKeys({
      message,
      workflowTasks,
      senderTaskKey,
    });
    for (const recipientTaskKey of recipients) {
      const targetTask = taskByKey.get(recipientTaskKey);
      if (!targetTask) {
        continue;
      }
      const targetNodeId = resolveTaskNodeIdFromStoredTask(targetTask);
      if (!targetNodeId || targetNodeId === senderNodeId) {
        continue;
      }
      const summary =
        truncateSingleLine(message.summary || "", 180) ||
        truncateSingleLine(message.content || "", 180);
      appendMailboxMessage({
        rootDir: WORKFLOW_NODE_SESSION_ROOT,
        workflowId,
        nodeId: targetNodeId,
        message: {
          fromNodeId: senderNodeId,
          fromTaskKey: senderTaskKey,
          fromAgent: String(teamIdentity?.agentName || "").trim(),
          fromAgentId: String(teamIdentity?.agentId || "").trim(),
          color: String(teamIdentity?.color || "").trim(),
          text: String(message.content || "").trim(),
          summary,
          type: String(message.type || "message").trim().toLowerCase(),
          timestamp: new Date().toISOString(),
        },
      });
      deliveries.push({
        type: String(message.type || "message").trim().toLowerCase(),
        recipientTaskKey,
        recipientNodeId: targetNodeId,
        content: truncateSingleLine(message.content || "", 220),
        summary,
      });
    }
  }

  return {
    dispatchCount: deliveries.length,
    deliveries,
  };
}

function toWorkflowNodeSessionSummary(session) {
  if (!session) {
    return null;
  }
  const permissionProfile = normalizePermissionProfile(
    session.permissionProfile && typeof session.permissionProfile === "object"
      ? session.permissionProfile
      : {}
  );
  return {
    nodeId: String(session.nodeId || "").trim(),
    sessionKey: String(session.sessionKey || "").trim(),
    workingDir: String(session.workingDir || "").trim(),
    permissionProfile: {
      mode: permissionProfile.mode,
      allowCommands: normalizeStringArray(permissionProfile.allowCommands).slice(
        0,
        120
      ),
      denyPatterns: normalizeStringArray(permissionProfile.denyPatterns).slice(0, 60),
    },
    memorySummary: truncateSingleLine(session.memorySummary || "", 600),
    memoryTailSize: Array.isArray(session.memoryTail) ? session.memoryTail.length : 0,
    agentId: String(session?.meta?.agentIdentity?.agentId || "").trim() || null,
    agentName: String(session?.meta?.agentIdentity?.agentName || "").trim() || null,
    role: String(session?.meta?.role || "").trim() || null,
    color: String(session?.meta?.color || "").trim() || null,
    teamName: String(session?.meta?.teamName || "").trim() || null,
    lastUsedAt: session.lastUsedAt || null,
  };
}

async function executeWorkflowTaskWithAgent({
  task,
  workflowId,
  taskTitle,
  taskKind,
  agent,
  agentLabel,
}) {
  const workflow = await repository.getWorkflow(workflowId);
  if (!workflow) {
    throw new Error(`workflow not found: ${workflowId}`);
  }
  const taskAgentId = String(agent?.id || task?.agentId || "").trim();
  const workflowTasks = await repository.listWorkflowTasks(workflowId);
  const taskNodeId = resolveTaskNodeId(task, taskAgentId);
  const taskKeyByNodeId = buildWorkflowTaskKeyByNodeId(workflowTasks);
  const teamIdentity = buildTeamIdentityForTask({
    workflowId,
    workflowTasks,
    task,
    nodeId: taskNodeId,
    agentLabel,
  });
  let nodeSession = await ensureWorkflowNodeSession({
    workflowId,
    task,
    agentId: taskAgentId || null,
    teamIdentity,
  });
  const sessionWorkingDir =
    String(nodeSession?.workingDir || "").trim() || WORKFLOW_LOCAL_EXEC_CWD;
  const sessionPermissionProfile = normalizePermissionProfile(
    nodeSession?.permissionProfile && typeof nodeSession.permissionProfile === "object"
      ? nodeSession.permissionProfile
      : resolveTaskPermissionProfile(task)
  );
  let inboxMessages = [];
  if (WORKFLOW_AGENT_TEAMS_ENABLED) {
    const unread = readUnreadMailboxMessages({
      rootDir: WORKFLOW_NODE_SESSION_ROOT,
      workflowId,
      nodeId: nodeSession.nodeId,
      maxMessages: WORKFLOW_AGENT_INBOX_MAX_MESSAGES,
    });
    if (unread.length > 0) {
      inboxMessages = toPromptInboxMessages(unread, taskKeyByNodeId);
      markMailboxMessagesRead({
        rootDir: WORKFLOW_NODE_SESSION_ROOT,
        workflowId,
        nodeId: nodeSession.nodeId,
        messageIds: unread
          .map((item) => String(item?.id || "").trim())
          .filter(Boolean),
      });
      await repository.appendWorkflowEvent({
        workflowId,
        taskId: task?.id || null,
        role: "agent",
        message: `${agentLabel} inbox 수신: ${inboxMessages.length}건`,
        meta: {
          taskKey: task.taskKey,
          inboxCount: inboxMessages.length,
          role: teamIdentity.role,
          color: teamIdentity.color,
        },
      });
    }
  }
  const teamContext = {
    enabled: WORKFLOW_AGENT_TEAMS_ENABLED,
    teamName: teamIdentity.teamName,
    self: {
      agentId: teamIdentity.agentId,
      agentName: teamIdentity.agentName,
      nodeId: taskNodeId,
      taskKey: String(task?.taskKey || "").trim(),
      role: teamIdentity.role,
      color: teamIdentity.color,
    },
    leader: {
      taskKey: teamIdentity.leaderTaskKey,
      nodeId: teamIdentity.leaderNodeId,
    },
    inboxMessages,
  };
  const commandHistory = [];

  const dependencyIds = new Set(
    (Array.isArray(task?.dependsOn) ? task.dependsOn : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  );
  const dependencyOutputs = workflowTasks
    .filter((item) => dependencyIds.has(String(item?.id || "").trim()))
    .map((item) => {
      const output =
        item?.output && typeof item.output === "object" ? item.output : null;
      const compactOutput = output
        ? {
            summary: truncateSingleLine(output.summary || "", 260),
            insights: normalizeShortTextArray(output.insights, {
              maxItems: 6,
              maxLength: 180,
            }),
            nextActions: normalizeShortTextArray(output.nextActions, {
              maxItems: 6,
              maxLength: 180,
            }),
            status: String(output.status || "completed").trim() || "completed",
            model: String(output.model || "").trim() || null,
          }
        : null;
      return {
        taskId: item.id,
        taskKey: item.taskKey,
        title: item.title,
        status: item.status,
        summary: truncateSingleLine(
          compactOutput?.summary || item?.errorMessage || "",
          260
        ),
        output: compactOutput,
      };
    });

  const datasetContext = await buildWorkflowTaskDatasetContext(workflow, task);
  const modelTier = String(agent?.modelTier || "").trim();
  const parsedModel = parseModelSelectionValue(modelTier);
  let modelProvider = parsedModel.provider;
  const requestedModelId = String(parsedModel.modelId || "").trim();
  const hasProviderPrefix = modelTier.includes("/");
  if (!hasProviderPrefix && requestedModelId) {
    const lowerModelId = requestedModelId.toLowerCase();
    if (lowerModelId.includes("codex")) {
      modelProvider = "openai-codex";
    } else if (
      lowerModelId.startsWith("gpt-") ||
      lowerModelId.startsWith("o1") ||
      lowerModelId.startsWith("o3") ||
      lowerModelId.startsWith("o4")
    ) {
      modelProvider = "openai";
    }
  }
  const isBalancedDefault =
    modelTier.toLowerCase() === "balanced (default)".toLowerCase();

  if (
    !modelProvider ||
    modelProvider === "system" ||
    !requestedModelId ||
    isBalancedDefault
  ) {
    const fallbackResult = {
      taskKey: task.taskKey,
      kind: taskKind,
      agent: agentLabel,
      model: "local/fallback",
      modelAutoResolved: false,
      status: "completed",
      summary: `${agentLabel}가 '${taskTitle}' 작업을 처리했습니다. (모델 미설정으로 로컬 요약만 기록)`,
      deliverable: `${agentLabel}는 현재 모델이 설정되지 않아 로컬 요약만 기록했습니다.`,
      insights: normalizeShortTextArray(
        [
          workflow?.goal ? `workflow goal: ${workflow.goal}` : "",
          datasetContext?.sourceName
            ? `dataset: ${datasetContext.sourceName} (${datasetContext.rowCount} rows)`
            : "dataset 미연결",
        ].filter(Boolean),
        { maxItems: 4, maxLength: 200 }
      ),
      nextActions: [],
      messages: [],
      messageDispatchCount: 0,
      dependencyTaskKeys: dependencyOutputs
        .map((item) => String(item.taskKey || "").trim())
        .filter(Boolean),
      usedFeatures: Array.isArray(datasetContext?.usedColumns)
        ? datasetContext.usedColumns
        : [],
      completedAt: new Date().toISOString(),
    };
    nodeSession = await updateWorkflowNodeSessionMemory({
      session: nodeSession,
      task,
      model: fallbackResult.model,
      parsed: {
        summary: fallbackResult.summary,
        deliverable: fallbackResult.deliverable,
        insights: fallbackResult.insights,
        nextActions: fallbackResult.nextActions,
        status: fallbackResult.status,
      },
      commandHistory,
    });
    return {
      ...fallbackResult,
      nodeSession: toWorkflowNodeSessionSummary(nodeSession),
    };
  }

  if (!["openai", "openai-codex"].includes(modelProvider)) {
    throw new Error(`unsupported task model provider: ${modelProvider}`);
  }

  const connection = await repository.getProviderAuthConnectionByProvider("openai");
  if (!connection || connection.status !== "connected") {
    throw new Error("OPEN AI provider OAuth 인증이 필요합니다.");
  }
  if (String(connection.authMode || "").trim().toLowerCase() !== "oauth") {
    throw new Error("OPEN AI provider must use oauth auth mode");
  }

  const tokenResult = await ensureOpenAIOAuthAccessToken(connection);
  const accessToken = tokenResult.accessToken;
  const openAIOAuthAccountId = String(
    tokenResult?.credential?.accountId ||
      getOpenAICodexAccountId(accessToken) ||
      ""
  ).trim();

  const useCodexTransport = modelProvider === "openai-codex";
  const codexDiscoveredModelIds = normalizeProviderModels("openai", connection?.models)
    .filter((entry) => entry.provider === "openai-codex")
    .map((entry) => entry.modelId);

  let availableOpenAIModelIds = [];
  if (!useCodexTransport) {
    try {
      availableOpenAIModelIds = await fetchOpenAIAvailableModelIds(accessToken);
    } catch {
      availableOpenAIModelIds = [];
    }
  }

  const modelResolution = useCodexTransport
    ? {
        modelId:
          buildOpenAICodexFallbackModelIds(
            requestedModelId,
            codexDiscoveredModelIds
          )[0] || requestedModelId,
        changed: false,
      }
    : resolveRequestedOpenAIModel({
        requestedModelId,
        availableModelIds: availableOpenAIModelIds,
      });

  let modelIdForExecution = String(
    modelResolution.modelId || requestedModelId
  ).trim();
  let modelAutoResolved =
    modelIdForExecution !== requestedModelId || Boolean(modelResolution.changed);

  const executionProvider = useCodexTransport ? "openai-codex" : "openai";
  const localExecEnabled = isTaskLocalExecutionEnabled(task, agent);
  const localExecConfig = {
    enabled: localExecEnabled,
    maxCommands: WORKFLOW_LOCAL_EXEC_MAX_COMMANDS,
    cwd: sessionWorkingDir,
    shell: WORKFLOW_LOCAL_EXEC_SHELL,
    permissionProfile: sessionPermissionProfile,
  };
  const maxSteps = Math.max(1, WORKFLOW_LOCAL_EXEC_MAX_STEPS);

  const requestModelOnce = async (prompts) => {
    try {
      return await requestWorkflowTaskResponseFromOpenAI({
        provider: executionProvider,
        accessToken,
        accountId: openAIOAuthAccountId,
        modelId: modelIdForExecution,
        systemPrompt: prompts.systemPrompt,
        userPrompt: prompts.userPrompt,
      });
    } catch (error) {
      const firstMessage = truncateSingleLine(
        String(error?.message || "task model execution failed"),
        240
      );
      const lower = firstMessage.toLowerCase();
      const isModelAccessError =
        lower.includes("does not exist") ||
        lower.includes("do not have access to it") ||
        (lower.includes("model") && lower.includes("not found"));

      if (!isModelAccessError) {
        throw new Error(firstMessage);
      }

      const retryModel = useCodexTransport
        ? buildOpenAICodexFallbackModelIds(
            modelIdForExecution,
            codexDiscoveredModelIds
          ).find((candidate) => candidate !== modelIdForExecution) || ""
        : pickPreferredOpenAIModel(availableOpenAIModelIds);

      if (!retryModel || retryModel === modelIdForExecution) {
        throw new Error(firstMessage);
      }

      const retryText = await requestWorkflowTaskResponseFromOpenAI({
        provider: executionProvider,
        accessToken,
        accountId: openAIOAuthAccountId,
        modelId: retryModel,
        systemPrompt: prompts.systemPrompt,
        userPrompt: prompts.userPrompt,
      });
      modelIdForExecution = retryModel;
      modelAutoResolved = true;
      return retryText;
    }
  };

  let parsed = null;
  for (let step = 1; step <= maxSteps; step += 1) {
    const prompts = buildWorkflowTaskPromptPayload({
      workflow,
      task,
      agent,
      agentLabel,
      taskTitle,
      taskKind,
      dependencyOutputs,
      datasetContext,
      localExec: localExecConfig,
      commandHistory,
      nodeSession,
      teamContext,
    });
    const modelText = await requestModelOnce(prompts);
    const nextParsed = parseWorkflowTaskModelOutput(modelText);
    const requestedCommands = normalizeCommandList(
      nextParsed.commands || [],
      WORKFLOW_LOCAL_EXEC_MAX_COMMANDS
    );

    if (!localExecEnabled && requestedCommands.length > 0) {
      parsed = {
        ...nextParsed,
        status: "blocked",
        commands: [],
        insights: normalizeShortTextArray(
          [
            ...(Array.isArray(nextParsed.insights) ? nextParsed.insights : []),
            "로컬 명령 실행이 비활성화되어 commands를 실행할 수 없습니다.",
          ],
          { maxItems: 12, maxLength: 220 }
        ),
      };
      break;
    }

    if (nextParsed.status !== "needs_commands" || requestedCommands.length === 0) {
      parsed = {
        ...nextParsed,
        commands: requestedCommands,
      };
      break;
    }

    await repository.appendWorkflowEvent({
      workflowId,
      taskId: task?.id || null,
      role: "agent",
      message: `${agentLabel} 명령 실행 요청: ${requestedCommands.length}개`,
      meta: {
        step,
        commandCount: requestedCommands.length,
      },
    });

    const commandRun = await runLocalCommands({
      commands: requestedCommands,
      cwd: localExecConfig.cwd,
      shell: localExecConfig.shell,
      timeoutMs: WORKFLOW_LOCAL_EXEC_TIMEOUT_MS,
      maxOutputChars: WORKFLOW_LOCAL_EXEC_OUTPUT_MAX_CHARS,
      maxCommands: WORKFLOW_LOCAL_EXEC_MAX_COMMANDS,
      permissionProfile: localExecConfig.permissionProfile,
    });

    const compactResults = (Array.isArray(commandRun.results) ? commandRun.results : []).map(
      (item) => compactCommandResultForModel(item)
    );
    commandHistory.push({
      step,
      commands: requestedCommands,
      results: compactResults,
      successCount: commandRun.successCount,
      failedCount: commandRun.failedCount,
    });

    for (const result of compactResults.slice(0, 8)) {
      await repository.appendWorkflowEvent({
        workflowId,
        taskId: task?.id || null,
        role: "agent",
        message: `${agentLabel} 명령 결과: ${summarizeCommandResultForEvent(result)}`,
        meta: {
          step,
          ok: Boolean(result.ok),
          exitCode: result.exitCode,
          command: result.command,
        },
      });
    }

    if (step >= maxSteps) {
      parsed = {
        ...nextParsed,
        status: commandRun.failedCount > 0 ? "blocked" : "completed",
        commands: [],
        insights: normalizeShortTextArray(
          [
            ...(Array.isArray(nextParsed.insights) ? nextParsed.insights : []),
            `명령 실행 ${commandRun.commandCount}개 완료 (성공 ${commandRun.successCount}, 실패 ${commandRun.failedCount})`,
          ],
          { maxItems: 12, maxLength: 220 }
        ),
      };
      break;
    }
  }

  if (!parsed) {
    parsed = {
      summary: `${agentLabel}가 '${taskTitle}' 작업을 완료했습니다.`,
      deliverable: "",
      insights: [],
      nextActions: [],
      commands: [],
      messages: [],
      status: "completed",
      rawText: "",
    };
  }

  const teamMessageDispatch = await dispatchWorkflowTeamMessages({
    workflowId,
    workflowTasks,
    senderTask: task,
    senderSession: nodeSession,
    teamIdentity,
    parsedMessages: parsed.messages,
  });
  for (const delivery of teamMessageDispatch.deliveries.slice(0, 24)) {
    await repository.appendWorkflowEvent({
      workflowId,
      taskId: task?.id || null,
      role: "agent",
      message: `${agentLabel} 메시지 → ${delivery.recipientTaskKey}: ${delivery.summary}`,
      meta: {
        taskKey: task.taskKey,
        type: "team_message",
        messageType: delivery.type,
        recipientTaskKey: delivery.recipientTaskKey,
        recipientNodeId: delivery.recipientNodeId,
      },
    });
  }

  const result = {
    taskKey: task.taskKey,
    kind: taskKind,
    agent: agentLabel,
    model: `${executionProvider}/${modelIdForExecution}`,
    modelAutoResolved,
    status: parsed.status,
    summary:
      parsed.summary ||
      `${agentLabel}가 '${taskTitle}' 작업을 완료했습니다.`,
    deliverable: String(parsed.deliverable || "").trim(),
    insights: parsed.insights,
    nextActions: parsed.nextActions,
    messages: Array.isArray(parsed.messages) ? parsed.messages : [],
    messageDispatchCount: teamMessageDispatch.dispatchCount,
    messageDeliveries: teamMessageDispatch.deliveries,
    commandRuns: commandHistory,
    commandExecutionEnabled: localExecEnabled,
    dependencyTaskKeys: dependencyOutputs
      .map((item) => String(item.taskKey || "").trim())
      .filter(Boolean),
    usedFeatures: Array.isArray(datasetContext?.usedColumns)
      ? datasetContext.usedColumns
      : [],
    rawResponse: parsed.rawText,
    completedAt: new Date().toISOString(),
  };
  nodeSession = await updateWorkflowNodeSessionMemory({
    session: nodeSession,
    task,
    model: result.model,
    parsed,
    commandHistory,
  });
  return {
    ...result,
    nodeSession: toWorkflowNodeSessionSummary(nodeSession),
  };
}

function toDataRunResponse(run, { includeRows = false } = {}) {
  if (!run) {
    return null;
  }

  const response = {
    id: run.id,
    model: run.model,
    command: run.command,
    sourceName: run.sourceName || null,
    action: run.action,
    inputRowCount: run.inputRowCount,
    outputRowCount: run.outputRowCount,
    stats: run.stats || {},
    report: run.report || "",
    preview: run.preview || {},
    createdAt: run.createdAt,
    expiresAt: run.expiresAt,
  };

  if (includeRows) {
    response.cleanedRows = Array.isArray(run.cleanedRows) ? run.cleanedRows : [];
  }

  return response;
}

function canAccessDataRun(run, authUser) {
  if (!run || !authUser) {
    return false;
  }
  const role = String(authUser.role || "").trim().toLowerCase();
  if (role === "owner" || role === "admin") {
    return true;
  }
  return String(run.userId || "").trim() === String(authUser.id || "").trim();
}

function toWorkflowApiResponse(
  workflow,
  { tasks = [], events = [], nodeSessions = [] } = {}
) {
  if (!workflow) {
    return null;
  }

  return {
    id: workflow.id,
    goal: workflow.goal,
    status: workflow.status,
    datasetId: workflow.datasetId || null,
    selectedFeatures: Array.isArray(workflow.selectedFeatures)
      ? workflow.selectedFeatures
      : [],
    createdBy: workflow.createdBy || null,
    meta: workflow.meta || {},
    errorMessage: workflow.errorMessage || null,
    createdAt: workflow.createdAt,
    updatedAt: workflow.updatedAt,
    startedAt: workflow.startedAt || null,
    completedAt: workflow.completedAt || null,
    taskCounts: summarizeWorkflowTaskCounts(tasks),
    tasks,
    events,
    nodeSessions: Array.isArray(nodeSessions) ? nodeSessions : [],
  };
}

async function ensureDefaultProviderAuthConnections() {
  for (const template of providerAuthTemplates) {
    await repository.upsertProviderAuthConnection({
      provider: template.provider,
      displayName: template.label,
      authMode: template.authMode,
    });
  }
}

async function listProviderAuthConnectionsForResponse() {
  await ensureDefaultProviderAuthConnections();
  const connections = await repository.listProviderAuthConnections(100);
  return connections.map((item) => toPublicProviderAuthConnection(item));
}

function parseCookieHeader(cookieHeader) {
  const out = {};
  const raw = String(cookieHeader || "");
  if (!raw) {
    return out;
  }

  for (const part of raw.split(";")) {
    const [nameRaw, ...rest] = part.split("=");
    const name = String(nameRaw || "").trim();
    if (!name) {
      continue;
    }
    const value = rest.join("=");
    const rawValue = String(value || "").trim();
    try {
      out[name] = decodeURIComponent(rawValue);
    } catch {
      out[name] = rawValue;
    }
  }
  return out;
}

function getRequestCookies(req) {
  return parseCookieHeader(req.headers?.cookie || "");
}

function getClientIp(req) {
  const forwarded = String(req.headers["x-forwarded-for"] || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  if (forwarded.length > 0) {
    return forwarded[0];
  }
  return req.socket?.remoteAddress || req.ip || "0.0.0.0";
}

function hashToken(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex");
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function sanitizeUser(user) {
  if (!user) {
    return null;
  }
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    role: user.role,
    status: user.status,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
  };
}

function getAuthBypassUser() {
  const now = new Date().toISOString();
  return {
    id: "auth-bypass-user",
    email: "public@local",
    name: "Public",
    role: "owner",
    status: "active",
    createdAt: now,
    updatedAt: now,
  };
}

function getCookieOptions(maxAgeSec) {
  return {
    httpOnly: true,
    secure: isProduction,
    sameSite: "lax",
    path: "/",
    domain: AUTH_COOKIE_DOMAIN,
    maxAge: Math.max(1, Number(maxAgeSec)) * 1000,
  };
}

function clearAuthCookies(res) {
  const options = {
    httpOnly: true,
    secure: isProduction,
    sameSite: "lax",
    path: "/",
    domain: AUTH_COOKIE_DOMAIN,
  };
  res.clearCookie(ACCESS_COOKIE_NAME, options);
  res.clearCookie(REFRESH_COOKIE_NAME, options);
}

function signAccessToken(user) {
  return jwt.sign(
    {
      sub: user.id,
      role: user.role,
      email: user.email,
      name: user.name,
      typ: "access",
    },
    JWT_ACCESS_SECRET,
    { expiresIn: ACCESS_TOKEN_TTL_SEC }
  );
}

function signRefreshToken(user) {
  return jwt.sign(
    {
      sub: user.id,
      role: user.role,
      typ: "refresh",
      jti: crypto.randomUUID(),
    },
    JWT_REFRESH_SECRET,
    { expiresIn: REFRESH_TOKEN_TTL_SEC }
  );
}

function setAuthCookies(res, accessToken, refreshToken) {
  res.cookie(ACCESS_COOKIE_NAME, accessToken, getCookieOptions(ACCESS_TOKEN_TTL_SEC));
  res.cookie(REFRESH_COOKIE_NAME, refreshToken, getCookieOptions(REFRESH_TOKEN_TTL_SEC));
}

function extractAccessToken(req) {
  const authHeader = String(req.headers?.authorization || "");
  if (authHeader.toLowerCase().startsWith("bearer ")) {
    return authHeader.slice(7).trim();
  }
  const cookies = getRequestCookies(req);
  return cookies[ACCESS_COOKIE_NAME] || "";
}

function extractRefreshToken(req) {
  const cookies = getRequestCookies(req);
  if (cookies[REFRESH_COOKIE_NAME]) {
    return cookies[REFRESH_COOKIE_NAME];
  }
  return String(req.body?.refreshToken || "").trim();
}

function getLoginRateKey(req, email) {
  return `${getClientIp(req)}::${normalizeEmail(email)}`;
}

function pruneLoginAttempts() {
  const now = Date.now();
  for (const [key, value] of loginAttempts.entries()) {
    if (!value || now > value.windowStart + LOGIN_WINDOW_MS) {
      loginAttempts.delete(key);
    }
  }
}

function isLoginBlocked(rateKey) {
  pruneLoginAttempts();
  const item = loginAttempts.get(rateKey);
  if (!item) {
    return false;
  }
  return item.count >= LOGIN_MAX_ATTEMPTS && Date.now() <= item.windowStart + LOGIN_WINDOW_MS;
}

function getLoginAttemptState(rateKey) {
  pruneLoginAttempts();
  return loginAttempts.get(rateKey) || null;
}

function getLoginRetryAfterSec(rateKey) {
  const state = getLoginAttemptState(rateKey);
  if (!state) {
    return 0;
  }
  const retryAfterMs = Math.max(0, state.windowStart + LOGIN_WINDOW_MS - Date.now());
  return Math.ceil(retryAfterMs / 1000);
}

function registerFailedLogin(rateKey) {
  const now = Date.now();
  const current = loginAttempts.get(rateKey);
  if (!current || now > current.windowStart + LOGIN_WINDOW_MS) {
    const next = { count: 1, windowStart: now };
    loginAttempts.set(rateKey, next);
    return next;
  }
  current.count += 1;
  loginAttempts.set(rateKey, current);
  return current;
}

function clearFailedLogin(rateKey) {
  loginAttempts.delete(rateKey);
}

function validateAuthConfig() {
  if (AUTH_DISABLED) {
    return;
  }
  if (!isProduction) {
    return;
  }

  if (
    !process.env.JWT_ACCESS_SECRET ||
    JWT_ACCESS_SECRET === DEFAULT_ACCESS_SECRET
  ) {
    throw new Error(
      "JWT_ACCESS_SECRET must be set with a non-default value in production"
    );
  }

  if (
    !process.env.JWT_REFRESH_SECRET ||
    JWT_REFRESH_SECRET === DEFAULT_REFRESH_SECRET
  ) {
    throw new Error(
      "JWT_REFRESH_SECRET must be set with a non-default value in production"
    );
  }
}

async function authenticate(req, res, next) {
  if (AUTH_DISABLED) {
    req.authUser = getAuthBypassUser();
    return next();
  }

  const token = extractAccessToken(req);
  if (!token) {
    return res.status(401).json({
      error: "unauthorized",
      message: "access token is required",
    });
  }

  let payload;
  try {
    payload = jwt.verify(token, JWT_ACCESS_SECRET);
  } catch (error) {
    return res.status(401).json({
      error: "unauthorized",
      message: error.name === "TokenExpiredError" ? "access token expired" : "invalid access token",
    });
  }

  try {
    const user = await repository.getUserById(payload?.sub);
    if (!user || user.status !== "active") {
      return res.status(401).json({
        error: "unauthorized",
        message: "user is not active",
      });
    }

    req.authUser = sanitizeUser(user);
    return next();
  } catch (error) {
    return next(error);
  }
}

function authorizeRoles(...roles) {
  if (AUTH_DISABLED) {
    return (req, res, next) => next();
  }
  const roleSet = new Set(roles.map((role) => String(role || "").trim()).filter(Boolean));
  return (req, res, next) => {
    const role = String(req.authUser?.role || "").trim();
    if (!role || !roleSet.has(role)) {
      return res.status(403).json({
        error: "forbidden",
        message: "insufficient role",
      });
    }
    return next();
  };
}

const requireOperatorRole = [authenticate, authorizeRoles("owner", "admin", "operator")];
const requireAdminRole = [authenticate, authorizeRoles("owner", "admin")];
const allowedRoles = new Set(["owner", "admin", "operator", "viewer"]);

function normalizeRole(value, fallback = "viewer") {
  const role = String(value || "").trim().toLowerCase();
  if (!role) {
    return fallback;
  }
  if (!allowedRoles.has(role)) {
    throw new Error(`invalid role: ${role}`);
  }
  return role;
}

async function ensureBootstrapUser() {
  if (AUTH_DISABLED) {
    return;
  }
  const bootstrapEmail = normalizeEmail(AUTH_BOOTSTRAP_EMAIL);
  if (!bootstrapEmail) {
    return;
  }

  const existing = await repository.findUserByEmail(bootstrapEmail);
  if (existing) {
    return;
  }

  if (!AUTH_BOOTSTRAP_PASSWORD || AUTH_BOOTSTRAP_PASSWORD.length < 8) {
    console.warn(
      "[auth] bootstrap user skipped: AUTH_BOOTSTRAP_PASSWORD must be at least 8 chars"
    );
    return;
  }

  const passwordHash = await bcrypt.hash(AUTH_BOOTSTRAP_PASSWORD, 12);
  try {
    await repository.createUser({
      email: bootstrapEmail,
      passwordHash,
      name: AUTH_BOOTSTRAP_NAME || "Owner",
      role: normalizeRole(AUTH_BOOTSTRAP_ROLE, "owner"),
      status: "active",
    });
    console.log(`[auth] bootstrap user created: ${bootstrapEmail}`);
  } catch (error) {
    if (
      String(error?.message || "").toLowerCase().includes("exists") ||
      String(error?.code || "").toLowerCase() === "23505"
    ) {
      return;
    }
    throw error;
  }
}

async function ensureInitialized() {
  if (initialized) {
    return;
  }

  if (!initPromise) {
    initPromise = (async () => {
      validateAuthConfig();
      await repository.init();

      const storedFields = await repository.listOntologyFields();
      if (storedFields.length > 0) {
        ontologyService.loadFields(storedFields);
      } else {
        await repository.upsertOntologyFields(ontologyService.listFields());
      }

      const storedOverrides = await repository.listColumnOverrides();
      ontologyService.loadOverrides(storedOverrides);

      await ensureBootstrapUser();
      await ensureDefaultProviderAuthConnections();

      initialized = true;
    })().catch((error) => {
      initPromise = null;
      throw error;
    });
  }

  await initPromise;
}

async function flushQueueForServerless() {
  if (!isServerlessRuntime) {
    return;
  }
  await jobQueue.processTick();
}

async function flushWorkflowSchedulerForServerless() {
  if (!isServerlessRuntime) {
    return;
  }
  await workflowScheduler.processTick();
}

app.use((req, res, next) => {
  const start = process.hrtime.bigint();
  requestsTotal += 1;
  inflightRequests += 1;

  res.on("finish", () => {
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    inflightRequests = Math.max(0, inflightRequests - 1);
    trackLatency(elapsedMs);
  });

  next();
});

app.use(
  express.static(path.join(__dirname, "public"), {
    etag: true,
    lastModified: true,
    maxAge: "1h",
  })
);
app.use(
  "/avatars",
  express.static(avatarStaticDir, {
    etag: true,
    lastModified: true,
    maxAge: "24h",
  })
);

app.get("/", (req, res, next) => {
  const cacheKey = "dashboard";
  const now = Date.now();

  if (useRouteCache) {
    const cached = routeCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      res.set("x-cache", "HIT");
      return res.type("html").send(cached.html);
    }
  }

  try {
    const html = renderDashboardView();

    if (useRouteCache) {
      routeCache.set(cacheKey, {
        html,
        expiresAt: now + MICROCACHE_TTL_MS,
      });
    }

    res.set("x-cache", "MISS");
    return res.type("html").send(html);
  } catch (error) {
    return next(error);
  }
});

app.get("/healthz", (req, res) => {
  res.status(200).json({
    ok: true,
    timestamp: new Date().toISOString(),
    storage: repository.type(),
    queue: {
      ingestion: {
        pollIntervalMs: JOB_POLL_INTERVAL_MS,
        batchSize: JOB_BATCH_SIZE,
      },
      workflow: {
        pollIntervalMs: WORKFLOW_POLL_INTERVAL_MS,
        batchSize: WORKFLOW_BATCH_SIZE,
      },
    },
  });
});

app.get("/ops/metrics", (req, res) => {
  const memory = process.memoryUsage();
  const eventLoopUtilization = performance.eventLoopUtilization();

  const payload = {
    timestamp: new Date().toISOString(),
    process: {
      pid: process.pid,
      uptime_s: round(process.uptime(), 2),
      rss_mb: round(memory.rss / MB, 2),
      heap_used_mb: round(memory.heapUsed / MB, 2),
      heap_total_mb: round(memory.heapTotal / MB, 2),
      external_mb: round(memory.external / MB, 2),
      array_buffers_mb: round(memory.arrayBuffers / MB, 2),
    },
    requests: {
      total: requestsTotal,
      inflight: inflightRequests,
      microcache_ttl_ms: MICROCACHE_TTL_MS,
      latency: getLatencySnapshot(),
    },
    event_loop: {
      min_ms: round(eventLoopDelay.min / 1e6),
      mean_ms: round(eventLoopDelay.mean / 1e6),
      p99_ms: round(eventLoopDelay.percentile(99) / 1e6),
      max_ms: round(eventLoopDelay.max / 1e6),
      stddev_ms: round(eventLoopDelay.stddev / 1e6),
      utilization: round(eventLoopUtilization.utilization, 6),
    },
  };

  eventLoopDelay.reset();
  res.json(payload);
});

app.get("/api/system/storage", (req, res) => {
  res.json({
    storage: repository.type(),
    databaseUrlConfigured: Boolean(process.env.DATABASE_URL),
  });
});

app.post("/api/auth/login", async (req, res) => {
  if (AUTH_DISABLED) {
    return res.json({
      user: getAuthBypassUser(),
      authDisabled: true,
    });
  }

  const email = normalizeEmail(req.body?.email);
  const password = String(req.body?.password || "");

  if (!email || !password) {
    return res.status(400).json({
      error: "invalid_credentials",
      message: "email and password are required",
    });
  }

  const rateKey = getLoginRateKey(req, email);
  if (isLoginBlocked(rateKey)) {
    const attemptState = getLoginAttemptState(rateKey);
    const retryAfterSec = getLoginRetryAfterSec(rateKey);
    return res.status(429).json({
      error: "too_many_attempts",
      message: "too many failed login attempts, try again later",
      attemptsRemaining: 0,
      maxAttempts: LOGIN_MAX_ATTEMPTS,
      retryAfterSec,
      blockedUntil: attemptState
        ? new Date(attemptState.windowStart + LOGIN_WINDOW_MS).toISOString()
        : null,
    });
  }

  try {
    const user = await repository.findUserByEmail(email);
    const isValidPassword =
      user && user.passwordHash
        ? await bcrypt.compare(password, user.passwordHash)
        : false;

    if (!user || !isValidPassword || user.status !== "active") {
      const attemptState = registerFailedLogin(rateKey);
      const attemptsRemaining = Math.max(0, LOGIN_MAX_ATTEMPTS - attemptState.count);
      if (attemptState.count >= LOGIN_MAX_ATTEMPTS) {
        return res.status(429).json({
          error: "too_many_attempts",
          message: "too many failed login attempts, try again later",
          attemptsRemaining: 0,
          maxAttempts: LOGIN_MAX_ATTEMPTS,
          retryAfterSec: getLoginRetryAfterSec(rateKey),
          blockedUntil: new Date(attemptState.windowStart + LOGIN_WINDOW_MS).toISOString(),
        });
      }
      return res.status(401).json({
        error: "invalid_credentials",
        message: "invalid email or password",
        attemptsRemaining,
        maxAttempts: LOGIN_MAX_ATTEMPTS,
      });
    }

    clearFailedLogin(rateKey);
    const sanitizedUser = sanitizeUser(user);
    const accessToken = signAccessToken(sanitizedUser);
    const refreshToken = signRefreshToken(sanitizedUser);
    const refreshTokenHash = hashToken(refreshToken);
    const refreshExpiresAt = new Date(Date.now() + REFRESH_TOKEN_TTL_SEC * 1000).toISOString();

    await repository.storeRefreshToken({
      userId: sanitizedUser.id,
      tokenHash: refreshTokenHash,
      expiresAt: refreshExpiresAt,
      userAgent: req.headers["user-agent"] || null,
      ipAddress: getClientIp(req),
    });

    setAuthCookies(res, accessToken, refreshToken);
    return res.json({
      user: sanitizedUser,
      accessToken,
      accessTokenExpiresInSec: ACCESS_TOKEN_TTL_SEC,
      refreshTokenExpiresInSec: REFRESH_TOKEN_TTL_SEC,
    });
  } catch (error) {
    return res.status(400).json({
      error: "login_failed",
      message: error.message,
    });
  }
});

app.post("/api/auth/refresh", async (req, res) => {
  if (AUTH_DISABLED) {
    return res.json({
      user: getAuthBypassUser(),
      authDisabled: true,
    });
  }

  const refreshToken = extractRefreshToken(req);
  if (!refreshToken) {
    return res.status(401).json({
      error: "unauthorized",
      message: "refresh token is required",
    });
  }

  const refreshTokenHash = hashToken(refreshToken);
  let refreshPayload;
  try {
    refreshPayload = jwt.verify(refreshToken, JWT_REFRESH_SECRET);
    if (refreshPayload?.typ !== "refresh") {
      clearAuthCookies(res);
      return res.status(401).json({
        error: "unauthorized",
        message: "invalid refresh token",
      });
    }
  } catch (error) {
    await repository.revokeRefreshTokenByHash(refreshTokenHash);
    clearAuthCookies(res);
    return res.status(401).json({
      error: "unauthorized",
      message: error.name === "TokenExpiredError" ? "refresh token expired" : "invalid refresh token",
    });
  }

  try {
    const storedToken = await repository.findRefreshTokenByHash(refreshTokenHash);
    if (!storedToken) {
      clearAuthCookies(res);
      return res.status(401).json({
        error: "unauthorized",
        message: "invalid refresh token",
      });
    }

    if (storedToken.revokedAt) {
      clearAuthCookies(res);
      return res.status(401).json({
        error: "unauthorized",
        message: "refresh token revoked",
      });
    }

    if (new Date(storedToken.expiresAt).getTime() <= Date.now()) {
      await repository.revokeRefreshTokenByHash(refreshTokenHash);
      clearAuthCookies(res);
      return res.status(401).json({
        error: "unauthorized",
        message: "refresh token expired",
      });
    }

    if (storedToken.userId !== String(refreshPayload?.sub || "")) {
      await repository.revokeRefreshTokenByHash(refreshTokenHash);
      clearAuthCookies(res);
      return res.status(401).json({
        error: "unauthorized",
        message: "invalid refresh token",
      });
    }

    const user = await repository.getUserById(storedToken.userId);
    if (!user || user.status !== "active") {
      await repository.revokeRefreshTokenByHash(refreshTokenHash);
      clearAuthCookies(res);
      return res.status(401).json({
        error: "unauthorized",
        message: "user is not active",
      });
    }

    await repository.revokeRefreshTokenByHash(refreshTokenHash);

    const sanitizedUser = sanitizeUser(user);
    const nextAccessToken = signAccessToken(sanitizedUser);
    const nextRefreshToken = signRefreshToken(sanitizedUser);
    const nextRefreshTokenHash = hashToken(nextRefreshToken);
    const nextRefreshExpiresAt = new Date(
      Date.now() + REFRESH_TOKEN_TTL_SEC * 1000
    ).toISOString();

    await repository.storeRefreshToken({
      userId: sanitizedUser.id,
      tokenHash: nextRefreshTokenHash,
      expiresAt: nextRefreshExpiresAt,
      userAgent: req.headers["user-agent"] || null,
      ipAddress: getClientIp(req),
    });

    setAuthCookies(res, nextAccessToken, nextRefreshToken);
    return res.json({
      user: sanitizedUser,
      accessToken: nextAccessToken,
      accessTokenExpiresInSec: ACCESS_TOKEN_TTL_SEC,
      refreshTokenExpiresInSec: REFRESH_TOKEN_TTL_SEC,
    });
  } catch (error) {
    return res.status(400).json({
      error: "refresh_failed",
      message: error.message,
    });
  }
});

app.post("/api/auth/logout", async (req, res) => {
  if (AUTH_DISABLED) {
    return res.json({ ok: true, authDisabled: true });
  }

  const refreshToken = extractRefreshToken(req);
  try {
    if (refreshToken) {
      await repository.revokeRefreshTokenByHash(hashToken(refreshToken));
    }
  } finally {
    clearAuthCookies(res);
  }

  return res.json({ ok: true });
});

app.get("/api/auth/me", authenticate, async (req, res) => {
  return res.json({
    user: req.authUser,
  });
});

app.get("/api/auth/users", ...requireAdminRole, async (req, res, next) => {
  try {
    const limit = toPositiveInt(req.query.limit, 100);
    const users = await repository.listUsers(limit);
    return res.json({
      users: users.map((user) => sanitizeUser(user)),
    });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/auth/users", ...requireAdminRole, async (req, res) => {
  const email = normalizeEmail(req.body?.email);
  const password = String(req.body?.password || "");
  const name = String(req.body?.name || "").trim() || email;
  let role = "viewer";
  try {
    role = normalizeRole(req.body?.role, "viewer");
  } catch (error) {
    return res.status(400).json({
      error: "invalid_user",
      message: error.message,
    });
  }

  if (req.authUser.role !== "owner" && role === "owner") {
    return res.status(403).json({
      error: "forbidden",
      message: "only owner can assign owner role",
    });
  }

  const status = String(req.body?.status || "active").trim().toLowerCase();
  if (!["active", "disabled"].includes(status)) {
    return res.status(400).json({
      error: "invalid_user",
      message: "status must be active or disabled",
    });
  }

  if (!email || !password) {
    return res.status(400).json({
      error: "invalid_user",
      message: "email and password are required",
    });
  }

  if (password.length < 8) {
    return res.status(400).json({
      error: "invalid_user",
      message: "password must be at least 8 characters",
    });
  }

  try {
    const existing = await repository.findUserByEmail(email);
    if (existing) {
      return res.status(409).json({
        error: "user_exists",
        message: "email already exists",
      });
    }

    const passwordHash = await bcrypt.hash(password, 12);
    const created = await repository.createUser({
      email,
      passwordHash,
      name,
      role,
      status,
    });
    return res.status(201).json({
      user: sanitizeUser(created),
    });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_user",
      message: error.message,
    });
  }
});

app.patch("/api/auth/users/:userId", ...requireAdminRole, async (req, res) => {
  const targetUserId = String(req.params.userId || "").trim();
  if (!targetUserId) {
    return res.status(400).json({
      error: "invalid_user",
      message: "userId is required",
    });
  }

  try {
    const target = await repository.getUserById(targetUserId);
    if (!target) {
      return res.status(404).json({
        error: "user_not_found",
        message: "user not found",
      });
    }

    if (req.authUser.role !== "owner" && target.role === "owner") {
      return res.status(403).json({
        error: "forbidden",
        message: "only owner can modify owner account",
      });
    }

    let nextRole = target.role;
    if (req.body?.role !== undefined) {
      try {
        nextRole = normalizeRole(req.body?.role, target.role);
      } catch (error) {
        return res.status(400).json({
          error: "invalid_user",
          message: error.message,
        });
      }

      if (req.authUser.role !== "owner" && nextRole === "owner") {
        return res.status(403).json({
          error: "forbidden",
          message: "only owner can assign owner role",
        });
      }
    }

    let nextStatus = target.status;
    if (req.body?.status !== undefined) {
      nextStatus = String(req.body?.status || "").trim().toLowerCase();
      if (!["active", "disabled"].includes(nextStatus)) {
        return res.status(400).json({
          error: "invalid_user",
          message: "status must be active or disabled",
        });
      }
    }

    const updated = await repository.updateUser(targetUserId, {
      role: nextRole,
      status: nextStatus,
    });

    if (!updated) {
      return res.status(404).json({
        error: "user_not_found",
        message: "user not found",
      });
    }

    if (nextStatus !== "active") {
      await repository.revokeRefreshTokensByUser(targetUserId);
    }

    return res.json({
      user: sanitizeUser(updated),
    });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_user",
      message: error.message,
    });
  }
});

app.get("/api/skills", (req, res) => {
  const skills = listAvailableSkills();
  res.json({
    skills,
  });
});

app.get("/api/provider-auth", authenticate, async (req, res, next) => {
  try {
    const providers = await listProviderAuthConnectionsForResponse();
    return res.json({
      providers,
      templates: providerAuthTemplates.map((item) => ({
        provider: item.provider,
        label: item.label,
        description: item.description,
        authMode: item.authMode,
      })),
    });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/provider-auth", ...requireAdminRole, async (req, res) => {
  try {
    const provider = normalizeProviderId(req.body?.provider);
    const template = getProviderTemplate(provider);
    if (!template) {
      return res.status(400).json({
        error: "invalid_provider",
        message: `unsupported provider: ${provider || "unknown"}`,
      });
    }

    const existing = await repository.getProviderAuthConnectionByProvider(provider);
    const displayName =
      String(req.body?.displayName || template.label).trim() || template.label;
    const connection = await repository.upsertProviderAuthConnection({
      provider,
      displayName,
      authMode: template.authMode,
    });

    return res.status(existing ? 200 : 201).json({
      created: !existing,
      provider: toPublicProviderAuthConnection(connection),
    });
  } catch (error) {
    return res.status(400).json({
      error: "provider_create_failed",
      message: error.message,
    });
  }
});

app.post(
  "/api/provider-auth/:connectionId/authenticate",
  ...requireAdminRole,
  async (req, res) => {
    const connectionId = String(req.params?.connectionId || "").trim();
    if (!connectionId) {
      return res.status(400).json({
        error: "invalid_provider",
        message: "connectionId is required",
      });
    }

    const apiKey = String(req.body?.apiKey || "").trim();
    if (!apiKey) {
      return res.status(400).json({
        error: "invalid_provider_auth",
        message: "apiKey is required",
      });
    }

    const connection = await repository.getProviderAuthConnection(connectionId);
    if (!connection) {
      return res.status(404).json({
        error: "provider_not_found",
        message: "provider connection not found",
      });
    }
    const authMode = String(connection.authMode || "").trim().toLowerCase();
    if (authMode !== "api_key") {
      return res.status(400).json({
        error: "invalid_provider_auth_mode",
        message: `${connection.provider} provider requires ${authMode || "oauth"} auth flow`,
      });
    }

    const now = new Date().toISOString();
    try {
      const verified = await verifyProviderApiKey({
        provider: connection.provider,
        apiKey,
      });

      const updated = await repository.updateProviderAuthConnection(connection.id, {
        status: "connected",
        secretEncrypted: encryptProviderSecret(apiKey),
        models: normalizeProviderModels(connection.provider, verified.models),
        meta: verified.meta || {},
        lastCheckedAt: now,
        errorMessage: null,
      });

      return res.json({
        provider: toPublicProviderAuthConnection(updated || connection),
        verifiedModels: Array.isArray(verified.models) ? verified.models.length : 0,
      });
    } catch (error) {
      const message = sanitizeProviderAuthError(error);
      try {
        await repository.updateProviderAuthConnection(connection.id, {
          status: "error",
          errorMessage: message,
          lastCheckedAt: now,
        });
      } catch {
        // ignore secondary update errors
      }
      return res.status(400).json({
        error: "provider_auth_failed",
        message,
      });
    }
  }
);

app.post(
  "/api/provider-auth/:connectionId/oauth/start",
  ...requireAdminRole,
  async (req, res) => {
    const connectionId = String(req.params?.connectionId || "").trim();
    if (!connectionId) {
      return res.status(400).json({
        error: "invalid_provider",
        message: "connectionId is required",
      });
    }

    const connection = await repository.getProviderAuthConnection(connectionId);
    if (!connection) {
      return res.status(404).json({
        error: "provider_not_found",
        message: "provider connection not found",
      });
    }

    const authMode = String(connection.authMode || "").trim().toLowerCase();
    if (authMode !== "oauth") {
      return res.status(400).json({
        error: "invalid_provider_auth_mode",
        message: `${connection.provider} provider requires api key auth flow`,
      });
    }
    if (normalizeProviderId(connection.provider) !== "openai") {
      return res.status(400).json({
        error: "unsupported_oauth_provider",
        message: `unsupported oauth provider: ${connection.provider}`,
      });
    }

    const oauth = createOpenAICodexOAuthChallenge({
      connectionId: connection.id,
      userId: req.authUser?.id || "",
    });
    trackOAuthChallengeState({
      state: oauth.state,
      challengeToken: oauth.challengeToken,
      connectionId: connection.id,
      userId: req.authUser?.id || "",
      expiresAt: oauth.expiresAt,
    });

    return res.json({
      provider: toPublicProviderAuthConnection(connection),
      oauth,
    });
  }
);

app.post(
  "/api/provider-auth/:connectionId/oauth/complete",
  ...requireAdminRole,
  async (req, res) => {
    const connectionId = String(req.params?.connectionId || "").trim();
    if (!connectionId) {
      return res.status(400).json({
        error: "invalid_provider",
        message: "connectionId is required",
      });
    }

    const challengeToken = String(req.body?.challengeToken || "").trim();
    const callbackInput = String(req.body?.callbackInput || "").trim();
    if (!challengeToken || !callbackInput) {
      return res.status(400).json({
        error: "invalid_provider_auth",
        message: "challengeToken and callbackInput are required",
      });
    }
    const callbackState = String(
      parseAuthorizationInput(callbackInput)?.state || ""
    ).trim();
    if (callbackState) {
      consumeOAuthChallengeState(callbackState);
    }

    const connection = await repository.getProviderAuthConnection(connectionId);
    if (!connection) {
      return res.status(404).json({
        error: "provider_not_found",
        message: "provider connection not found",
      });
    }

    const authMode = String(connection.authMode || "").trim().toLowerCase();
    if (authMode !== "oauth") {
      return res.status(400).json({
        error: "invalid_provider_auth_mode",
        message: `${connection.provider} provider requires api key auth flow`,
      });
    }

    const now = new Date().toISOString();
    try {
      const verified = await verifyProviderOAuth({
        provider: connection.provider,
        challengeToken,
        callbackInput,
        expectedConnectionId: connection.id,
        expectedUserId: req.authUser?.id || "",
      });

      const updated = await repository.updateProviderAuthConnection(connection.id, {
        status: "connected",
        secretEncrypted: encryptProviderSecret(JSON.stringify(verified.secret || {})),
        models: normalizeProviderModels(connection.provider, verified.models),
        meta: verified.meta || {},
        lastCheckedAt: now,
        errorMessage: null,
      });

      return res.json({
        provider: toPublicProviderAuthConnection(updated || connection),
        verifiedModels: Array.isArray(verified.models) ? verified.models.length : 0,
      });
    } catch (error) {
      const message = sanitizeProviderAuthError(error);
      try {
        await repository.updateProviderAuthConnection(connection.id, {
          status: "error",
          errorMessage: message,
          lastCheckedAt: now,
        });
      } catch {
        // ignore secondary update errors
      }
      return res.status(400).json({
        error: "provider_auth_failed",
        message,
      });
    }
  }
);

app.post(
  "/api/provider-auth/:connectionId/disconnect",
  ...requireAdminRole,
  async (req, res) => {
    const connectionId = String(req.params?.connectionId || "").trim();
    if (!connectionId) {
      return res.status(400).json({
        error: "invalid_provider",
        message: "connectionId is required",
      });
    }

    const connection = await repository.getProviderAuthConnection(connectionId);
    if (!connection) {
      return res.status(404).json({
        error: "provider_not_found",
        message: "provider connection not found",
      });
    }

    const updated = await repository.updateProviderAuthConnection(connection.id, {
      status: "pending",
      secretEncrypted: null,
      models: [],
      errorMessage: null,
      lastCheckedAt: new Date().toISOString(),
    });

    return res.json({
      provider: toPublicProviderAuthConnection(updated || connection),
    });
  }
);

app.get("/api/models", authenticate, async (req, res, next) => {
  try {
    const providers = await listProviderAuthConnectionsForResponse();
    const models = getModelCatalogFromConnections(providers);
    return res.json({
      models,
      providers,
    });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/avatars/random", async (req, res) => {
  try {
    const { files, source, client } = await loadAvatarCatalog();
    if (files.length === 0) {
      const avatar = createGeneratedAvatar();
      return res.json({
        avatar,
        total: 0,
        fallback: true,
        source: "generated",
      });
    }

    const index = Math.floor(Math.random() * files.length);
    const relativePath = files[index];
    let url = "";

    if (source === "supabase") {
      url = await resolveSupabaseAvatarUrl(client, relativePath);
    } else {
      const encodedPath = encodePathSegments(relativePath);
      url = `/avatars/${encodedPath}`;
    }

    if (!url) {
      const avatar = createGeneratedAvatar();
      return res.json({
        avatar,
        total: files.length,
        fallback: true,
        source: "generated",
      });
    }

    return res.json({
      avatar: {
        path: relativePath,
        name: path.posix.basename(relativePath),
        url,
      },
      total: files.length,
      fallback: false,
      source,
    });
  } catch (error) {
    console.error("[avatar] random avatar failed:", error.message);
    const avatar = createGeneratedAvatar();
    return res.json({
      avatar,
      total: 0,
      fallback: true,
      source: "generated",
    });
  }
});

app.get("/api/ontology/fields", async (req, res, next) => {
  try {
    res.json({
      fields: ontologyService.listFields(),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ontology/fields", ...requireOperatorRole, async (req, res) => {
  try {
    const fields = Array.isArray(req.body?.fields) ? req.body.fields : [req.body];
    const registered = ontologyService.registerFields(fields);
    await repository.upsertOntologyFields(registered);
    return res.status(201).json({ registered });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_ontology_field",
      message: error.message,
    });
  }
});

app.get("/api/ontology/overrides", async (req, res, next) => {
  try {
    const companyName = String(req.query.companyName || "").trim().toLowerCase();
    const overrides = ontologyService
      .listOverrides()
      .filter((item) => !companyName || item.companyScope === companyName || item.companyScope === "*");

    return res.json({ overrides });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/ontology/overrides", ...requireOperatorRole, async (req, res) => {
  try {
    const sourceColumn = String(req.body?.sourceColumn || "").trim();
    const canonicalField = String(req.body?.canonicalField || "").trim();

    if (!sourceColumn || !canonicalField) {
      return res.status(400).json({
        error: "invalid_override",
        message: "sourceColumn and canonicalField are required",
      });
    }

    if (!ontologyService.hasField(canonicalField)) {
      return res.status(400).json({
        error: "invalid_override",
        message: `canonical field not found: ${canonicalField}`,
      });
    }

    const saved = await repository.upsertColumnOverride({
      companyName: req.body?.companyName,
      sourceColumn,
      canonicalField,
    });

    const overrides = await repository.listColumnOverrides();
    ontologyService.loadOverrides(overrides);

    return res.status(201).json({
      override: saved,
    });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_override",
      message: error.message,
    });
  }
});

app.get("/api/agents", async (req, res, next) => {
  try {
    const limit = toPositiveInt(req.query.limit, 50);
    const agents = await repository.listAgents(limit);
    return res.json({ agents });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/agents/:agentId", async (req, res, next) => {
  try {
    const agent = await repository.getAgent(req.params.agentId);
    if (!agent) {
      return res.status(404).json({
        error: "agent_not_found",
        message: `agent not found: ${req.params.agentId}`,
      });
    }
    return res.json({ agent });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/agents", ...requireOperatorRole, async (req, res) => {
  try {
    const name = String(req.body?.name || "").trim();
    const systemPrompt = String(req.body?.systemPrompt || "").trim();
    const modelTier = String(req.body?.modelTier || "Balanced (default)").trim();

    if (!name) {
      return res.status(400).json({
        error: "invalid_agent",
        message: "name is required",
      });
    }

    if (!systemPrompt) {
      return res.status(400).json({
        error: "invalid_agent",
        message: "systemPrompt is required",
      });
    }

    const toolsFromArray = normalizeToolSelection(req.body?.tools);
    const toolsFromString = parseCommaSeparated(req.body?.toolsCsv || req.body?.toolsText);
    const tools = [...new Set([...toolsFromArray, ...toolsFromString])];
    const skillsFromArray = normalizeToolSelection(req.body?.skills);
    const skillsFromString = parseCommaSeparated(req.body?.skillsCsv || req.body?.skillsText);
    const skills = [...new Set([...skillsFromArray, ...skillsFromString])];
    const avatarUrl = String(req.body?.avatarUrl || "").trim() || null;

    const agent = await repository.createAgent({
      name,
      modelTier,
      systemPrompt,
      tools,
      skills,
      avatarUrl,
    });

    return res.status(201).json({ agent });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_agent",
      message: error.message,
    });
  }
});

app.get("/api/deployments", async (req, res, next) => {
  try {
    const limit = toPositiveInt(req.query.limit, 100);
    const [deployments, agents] = await Promise.all([
      repository.listDeployments(limit),
      repository.listAgents(500),
    ]);

    const agentNameById = new Map(agents.map((agent) => [agent.id, agent.name]));
    const enriched = deployments.map((deployment) => ({
      ...deployment,
      agentName: agentNameById.get(deployment.agentId) || null,
    }));

    return res.json({ deployments: enriched });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/deployments", ...requireOperatorRole, async (req, res) => {
  try {
    const agentId = String(req.body?.agentId || "").trim();
    if (!agentId) {
      return res.status(400).json({
        error: "invalid_deployment",
        message: "agentId is required",
      });
    }

    const agent = await repository.getAgent(agentId);
    if (!agent) {
      return res.status(404).json({
        error: "agent_not_found",
        message: `agent not found: ${agentId}`,
      });
    }

    let policy = req.body?.policy;
    if (typeof policy === "string") {
      try {
        policy = JSON.parse(policy);
      } catch {
        policy = {};
      }
    }

    const deployment = await repository.createDeployment({
      agentId,
      queueName: req.body?.queueName || "default",
      environment: req.body?.environment || "production",
      desiredReplicas: toPositiveInt(req.body?.desiredReplicas, 1),
      policy: policy && typeof policy === "object" ? policy : {},
    });

    return res.status(201).json({
      deployment: {
        ...deployment,
        agentName: agent.name,
      },
    });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_deployment",
      message: error.message,
    });
  }
});

app.patch("/api/deployments/:deploymentId/scale", ...requireOperatorRole, async (req, res) => {
  try {
    const desiredReplicas = toPositiveInt(req.body?.desiredReplicas, 1);
    const deployment = await repository.updateDeploymentScale(
      req.params.deploymentId,
      desiredReplicas
    );

    if (!deployment) {
      return res.status(404).json({
        error: "deployment_not_found",
        message: `deployment not found: ${req.params.deploymentId}`,
      });
    }

    const agent = await repository.getAgent(deployment.agentId);
    return res.json({
      deployment: {
        ...deployment,
        agentName: agent?.name || null,
      },
    });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_scale",
      message: error.message,
    });
  }
});

app.get("/api/workflows", authenticate, async (req, res, next) => {
  try {
    await flushWorkflowSchedulerForServerless();
    const limit = toPositiveInt(req.query.limit, 50);
    const workflows = await repository.listWorkflows(limit);
    const enriched = await Promise.all(
      workflows.map(async (workflow) => {
        const tasks = await repository.listWorkflowTasks(workflow.id);
        return {
          ...workflow,
          taskCounts: summarizeWorkflowTaskCounts(tasks),
        };
      })
    );
    return res.json({ workflows: enriched });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/workflows/:workflowId", authenticate, async (req, res, next) => {
  try {
    await flushWorkflowSchedulerForServerless();
    const workflowId = String(req.params.workflowId || "").trim();
    const workflow = await repository.getWorkflow(workflowId);
    if (!workflow) {
      return res.status(404).json({
        error: "workflow_not_found",
        message: `workflow not found: ${workflowId}`,
      });
    }

    const eventLimit = toPositiveInt(req.query.eventLimit, 200);
    const loadNodeSessions =
      typeof repository.listWorkflowNodeSessions === "function"
        ? repository.listWorkflowNodeSessions(workflowId)
        : Promise.resolve([]);
    const [tasks, events, nodeSessions] = await Promise.all([
      repository.listWorkflowTasks(workflowId),
      repository.listWorkflowEvents(workflowId, eventLimit),
      loadNodeSessions,
    ]);

    return res.json({
      workflow: toWorkflowApiResponse(workflow, {
        tasks,
        events,
        nodeSessions,
      }),
    });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/workflows/:workflowId/tasks", authenticate, async (req, res, next) => {
  try {
    await flushWorkflowSchedulerForServerless();
    const workflowId = String(req.params.workflowId || "").trim();
    const workflow = await repository.getWorkflow(workflowId);
    if (!workflow) {
      return res.status(404).json({
        error: "workflow_not_found",
        message: `workflow not found: ${workflowId}`,
      });
    }

    const tasks = await repository.listWorkflowTasks(workflowId);
    return res.json({
      tasks,
      taskCounts: summarizeWorkflowTaskCounts(tasks),
    });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/workflows/:workflowId/events", authenticate, async (req, res, next) => {
  try {
    await flushWorkflowSchedulerForServerless();
    const workflowId = String(req.params.workflowId || "").trim();
    const workflow = await repository.getWorkflow(workflowId);
    if (!workflow) {
      return res.status(404).json({
        error: "workflow_not_found",
        message: `workflow not found: ${workflowId}`,
      });
    }

    const limit = toPositiveInt(req.query.limit, 200);
    const events = await repository.listWorkflowEvents(workflowId, limit);
    return res.json({ events });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/workflows/:workflowId/sessions", authenticate, async (req, res, next) => {
  try {
    await flushWorkflowSchedulerForServerless();
    const workflowId = String(req.params.workflowId || "").trim();
    const workflow = await repository.getWorkflow(workflowId);
    if (!workflow) {
      return res.status(404).json({
        error: "workflow_not_found",
        message: `workflow not found: ${workflowId}`,
      });
    }
    const nodeSessions =
      typeof repository.listWorkflowNodeSessions === "function"
        ? await repository.listWorkflowNodeSessions(workflowId)
        : [];
    return res.json({ nodeSessions });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/workflows", ...requireOperatorRole, async (req, res) => {
  try {
    const goal = String(req.body?.goal || "").trim();
    if (!goal) {
      return res.status(400).json({
        error: "invalid_workflow",
        message: "goal is required",
      });
    }

    const datasetId = String(req.body?.datasetId || "").trim() || null;
    if (datasetId) {
      const dataset = await repository.getDataset(datasetId);
      if (!dataset) {
        return res.status(404).json({
          error: "dataset_not_found",
          message: `dataset not found: ${datasetId}`,
        });
      }
    }

    const selectedFeatures = normalizeStringArray(
      req.body?.selectedFeatures || req.body?.features
    );
    const meta =
      req.body?.meta && typeof req.body.meta === "object" ? req.body.meta : {};

    const agents = await repository.listAgents(500);
    const tasks = buildWorkflowTasksFromRequest({
      tasksInput: req.body?.tasks,
      nodesInput: req.body?.nodes,
      edgesInput: req.body?.edges,
      agents,
      goal,
    });

    if (!Array.isArray(tasks) || tasks.length === 0) {
      return res.status(400).json({
        error: "invalid_workflow",
        message: "workflow requires at least one task",
      });
    }

    const workflow = await repository.createWorkflow({
      goal,
      datasetId,
      selectedFeatures,
      createdBy: AUTH_DISABLED ? null : req.authUser?.id || null,
      tasks,
      meta,
    });

    await repository.appendWorkflowEvent({
      workflowId: workflow.id,
      role: "user",
      message: goal,
      meta: {
        source: "workflow_goal",
      },
    });
    await repository.appendWorkflowEvent({
      workflowId: workflow.id,
      role: "system",
      message: `workflow created with ${tasks.length} task(s)`,
      meta: {
        taskCount: tasks.length,
        hasGraph: Array.isArray(req.body?.nodes) && req.body.nodes.length > 0,
      },
    });

    if (isServerlessRuntime) {
      await workflowScheduler.processTick();
    }

    const loadNodeSessions =
      typeof repository.listWorkflowNodeSessions === "function"
        ? repository.listWorkflowNodeSessions(workflow.id)
        : Promise.resolve([]);
    const [storedWorkflow, storedTasks, storedEvents, nodeSessions] = await Promise.all([
      repository.getWorkflow(workflow.id),
      repository.listWorkflowTasks(workflow.id),
      repository.listWorkflowEvents(workflow.id, 200),
      loadNodeSessions,
    ]);

    return res.status(201).json({
      workflow: toWorkflowApiResponse(storedWorkflow || workflow, {
        tasks: storedTasks,
        events: storedEvents,
        nodeSessions,
      }),
    });
  } catch (error) {
    return res.status(400).json({
      error: "invalid_workflow",
      message: error.message,
    });
  }
});

app.post("/api/workflows/:workflowId/tick", ...requireOperatorRole, async (req, res, next) => {
  try {
    const workflowId = String(req.params.workflowId || "").trim();
    const workflow = await repository.getWorkflow(workflowId);
    if (!workflow) {
      return res.status(404).json({
        error: "workflow_not_found",
        message: `workflow not found: ${workflowId}`,
      });
    }

    await workflowScheduler.processTick();
    await repository.reconcileWorkflowStatus(workflowId);

    const loadNodeSessions =
      typeof repository.listWorkflowNodeSessions === "function"
        ? repository.listWorkflowNodeSessions(workflowId)
        : Promise.resolve([]);
    const [updatedWorkflow, tasks, events, nodeSessions] = await Promise.all([
      repository.getWorkflow(workflowId),
      repository.listWorkflowTasks(workflowId),
      repository.listWorkflowEvents(workflowId, 200),
      loadNodeSessions,
    ]);

    return res.json({
      workflow: toWorkflowApiResponse(updatedWorkflow || workflow, {
        tasks,
        events,
        nodeSessions,
      }),
    });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/data/upload", ...requireOperatorRole, upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      error: "file_required",
      message: "Attach an Excel/CSV file in form field 'file'.",
    });
  }

  let parsedWorkbook;
  try {
    parsedWorkbook = parseWorkbookRows(req.file.buffer);
  } catch (error) {
    return res.status(400).json({
      error: "invalid_file",
      message: error.message,
    });
  }

  if (parsedWorkbook.rows.length === 0) {
    return res.status(400).json({
      error: "empty_table",
      message: "Uploaded sheet has no data rows.",
    });
  }

  try {
    const job = await jobQueue.enqueueIngestionJob({
      companyName: req.body.companyName || req.body.company || "Unknown Company",
      sourceName: req.file.originalname,
      rows: parsedWorkbook.rows,
      metadata: {
        fileName: req.file.originalname,
        mimeType: req.file.mimetype,
        sheetName: parsedWorkbook.sheetName,
      },
    });

    await flushQueueForServerless();
    const resolvedJob = isServerlessRuntime
      ? await repository.getJob(job.id)
      : job;

    return res.status(202).json({
      job: summarizeJob(resolvedJob || job),
      source: {
        sheetName: parsedWorkbook.sheetName,
        fileName: req.file.originalname,
        mimeType: req.file.mimetype,
      },
    });
  } catch (error) {
    return res.status(400).json({
      error: "ingest_failed",
      message: error.message,
    });
  }
});

app.post("/api/data/table", ...requireOperatorRole, async (req, res) => {
  let table = req.body?.table ?? req.body?.rows;

  if (typeof table === "string") {
    try {
      table = JSON.parse(table);
    } catch (error) {
      return res.status(400).json({
        error: "invalid_json_table",
        message: "table must be valid JSON",
      });
    }
  }

  if (!Array.isArray(table) || table.length === 0) {
    return res.status(400).json({
      error: "table_required",
      message: "table must be a non-empty array of row objects",
    });
  }

  try {
    const job = await jobQueue.enqueueIngestionJob({
      companyName: req.body.companyName || req.body.company || "Unknown Company",
      sourceName: req.body.sourceName || "json-table",
      rows: sanitizeTableRows(table),
      metadata: {
        type: "json-table",
      },
    });

    await flushQueueForServerless();
    const resolvedJob = isServerlessRuntime
      ? await repository.getJob(job.id)
      : job;

    return res.status(202).json({
      job: summarizeJob(resolvedJob || job),
    });
  } catch (error) {
    return res.status(400).json({
      error: "ingest_failed",
      message: error.message,
    });
  }
});

app.get("/api/jobs", async (req, res, next) => {
  try {
    await flushQueueForServerless();
    const limit = toPositiveInt(req.query.limit, 50);
    const jobs = await repository.listJobs(limit);
    return res.json({
      jobs: jobs.map((job) => summarizeJob(job)),
    });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/jobs/:jobId", async (req, res, next) => {
  try {
    await flushQueueForServerless();
    const job = await repository.getJob(req.params.jobId);
    if (!job) {
      return res.status(404).json({
        error: "job_not_found",
        message: `job not found: ${req.params.jobId}`,
      });
    }

    return res.json({
      job: summarizeJob(job),
    });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/data/datasets", async (req, res, next) => {
  try {
    await flushQueueForServerless();
    const datasets = await repository.listDatasets();
    return res.json({ datasets });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/data/datasets/:datasetId", async (req, res, next) => {
  try {
    const previewRows = toPositiveInt(req.query.rows, 20);
    const dataset = await repository.getDataset(req.params.datasetId);

    if (!dataset) {
      return res.status(404).json({
        error: "dataset_not_found",
        message: `dataset not found: ${req.params.datasetId}`,
      });
    }

    return res.json({ dataset: serializeDataset(dataset, previewRows) });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/data/merge", ...requireOperatorRole, async (req, res) => {
  try {
    const requestedIds = req.body?.datasetIds;
    const datasets = await repository.getDatasetsByIds(requestedIds);

    if (Array.isArray(requestedIds) && requestedIds.length > 0) {
      const resolved = new Set(datasets.map((dataset) => dataset.id));
      const missing = requestedIds.filter((id) => !resolved.has(id));
      if (missing.length > 0) {
        return res.status(404).json({
          error: "dataset_not_found",
          message: `dataset not found: ${missing.join(", ")}`,
          missingDatasetIds: missing,
        });
      }
    }

    const result = ontologyService.mergeDatasets({
      datasets,
      limit: toPositiveInt(req.body?.limit, 5000),
    });

    return res.json(result);
  } catch (error) {
    return res.status(400).json({
      error: "merge_failed",
      message: error.message,
    });
  }
});

app.post("/api/data/commands/execute", ...requireOperatorRole, async (req, res) => {
  const command = String(req.body?.command || "").trim();
  const modelRaw = String(req.body?.model || "").trim();
  const sourceName = String(req.body?.sourceName || "").trim() || null;
  const parsedModel = parseModelSelectionValue(modelRaw);

  if (!command) {
    return res.status(400).json({
      error: "invalid_command",
      message: "command is required",
    });
  }

  if (
    !modelRaw ||
    !parsedModel.modelId ||
    !["openai-codex", "openai"].includes(parsedModel.provider)
  ) {
    return res.status(400).json({
      error: "invalid_model",
      message: "OpenAI OAuth 모델(openai-codex/... 또는 openai/...)을 선택하세요.",
    });
  }

  let table = req.body?.table ?? req.body?.rows;
  if (typeof table === "string") {
    try {
      table = JSON.parse(table);
    } catch {
      return res.status(400).json({
        error: "invalid_table",
        message: "table must be valid JSON",
      });
    }
  }

  if (!Array.isArray(table) || table.length === 0) {
    return res.status(400).json({
      error: "invalid_table",
      message: "table must be a non-empty array",
    });
  }

  const sanitized = sanitizeExecutionRows(table, {
    maxRows: DATA_AI_MAX_ROWS,
    maxColumns: DATA_AI_MAX_COLUMNS,
    maxCellLength: DATA_AI_CELL_MAX_LENGTH,
  });

  if (sanitized.totalInputRows > DATA_AI_MAX_ROWS || sanitized.truncated) {
    return res.status(413).json({
      error: "rows_limit_exceeded",
      message: `table rows exceed limit (${DATA_AI_MAX_ROWS})`,
      limit: DATA_AI_MAX_ROWS,
      totalRows: sanitized.totalInputRows,
    });
  }

  if (!Array.isArray(sanitized.rows) || sanitized.rows.length === 0) {
    return res.status(400).json({
      error: "invalid_table",
      message: "table rows must include object columns",
    });
  }

  const connection = await repository.getProviderAuthConnectionByProvider("openai");
  if (!connection || connection.status !== "connected") {
    return res.status(400).json({
      error: "provider_not_connected",
      message: "OPEN AI provider OAuth 인증이 필요합니다.",
    });
  }
  if (String(connection.authMode || "").trim().toLowerCase() !== "oauth") {
    return res.status(400).json({
      error: "provider_auth_invalid",
      message: "OPEN AI provider must use oauth auth mode",
    });
  }

  const useCodexTransport = parsedModel.provider === "openai-codex";
  let accessToken = "";
  let openAIOAuthAccountId = "";
  try {
    const tokenResult = await ensureOpenAIOAuthAccessToken(connection);
    accessToken = tokenResult.accessToken;
    openAIOAuthAccountId = String(
      tokenResult?.credential?.accountId ||
        getOpenAICodexAccountId(tokenResult.accessToken) ||
        ""
    ).trim();
  } catch (error) {
    const message = sanitizeProviderAuthError(error);
    try {
      await repository.updateProviderAuthConnection(connection.id, {
        status: "error",
        errorMessage: message,
        lastCheckedAt: new Date().toISOString(),
      });
    } catch {
      // ignore secondary update error
    }
    return res.status(400).json({
      error: "provider_auth_failed",
      message,
    });
  }

  const action = deriveActionFromCommand(command);
  const summary = buildModelInputSummary(sanitized.rows, {
    sampleRows: DATA_AI_SAMPLE_ROWS,
    maxColumns: Math.min(DATA_AI_MAX_COLUMNS, 48),
    maxRowsInPrompt: 40,
    maxCellLength: 140,
  });

  const codexDiscoveredModelIds = normalizeProviderModels("openai", connection?.models)
    .filter((entry) => entry.provider === "openai-codex")
    .map((entry) => entry.modelId);

  let availableOpenAIModelIds = [];
  if (!useCodexTransport) {
    try {
      availableOpenAIModelIds = await fetchOpenAIAvailableModelIds(accessToken);
    } catch {
      availableOpenAIModelIds = [];
    }
  }

  const modelResolution = useCodexTransport
    ? {
        modelId:
          buildOpenAICodexFallbackModelIds(
            parsedModel.modelId,
            codexDiscoveredModelIds
          )[0] || parsedModel.modelId,
        changed: false,
      }
    : resolveRequestedOpenAIModel({
        requestedModelId: parsedModel.modelId,
        availableModelIds: availableOpenAIModelIds,
      });

  let modelIdForExecution = String(modelResolution.modelId || parsedModel.modelId).trim();
  const modelUsedProvider = useCodexTransport ? "openai-codex" : "openai";
  let modelAutoResolved =
    modelIdForExecution !== parsedModel.modelId || Boolean(modelResolution.changed);

  let planText = "";
  try {
    planText = await requestDataTransformPlanFromOpenAI({
      provider: modelUsedProvider,
      accessToken,
      accountId: openAIOAuthAccountId,
      modelId: modelIdForExecution,
      command,
      action,
      summary,
    });
  } catch (error) {
    const firstMessage = String(error?.message || "model execution failed")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 240);
    const lower = firstMessage.toLowerCase();
    const isModelAccessError =
      lower.includes("does not exist") ||
      lower.includes("do not have access to it") ||
      (lower.includes("model") && lower.includes("not found"));

    if (isModelAccessError) {
      const retryModel = useCodexTransport
        ? buildOpenAICodexFallbackModelIds(
            modelIdForExecution,
            codexDiscoveredModelIds
          ).find((candidate) => candidate !== modelIdForExecution) || ""
        : pickPreferredOpenAIModel(availableOpenAIModelIds) ||
          (modelIdForExecution === "gpt-4o-mini" ? "gpt-4.1-mini" : "gpt-4o-mini");
      if (retryModel && retryModel !== modelIdForExecution) {
        try {
          const retryPlan = await requestDataTransformPlanFromOpenAI({
            provider: modelUsedProvider,
            accessToken,
            accountId: openAIOAuthAccountId,
            modelId: retryModel,
            command,
            action,
            summary,
          });
          planText = retryPlan;
          modelIdForExecution = retryModel;
          modelAutoResolved = true;
        } catch (retryError) {
          const retryMessage = String(retryError?.message || firstMessage)
            .replace(/\s+/g, " ")
            .trim()
            .slice(0, 240);
          return res.status(400).json({
            error: "model_execution_failed",
            message: retryMessage,
          });
        }
      } else {
        return res.status(400).json({
          error: "model_execution_failed",
          message: firstMessage,
        });
      }
    }

    if (!planText) {
      const message = firstMessage;

      if (/unauthorized|invalid token|expired|refresh/i.test(message)) {
        try {
          await repository.updateProviderAuthConnection(connection.id, {
            status: "error",
            errorMessage: message,
            lastCheckedAt: new Date().toISOString(),
          });
        } catch {
          // ignore secondary update error
        }
      }

      return res.status(400).json({
        error: "model_execution_failed",
        message,
      });
    }
  }
  const modelUsedValue = `${modelUsedProvider}/${modelIdForExecution}`;

  let plan = parseModelTransformPlan(planText, action);
  if (
    (!Array.isArray(plan.operations) || plan.operations.length === 0) &&
    plan.action !== "analyze"
  ) {
    const inferredOperations = inferFallbackOperationsFromCommand(
      command,
      plan.action,
      sanitized.columns
    );
    if (inferredOperations.length > 0) {
      plan = {
        ...plan,
        action: "clean",
        operations: inferredOperations,
        report: String(plan.report || "").trim()
          ? `${String(plan.report || "").trim()}\n요청 문장에서 컬럼명 변경 의도를 감지해 변경 작업을 제안했습니다.`
          : "요청 문장에서 컬럼명 변경 의도를 감지해 변경 작업을 제안했습니다.",
      };
    }
  }
  const shouldTransform =
    plan.action !== "analyze" || (Array.isArray(plan.operations) && plan.operations.length > 0);

  let finalRows = sanitized.rows;
  let stats = {
    inputRows: sanitized.rows.length,
    outputRows: sanitized.rows.length,
    modifiedCells: 0,
    droppedRows: 0,
    removedColumns: 0,
    normalizedColumns: collectColumns(sanitized.rows).length,
    operationsApplied: Array.isArray(plan.operations) ? plan.operations.length : 0,
  };
  let transformEngine = "javascript";
  let transformWarning = "";
  let transformDiagnostics = {};

  if (shouldTransform) {
    if (DATA_AI_PYTHON_TOOL_ENABLED) {
      try {
        const pythonTransformed = await runPythonDataTransform({
          rows: sanitized.rows,
          operations: Array.isArray(plan.operations) ? plan.operations : [],
          pythonBin: DATA_AI_PYTHON_BIN,
          timeoutMs: DATA_AI_PYTHON_TIMEOUT_MS,
          scriptPath: DATA_AI_PYTHON_SCRIPT,
        });
        finalRows = Array.isArray(pythonTransformed.rows)
          ? pythonTransformed.rows
          : [];
        stats = pythonTransformed.stats || stats;
        transformEngine = "python";
        transformDiagnostics =
          pythonTransformed.diagnostics &&
          typeof pythonTransformed.diagnostics === "object"
            ? pythonTransformed.diagnostics
            : {};
      } catch (error) {
        transformWarning = String(
          error?.message || "python transform failed, javascript fallback used"
        )
          .replace(/\s+/g, " ")
          .trim()
          .slice(0, 240);
        const transformed = applyModelTransformPlan(sanitized.rows, plan);
        finalRows = transformed.rows;
        stats = transformed.stats;
        transformEngine = "javascript_fallback";
      }
    } else {
      const transformed = applyModelTransformPlan(sanitized.rows, plan);
      finalRows = transformed.rows;
      stats = transformed.stats;
      transformEngine = "javascript";
    }
  }

  stats = {
    ...stats,
    transformEngine,
    transformWarning: transformWarning || null,
    transformDurationMs:
      Number(transformDiagnostics?.durationMs) > 0
        ? Number(transformDiagnostics.durationMs)
        : null,
  };

  const diff = buildRowsDiffPreview(sanitized.rows, finalRows, {
    maxCells: 120,
    maxRows: 80,
    maxValueLength: 120,
  });
  const hasMutations =
    Number(stats.modifiedCells || 0) > 0 ||
    Number(stats.droppedRows || 0) > 0 ||
    Number(stats.removedColumns || 0) > 0 ||
    Number(stats.inputRows || 0) !== Number(stats.outputRows || 0);
  const requiresConfirmation =
    hasMutations && Array.isArray(plan.operations) && plan.operations.length > 0;
  const confirmationPrompt = requiresConfirmation
    ? `데이터 변경 제안 ${plan.operations.length}개가 생성되었습니다. diff를 확인한 뒤 적용 여부를 선택하세요.`
    : "";

  const preview = buildTablePreview(finalRows, {
    maxRows: 20,
    maxColumns: 16,
  });
  const modelReport = String(plan.report || "").trim();
  const finalReport =
    modelReport ||
    buildFallbackDataReport({
      command,
      action: plan.action,
      rows: finalRows,
      stats,
      operations: plan.operations,
    });
  const reportSource = modelReport ? "model" : "local_fallback";

  const run = await repository.createDataAiRun({
    userId: req.authUser?.id || "",
    model: modelUsedValue,
    command,
    sourceName,
    action: plan.action,
    inputRowCount: stats.inputRows,
    outputRowCount: stats.outputRows,
    stats,
    report: finalReport,
    preview: {
      columns: preview.previewColumns,
      rows: preview.previewRows,
      totalRows: preview.totalRows,
      totalColumns: preview.totalColumns,
    },
    cleanedRows: finalRows,
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + DATA_AI_RUN_TTL_SEC * 1000).toISOString(),
  });

  return res.json({
    runId: run.id,
    action: run.action,
    modelRequested: parsedModel.value,
    modelUsed: modelUsedValue,
    modelAutoResolved,
    transformEngine,
    transformWarning,
    requiresConfirmation,
    confirmationPrompt,
    report: run.report,
    reportSource,
    operations: Array.isArray(plan.operations) ? plan.operations : [],
    stats: run.stats,
    diff,
    features: preview.allColumns,
    preview: run.preview,
    download: {
      csvUrl: `/api/data/runs/${encodeURIComponent(run.id)}/export.csv`,
      expiresAt: run.expiresAt,
    },
  });
});

app.get("/api/data/runs/:runId", authenticate, async (req, res, next) => {
  try {
    const run = await repository.getDataAiRun(req.params.runId);
    if (!run) {
      return res.status(404).json({
        error: "run_not_found",
        message: `run not found: ${req.params.runId}`,
      });
    }

    if (!canAccessDataRun(run, req.authUser)) {
      return res.status(403).json({
        error: "forbidden",
        message: "run access denied",
      });
    }

    const includeRowsRaw = String(req.query?.includeRows || "")
      .trim()
      .toLowerCase();
    const includeRows =
      includeRowsRaw === "1" ||
      includeRowsRaw === "true" ||
      includeRowsRaw === "yes";

    return res.json({
      run: toDataRunResponse(run, { includeRows }),
      download: {
        csvUrl: `/api/data/runs/${encodeURIComponent(run.id)}/export.csv`,
        expiresAt: run.expiresAt,
      },
    });
  } catch (error) {
    return next(error);
  }
});

app.get("/api/data/runs/:runId/export.csv", authenticate, async (req, res, next) => {
  try {
    const run = await repository.getDataAiRun(req.params.runId);
    if (!run) {
      return res.status(404).json({
        error: "run_not_found",
        message: `run not found: ${req.params.runId}`,
      });
    }

    if (!canAccessDataRun(run, req.authUser)) {
      return res.status(403).json({
        error: "forbidden",
        message: "run access denied",
      });
    }

    const cleanedRows = Array.isArray(run.cleanedRows) ? run.cleanedRows : [];
    const csv = rowsToCsv(cleanedRows);
    const sourcePart = String(run.sourceName || "")
      .replace(/\\.[a-z0-9]+$/i, "")
      .replace(/[^a-z0-9._-]+/gi, "_")
      .replace(/^_+|_+$/g, "");
    const filename = `${sourcePart || `run_${run.id.slice(0, 8)}`}_cleaned.csv`;

    res.setHeader("content-type", "text/csv; charset=utf-8");
    res.setHeader("content-disposition", `attachment; filename=\"${filename}\"`);
    return res.send(`\\uFEFF${csv}`);
  } catch (error) {
    return next(error);
  }
});

app.use((error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    return res.status(400).json({
      error: "upload_error",
      message: error.message,
    });
  }

  console.error("Request processing failed:", error);
  if (res.headersSent) {
    return next(error);
  }
  return res.status(500).json({ error: "internal_server_error" });
});

function ensureQueueStarted() {
  if (queueStarted || isServerlessRuntime) {
    return;
  }
  jobQueue.start();
  queueStarted = true;
}

function ensureWorkflowSchedulerStarted() {
  if (workflowSchedulerStarted || isServerlessRuntime) {
    return;
  }
  workflowScheduler.start();
  workflowSchedulerStarted = true;
}

function registerShutdownHooks() {
  if (shutdownHooksRegistered) {
    return;
  }

  const closeOAuthBridge = (done) => {
    if (!oauthCallbackBridgeServer) {
      done();
      return;
    }

    const bridge = oauthCallbackBridgeServer;
    oauthCallbackBridgeServer = null;
    try {
      bridge.close(() => done());
    } catch {
      done();
    }
  };

  const shutdown = (signal) => {
    console.log(`Received ${signal}. Shutting down...`);
    jobQueue.stop();
    workflowScheduler.stop();
    if (httpServer) {
      httpServer.close(() => {
        closeOAuthBridge(() => process.exit(0));
      });
      return;
    }
    closeOAuthBridge(() => process.exit(0));
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  shutdownHooksRegistered = true;
}

function ensureHttpServerStarted() {
  if (httpServer) {
    return httpServer;
  }

  httpServer = app.listen(port, () => {
    console.log(
      `Dashboard running at http://localhost:${port} (storage=${repository.type()}, ingestQueue=${JOB_POLL_INTERVAL_MS}ms, workflowQueue=${WORKFLOW_POLL_INTERVAL_MS}ms, mode=${isServerlessRuntime ? "serverless" : "node"})`
    );
  });

  return httpServer;
}

async function bootstrap({
  startQueue = !isServerlessRuntime,
  startServer = true,
} = {}) {
  await ensureInitialized();

  if (startQueue) {
    ensureQueueStarted();
    ensureWorkflowSchedulerStarted();
  }

  if (startServer) {
    registerShutdownHooks();
    ensureOAuthCallbackBridgeStarted();
    return ensureHttpServerStarted();
  }

  return null;
}

if (require.main === module) {
  bootstrap().catch((error) => {
    console.error("Bootstrap failed:", error);
    process.exit(1);
  });
}

module.exports = {
  app,
  bootstrap,
  ensureInitialized,
};
