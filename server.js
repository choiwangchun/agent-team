const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const ejs = require("ejs");
const multer = require("multer");
const XLSX = require("xlsx");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { monitorEventLoopDelay, performance } = require("perf_hooks");
const { OntologyService } = require("./src/ontology/service");
const { createRepository } = require("./src/data/repository");
const { IngestionJobQueue } = require("./src/pipeline/job-queue");

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
const MB = 1024 * 1024;

const repository = createRepository();
const ontologyService = new OntologyService();
const jobQueue = new IngestionJobQueue({
  repository,
  ontologyService,
  pollIntervalMs: JOB_POLL_INTERVAL_MS,
  maxJobsPerTick: JOB_BATCH_SIZE,
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
const avatarsDir = path.join(__dirname, "avatars");
const avatarCatalogCache = {
  files: [],
  loadedAt: 0,
};
const AVATAR_CATALOG_TTL_MS = 5 * 60 * 1000;
const skillSourceRoots = [
  path.join(process.env.HOME || "", ".codex/skills"),
  path.join(process.env.HOME || "", ".agents/skills"),
];

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
let httpServer = null;
let shutdownHooksRegistered = false;
const loginAttempts = new Map();

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

function loadAvatarCatalog() {
  const now = Date.now();
  if (
    avatarCatalogCache.files.length > 0 &&
    now - avatarCatalogCache.loadedAt < AVATAR_CATALOG_TTL_MS
  ) {
    return avatarCatalogCache.files;
  }

  const files = listFilesRecursive(avatarsDir, (item) =>
    item.toLowerCase().endsWith(".svg")
  );
  avatarCatalogCache.files = files;
  avatarCatalogCache.loadedAt = now;
  return files;
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
  express.static(avatarsDir, {
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
      pollIntervalMs: JOB_POLL_INTERVAL_MS,
      batchSize: JOB_BATCH_SIZE,
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

app.get("/api/avatars/random", (req, res) => {
  const files = loadAvatarCatalog();
  if (files.length === 0) {
    return res.status(404).json({
      error: "avatar_not_found",
      message: "No SVG avatar files were found in avatars/ directory.",
    });
  }

  const index = Math.floor(Math.random() * files.length);
  const relativePath = files[index];
  const encodedPath = encodePathSegments(relativePath);

  return res.json({
    avatar: {
      path: relativePath,
      name: path.posix.basename(relativePath),
      url: `/avatars/${encodedPath}`,
    },
    total: files.length,
  });
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

function registerShutdownHooks() {
  if (shutdownHooksRegistered) {
    return;
  }

  const shutdown = (signal) => {
    console.log(`Received ${signal}. Shutting down...`);
    jobQueue.stop();
    if (httpServer) {
      httpServer.close(() => {
        process.exit(0);
      });
      return;
    }
    process.exit(0);
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
      `Dashboard running at http://localhost:${port} (storage=${repository.type()}, queue=${JOB_POLL_INTERVAL_MS}ms, mode=${isServerlessRuntime ? "serverless" : "node"})`
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
  }

  if (startServer) {
    registerShutdownHooks();
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
