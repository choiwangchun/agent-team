const fs = require("fs");
const path = require("path");
const express = require("express");
const ejs = require("ejs");
const multer = require("multer");
const XLSX = require("xlsx");
const { monitorEventLoopDelay, performance } = require("perf_hooks");
const { OntologyService } = require("./src/ontology/service");
const { createRepository } = require("./src/data/repository");
const { IngestionJobQueue } = require("./src/pipeline/job-queue");

const app = express();
const port = Number(process.env.PORT) || 3000;
const MICROCACHE_TTL_MS = Math.max(0, Number(process.env.MICROCACHE_TTL_MS || 0));
const LATENCY_SAMPLE_SIZE = Math.max(1, Number(process.env.LATENCY_SAMPLE_SIZE || 2048));
const MAX_UPLOAD_SIZE_MB = Math.max(1, Number(process.env.MAX_UPLOAD_SIZE_MB || 20));
const JOB_POLL_INTERVAL_MS = Math.max(100, Number(process.env.JOB_POLL_INTERVAL_MS || 750));
const JOB_BATCH_SIZE = Math.max(1, Number(process.env.JOB_BATCH_SIZE || 5));
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
const dashboardTemplateSource = fs.readFileSync(dashboardTemplatePath, "utf8");
const renderDashboard = ejs.compile(dashboardTemplateSource, {
  filename: dashboardTemplatePath,
  rmWhitespace: true,
});
const cssPath = path.join(__dirname, "public/css/tailwind.css");
const cssVersion = fs.existsSync(cssPath)
  ? String(Math.floor(fs.statSync(cssPath).mtimeMs))
  : String(Date.now());
const dashboardData = Object.freeze({
  pageTitle: "SaaS Control Center",
  assetVersion: cssVersion,
  user: Object.freeze({
    name: "Choi",
    role: "Founder",
  }),
});
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
const eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
const latencySamples = new Float64Array(LATENCY_SAMPLE_SIZE);
let latencyIndex = 0;
let latencyCount = 0;
let requestsTotal = 0;
let inflightRequests = 0;

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

  if (MICROCACHE_TTL_MS > 0) {
    const cached = routeCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      res.set("x-cache", "HIT");
      return res.type("html").send(cached.html);
    }
  }

  try {
    const html = renderDashboard(dashboardData);

    if (MICROCACHE_TTL_MS > 0) {
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

app.post("/api/ontology/fields", async (req, res) => {
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

app.post("/api/ontology/overrides", async (req, res) => {
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

app.post("/api/agents", async (req, res) => {
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

app.post("/api/deployments", async (req, res) => {
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

app.patch("/api/deployments/:deploymentId/scale", async (req, res) => {
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

app.post("/api/data/upload", upload.single("file"), async (req, res) => {
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

    return res.status(202).json({
      job: summarizeJob(job),
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

app.post("/api/data/table", async (req, res) => {
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

    return res.status(202).json({
      job: summarizeJob(job),
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

app.post("/api/data/merge", async (req, res) => {
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

async function bootstrap() {
  await repository.init();

  const storedFields = await repository.listOntologyFields();
  if (storedFields.length > 0) {
    ontologyService.loadFields(storedFields);
  } else {
    await repository.upsertOntologyFields(ontologyService.listFields());
  }

  const storedOverrides = await repository.listColumnOverrides();
  ontologyService.loadOverrides(storedOverrides);

  jobQueue.start();

  const server = app.listen(port, () => {
    console.log(
      `Dashboard running at http://localhost:${port} (storage=${repository.type()}, queue=${JOB_POLL_INTERVAL_MS}ms)`
    );
  });

  const shutdown = (signal) => {
    console.log(`Received ${signal}. Shutting down...`);
    jobQueue.stop();
    server.close(() => {
      process.exit(0);
    });
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

bootstrap().catch((error) => {
  console.error("Bootstrap failed:", error);
  process.exit(1);
});
