const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function normalizeScope(value) {
  if (value === null || value === undefined) {
    return "*";
  }
  const text = String(value).trim().toLowerCase();
  return text.length === 0 ? "*" : text;
}

function normalizeColumnName(value) {
  return String(value || "")
    .normalize("NFKC")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function toJobResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    jobType: row.job_type ?? row.jobType,
    status: row.status,
    payload: row.payload ?? null,
    result: row.result ?? null,
    error: row.error ?? null,
    attempts: row.attempts ?? 0,
    createdAt: row.created_at ?? row.createdAt,
    startedAt: row.started_at ?? row.startedAt ?? null,
    completedAt: row.completed_at ?? row.completedAt ?? null,
  };
}

function toUserResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    email: row.email,
    passwordHash: row.password_hash ?? row.passwordHash,
    name: row.name,
    role: row.role,
    status: row.status ?? "active",
    createdAt: row.created_at ?? row.createdAt,
    updatedAt: row.updated_at ?? row.updatedAt,
  };
}

function toRefreshTokenResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    userId: row.user_id ?? row.userId,
    tokenHash: row.token_hash ?? row.tokenHash,
    expiresAt: row.expires_at ?? row.expiresAt,
    revokedAt: row.revoked_at ?? row.revokedAt ?? null,
    userAgent: row.user_agent ?? row.userAgent ?? null,
    ipAddress: row.ip_address ?? row.ipAddress ?? null,
    createdAt: row.created_at ?? row.createdAt,
  };
}

function normalizeProvider(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-");
}

function toProviderAuthResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    provider: row.provider,
    displayName: row.display_name ?? row.displayName ?? row.provider,
    authMode: row.auth_mode ?? row.authMode ?? "api_key",
    status: row.status ?? "pending",
    secretEncrypted: row.secret_encrypted ?? row.secretEncrypted ?? null,
    meta: row.meta_json ?? row.meta ?? {},
    models: row.models_json ?? row.models ?? [],
    lastCheckedAt: row.last_checked_at ?? row.lastCheckedAt ?? null,
    errorMessage: row.error_message ?? row.errorMessage ?? null,
    createdAt: row.created_at ?? row.createdAt,
    updatedAt: row.updated_at ?? row.updatedAt,
  };
}

function toDataAiRunResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    userId: row.user_id ?? row.userId,
    model: row.model,
    command: row.command,
    sourceName: row.source_name ?? row.sourceName ?? null,
    action: row.action ?? "analyze",
    inputRowCount: row.input_row_count ?? row.inputRowCount ?? 0,
    outputRowCount: row.output_row_count ?? row.outputRowCount ?? 0,
    stats: row.stats_json ?? row.stats ?? {},
    report: row.report_text ?? row.report ?? "",
    preview: row.preview_json ?? row.preview ?? {},
    cleanedRows: row.cleaned_rows_json ?? row.cleanedRows ?? [],
    createdAt: row.created_at ?? row.createdAt,
    expiresAt: row.expires_at ?? row.expiresAt,
  };
}

function toWorkflowResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    goal: row.goal,
    status: row.status,
    datasetId: row.dataset_id ?? row.datasetId ?? null,
    selectedFeatures:
      row.selected_features_json ?? row.selectedFeatures ?? [],
    createdBy: row.created_by ?? row.createdBy ?? null,
    meta: row.meta_json ?? row.meta ?? {},
    errorMessage: row.error_message ?? row.errorMessage ?? null,
    createdAt: row.created_at ?? row.createdAt,
    updatedAt: row.updated_at ?? row.updatedAt,
    startedAt: row.started_at ?? row.startedAt ?? null,
    completedAt: row.completed_at ?? row.completedAt ?? null,
  };
}

function toWorkflowTaskResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    workflowId: row.workflow_id ?? row.workflowId,
    agentId: row.agent_id ?? row.agentId ?? null,
    taskKey: row.task_key ?? row.taskKey,
    title: row.title,
    kind: row.kind ?? "general",
    status: row.status ?? "pending",
    dependsOn: row.depends_on_json ?? row.dependsOn ?? [],
    input: row.input_json ?? row.input ?? {},
    output: row.output_json ?? row.output ?? null,
    attempts: row.attempts ?? 0,
    errorMessage: row.error_message ?? row.errorMessage ?? null,
    createdAt: row.created_at ?? row.createdAt,
    updatedAt: row.updated_at ?? row.updatedAt,
    startedAt: row.started_at ?? row.startedAt ?? null,
    completedAt: row.completed_at ?? row.completedAt ?? null,
  };
}

function toWorkflowEventResponse(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    workflowId: row.workflow_id ?? row.workflowId,
    taskId: row.task_id ?? row.taskId ?? null,
    role: row.role ?? "system",
    message: row.message ?? "",
    meta: row.meta_json ?? row.meta ?? {},
    createdAt: row.created_at ?? row.createdAt,
  };
}

class InMemoryRepository {
  constructor(options = {}) {
    this.ontologyFields = new Map();
    this.columnOverrides = new Map();
    this.datasets = new Map();
    this.jobs = new Map();
    this.agents = new Map();
    this.deployments = new Map();
    this.users = new Map();
    this.userIdsByEmail = new Map();
    this.refreshTokens = new Map();
    this.providerAuthConnections = new Map();
    this.providerAuthIdsByProvider = new Map();
    this.dataAiRuns = new Map();
    this.workflows = new Map();
    this.workflowTasks = new Map();
    this.workflowEvents = new Map();

    this.persistEnabled = Boolean(options.persistEnabled);
    this.stateFilePath = this.persistEnabled
      ? String(options.stateFilePath || "").trim()
      : "";

    if (this.persistEnabled && this.stateFilePath) {
      this.loadPersistedState();
    }
  }

  async init() {}

  type() {
    return "memory";
  }

  loadPersistedState() {
    if (!this.persistEnabled || !this.stateFilePath) {
      return;
    }

    try {
      if (!fs.existsSync(this.stateFilePath)) {
        return;
      }

      const raw = fs.readFileSync(this.stateFilePath, "utf8");
      if (!raw.trim()) {
        return;
      }

      const parsed = JSON.parse(raw);
      const connections = Array.isArray(parsed?.providerAuthConnections)
        ? parsed.providerAuthConnections
        : [];

      this.providerAuthConnections.clear();
      this.providerAuthIdsByProvider.clear();

      for (const row of connections) {
        const record = toProviderAuthResponse(row);
        if (!record?.id || !record?.provider) {
          continue;
        }
        this.providerAuthConnections.set(record.id, deepClone(record));
        this.providerAuthIdsByProvider.set(record.provider, record.id);
      }
    } catch (error) {
      console.warn(
        `[repository] failed to load memory state (${this.stateFilePath}): ${
          error?.message || String(error)
        }`
      );
    }
  }

  persistState() {
    if (!this.persistEnabled || !this.stateFilePath) {
      return;
    }

    const snapshot = {
      version: 1,
      savedAt: new Date().toISOString(),
      providerAuthConnections: Array.from(this.providerAuthConnections.values()).map((item) =>
        deepClone(item)
      ),
    };

    try {
      const dir = path.dirname(this.stateFilePath);
      fs.mkdirSync(dir, { recursive: true });
      const tmpPath = `${this.stateFilePath}.tmp-${process.pid}-${Date.now()}`;
      fs.writeFileSync(tmpPath, JSON.stringify(snapshot, null, 2), "utf8");
      fs.renameSync(tmpPath, this.stateFilePath);
    } catch (error) {
      console.warn(
        `[repository] failed to persist memory state (${this.stateFilePath}): ${
          error?.message || String(error)
        }`
      );
    }
  }

  async createUser({ email, passwordHash, name, role, status = "active" }) {
    const normalizedEmail = normalizeEmail(email);
    if (!normalizedEmail) {
      throw new Error("email is required");
    }
    if (this.userIdsByEmail.has(normalizedEmail)) {
      throw new Error("email already exists");
    }

    const now = new Date().toISOString();
    const user = {
      id: crypto.randomUUID(),
      email: normalizedEmail,
      passwordHash: String(passwordHash || ""),
      name: String(name || "").trim() || normalizedEmail,
      role: String(role || "viewer").trim() || "viewer",
      status: String(status || "active").trim() || "active",
      createdAt: now,
      updatedAt: now,
    };

    this.users.set(user.id, user);
    this.userIdsByEmail.set(normalizedEmail, user.id);
    return deepClone(user);
  }

  async findUserByEmail(email) {
    const normalizedEmail = normalizeEmail(email);
    const userId = this.userIdsByEmail.get(normalizedEmail);
    if (!userId) {
      return null;
    }
    return this.getUserById(userId);
  }

  async getUserById(userId) {
    const user = this.users.get(String(userId || ""));
    return user ? deepClone(user) : null;
  }

  async listUsers(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const users = Array.from(this.users.values()).sort((a, b) => {
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });
    return users.slice(0, safeLimit).map((user) => deepClone(user));
  }

  async updateUser(userId, { role, status }) {
    const key = String(userId || "");
    const current = this.users.get(key);
    if (!current) {
      return null;
    }

    const next = {
      ...current,
      role: role !== undefined ? String(role || "").trim() || current.role : current.role,
      status: status !== undefined ? String(status || "").trim() || current.status : current.status,
      updatedAt: new Date().toISOString(),
    };

    this.users.set(key, next);
    return deepClone(next);
  }

  async storeRefreshToken({ userId, tokenHash, expiresAt, userAgent = null, ipAddress = null }) {
    const record = {
      id: crypto.randomUUID(),
      userId: String(userId || ""),
      tokenHash: String(tokenHash || ""),
      expiresAt: new Date(expiresAt).toISOString(),
      revokedAt: null,
      userAgent: userAgent ? String(userAgent) : null,
      ipAddress: ipAddress ? String(ipAddress) : null,
      createdAt: new Date().toISOString(),
    };

    this.refreshTokens.set(record.tokenHash, record);
    return deepClone(record);
  }

  async findRefreshTokenByHash(tokenHash) {
    const record = this.refreshTokens.get(String(tokenHash || ""));
    return record ? deepClone(record) : null;
  }

  async revokeRefreshTokenByHash(tokenHash) {
    const key = String(tokenHash || "");
    const record = this.refreshTokens.get(key);
    if (!record) {
      return null;
    }
    record.revokedAt = new Date().toISOString();
    this.refreshTokens.set(key, record);
    return deepClone(record);
  }

  async revokeRefreshTokensByUser(userId) {
    const key = String(userId || "");
    let revokedCount = 0;
    for (const [tokenHash, record] of this.refreshTokens.entries()) {
      if (record.userId !== key || record.revokedAt) {
        continue;
      }
      record.revokedAt = new Date().toISOString();
      this.refreshTokens.set(tokenHash, record);
      revokedCount += 1;
    }
    return { revokedCount };
  }

  async listOntologyFields() {
    return Array.from(this.ontologyFields.values()).map((field) => deepClone(field));
  }

  async upsertOntologyFields(fields) {
    for (const field of fields) {
      const now = new Date().toISOString();
      const previous = this.ontologyFields.get(field.key);
      this.ontologyFields.set(field.key, {
        key: field.key,
        label: field.label,
        aliases: deepClone(field.aliases || []),
        valueAliases: deepClone(field.valueAliases || {}),
        createdAt: previous?.createdAt || now,
        updatedAt: now,
      });
    }
  }

  async listColumnOverrides() {
    return Array.from(this.columnOverrides.values()).map((entry) => deepClone(entry));
  }

  async upsertColumnOverride({ companyName, sourceColumn, canonicalField }) {
    const companyScope = normalizeScope(companyName);
    const sourceColumnNorm = normalizeColumnName(sourceColumn);
    const key = `${companyScope}::${sourceColumnNorm}`;
    const now = new Date().toISOString();
    const previous = this.columnOverrides.get(key);

    const value = {
      id: previous?.id || crypto.randomUUID(),
      companyScope,
      sourceColumn: String(sourceColumn || "").trim(),
      sourceColumnNorm,
      canonicalField,
      createdAt: previous?.createdAt || now,
      updatedAt: now,
    };

    this.columnOverrides.set(key, value);
    return deepClone(value);
  }

  async createDataset(dataset) {
    this.datasets.set(dataset.id, deepClone(dataset));
    return deepClone(dataset);
  }

  async listDatasets() {
    return Array.from(this.datasets.values()).map((dataset) => ({
      id: dataset.id,
      companyName: dataset.companyName,
      sourceName: dataset.sourceName,
      createdAt: dataset.createdAt,
      rowCount: dataset.rowCount,
      mappedColumns: Array.isArray(dataset.columnMapping) ? dataset.columnMapping.length : 0,
    }));
  }

  async getDataset(datasetId) {
    const dataset = this.datasets.get(datasetId);
    return dataset ? deepClone(dataset) : null;
  }

  async getDatasetsByIds(datasetIds) {
    if (!Array.isArray(datasetIds) || datasetIds.length === 0) {
      return Array.from(this.datasets.values()).map((dataset) => deepClone(dataset));
    }

    return datasetIds
      .map((id) => this.datasets.get(id))
      .filter(Boolean)
      .map((dataset) => deepClone(dataset));
  }

  async enqueueJob({ jobType, payload }) {
    const now = new Date().toISOString();
    const id = crypto.randomUUID();
    const job = {
      id,
      jobType,
      status: "pending",
      payload: deepClone(payload),
      result: null,
      error: null,
      attempts: 0,
      createdAt: now,
      startedAt: null,
      completedAt: null,
    };

    this.jobs.set(id, job);
    return deepClone(job);
  }

  async listJobs(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const jobs = Array.from(this.jobs.values()).sort((a, b) => {
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });
    return jobs.slice(0, safeLimit).map((job) => deepClone(job));
  }

  async getJob(jobId) {
    const job = this.jobs.get(jobId);
    return job ? deepClone(job) : null;
  }

  async claimPendingJob() {
    const pending = Array.from(this.jobs.values())
      .filter((job) => job.status === "pending")
      .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())[0];

    if (!pending) {
      return null;
    }

    pending.status = "running";
    pending.startedAt = new Date().toISOString();
    pending.attempts += 1;
    pending.error = null;
    this.jobs.set(pending.id, pending);
    return deepClone(pending);
  }

  async completeJob(jobId, result) {
    const job = this.jobs.get(jobId);
    if (!job) {
      return null;
    }
    job.status = "completed";
    job.result = deepClone(result || {});
    job.error = null;
    job.completedAt = new Date().toISOString();
    this.jobs.set(jobId, job);
    return deepClone(job);
  }

  async failJob(jobId, errorMessage) {
    const job = this.jobs.get(jobId);
    if (!job) {
      return null;
    }
    job.status = "failed";
    job.error = String(errorMessage || "unknown_error");
    job.completedAt = new Date().toISOString();
    this.jobs.set(jobId, job);
    return deepClone(job);
  }

  async upsertProviderAuthConnection({ provider, displayName, authMode = "api_key" }) {
    const normalizedProvider = normalizeProvider(provider);
    if (!normalizedProvider) {
      throw new Error("provider is required");
    }

    const existingId = this.providerAuthIdsByProvider.get(normalizedProvider);
    if (existingId) {
      const existing = this.providerAuthConnections.get(existingId);
      if (!existing) {
        this.providerAuthIdsByProvider.delete(normalizedProvider);
      } else {
        const nextDisplayName = String(displayName || "").trim();
        if (nextDisplayName && nextDisplayName !== existing.displayName) {
          existing.displayName = nextDisplayName;
          existing.updatedAt = new Date().toISOString();
          this.providerAuthConnections.set(existing.id, existing);
          this.persistState();
        }
        return deepClone(existing);
      }
    }

    const now = new Date().toISOString();
    const record = {
      id: crypto.randomUUID(),
      provider: normalizedProvider,
      displayName: String(displayName || normalizedProvider.toUpperCase()).trim() || normalizedProvider.toUpperCase(),
      authMode: String(authMode || "api_key").trim().toLowerCase() || "api_key",
      status: "pending",
      secretEncrypted: null,
      meta: {},
      models: [],
      lastCheckedAt: null,
      errorMessage: null,
      createdAt: now,
      updatedAt: now,
    };

    this.providerAuthConnections.set(record.id, record);
    this.providerAuthIdsByProvider.set(normalizedProvider, record.id);
    this.persistState();
    return deepClone(record);
  }

  async listProviderAuthConnections(limit = 100) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 100));
    const items = Array.from(this.providerAuthConnections.values())
      .sort((a, b) => a.provider.localeCompare(b.provider))
      .slice(0, safeLimit);
    return items.map((item) => deepClone(item));
  }

  async getProviderAuthConnection(connectionId) {
    const record = this.providerAuthConnections.get(String(connectionId || ""));
    return record ? deepClone(record) : null;
  }

  async getProviderAuthConnectionByProvider(provider) {
    const normalizedProvider = normalizeProvider(provider);
    const recordId = this.providerAuthIdsByProvider.get(normalizedProvider);
    if (!recordId) {
      return null;
    }
    return this.getProviderAuthConnection(recordId);
  }

  async updateProviderAuthConnection(connectionId, patch = {}) {
    const key = String(connectionId || "");
    const current = this.providerAuthConnections.get(key);
    if (!current) {
      return null;
    }

    const next = {
      ...current,
      displayName:
        patch.displayName !== undefined
          ? String(patch.displayName || "").trim() || current.displayName
          : current.displayName,
      authMode:
        patch.authMode !== undefined
          ? String(patch.authMode || "").trim().toLowerCase() || current.authMode
          : current.authMode,
      status:
        patch.status !== undefined
          ? String(patch.status || "").trim().toLowerCase() || current.status
          : current.status,
      secretEncrypted:
        patch.secretEncrypted !== undefined
          ? String(patch.secretEncrypted || "").trim() || null
          : current.secretEncrypted,
      meta:
        patch.meta !== undefined
          ? patch.meta && typeof patch.meta === "object"
            ? deepClone(patch.meta)
            : {}
          : current.meta,
      models:
        patch.models !== undefined
          ? Array.isArray(patch.models)
            ? deepClone(patch.models)
            : []
          : current.models,
      lastCheckedAt:
        patch.lastCheckedAt !== undefined
          ? patch.lastCheckedAt
            ? new Date(patch.lastCheckedAt).toISOString()
            : null
          : current.lastCheckedAt,
      errorMessage:
        patch.errorMessage !== undefined
          ? String(patch.errorMessage || "").trim() || null
          : current.errorMessage,
      updatedAt: new Date().toISOString(),
    };

    this.providerAuthConnections.set(key, next);
    this.persistState();
    return deepClone(next);
  }

  async createAgent({ name, modelTier, systemPrompt, tools, skills, avatarUrl }) {
    const now = new Date().toISOString();
    const id = crypto.randomUUID();
    const normalizedTools = Array.isArray(tools)
      ? [...new Set(tools.map((item) => String(item).trim()).filter(Boolean))]
      : [];
    const normalizedSkills = Array.isArray(skills)
      ? [...new Set(skills.map((item) => String(item).trim()).filter(Boolean))]
      : normalizedTools;
    const agent = {
      id,
      name: String(name || "").trim(),
      modelTier: String(modelTier || "Balanced (default)").trim(),
      systemPrompt: String(systemPrompt || "").trim(),
      tools: normalizedTools,
      skills: normalizedSkills,
      avatarUrl: String(avatarUrl || "").trim() || null,
      status: "active",
      createdAt: now,
      updatedAt: now,
    };

    this.agents.set(id, agent);
    return deepClone(agent);
  }

  async listAgents(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const agents = Array.from(this.agents.values()).sort((a, b) => {
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });
    return agents.slice(0, safeLimit).map((agent) => deepClone(agent));
  }

  async getAgent(agentId) {
    const agent = this.agents.get(agentId);
    return agent ? deepClone(agent) : null;
  }

  async createDeployment({ agentId, queueName, environment, desiredReplicas, policy }) {
    const now = new Date().toISOString();
    const id = crypto.randomUUID();
    const deployment = {
      id,
      agentId,
      queueName: String(queueName || "default").trim(),
      environment: String(environment || "production").trim(),
      desiredReplicas: Math.max(1, Number(desiredReplicas) || 1),
      runningReplicas: Math.max(0, Number(desiredReplicas) || 1),
      status: "running",
      policy: policy && typeof policy === "object" ? deepClone(policy) : {},
      createdAt: now,
      updatedAt: now,
    };

    this.deployments.set(id, deployment);
    return deepClone(deployment);
  }

  async listDeployments(limit = 100) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 100));
    const deployments = Array.from(this.deployments.values()).sort((a, b) => {
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });
    return deployments.slice(0, safeLimit).map((deployment) => deepClone(deployment));
  }

  async getDeployment(deploymentId) {
    const deployment = this.deployments.get(deploymentId);
    return deployment ? deepClone(deployment) : null;
  }

  async updateDeploymentScale(deploymentId, desiredReplicas) {
    const deployment = this.deployments.get(deploymentId);
    if (!deployment) {
      return null;
    }

    const nextReplicas = Math.max(1, Number(desiredReplicas) || 1);
    deployment.desiredReplicas = nextReplicas;
    deployment.runningReplicas = nextReplicas;
    deployment.updatedAt = new Date().toISOString();
    this.deployments.set(deploymentId, deployment);
    return deepClone(deployment);
  }

  async purgeExpiredDataAiRuns() {
    const now = Date.now();
    for (const [runId, run] of this.dataAiRuns.entries()) {
      const expiresAtMs = new Date(run.expiresAt).getTime();
      if (Number.isFinite(expiresAtMs) && expiresAtMs <= now) {
        this.dataAiRuns.delete(runId);
      }
    }
  }

  async createDataAiRun({
    userId,
    model,
    command,
    sourceName = null,
    action = "analyze",
    inputRowCount = 0,
    outputRowCount = 0,
    stats = {},
    report = "",
    preview = {},
    cleanedRows = [],
    createdAt = new Date().toISOString(),
    expiresAt,
  }) {
    await this.purgeExpiredDataAiRuns();
    const record = {
      id: crypto.randomUUID(),
      userId: String(userId || "").trim(),
      model: String(model || "").trim(),
      command: String(command || "").trim(),
      sourceName: sourceName ? String(sourceName).trim() : null,
      action: String(action || "analyze").trim() || "analyze",
      inputRowCount: Math.max(0, Number(inputRowCount) || 0),
      outputRowCount: Math.max(0, Number(outputRowCount) || 0),
      stats: stats && typeof stats === "object" ? deepClone(stats) : {},
      report: String(report || "").trim(),
      preview: preview && typeof preview === "object" ? deepClone(preview) : {},
      cleanedRows: Array.isArray(cleanedRows) ? deepClone(cleanedRows) : [],
      createdAt: new Date(createdAt).toISOString(),
      expiresAt: new Date(expiresAt).toISOString(),
    };

    this.dataAiRuns.set(record.id, record);
    return deepClone(record);
  }

  async getDataAiRun(runId) {
    await this.purgeExpiredDataAiRuns();
    const run = this.dataAiRuns.get(String(runId || "").trim());
    return run ? deepClone(run) : null;
  }

  async createWorkflow({
    goal,
    datasetId = null,
    selectedFeatures = [],
    createdBy = null,
    tasks = [],
    meta = {},
  }) {
    const now = new Date().toISOString();
    const workflowId = crypto.randomUUID();
    const workflow = {
      id: workflowId,
      goal: String(goal || "").trim(),
      status: "pending",
      datasetId: datasetId ? String(datasetId).trim() : null,
      selectedFeatures: Array.isArray(selectedFeatures)
        ? [...new Set(selectedFeatures.map((item) => String(item || "").trim()).filter(Boolean))]
        : [],
      createdBy: createdBy ? String(createdBy).trim() : null,
      meta: meta && typeof meta === "object" ? deepClone(meta) : {},
      errorMessage: null,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      completedAt: null,
    };

    const normalizedTasks = Array.isArray(tasks) ? tasks : [];
    const taskIdByKey = new Map();
    for (const item of normalizedTasks) {
      const taskKey = String(item?.taskKey || "").trim();
      if (!taskKey) {
        continue;
      }
      if (taskIdByKey.has(taskKey)) {
        throw new Error(`duplicate workflow task key: ${taskKey}`);
      }
      taskIdByKey.set(taskKey, crypto.randomUUID());
    }

    for (const item of normalizedTasks) {
      const taskKey = String(item?.taskKey || "").trim();
      if (!taskKey || !taskIdByKey.has(taskKey)) {
        continue;
      }

      const dependsOnTaskKeys = Array.isArray(item?.dependsOnTaskKeys)
        ? item.dependsOnTaskKeys
        : [];
      const dependsOnIds = [];
      for (const dependsOnTaskKeyRaw of dependsOnTaskKeys) {
        const dependsOnTaskKey = String(dependsOnTaskKeyRaw || "").trim();
        if (!dependsOnTaskKey) {
          continue;
        }
        const dependencyId = taskIdByKey.get(dependsOnTaskKey);
        if (!dependencyId) {
          throw new Error(
            `workflow task dependency not found: ${dependsOnTaskKey}`
          );
        }
        if (dependencyId !== taskIdByKey.get(taskKey)) {
          dependsOnIds.push(dependencyId);
        }
      }

      const taskRecord = {
        id: taskIdByKey.get(taskKey),
        workflowId,
        agentId: item?.agentId ? String(item.agentId).trim() : null,
        taskKey,
        title: String(item?.title || taskKey).trim() || taskKey,
        kind: String(item?.kind || "general").trim() || "general",
        status: "pending",
        dependsOn: [...new Set(dependsOnIds)],
        input: item?.input && typeof item.input === "object" ? deepClone(item.input) : {},
        output: null,
        attempts: 0,
        errorMessage: null,
        createdAt: now,
        updatedAt: now,
        startedAt: null,
        completedAt: null,
      };

      this.workflowTasks.set(taskRecord.id, taskRecord);
    }

    this.workflows.set(workflowId, workflow);
    return deepClone(workflow);
  }

  async listWorkflows(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const workflows = Array.from(this.workflows.values()).sort((a, b) => {
      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
    });
    return workflows.slice(0, safeLimit).map((workflow) => deepClone(workflow));
  }

  async getWorkflow(workflowId) {
    const workflow = this.workflows.get(String(workflowId || "").trim());
    return workflow ? deepClone(workflow) : null;
  }

  async listWorkflowTasks(workflowId) {
    const key = String(workflowId || "").trim();
    const tasks = Array.from(this.workflowTasks.values())
      .filter((task) => task.workflowId === key)
      .sort((a, b) => {
        return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
      });
    return tasks.map((task) => deepClone(task));
  }

  async getWorkflowTask(taskId) {
    const task = this.workflowTasks.get(String(taskId || "").trim());
    return task ? deepClone(task) : null;
  }

  async appendWorkflowEvent({
    workflowId,
    taskId = null,
    role = "system",
    message,
    meta = {},
  }) {
    const event = {
      id: crypto.randomUUID(),
      workflowId: String(workflowId || "").trim(),
      taskId: taskId ? String(taskId).trim() : null,
      role: String(role || "system").trim() || "system",
      message: String(message || "").trim(),
      meta: meta && typeof meta === "object" ? deepClone(meta) : {},
      createdAt: new Date().toISOString(),
    };
    this.workflowEvents.set(event.id, event);
    return deepClone(event);
  }

  async listWorkflowEvents(workflowId, limit = 200) {
    const key = String(workflowId || "").trim();
    const safeLimit = Math.max(1, Math.min(2000, Number(limit) || 200));
    const events = Array.from(this.workflowEvents.values())
      .filter((event) => event.workflowId === key)
      .sort((a, b) => {
        return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
      });
    const sliced = events.slice(Math.max(0, events.length - safeLimit));
    return sliced.map((event) => deepClone(event));
  }

  async claimPendingWorkflowTask() {
    const pendingTasks = Array.from(this.workflowTasks.values())
      .filter((task) => task.status === "pending")
      .sort((a, b) => {
        return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
      });

    for (const task of pendingTasks) {
      const workflow = this.workflows.get(task.workflowId);
      if (!workflow) {
        continue;
      }
      if (!["pending", "running"].includes(String(workflow.status || "").trim())) {
        continue;
      }

      const isReady = (task.dependsOn || []).every((dependencyId) => {
        const dependency = this.workflowTasks.get(dependencyId);
        return dependency && dependency.status === "completed";
      });
      if (!isReady) {
        continue;
      }

      task.status = "running";
      task.startedAt = task.startedAt || new Date().toISOString();
      task.updatedAt = new Date().toISOString();
      task.attempts = Math.max(0, Number(task.attempts) || 0) + 1;
      task.errorMessage = null;
      this.workflowTasks.set(task.id, task);

      if (workflow.status === "pending") {
        workflow.status = "running";
        workflow.startedAt = workflow.startedAt || new Date().toISOString();
        workflow.updatedAt = new Date().toISOString();
        this.workflows.set(workflow.id, workflow);
      }

      return deepClone(task);
    }

    return null;
  }

  async completeWorkflowTask(taskId, output = {}) {
    const key = String(taskId || "").trim();
    const task = this.workflowTasks.get(key);
    if (!task) {
      return null;
    }

    task.status = "completed";
    task.output = output && typeof output === "object" ? deepClone(output) : {};
    task.errorMessage = null;
    task.completedAt = new Date().toISOString();
    task.updatedAt = task.completedAt;
    this.workflowTasks.set(key, task);
    return deepClone(task);
  }

  async failWorkflowTask(taskId, errorMessage) {
    const key = String(taskId || "").trim();
    const task = this.workflowTasks.get(key);
    if (!task) {
      return null;
    }

    const failureMessage = String(errorMessage || "workflow_task_failed").trim();
    task.status = "failed";
    task.errorMessage = failureMessage;
    task.completedAt = new Date().toISOString();
    task.updatedAt = task.completedAt;
    this.workflowTasks.set(key, task);

    const workflow = this.workflows.get(task.workflowId);
    if (workflow) {
      workflow.errorMessage = failureMessage;
      workflow.updatedAt = new Date().toISOString();
      this.workflows.set(workflow.id, workflow);
    }

    return deepClone(task);
  }

  async reconcileWorkflowStatus(workflowId) {
    const key = String(workflowId || "").trim();
    const workflow = this.workflows.get(key);
    if (!workflow) {
      return null;
    }

    const tasks = Array.from(this.workflowTasks.values()).filter(
      (task) => task.workflowId === key
    );
    const total = tasks.length;
    const failed = tasks.filter((task) => task.status === "failed").length;
    const completed = tasks.filter((task) => task.status === "completed").length;
    const running = tasks.filter((task) => task.status === "running").length;

    let nextStatus = "pending";
    if (failed > 0) {
      nextStatus = "failed";
    } else if (total > 0 && completed === total) {
      nextStatus = "completed";
    } else if (running > 0 || completed > 0) {
      nextStatus = "running";
    }

    workflow.status = nextStatus;
    workflow.updatedAt = new Date().toISOString();
    if (nextStatus === "running") {
      workflow.startedAt = workflow.startedAt || workflow.updatedAt;
      workflow.completedAt = null;
    } else if (nextStatus === "completed" || nextStatus === "failed") {
      workflow.completedAt = workflow.completedAt || workflow.updatedAt;
    } else {
      workflow.completedAt = null;
    }

    this.workflows.set(key, workflow);
    return deepClone(workflow);
  }
}

class PostgresRepository {
  constructor(connectionString) {
    this.pool = new Pool({
      connectionString,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });
  }

  type() {
    return "postgres";
  }

  async init() {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ontology_fields (
        field_key TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        aliases JSONB NOT NULL,
        value_aliases JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS column_overrides (
        id TEXT PRIMARY KEY,
        company_scope TEXT NOT NULL,
        source_column TEXT NOT NULL,
        source_column_norm TEXT NOT NULL,
        canonical_field TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (company_scope, source_column_norm)
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS datasets (
        id TEXT PRIMARY KEY,
        company_name TEXT NOT NULL,
        source_name TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        row_count INTEGER NOT NULL,
        column_mapping JSONB NOT NULL,
        normalized_rows JSONB NOT NULL
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ingestion_jobs (
        id TEXT PRIMARY KEY,
        job_type TEXT NOT NULL,
        status TEXT NOT NULL,
        payload JSONB NOT NULL,
        result JSONB,
        error TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS agents (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        model_tier TEXT NOT NULL,
        system_prompt TEXT NOT NULL,
        tools JSONB NOT NULL DEFAULT '[]'::jsonb,
        skills JSONB NOT NULL DEFAULT '[]'::jsonb,
        avatar_url TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      );
    `);

    await this.pool.query(`
      ALTER TABLE agents
      ADD COLUMN IF NOT EXISTS skills JSONB NOT NULL DEFAULT '[]'::jsonb;
    `);

    await this.pool.query(`
      ALTER TABLE agents
      ADD COLUMN IF NOT EXISTS avatar_url TEXT;
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS agent_deployments (
        id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        queue_name TEXT NOT NULL,
        environment TEXT NOT NULL,
        desired_replicas INTEGER NOT NULL,
        running_replicas INTEGER NOT NULL,
        status TEXT NOT NULL,
        policy JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS app_users (
        id TEXT PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS auth_refresh_tokens (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
        token_hash TEXT NOT NULL UNIQUE,
        expires_at TIMESTAMPTZ NOT NULL,
        revoked_at TIMESTAMPTZ,
        user_agent TEXT,
        ip_address TEXT,
        created_at TIMESTAMPTZ NOT NULL
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS provider_auth_connections (
        id TEXT PRIMARY KEY,
        provider TEXT NOT NULL UNIQUE,
        display_name TEXT NOT NULL,
        auth_mode TEXT NOT NULL DEFAULT 'api_key',
        status TEXT NOT NULL DEFAULT 'pending',
        secret_encrypted TEXT,
        meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        models_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        last_checked_at TIMESTAMPTZ,
        error_message TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS data_ai_runs (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
        model TEXT NOT NULL,
        command TEXT NOT NULL,
        source_name TEXT,
        action TEXT NOT NULL,
        input_row_count INTEGER NOT NULL,
        output_row_count INTEGER NOT NULL,
        stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        report_text TEXT NOT NULL DEFAULT '',
        preview_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        cleaned_rows_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL
      );
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_data_ai_runs_expires_at
      ON data_ai_runs (expires_at);
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS workflows (
        id TEXT PRIMARY KEY,
        goal TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
        selected_features_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_by TEXT REFERENCES app_users(id) ON DELETE SET NULL,
        meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        error_message TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS workflow_tasks (
        id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
        agent_id TEXT REFERENCES agents(id) ON DELETE SET NULL,
        task_key TEXT NOT NULL,
        title TEXT NOT NULL,
        kind TEXT NOT NULL DEFAULT 'general',
        status TEXT NOT NULL DEFAULT 'pending',
        depends_on_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        output_json JSONB,
        attempts INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        UNIQUE (workflow_id, task_key)
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS workflow_events (
        id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
        task_id TEXT REFERENCES workflow_tasks(id) ON DELETE SET NULL,
        role TEXT NOT NULL DEFAULT 'system',
        message TEXT NOT NULL,
        meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL
      );
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_workflows_created_at
      ON workflows (created_at DESC);
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_workflow_tasks_workflow_status
      ON workflow_tasks (workflow_id, status, created_at);
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow_created
      ON workflow_events (workflow_id, created_at);
    `);
  }

  async listOntologyFields() {
    const { rows } = await this.pool.query(
      `
      SELECT field_key, label, aliases, value_aliases, created_at, updated_at
      FROM ontology_fields
      ORDER BY field_key ASC
      `
    );

    return rows.map((row) => ({
      key: row.field_key,
      label: row.label,
      aliases: row.aliases || [],
      valueAliases: row.value_aliases || {},
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }));
  }

  async upsertOntologyFields(fields) {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      for (const field of fields) {
        await client.query(
          `
          INSERT INTO ontology_fields (field_key, label, aliases, value_aliases, created_at, updated_at)
          VALUES ($1, $2, $3::jsonb, $4::jsonb, NOW(), NOW())
          ON CONFLICT (field_key)
          DO UPDATE SET
            label = EXCLUDED.label,
            aliases = EXCLUDED.aliases,
            value_aliases = EXCLUDED.value_aliases,
            updated_at = NOW()
          `,
          [field.key, field.label, JSON.stringify(field.aliases || []), JSON.stringify(field.valueAliases || {})]
        );
      }

      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async listColumnOverrides() {
    const { rows } = await this.pool.query(
      `
      SELECT id, company_scope, source_column, source_column_norm, canonical_field, created_at, updated_at
      FROM column_overrides
      ORDER BY updated_at DESC
      `
    );

    return rows.map((row) => ({
      id: row.id,
      companyScope: row.company_scope,
      sourceColumn: row.source_column,
      sourceColumnNorm: row.source_column_norm,
      canonicalField: row.canonical_field,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }));
  }

  async upsertColumnOverride({ companyName, sourceColumn, canonicalField }) {
    const companyScope = normalizeScope(companyName);
    const sourceColumnNorm = normalizeColumnName(sourceColumn);
    const id = crypto.randomUUID();

    const { rows } = await this.pool.query(
      `
      INSERT INTO column_overrides (
        id, company_scope, source_column, source_column_norm, canonical_field, created_at, updated_at
      )
      VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
      ON CONFLICT (company_scope, source_column_norm)
      DO UPDATE SET
        source_column = EXCLUDED.source_column,
        canonical_field = EXCLUDED.canonical_field,
        updated_at = NOW()
      RETURNING id, company_scope, source_column, source_column_norm, canonical_field, created_at, updated_at
      `,
      [id, companyScope, String(sourceColumn || "").trim(), sourceColumnNorm, canonicalField]
    );

    return {
      id: rows[0].id,
      companyScope: rows[0].company_scope,
      sourceColumn: rows[0].source_column,
      sourceColumnNorm: rows[0].source_column_norm,
      canonicalField: rows[0].canonical_field,
      createdAt: rows[0].created_at,
      updatedAt: rows[0].updated_at,
    };
  }

  async createUser({ email, passwordHash, name, role, status = "active" }) {
    const id = crypto.randomUUID();
    const normalizedEmail = normalizeEmail(email);
    const now = new Date().toISOString();

    const { rows } = await this.pool.query(
      `
      INSERT INTO app_users (id, email, password_hash, name, role, status, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz, $7::timestamptz)
      RETURNING id, email, password_hash, name, role, status, created_at, updated_at
      `,
      [
        id,
        normalizedEmail,
        String(passwordHash || ""),
        String(name || "").trim() || normalizedEmail,
        String(role || "viewer").trim() || "viewer",
        String(status || "active").trim() || "active",
        now,
      ]
    );

    return toUserResponse(rows[0]);
  }

  async findUserByEmail(email) {
    const normalizedEmail = normalizeEmail(email);
    const { rows } = await this.pool.query(
      `
      SELECT id, email, password_hash, name, role, status, created_at, updated_at
      FROM app_users
      WHERE email = $1
      LIMIT 1
      `,
      [normalizedEmail]
    );

    return rows.length > 0 ? toUserResponse(rows[0]) : null;
  }

  async getUserById(userId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, email, password_hash, name, role, status, created_at, updated_at
      FROM app_users
      WHERE id = $1
      LIMIT 1
      `,
      [String(userId || "")]
    );

    return rows.length > 0 ? toUserResponse(rows[0]) : null;
  }

  async listUsers(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const { rows } = await this.pool.query(
      `
      SELECT id, email, password_hash, name, role, status, created_at, updated_at
      FROM app_users
      ORDER BY created_at DESC
      LIMIT $1
      `,
      [safeLimit]
    );

    return rows.map((row) => toUserResponse(row));
  }

  async updateUser(userId, { role, status }) {
    const updates = [];
    const values = [String(userId || "")];
    let index = 2;

    if (role !== undefined) {
      updates.push(`role = $${index++}`);
      values.push(String(role || "").trim());
    }

    if (status !== undefined) {
      updates.push(`status = $${index++}`);
      values.push(String(status || "").trim());
    }

    if (updates.length === 0) {
      return this.getUserById(userId);
    }

    updates.push("updated_at = NOW()");

    const { rows } = await this.pool.query(
      `
      UPDATE app_users
      SET ${updates.join(", ")}
      WHERE id = $1
      RETURNING id, email, password_hash, name, role, status, created_at, updated_at
      `,
      values
    );

    return rows.length > 0 ? toUserResponse(rows[0]) : null;
  }

  async storeRefreshToken({ userId, tokenHash, expiresAt, userAgent = null, ipAddress = null }) {
    const id = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const { rows } = await this.pool.query(
      `
      INSERT INTO auth_refresh_tokens (
        id, user_id, token_hash, expires_at, revoked_at, user_agent, ip_address, created_at
      )
      VALUES ($1, $2, $3, $4::timestamptz, NULL, $5, $6, $7::timestamptz)
      RETURNING id, user_id, token_hash, expires_at, revoked_at, user_agent, ip_address, created_at
      `,
      [
        id,
        String(userId || ""),
        String(tokenHash || ""),
        new Date(expiresAt).toISOString(),
        userAgent ? String(userAgent) : null,
        ipAddress ? String(ipAddress) : null,
        createdAt,
      ]
    );

    return toRefreshTokenResponse(rows[0]);
  }

  async findRefreshTokenByHash(tokenHash) {
    const { rows } = await this.pool.query(
      `
      SELECT id, user_id, token_hash, expires_at, revoked_at, user_agent, ip_address, created_at
      FROM auth_refresh_tokens
      WHERE token_hash = $1
      LIMIT 1
      `,
      [String(tokenHash || "")]
    );

    return rows.length > 0 ? toRefreshTokenResponse(rows[0]) : null;
  }

  async revokeRefreshTokenByHash(tokenHash) {
    const { rows } = await this.pool.query(
      `
      UPDATE auth_refresh_tokens
      SET revoked_at = NOW()
      WHERE token_hash = $1
      RETURNING id, user_id, token_hash, expires_at, revoked_at, user_agent, ip_address, created_at
      `,
      [String(tokenHash || "")]
    );

    return rows.length > 0 ? toRefreshTokenResponse(rows[0]) : null;
  }

  async revokeRefreshTokensByUser(userId) {
    const { rowCount } = await this.pool.query(
      `
      UPDATE auth_refresh_tokens
      SET revoked_at = NOW()
      WHERE user_id = $1 AND revoked_at IS NULL
      `,
      [String(userId || "")]
    );

    return { revokedCount: Number(rowCount || 0) };
  }

  async createDataset(dataset) {
    await this.pool.query(
      `
      INSERT INTO datasets (id, company_name, source_name, created_at, row_count, column_mapping, normalized_rows)
      VALUES ($1, $2, $3, $4::timestamptz, $5, $6::jsonb, $7::jsonb)
      `,
      [
        dataset.id,
        dataset.companyName,
        dataset.sourceName,
        dataset.createdAt,
        dataset.rowCount,
        JSON.stringify(dataset.columnMapping || []),
        JSON.stringify(dataset.normalizedRows || []),
      ]
    );

    return deepClone(dataset);
  }

  async listDatasets() {
    const { rows } = await this.pool.query(
      `
      SELECT id, company_name, source_name, created_at, row_count, column_mapping
      FROM datasets
      ORDER BY created_at DESC
      `
    );

    return rows.map((row) => ({
      id: row.id,
      companyName: row.company_name,
      sourceName: row.source_name,
      createdAt: row.created_at,
      rowCount: row.row_count,
      mappedColumns: Array.isArray(row.column_mapping) ? row.column_mapping.length : 0,
    }));
  }

  async getDataset(datasetId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, company_name, source_name, created_at, row_count, column_mapping, normalized_rows
      FROM datasets
      WHERE id = $1
      LIMIT 1
      `,
      [datasetId]
    );

    if (rows.length === 0) {
      return null;
    }

    const row = rows[0];
    return {
      id: row.id,
      companyName: row.company_name,
      sourceName: row.source_name,
      createdAt: row.created_at,
      rowCount: row.row_count,
      columnMapping: row.column_mapping || [],
      normalizedRows: row.normalized_rows || [],
      sampleRows: (row.normalized_rows || []).slice(0, 10),
    };
  }

  async getDatasetsByIds(datasetIds) {
    if (!Array.isArray(datasetIds) || datasetIds.length === 0) {
      const { rows } = await this.pool.query(
        `
        SELECT id, company_name, source_name, created_at, row_count, column_mapping, normalized_rows
        FROM datasets
        ORDER BY created_at ASC
        `
      );
      return rows.map((row) => ({
        id: row.id,
        companyName: row.company_name,
        sourceName: row.source_name,
        createdAt: row.created_at,
        rowCount: row.row_count,
        columnMapping: row.column_mapping || [],
        normalizedRows: row.normalized_rows || [],
        sampleRows: (row.normalized_rows || []).slice(0, 10),
      }));
    }

    const { rows } = await this.pool.query(
      `
      SELECT id, company_name, source_name, created_at, row_count, column_mapping, normalized_rows
      FROM datasets
      WHERE id = ANY($1::text[])
      ORDER BY created_at ASC
      `,
      [datasetIds]
    );

    return rows.map((row) => ({
      id: row.id,
      companyName: row.company_name,
      sourceName: row.source_name,
      createdAt: row.created_at,
      rowCount: row.row_count,
      columnMapping: row.column_mapping || [],
      normalizedRows: row.normalized_rows || [],
      sampleRows: (row.normalized_rows || []).slice(0, 10),
    }));
  }

  async enqueueJob({ jobType, payload }) {
    const id = crypto.randomUUID();
    const createdAt = new Date().toISOString();

    await this.pool.query(
      `
      INSERT INTO ingestion_jobs (id, job_type, status, payload, attempts, created_at)
      VALUES ($1, $2, 'pending', $3::jsonb, 0, $4::timestamptz)
      `,
      [id, jobType, JSON.stringify(payload || {}), createdAt]
    );

    return {
      id,
      jobType,
      status: "pending",
      payload: deepClone(payload || {}),
      result: null,
      error: null,
      attempts: 0,
      createdAt,
      startedAt: null,
      completedAt: null,
    };
  }

  async listJobs(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const { rows } = await this.pool.query(
      `
      SELECT id, job_type, status, payload, result, error, attempts, created_at, started_at, completed_at
      FROM ingestion_jobs
      ORDER BY created_at DESC
      LIMIT $1
      `,
      [safeLimit]
    );

    return rows.map((row) => toJobResponse(row));
  }

  async getJob(jobId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, job_type, status, payload, result, error, attempts, created_at, started_at, completed_at
      FROM ingestion_jobs
      WHERE id = $1
      LIMIT 1
      `,
      [jobId]
    );
    return rows.length > 0 ? toJobResponse(rows[0]) : null;
  }

  async claimPendingJob() {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      const candidate = await client.query(
        `
        SELECT id
        FROM ingestion_jobs
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        `
      );

      if (candidate.rows.length === 0) {
        await client.query("COMMIT");
        return null;
      }

      const jobId = candidate.rows[0].id;
      const updated = await client.query(
        `
        UPDATE ingestion_jobs
        SET status = 'running', started_at = NOW(), attempts = attempts + 1, error = NULL
        WHERE id = $1
        RETURNING id, job_type, status, payload, result, error, attempts, created_at, started_at, completed_at
        `,
        [jobId]
      );

      await client.query("COMMIT");
      return toJobResponse(updated.rows[0]);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async completeJob(jobId, result) {
    const { rows } = await this.pool.query(
      `
      UPDATE ingestion_jobs
      SET status = 'completed', result = $2::jsonb, completed_at = NOW(), error = NULL
      WHERE id = $1
      RETURNING id, job_type, status, payload, result, error, attempts, created_at, started_at, completed_at
      `,
      [jobId, JSON.stringify(result || {})]
    );

    return rows.length > 0 ? toJobResponse(rows[0]) : null;
  }

  async failJob(jobId, errorMessage) {
    const { rows } = await this.pool.query(
      `
      UPDATE ingestion_jobs
      SET status = 'failed', error = $2, completed_at = NOW()
      WHERE id = $1
      RETURNING id, job_type, status, payload, result, error, attempts, created_at, started_at, completed_at
      `,
      [jobId, String(errorMessage || "unknown_error")]
    );

    return rows.length > 0 ? toJobResponse(rows[0]) : null;
  }

  async upsertProviderAuthConnection({ provider, displayName, authMode = "api_key" }) {
    const normalizedProvider = normalizeProvider(provider);
    if (!normalizedProvider) {
      throw new Error("provider is required");
    }

    const id = crypto.randomUUID();
    const now = new Date().toISOString();
    const normalizedDisplayName =
      String(displayName || normalizedProvider.toUpperCase()).trim() || normalizedProvider.toUpperCase();
    const normalizedAuthMode = String(authMode || "api_key").trim().toLowerCase() || "api_key";

    const { rows } = await this.pool.query(
      `
      INSERT INTO provider_auth_connections (
        id, provider, display_name, auth_mode, status, secret_encrypted, meta_json, models_json, last_checked_at, error_message, created_at, updated_at
      )
      VALUES ($1, $2, $3, $4, 'pending', NULL, '{}'::jsonb, '[]'::jsonb, NULL, NULL, $5::timestamptz, $5::timestamptz)
      ON CONFLICT (provider)
      DO UPDATE SET
        display_name = EXCLUDED.display_name,
        auth_mode = EXCLUDED.auth_mode,
        updated_at = NOW()
      RETURNING id, provider, display_name, auth_mode, status, secret_encrypted, meta_json, models_json, last_checked_at, error_message, created_at, updated_at
      `,
      [id, normalizedProvider, normalizedDisplayName, normalizedAuthMode, now]
    );

    return toProviderAuthResponse(rows[0]);
  }

  async listProviderAuthConnections(limit = 100) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 100));
    const { rows } = await this.pool.query(
      `
      SELECT id, provider, display_name, auth_mode, status, secret_encrypted, meta_json, models_json, last_checked_at, error_message, created_at, updated_at
      FROM provider_auth_connections
      ORDER BY provider ASC
      LIMIT $1
      `,
      [safeLimit]
    );
    return rows.map((row) => toProviderAuthResponse(row));
  }

  async getProviderAuthConnection(connectionId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, provider, display_name, auth_mode, status, secret_encrypted, meta_json, models_json, last_checked_at, error_message, created_at, updated_at
      FROM provider_auth_connections
      WHERE id = $1
      LIMIT 1
      `,
      [String(connectionId || "")]
    );
    return rows.length > 0 ? toProviderAuthResponse(rows[0]) : null;
  }

  async getProviderAuthConnectionByProvider(provider) {
    const normalizedProvider = normalizeProvider(provider);
    const { rows } = await this.pool.query(
      `
      SELECT id, provider, display_name, auth_mode, status, secret_encrypted, meta_json, models_json, last_checked_at, error_message, created_at, updated_at
      FROM provider_auth_connections
      WHERE provider = $1
      LIMIT 1
      `,
      [normalizedProvider]
    );
    return rows.length > 0 ? toProviderAuthResponse(rows[0]) : null;
  }

  async updateProviderAuthConnection(connectionId, patch = {}) {
    const current = await this.getProviderAuthConnection(connectionId);
    if (!current) {
      return null;
    }

    const nextDisplayName =
      patch.displayName !== undefined
        ? String(patch.displayName || "").trim() || current.displayName
        : current.displayName;
    const nextAuthMode =
      patch.authMode !== undefined
        ? String(patch.authMode || "").trim().toLowerCase() || current.authMode
        : current.authMode;
    const nextStatus =
      patch.status !== undefined
        ? String(patch.status || "").trim().toLowerCase() || current.status
        : current.status;
    const nextSecretEncrypted =
      patch.secretEncrypted !== undefined
        ? String(patch.secretEncrypted || "").trim() || null
        : current.secretEncrypted;
    const nextMeta =
      patch.meta !== undefined
        ? patch.meta && typeof patch.meta === "object"
          ? patch.meta
          : {}
        : current.meta;
    const nextModels =
      patch.models !== undefined
        ? Array.isArray(patch.models)
          ? patch.models
          : []
        : current.models;
    const nextLastCheckedAt =
      patch.lastCheckedAt !== undefined
        ? patch.lastCheckedAt
          ? new Date(patch.lastCheckedAt).toISOString()
          : null
        : current.lastCheckedAt;
    const nextErrorMessage =
      patch.errorMessage !== undefined
        ? String(patch.errorMessage || "").trim() || null
        : current.errorMessage;

    const { rows } = await this.pool.query(
      `
      UPDATE provider_auth_connections
      SET
        display_name = $2,
        auth_mode = $3,
        status = $4,
        secret_encrypted = $5,
        meta_json = $6::jsonb,
        models_json = $7::jsonb,
        last_checked_at = $8::timestamptz,
        error_message = $9,
        updated_at = NOW()
      WHERE id = $1
      RETURNING id, provider, display_name, auth_mode, status, secret_encrypted, meta_json, models_json, last_checked_at, error_message, created_at, updated_at
      `,
      [
        current.id,
        nextDisplayName,
        nextAuthMode,
        nextStatus,
        nextSecretEncrypted,
        JSON.stringify(nextMeta || {}),
        JSON.stringify(nextModels || []),
        nextLastCheckedAt,
        nextErrorMessage,
      ]
    );

    return rows.length > 0 ? toProviderAuthResponse(rows[0]) : null;
  }

  async createAgent({ name, modelTier, systemPrompt, tools, skills, avatarUrl }) {
    const id = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const normalizedTools = Array.isArray(tools)
      ? [...new Set(tools.map((item) => String(item).trim()).filter(Boolean))]
      : [];
    const normalizedSkills = Array.isArray(skills)
      ? [...new Set(skills.map((item) => String(item).trim()).filter(Boolean))]
      : normalizedTools;

    const { rows } = await this.pool.query(
      `
      INSERT INTO agents (id, name, model_tier, system_prompt, tools, skills, avatar_url, status, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, 'active', $8::timestamptz, $8::timestamptz)
      RETURNING id, name, model_tier, system_prompt, tools, skills, avatar_url, status, created_at, updated_at
      `,
      [
        id,
        String(name || "").trim(),
        String(modelTier || "Balanced (default)").trim(),
        String(systemPrompt || "").trim(),
        JSON.stringify(normalizedTools),
        JSON.stringify(normalizedSkills),
        String(avatarUrl || "").trim() || null,
        createdAt,
      ]
    );

    const row = rows[0];
    return {
      id: row.id,
      name: row.name,
      modelTier: row.model_tier,
      systemPrompt: row.system_prompt,
      tools: row.tools || [],
      skills: row.skills || row.tools || [],
      avatarUrl: row.avatar_url || null,
      status: row.status,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  async listAgents(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const { rows } = await this.pool.query(
      `
      SELECT id, name, model_tier, system_prompt, tools, skills, avatar_url, status, created_at, updated_at
      FROM agents
      ORDER BY created_at DESC
      LIMIT $1
      `,
      [safeLimit]
    );

    return rows.map((row) => ({
      id: row.id,
      name: row.name,
      modelTier: row.model_tier,
      systemPrompt: row.system_prompt,
      tools: row.tools || [],
      skills: row.skills || row.tools || [],
      avatarUrl: row.avatar_url || null,
      status: row.status,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }));
  }

  async getAgent(agentId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, name, model_tier, system_prompt, tools, skills, avatar_url, status, created_at, updated_at
      FROM agents
      WHERE id = $1
      LIMIT 1
      `,
      [agentId]
    );

    if (rows.length === 0) {
      return null;
    }

    const row = rows[0];
    return {
      id: row.id,
      name: row.name,
      modelTier: row.model_tier,
      systemPrompt: row.system_prompt,
      tools: row.tools || [],
      skills: row.skills || row.tools || [],
      avatarUrl: row.avatar_url || null,
      status: row.status,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  async createDeployment({ agentId, queueName, environment, desiredReplicas, policy }) {
    const id = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const replicas = Math.max(1, Number(desiredReplicas) || 1);

    const { rows } = await this.pool.query(
      `
      INSERT INTO agent_deployments (
        id, agent_id, queue_name, environment, desired_replicas, running_replicas, status, policy, created_at, updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $5, 'running', $6::jsonb, $7::timestamptz, $7::timestamptz)
      RETURNING id, agent_id, queue_name, environment, desired_replicas, running_replicas, status, policy, created_at, updated_at
      `,
      [
        id,
        agentId,
        String(queueName || "default").trim(),
        String(environment || "production").trim(),
        replicas,
        JSON.stringify(policy && typeof policy === "object" ? policy : {}),
        createdAt,
      ]
    );

    const row = rows[0];
    return {
      id: row.id,
      agentId: row.agent_id,
      queueName: row.queue_name,
      environment: row.environment,
      desiredReplicas: row.desired_replicas,
      runningReplicas: row.running_replicas,
      status: row.status,
      policy: row.policy || {},
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  async listDeployments(limit = 100) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 100));
    const { rows } = await this.pool.query(
      `
      SELECT id, agent_id, queue_name, environment, desired_replicas, running_replicas, status, policy, created_at, updated_at
      FROM agent_deployments
      ORDER BY created_at DESC
      LIMIT $1
      `,
      [safeLimit]
    );

    return rows.map((row) => ({
      id: row.id,
      agentId: row.agent_id,
      queueName: row.queue_name,
      environment: row.environment,
      desiredReplicas: row.desired_replicas,
      runningReplicas: row.running_replicas,
      status: row.status,
      policy: row.policy || {},
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }));
  }

  async getDeployment(deploymentId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, agent_id, queue_name, environment, desired_replicas, running_replicas, status, policy, created_at, updated_at
      FROM agent_deployments
      WHERE id = $1
      LIMIT 1
      `,
      [deploymentId]
    );

    if (rows.length === 0) {
      return null;
    }

    const row = rows[0];
    return {
      id: row.id,
      agentId: row.agent_id,
      queueName: row.queue_name,
      environment: row.environment,
      desiredReplicas: row.desired_replicas,
      runningReplicas: row.running_replicas,
      status: row.status,
      policy: row.policy || {},
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  async updateDeploymentScale(deploymentId, desiredReplicas) {
    const replicas = Math.max(1, Number(desiredReplicas) || 1);
    const { rows } = await this.pool.query(
      `
      UPDATE agent_deployments
      SET desired_replicas = $2, running_replicas = $2, updated_at = NOW()
      WHERE id = $1
      RETURNING id, agent_id, queue_name, environment, desired_replicas, running_replicas, status, policy, created_at, updated_at
      `,
      [deploymentId, replicas]
    );

    if (rows.length === 0) {
      return null;
    }

    const row = rows[0];
    return {
      id: row.id,
      agentId: row.agent_id,
      queueName: row.queue_name,
      environment: row.environment,
      desiredReplicas: row.desired_replicas,
      runningReplicas: row.running_replicas,
      status: row.status,
      policy: row.policy || {},
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  async purgeExpiredDataAiRuns() {
    await this.pool.query(
      `
      DELETE FROM data_ai_runs
      WHERE expires_at <= NOW()
      `
    );
  }

  async createDataAiRun({
    userId,
    model,
    command,
    sourceName = null,
    action = "analyze",
    inputRowCount = 0,
    outputRowCount = 0,
    stats = {},
    report = "",
    preview = {},
    cleanedRows = [],
    createdAt = new Date().toISOString(),
    expiresAt,
  }) {
    await this.purgeExpiredDataAiRuns();
    const runId = crypto.randomUUID();
    const { rows } = await this.pool.query(
      `
      INSERT INTO data_ai_runs (
        id, user_id, model, command, source_name, action, input_row_count, output_row_count,
        stats_json, report_text, preview_json, cleaned_rows_json, created_at, expires_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8,
        $9::jsonb, $10, $11::jsonb, $12::jsonb, $13::timestamptz, $14::timestamptz
      )
      RETURNING id, user_id, model, command, source_name, action, input_row_count, output_row_count,
                stats_json, report_text, preview_json, cleaned_rows_json, created_at, expires_at
      `,
      [
        runId,
        String(userId || "").trim(),
        String(model || "").trim(),
        String(command || "").trim(),
        sourceName ? String(sourceName).trim() : null,
        String(action || "analyze").trim() || "analyze",
        Math.max(0, Number(inputRowCount) || 0),
        Math.max(0, Number(outputRowCount) || 0),
        JSON.stringify(stats && typeof stats === "object" ? stats : {}),
        String(report || "").trim(),
        JSON.stringify(preview && typeof preview === "object" ? preview : {}),
        JSON.stringify(Array.isArray(cleanedRows) ? cleanedRows : []),
        new Date(createdAt).toISOString(),
        new Date(expiresAt).toISOString(),
      ]
    );

    return toDataAiRunResponse(rows[0]);
  }

  async getDataAiRun(runId) {
    await this.purgeExpiredDataAiRuns();
    const { rows } = await this.pool.query(
      `
      SELECT id, user_id, model, command, source_name, action, input_row_count, output_row_count,
             stats_json, report_text, preview_json, cleaned_rows_json, created_at, expires_at
      FROM data_ai_runs
      WHERE id = $1
      LIMIT 1
      `,
      [String(runId || "").trim()]
    );

    return rows.length > 0 ? toDataAiRunResponse(rows[0]) : null;
  }

  async createWorkflow({
    goal,
    datasetId = null,
    selectedFeatures = [],
    createdBy = null,
    tasks = [],
    meta = {},
  }) {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      const workflowId = crypto.randomUUID();
      const createdAt = new Date().toISOString();
      const normalizedFeatures = Array.isArray(selectedFeatures)
        ? [...new Set(selectedFeatures.map((item) => String(item || "").trim()).filter(Boolean))]
        : [];

      const insertedWorkflow = await client.query(
        `
        INSERT INTO workflows (
          id, goal, status, dataset_id, selected_features_json, created_by, meta_json,
          error_message, created_at, updated_at, started_at, completed_at
        )
        VALUES (
          $1, $2, 'pending', $3, $4::jsonb, $5, $6::jsonb,
          NULL, $7::timestamptz, $7::timestamptz, NULL, NULL
        )
        RETURNING id, goal, status, dataset_id, selected_features_json, created_by, meta_json,
                  error_message, created_at, updated_at, started_at, completed_at
        `,
        [
          workflowId,
          String(goal || "").trim(),
          datasetId ? String(datasetId).trim() : null,
          JSON.stringify(normalizedFeatures),
          createdBy ? String(createdBy).trim() : null,
          JSON.stringify(meta && typeof meta === "object" ? meta : {}),
          createdAt,
        ]
      );

      const normalizedTasks = Array.isArray(tasks) ? tasks : [];
      const taskIdByKey = new Map();
      for (const item of normalizedTasks) {
        const taskKey = String(item?.taskKey || "").trim();
        if (!taskKey) {
          continue;
        }
        if (taskIdByKey.has(taskKey)) {
          throw new Error(`duplicate workflow task key: ${taskKey}`);
        }
        taskIdByKey.set(taskKey, crypto.randomUUID());
      }

      for (const item of normalizedTasks) {
        const taskKey = String(item?.taskKey || "").trim();
        if (!taskKey || !taskIdByKey.has(taskKey)) {
          continue;
        }

        const dependsOnTaskKeys = Array.isArray(item?.dependsOnTaskKeys)
          ? item.dependsOnTaskKeys
          : [];
        const dependsOnIds = [];
        for (const dependsOnTaskKeyRaw of dependsOnTaskKeys) {
          const dependsOnTaskKey = String(dependsOnTaskKeyRaw || "").trim();
          if (!dependsOnTaskKey) {
            continue;
          }
          const dependencyId = taskIdByKey.get(dependsOnTaskKey);
          if (!dependencyId) {
            throw new Error(
              `workflow task dependency not found: ${dependsOnTaskKey}`
            );
          }
          if (dependencyId !== taskIdByKey.get(taskKey)) {
            dependsOnIds.push(dependencyId);
          }
        }

        await client.query(
          `
          INSERT INTO workflow_tasks (
            id, workflow_id, agent_id, task_key, title, kind, status, depends_on_json, input_json, output_json,
            attempts, error_message, created_at, updated_at, started_at, completed_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, 'pending', $7::jsonb, $8::jsonb, NULL,
            0, NULL, $9::timestamptz, $9::timestamptz, NULL, NULL
          )
          `,
          [
            taskIdByKey.get(taskKey),
            workflowId,
            item?.agentId ? String(item.agentId).trim() : null,
            taskKey,
            String(item?.title || taskKey).trim() || taskKey,
            String(item?.kind || "general").trim() || "general",
            JSON.stringify([...new Set(dependsOnIds)]),
            JSON.stringify(
              item?.input && typeof item.input === "object" ? item.input : {}
            ),
            createdAt,
          ]
        );
      }

      await client.query("COMMIT");
      return toWorkflowResponse(insertedWorkflow.rows[0]);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async listWorkflows(limit = 50) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const { rows } = await this.pool.query(
      `
      SELECT id, goal, status, dataset_id, selected_features_json, created_by, meta_json,
             error_message, created_at, updated_at, started_at, completed_at
      FROM workflows
      ORDER BY created_at DESC
      LIMIT $1
      `,
      [safeLimit]
    );
    return rows.map((row) => toWorkflowResponse(row));
  }

  async getWorkflow(workflowId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, goal, status, dataset_id, selected_features_json, created_by, meta_json,
             error_message, created_at, updated_at, started_at, completed_at
      FROM workflows
      WHERE id = $1
      LIMIT 1
      `,
      [String(workflowId || "").trim()]
    );
    return rows.length > 0 ? toWorkflowResponse(rows[0]) : null;
  }

  async listWorkflowTasks(workflowId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, workflow_id, agent_id, task_key, title, kind, status, depends_on_json, input_json, output_json,
             attempts, error_message, created_at, updated_at, started_at, completed_at
      FROM workflow_tasks
      WHERE workflow_id = $1
      ORDER BY created_at ASC
      `,
      [String(workflowId || "").trim()]
    );
    return rows.map((row) => toWorkflowTaskResponse(row));
  }

  async getWorkflowTask(taskId) {
    const { rows } = await this.pool.query(
      `
      SELECT id, workflow_id, agent_id, task_key, title, kind, status, depends_on_json, input_json, output_json,
             attempts, error_message, created_at, updated_at, started_at, completed_at
      FROM workflow_tasks
      WHERE id = $1
      LIMIT 1
      `,
      [String(taskId || "").trim()]
    );
    return rows.length > 0 ? toWorkflowTaskResponse(rows[0]) : null;
  }

  async appendWorkflowEvent({
    workflowId,
    taskId = null,
    role = "system",
    message,
    meta = {},
  }) {
    const id = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const { rows } = await this.pool.query(
      `
      INSERT INTO workflow_events (id, workflow_id, task_id, role, message, meta_json, created_at)
      VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::timestamptz)
      RETURNING id, workflow_id, task_id, role, message, meta_json, created_at
      `,
      [
        id,
        String(workflowId || "").trim(),
        taskId ? String(taskId).trim() : null,
        String(role || "system").trim() || "system",
        String(message || "").trim(),
        JSON.stringify(meta && typeof meta === "object" ? meta : {}),
        createdAt,
      ]
    );
    return toWorkflowEventResponse(rows[0]);
  }

  async listWorkflowEvents(workflowId, limit = 200) {
    const safeLimit = Math.max(1, Math.min(2000, Number(limit) || 200));
    const { rows } = await this.pool.query(
      `
      SELECT id, workflow_id, task_id, role, message, meta_json, created_at
      FROM workflow_events
      WHERE workflow_id = $1
      ORDER BY created_at ASC
      LIMIT $2
      `,
      [String(workflowId || "").trim(), safeLimit]
    );
    return rows.map((row) => toWorkflowEventResponse(row));
  }

  async claimPendingWorkflowTask() {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      const candidate = await client.query(
        `
        SELECT t.id
        FROM workflow_tasks t
        JOIN workflows w ON w.id = t.workflow_id
        WHERE t.status = 'pending'
          AND w.status IN ('pending', 'running')
          AND NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(COALESCE(t.depends_on_json, '[]'::jsonb)) dep(dep_task_id)
            JOIN workflow_tasks d ON d.id = dep.dep_task_id
            WHERE d.status <> 'completed'
          )
        ORDER BY t.created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        `
      );

      if (candidate.rows.length === 0) {
        await client.query("COMMIT");
        return null;
      }

      const taskId = candidate.rows[0].id;
      const claimed = await client.query(
        `
        UPDATE workflow_tasks
        SET
          status = 'running',
          started_at = COALESCE(started_at, NOW()),
          updated_at = NOW(),
          attempts = attempts + 1,
          error_message = NULL
        WHERE id = $1
        RETURNING id, workflow_id, agent_id, task_key, title, kind, status, depends_on_json, input_json, output_json,
                  attempts, error_message, created_at, updated_at, started_at, completed_at
        `,
        [taskId]
      );

      if (claimed.rows.length > 0) {
        const workflowId = claimed.rows[0].workflow_id;
        await client.query(
          `
          UPDATE workflows
          SET
            status = CASE WHEN status = 'pending' THEN 'running' ELSE status END,
            started_at = COALESCE(started_at, NOW()),
            updated_at = NOW()
          WHERE id = $1
          `,
          [workflowId]
        );
      }

      await client.query("COMMIT");
      return claimed.rows.length > 0
        ? toWorkflowTaskResponse(claimed.rows[0])
        : null;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async completeWorkflowTask(taskId, output = {}) {
    const { rows } = await this.pool.query(
      `
      UPDATE workflow_tasks
      SET
        status = 'completed',
        output_json = $2::jsonb,
        error_message = NULL,
        completed_at = NOW(),
        updated_at = NOW()
      WHERE id = $1
      RETURNING id, workflow_id, agent_id, task_key, title, kind, status, depends_on_json, input_json, output_json,
                attempts, error_message, created_at, updated_at, started_at, completed_at
      `,
      [
        String(taskId || "").trim(),
        JSON.stringify(output && typeof output === "object" ? output : {}),
      ]
    );
    return rows.length > 0 ? toWorkflowTaskResponse(rows[0]) : null;
  }

  async failWorkflowTask(taskId, errorMessage) {
    const message = String(errorMessage || "workflow_task_failed").trim();
    const { rows } = await this.pool.query(
      `
      UPDATE workflow_tasks
      SET
        status = 'failed',
        error_message = $2,
        completed_at = NOW(),
        updated_at = NOW()
      WHERE id = $1
      RETURNING id, workflow_id, agent_id, task_key, title, kind, status, depends_on_json, input_json, output_json,
                attempts, error_message, created_at, updated_at, started_at, completed_at
      `,
      [String(taskId || "").trim(), message]
    );
    if (rows.length === 0) {
      return null;
    }

    await this.pool.query(
      `
      UPDATE workflows
      SET error_message = $2, updated_at = NOW()
      WHERE id = $1
      `,
      [rows[0].workflow_id, message]
    );
    return toWorkflowTaskResponse(rows[0]);
  }

  async reconcileWorkflowStatus(workflowId) {
    const key = String(workflowId || "").trim();
    const { rows } = await this.pool.query(
      `
      SELECT
        COUNT(*)::int AS total_count,
        COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_count,
        COUNT(*) FILTER (WHERE status = 'running')::int AS running_count,
        COUNT(*) FILTER (WHERE status = 'completed')::int AS completed_count,
        COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_count
      FROM workflow_tasks
      WHERE workflow_id = $1
      `,
      [key]
    );
    const summary = rows[0] || {
      total_count: 0,
      pending_count: 0,
      running_count: 0,
      completed_count: 0,
      failed_count: 0,
    };

    const total = Number(summary.total_count) || 0;
    const failed = Number(summary.failed_count) || 0;
    const completed = Number(summary.completed_count) || 0;
    const running = Number(summary.running_count) || 0;

    let nextStatus = "pending";
    if (failed > 0) {
      nextStatus = "failed";
    } else if (total > 0 && completed === total) {
      nextStatus = "completed";
    } else if (running > 0 || completed > 0) {
      nextStatus = "running";
    }

    const update = await this.pool.query(
      `
      UPDATE workflows
      SET
        status = $2,
        started_at = CASE
          WHEN $2 = 'running' THEN COALESCE(started_at, NOW())
          ELSE started_at
        END,
        completed_at = CASE
          WHEN $2 IN ('completed', 'failed') THEN COALESCE(completed_at, NOW())
          WHEN $2 = 'running' THEN NULL
          ELSE completed_at
        END,
        updated_at = NOW()
      WHERE id = $1
      RETURNING id, goal, status, dataset_id, selected_features_json, created_by, meta_json,
                error_message, created_at, updated_at, started_at, completed_at
      `,
      [key, nextStatus]
    );

    return update.rows.length > 0
      ? toWorkflowResponse(update.rows[0])
      : null;
  }
}

function createRepository() {
  if (process.env.DATABASE_URL) {
    return new PostgresRepository(process.env.DATABASE_URL);
  }

  const persistEnabledRaw = String(
    process.env.MEMORY_REPO_PERSIST || (process.env.NODE_ENV === "test" ? "false" : "true")
  )
    .trim()
    .toLowerCase();
  const persistEnabled = persistEnabledRaw !== "false";
  const configuredStateFile = String(process.env.MEMORY_REPO_STATE_FILE || "").trim();
  const stateFilePath =
    configuredStateFile || path.join(process.cwd(), ".cache", "memory-repository.json");

  return new InMemoryRepository({
    persistEnabled,
    stateFilePath,
  });
}

module.exports = {
  createRepository,
  normalizeScope,
  normalizeColumnName,
};
