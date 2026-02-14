const crypto = require("crypto");

class IngestionJobQueue {
  constructor({
    repository,
    ontologyService,
    pollIntervalMs = 750,
    maxJobsPerTick = 5,
  }) {
    this.repository = repository;
    this.ontologyService = ontologyService;
    this.pollIntervalMs = Math.max(100, Number(pollIntervalMs) || 750);
    this.maxJobsPerTick = Math.max(1, Number(maxJobsPerTick) || 5);
    this.timer = null;
    this.running = false;
  }

  start() {
    if (this.timer) {
      return;
    }

    this.timer = setInterval(() => {
      this.processTick().catch((error) => {
        console.error("Job queue tick failed:", error);
      });
    }, this.pollIntervalMs);

    this.timer.unref?.();

    this.processTick().catch((error) => {
      console.error("Initial job queue tick failed:", error);
    });
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async enqueueIngestionJob({ companyName, sourceName, rows, metadata }) {
    if (!Array.isArray(rows) || rows.length === 0) {
      throw new Error("rows must be a non-empty array");
    }

    return this.repository.enqueueJob({
      jobType: "ingest_table",
      payload: {
        companyName: companyName || "Unknown Company",
        sourceName: sourceName || "unknown-source",
        rows,
        metadata: metadata || {},
      },
    });
  }

  async processTick() {
    if (this.running) {
      return;
    }

    this.running = true;
    try {
      for (let index = 0; index < this.maxJobsPerTick; index += 1) {
        const job = await this.repository.claimPendingJob();
        if (!job) {
          break;
        }
        await this.processJob(job);
      }
    } finally {
      this.running = false;
    }
  }

  async processJob(job) {
    const startedAtMs = Date.now();

    try {
      if (job.jobType !== "ingest_table") {
        throw new Error(`unsupported job type: ${job.jobType}`);
      }

      const payload = job.payload || {};
      const rows = payload.rows;
      if (!Array.isArray(rows) || rows.length === 0) {
        throw new Error("job payload rows must be a non-empty array");
      }

      const transformed = this.ontologyService.mapRows({
        companyName: payload.companyName,
        rows,
      });

      const dataset = {
        id: crypto.randomUUID(),
        companyName: payload.companyName || "Unknown Company",
        sourceName: payload.sourceName || "unknown-source",
        createdAt: new Date().toISOString(),
        rowCount: transformed.rowCount,
        columnMapping: transformed.columnMapping,
        normalizedRows: transformed.normalizedRows,
        sampleRows: transformed.sampleRows,
      };

      await this.repository.createDataset(dataset);

      const finishedAtMs = Date.now();
      const result = {
        datasetId: dataset.id,
        companyName: dataset.companyName,
        sourceName: dataset.sourceName,
        rowCount: dataset.rowCount,
        durationMs: finishedAtMs - startedAtMs,
        columnMapping: dataset.columnMapping,
      };

      await this.repository.completeJob(job.id, result);
    } catch (error) {
      await this.repository.failJob(job.id, error.message || "job_processing_failed");
    }
  }
}

module.exports = {
  IngestionJobQueue,
};
