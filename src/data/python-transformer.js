const path = require("path");
const { spawn } = require("child_process");

function toPositiveInt(value, fallback, min = 1) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, Math.floor(parsed));
}

function normalizeStats(rawStats, fallbackRowsLength = 0) {
  const source = rawStats && typeof rawStats === "object" ? rawStats : {};
  const inputRows = toPositiveInt(source.inputRows, Math.max(0, Number(fallbackRowsLength) || 0), 0);
  const outputRows = toPositiveInt(source.outputRows, inputRows, 0);
  return {
    inputRows,
    outputRows,
    modifiedCells: toPositiveInt(source.modifiedCells, 0, 0),
    droppedRows: toPositiveInt(source.droppedRows, 0, 0),
    removedColumns: toPositiveInt(source.removedColumns, 0, 0),
    normalizedColumns: toPositiveInt(source.normalizedColumns, 0, 0),
    operationsApplied: toPositiveInt(source.operationsApplied, 0, 0),
  };
}

async function runPythonDataTransform(options = {}) {
  const rows = Array.isArray(options.rows) ? options.rows : [];
  const operations = Array.isArray(options.operations) ? options.operations : [];
  const pythonBin = String(options.pythonBin || "python3").trim() || "python3";
  const timeoutMs = Math.max(1000, Number(options.timeoutMs) || 20000);
  const scriptPath =
    String(options.scriptPath || "").trim() ||
    path.join(process.cwd(), "scripts", "data_transformer.py");

  const payload = JSON.stringify({ rows, operations });
  const startedAt = Date.now();

  return new Promise((resolve, reject) => {
    const child = spawn(pythonBin, [scriptPath], {
      stdio: ["pipe", "pipe", "pipe"],
      env: process.env,
    });

    const stdoutChunks = [];
    const stderrChunks = [];
    let stdoutSize = 0;
    let stderrSize = 0;
    const maxBytes = 64 * 1024 * 1024;
    let overflowKilled = false;

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`python transform timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    const finishReject = (error) => {
      clearTimeout(timer);
      reject(error);
    };

    child.on("error", (error) => {
      finishReject(
        new Error(`python transform process failed: ${error?.message || "unknown error"}`)
      );
    });

    child.stdout.on("data", (chunk) => {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      stdoutSize += buffer.length;
      if (stdoutSize > maxBytes) {
        overflowKilled = true;
        child.kill("SIGKILL");
        return;
      }
      stdoutChunks.push(buffer);
    });

    child.stderr.on("data", (chunk) => {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      stderrSize += buffer.length;
      if (stderrSize > maxBytes) {
        overflowKilled = true;
        child.kill("SIGKILL");
        return;
      }
      stderrChunks.push(buffer);
    });

    child.on("close", (code, signal) => {
      clearTimeout(timer);

      if (overflowKilled) {
        reject(new Error("python transform output exceeded limit"));
        return;
      }

      const stdout = Buffer.concat(stdoutChunks).toString("utf8").trim();
      const stderr = Buffer.concat(stderrChunks).toString("utf8").trim();

      if (code !== 0) {
        const reason = stderr || stdout || signal || `exit code ${code}`;
        reject(new Error(`python transform failed: ${String(reason).slice(0, 260)}`));
        return;
      }

      let parsed;
      try {
        parsed = stdout ? JSON.parse(stdout) : {};
      } catch (error) {
        reject(
          new Error(
            `python transform returned invalid JSON: ${
              error?.message || "parse error"
            }`
          )
        );
        return;
      }

      const resultRows = Array.isArray(parsed?.rows) ? parsed.rows : [];
      const resultStats = normalizeStats(parsed?.stats, rows.length);
      resolve({
        rows: resultRows,
        stats: resultStats,
        diagnostics: {
          durationMs: Date.now() - startedAt,
          stderr: stderr.slice(0, 4000),
        },
      });
    });

    child.stdin.on("error", (error) => {
      finishReject(
        new Error(`python transform stdin failed: ${error?.message || "unknown error"}`)
      );
    });

    child.stdin.end(payload);
  });
}

module.exports = {
  runPythonDataTransform,
};
