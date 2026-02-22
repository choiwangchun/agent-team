const { spawn } = require("child_process");

function toPositiveInt(value, fallback, min = 1) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, Math.floor(parsed));
}

function truncateText(value, maxChars = 8000) {
  const limit = Math.max(128, Number(maxChars) || 8000);
  const text = String(value || "");
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, Math.max(0, limit - 3))}...`;
}

function normalizeCommandList(commands, maxCommands = 3) {
  const limit = toPositiveInt(maxCommands, 3, 1);
  if (!Array.isArray(commands)) {
    return [];
  }
  const list = [];
  for (const raw of commands) {
    const cmd = String(raw || "").trim();
    if (!cmd) {
      continue;
    }
    list.push(cmd);
    if (list.length >= limit) {
      break;
    }
  }
  return list;
}

const DEFAULT_ALWAYS_BLOCKED_PATTERNS = [
  /(^|\s)rm\s+-rf\s+\/($|\s)/i,
  /(^|\s)mkfs(\.|$|\s)/i,
  /(^|\s)shutdown(\s|$)/i,
  /(^|\s)reboot(\s|$)/i,
  /(^|\s)poweroff(\s|$)/i,
  /(:\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\};\s*:)/i,
  /(^|\s)dd\s+if=/i,
  /(^|\s)chown\s+-R\s+\/($|\s)/i,
  /(^|\s)chmod\s+-R\s+777\s+\/($|\s)/i,
];

const PROFILE_ALLOWLISTS = Object.freeze({
  read_only: [
    "pwd",
    "ls",
    "find",
    "cat",
    "head",
    "tail",
    "wc",
    "rg",
    "grep",
    "sed",
    "awk",
    "cut",
    "sort",
    "uniq",
    "echo",
    "git status",
    "git log",
    "git show",
  ],
  standard: [
    "pwd",
    "ls",
    "find",
    "cat",
    "head",
    "tail",
    "wc",
    "rg",
    "grep",
    "sed",
    "awk",
    "cut",
    "sort",
    "uniq",
    "echo",
    "mkdir",
    "cp",
    "mv",
    "touch",
    "npm",
    "node",
    "python",
    "python3",
    "git",
  ],
  full: [],
});

function commandStartsWith(command, allowed) {
  const cmd = String(command || "").trim().toLowerCase();
  const allow = String(allowed || "").trim().toLowerCase();
  if (!cmd || !allow) {
    return false;
  }
  return cmd === allow || cmd.startsWith(`${allow} `);
}

function normalizePermissionProfile(permissionProfile = {}) {
  const profile =
    permissionProfile && typeof permissionProfile === "object"
      ? permissionProfile
      : {};
  const modeRaw = String(profile.mode || "").trim().toLowerCase();
  const mode = ["read_only", "standard", "full", "custom"].includes(modeRaw)
    ? modeRaw
    : "standard";

  const customAllow = Array.isArray(profile.allowCommands)
    ? profile.allowCommands
        .map((item) => String(item || "").trim())
        .filter(Boolean)
    : [];
  const customDeny = Array.isArray(profile.denyPatterns)
    ? profile.denyPatterns
        .map((item) => String(item || "").trim())
        .filter(Boolean)
    : [];

  const allowCommands =
    mode === "custom"
      ? customAllow
      : [...(PROFILE_ALLOWLISTS[mode] || [])];

  const denyPatterns = [...customDeny];
  return {
    mode,
    allowCommands,
    denyPatterns,
  };
}

function checkCommandPermission(command, permissionProfile = {}) {
  const cmd = String(command || "").trim();
  if (!cmd) {
    return { allowed: false, reason: "empty_command" };
  }

  for (const pattern of DEFAULT_ALWAYS_BLOCKED_PATTERNS) {
    if (pattern.test(cmd)) {
      return { allowed: false, reason: "dangerous_command_blocked" };
    }
  }

  const profile = normalizePermissionProfile(permissionProfile);
  for (const patternText of profile.denyPatterns) {
    try {
      const regex = new RegExp(patternText, "i");
      if (regex.test(cmd)) {
        return { allowed: false, reason: "command_denied_by_profile" };
      }
    } catch {
      if (cmd.toLowerCase().includes(patternText.toLowerCase())) {
        return { allowed: false, reason: "command_denied_by_profile" };
      }
    }
  }

  if (profile.mode === "full") {
    return { allowed: true, reason: "" };
  }

  const allowList = Array.isArray(profile.allowCommands)
    ? profile.allowCommands
    : [];
  if (allowList.length === 0) {
    return { allowed: false, reason: "no_allowlist_configured" };
  }
  const matched = allowList.some((entry) => commandStartsWith(cmd, entry));
  if (!matched) {
    return { allowed: false, reason: "command_not_in_allowlist" };
  }
  return { allowed: true, reason: "" };
}

async function runSingleCommand({
  command,
  cwd,
  shell,
  timeoutMs = 30000,
  maxOutputChars = 12000,
}) {
  const startedAt = Date.now();
  const safeCommand = String(command || "").trim();
  if (!safeCommand) {
    return {
      command: "",
      ok: false,
      exitCode: null,
      signal: null,
      durationMs: 0,
      stdout: "",
      stderr: "empty command",
      timedOut: false,
    };
  }

  return new Promise((resolve) => {
    const child = spawn(shell, ["-lc", safeCommand], {
      cwd,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let settled = false;
    let timedOut = false;
    const outputLimit = Math.max(256, Number(maxOutputChars) || 12000);
    const timeout = Math.max(500, Number(timeoutMs) || 30000);
    const killTimer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeout);

    const finish = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(killTimer);
      resolve(result);
    };

    child.on("error", (error) => {
      finish({
        command: safeCommand,
        ok: false,
        exitCode: null,
        signal: null,
        durationMs: Date.now() - startedAt,
        stdout: truncateText(stdout, outputLimit),
        stderr: truncateText(error?.message || "spawn failed", outputLimit),
        timedOut,
      });
    });

    child.stdout.on("data", (chunk) => {
      stdout = truncateText(`${stdout}${chunk.toString("utf8")}`, outputLimit);
    });

    child.stderr.on("data", (chunk) => {
      stderr = truncateText(`${stderr}${chunk.toString("utf8")}`, outputLimit);
    });

    child.on("close", (code, signal) => {
      const exitCode =
        Number.isFinite(Number(code)) && code !== null ? Number(code) : null;
      finish({
        command: safeCommand,
        ok: exitCode === 0 && !timedOut,
        exitCode,
        signal: signal || null,
        durationMs: Date.now() - startedAt,
        stdout: truncateText(stdout, outputLimit),
        stderr: truncateText(stderr, outputLimit),
        timedOut,
      });
    });
  });
}

async function runLocalCommands({
  commands = [],
  cwd,
  shell = process.env.SHELL || "zsh",
  timeoutMs = 30000,
  maxOutputChars = 12000,
  maxCommands = 3,
  permissionProfile = {},
}) {
  const commandList = normalizeCommandList(commands, maxCommands);
  const results = [];

  for (const command of commandList) {
    const permission = checkCommandPermission(command, permissionProfile);
    if (!permission.allowed) {
      results.push({
        command: String(command || "").trim(),
        ok: false,
        exitCode: null,
        signal: null,
        durationMs: 0,
        stdout: "",
        stderr: `blocked by permission policy: ${permission.reason}`,
        timedOut: false,
        blocked: true,
      });
      continue;
    }

    const result = await runSingleCommand({
      command,
      cwd,
      shell,
      timeoutMs,
      maxOutputChars,
    });
    results.push(result);
  }

  return {
    results,
    commandCount: commandList.length,
    successCount: results.filter((item) => item.ok).length,
    failedCount: results.filter((item) => !item.ok).length,
  };
}

module.exports = {
  runLocalCommands,
  normalizeCommandList,
  normalizePermissionProfile,
  checkCommandPermission,
};
