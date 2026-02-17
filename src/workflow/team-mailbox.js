const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

function toSafePathSegment(value, fallback = "node") {
  const normalized = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || fallback;
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function getWorkflowMailboxDir(rootDir, workflowId) {
  const workflowKey = toSafePathSegment(workflowId, "workflow");
  return path.join(String(rootDir || "").trim(), workflowKey, "team-mailboxes");
}

function getMailboxPath({ rootDir, workflowId, nodeId }) {
  const dirPath = getWorkflowMailboxDir(rootDir, workflowId);
  const fileName = `${toSafePathSegment(nodeId, "node")}.json`;
  return path.join(dirPath, fileName);
}

function readMailbox(filePath) {
  try {
    if (!fs.existsSync(filePath)) {
      return { messages: [] };
    }
    const raw = fs.readFileSync(filePath, "utf8");
    if (!raw.trim()) {
      return { messages: [] };
    }
    const parsed = JSON.parse(raw);
    const messages = Array.isArray(parsed?.messages) ? parsed.messages : [];
    return { messages };
  } catch {
    return { messages: [] };
  }
}

function writeMailbox(filePath, messages) {
  const dirPath = path.dirname(filePath);
  ensureDir(dirPath);
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  const payload = JSON.stringify(
    {
      messages: Array.isArray(messages) ? messages : [],
      updatedAt: new Date().toISOString(),
    },
    null,
    2
  );
  fs.writeFileSync(tempPath, payload, "utf8");
  fs.renameSync(tempPath, filePath);
}

function appendMailboxMessage({
  rootDir,
  workflowId,
  nodeId,
  message,
}) {
  const filePath = getMailboxPath({ rootDir, workflowId, nodeId });
  const state = readMailbox(filePath);
  const messages = Array.isArray(state.messages) ? state.messages : [];
  const now = new Date().toISOString();
  messages.push({
    id: crypto.randomUUID(),
    fromNodeId: String(message?.fromNodeId || "").trim(),
    fromTaskKey: String(message?.fromTaskKey || "").trim(),
    fromAgent: String(message?.fromAgent || "").trim(),
    fromAgentId: String(message?.fromAgentId || "").trim(),
    color: String(message?.color || "").trim(),
    text: String(message?.text || "").trim(),
    summary: String(message?.summary || "").trim(),
    type: String(message?.type || "message").trim().toLowerCase() || "message",
    timestamp: String(message?.timestamp || now).trim() || now,
    read: false,
  });
  writeMailbox(filePath, messages);
}

function readUnreadMailboxMessages({
  rootDir,
  workflowId,
  nodeId,
  maxMessages = 20,
}) {
  const filePath = getMailboxPath({ rootDir, workflowId, nodeId });
  const state = readMailbox(filePath);
  const limit = Math.max(1, Number(maxMessages) || 20);
  const unread = [];
  for (const message of Array.isArray(state.messages) ? state.messages : []) {
    if (message?.read) {
      continue;
    }
    unread.push(message);
    if (unread.length >= limit) {
      break;
    }
  }
  return unread;
}

function markMailboxMessagesRead({
  rootDir,
  workflowId,
  nodeId,
  messageIds = [],
}) {
  const ids = new Set(
    Array.isArray(messageIds)
      ? messageIds.map((item) => String(item || "").trim()).filter(Boolean)
      : []
  );
  if (ids.size === 0) {
    return 0;
  }

  const filePath = getMailboxPath({ rootDir, workflowId, nodeId });
  const state = readMailbox(filePath);
  const messages = Array.isArray(state.messages) ? state.messages : [];
  let changed = 0;
  const next = messages.map((message) => {
    const id = String(message?.id || "").trim();
    if (!id || !ids.has(id) || message?.read) {
      return message;
    }
    changed += 1;
    return {
      ...message,
      read: true,
    };
  });
  if (changed > 0) {
    writeMailbox(filePath, next);
  }
  return changed;
}

module.exports = {
  appendMailboxMessage,
  readUnreadMailboxMessages,
  markMailboxMessagesRead,
  getWorkflowMailboxDir,
};
