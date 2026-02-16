function normalizeColumnName(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function clampInt(value, fallback, min = 1) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, Math.floor(parsed));
}

function truncateText(value, maxLength) {
  const text = String(value || "");
  const safeMax = clampInt(maxLength, 512, 32);
  if (text.length <= safeMax) {
    return text;
  }
  return `${text.slice(0, safeMax - 3)}...`;
}

function toCellString(value, maxLength = 200) {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value === "string") {
    return truncateText(value, maxLength);
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  try {
    return truncateText(JSON.stringify(value), maxLength);
  } catch {
    return "[object]";
  }
}

function sanitizeCellValue(value, maxCellLength = 2048) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "string") {
    return truncateText(value, maxCellLength);
  }

  try {
    return truncateText(JSON.stringify(value), maxCellLength);
  } catch {
    return truncateText(String(value), maxCellLength);
  }
}

function sanitizeExecutionRows(rows, options = {}) {
  const maxRows = clampInt(options.maxRows, 5000, 1);
  const maxColumns = clampInt(options.maxColumns, 120, 1);
  const maxCellLength = clampInt(options.maxCellLength, 2048, 32);

  const input = Array.isArray(rows) ? rows : [];
  const totalInputRows = input.length;
  const sanitized = [];
  const columnSet = new Set();
  let truncated = false;

  for (const row of input) {
    if (sanitized.length >= maxRows) {
      truncated = true;
      break;
    }

    if (!row || typeof row !== "object" || Array.isArray(row)) {
      continue;
    }

    const nextRow = {};
    let writtenColumns = 0;

    for (const [keyRaw, value] of Object.entries(row)) {
      const key = normalizeColumnName(keyRaw);
      if (!key) {
        continue;
      }
      if (!(key in nextRow) && writtenColumns >= maxColumns) {
        continue;
      }
      if (!(key in nextRow)) {
        writtenColumns += 1;
      }
      nextRow[key] = sanitizeCellValue(value, maxCellLength);
      columnSet.add(key);
    }

    if (Object.keys(nextRow).length > 0) {
      sanitized.push(nextRow);
    }
  }

  return {
    rows: sanitized,
    totalInputRows,
    truncated,
    columns: Array.from(columnSet),
  };
}

function collectColumns(rows) {
  const set = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || typeof row !== "object" || Array.isArray(row)) {
      continue;
    }
    for (const key of Object.keys(row)) {
      const normalized = normalizeColumnName(key);
      if (normalized) {
        set.add(normalized);
      }
    }
  }
  return Array.from(set);
}

function buildTablePreview(rows, options = {}) {
  const maxRows = clampInt(options.maxRows, 20, 1);
  const maxColumns = clampInt(options.maxColumns, 16, 1);
  const allColumns = collectColumns(rows);
  const previewColumns = allColumns.slice(0, maxColumns);

  const previewRows = (Array.isArray(rows) ? rows : [])
    .slice(0, maxRows)
    .map((row) =>
      previewColumns.map((column) => toCellString(row?.[column], 160))
    );

  return {
    allColumns,
    previewColumns,
    previewRows,
    totalRows: Array.isArray(rows) ? rows.length : 0,
    totalColumns: allColumns.length,
  };
}

function shuffleArray(input) {
  const list = [...input];
  for (let i = list.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [list[i], list[j]] = [list[j], list[i]];
  }
  return list;
}

function sampleRowsForModel(rows, sampleSize = 40) {
  const input = Array.isArray(rows) ? rows : [];
  const safeSampleSize = clampInt(sampleSize, 40, 1);
  if (input.length <= safeSampleSize) {
    return input;
  }

  const headSize = Math.min(12, Math.floor(safeSampleSize / 3));
  const head = input.slice(0, headSize);
  const tailPool = input.slice(headSize);
  const randomTail = shuffleArray(tailPool).slice(0, safeSampleSize - head.length);
  return [...head, ...randomTail];
}

function summarizeColumns(rows, maxColumns = 80) {
  const cols = collectColumns(rows).slice(0, clampInt(maxColumns, 80, 1));
  return cols.map((column) => {
    let nonNull = 0;
    const samples = [];

    for (const row of rows) {
      const value = row?.[column];
      if (value === null || value === undefined || String(value).trim() === "") {
        continue;
      }
      nonNull += 1;
      if (samples.length < 6) {
        samples.push(toCellString(value, 80));
      }
    }

    return {
      column,
      nonNull,
      sampleValues: samples,
    };
  });
}

function buildModelInputSummary(rows, options = {}) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const sample = sampleRowsForModel(safeRows, options.sampleRows || 40);
  const sampleColumns = collectColumns(sample).slice(0, clampInt(options.maxColumns, 48, 1));

  return {
    totalRows: safeRows.length,
    sampledRows: sample.length,
    sampledColumns: sampleColumns,
    columnStats: summarizeColumns(sample, options.maxColumns || 48),
    sampleRows: sample.slice(0, clampInt(options.maxRowsInPrompt, 40, 1)).map((row) => {
      const next = {};
      for (const column of sampleColumns) {
        next[column] = toCellString(row?.[column], options.maxCellLength || 140);
      }
      return next;
    }),
  };
}

function deriveActionFromCommand(commandRaw) {
  const command = String(commandRaw || "").trim().toLowerCase();
  if (!command) {
    return "analyze";
  }

  const formatKeywords = [
    "format",
    "양식",
    "정갈",
    "표준화",
    "포맷",
    "normalize format",
    "날짜 포맷",
    "date format",
  ];
  if (formatKeywords.some((keyword) => command.includes(keyword))) {
    return "format";
  }

  const cleanKeywords = [
    "clean",
    "cleanup",
    "정제",
    "수정",
    "correct",
    "fix",
    "정리",
    "변경",
    "바꿔",
    "바꿔줘",
    "rename",
    "이름 변경",
    "통일",
    "삭제",
    "제거",
    "drop",
    "replace",
    "치환",
  ];
  if (cleanKeywords.some((keyword) => command.includes(keyword))) {
    return "clean";
  }

  const analysisKeywords = [
    "analyze",
    "analysis",
    "분석",
    "알려",
    "보여",
    "조회",
    "확인",
    "몇",
    "통계",
    "요약",
    "인사이트",
    "value",
    "values",
    "count",
    "분포",
  ];
  if (analysisKeywords.some((keyword) => command.includes(keyword))) {
    return "analyze";
  }

  return "analyze";
}

function extractJsonObjectText(text) {
  const raw = String(text || "").trim();
  if (!raw) {
    return "";
  }

  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced && fenced[1]) {
    return fenced[1].trim();
  }

  const start = raw.indexOf("{");
  const end = raw.lastIndexOf("}");
  if (start >= 0 && end > start) {
    return raw.slice(start, end + 1);
  }

  return raw;
}

function parseJsonObject(text) {
  const candidate = extractJsonObjectText(text);
  if (!candidate) {
    return {};
  }

  try {
    const parsed = JSON.parse(candidate);
    if (parsed && typeof parsed === "object") {
      return parsed;
    }
  } catch {
    // ignore
  }

  return {};
}

function toColumnList(value) {
  if (Array.isArray(value)) {
    return [...new Set(value.map((entry) => normalizeColumnName(entry)).filter(Boolean))];
  }
  if (typeof value === "string") {
    return [...new Set(value.split(",").map((entry) => normalizeColumnName(entry)).filter(Boolean))];
  }
  return [];
}

function normalizeMappings(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return {};
  }

  const next = {};
  for (const [keyRaw, valueRaw] of Object.entries(input)) {
    const key = String(keyRaw || "").trim();
    if (!key) {
      continue;
    }
    const value = String(valueRaw ?? "").trim();
    if (!value) {
      continue;
    }
    next[key] = value;
  }
  return next;
}

function normalizeOperation(operation) {
  if (!operation || typeof operation !== "object" || Array.isArray(operation)) {
    return null;
  }

  const type = String(operation.type || "").trim().toLowerCase();
  if (!type) {
    return null;
  }

  if (type === "rename_column") {
    const from = normalizeColumnName(operation.from);
    const to = normalizeColumnName(operation.to);
    if (!from || !to || from === to) {
      return null;
    }
    return { type, from, to };
  }

  if (type === "trim_whitespace") {
    return { type, columns: toColumnList(operation.columns) };
  }

  if (type === "normalize_empty_to_null") {
    return { type, columns: toColumnList(operation.columns) };
  }

  if (type === "value_map") {
    const column = normalizeColumnName(operation.column);
    const mappings = normalizeMappings(operation.mappings);
    if (!column || Object.keys(mappings).length === 0) {
      return null;
    }
    return {
      type,
      column,
      mappings,
      caseInsensitive: operation.caseInsensitive !== false,
    };
  }

  if (type === "parse_number") {
    const column = normalizeColumnName(operation.column);
    if (!column) {
      return null;
    }
    return { type, column };
  }

  if (type === "normalize_date") {
    const column = normalizeColumnName(operation.column);
    if (!column) {
      return null;
    }
    return {
      type,
      column,
      outputFormat:
        String(operation.outputFormat || "YYYY-MM-DD").trim() || "YYYY-MM-DD",
    };
  }

  if (type === "drop_columns") {
    const columns = toColumnList(operation.columns);
    if (columns.length === 0) {
      return null;
    }
    return { type, columns };
  }

  return null;
}

function parseModelTransformPlan(text, fallbackAction = "analyze") {
  const parsed = parseJsonObject(text);
  const parsedKeys = parsed && typeof parsed === "object" ? Object.keys(parsed) : [];
  const rawText = String(text || "").trim();
  const looksLikeSseTranscript = /(?:^|\n)\s*(?:event|data)\s*:/i.test(rawText);
  const actionRaw = String(parsed.action || "").trim().toLowerCase();
  const action = ["analyze", "clean", "format"].includes(actionRaw)
    ? actionRaw
    : fallbackAction;

  const fallbackReport = parsedKeys.length === 0 && !looksLikeSseTranscript ? rawText : "";
  const report = truncateText(String(parsed.report || fallbackReport || "").trim(), 12000);
  const operationsRaw = Array.isArray(parsed.operations) ? parsed.operations : [];
  const operations = operationsRaw
    .map((operation) => normalizeOperation(operation))
    .filter(Boolean);

  return {
    action,
    report,
    operations,
    raw: parsed,
  };
}

function parseNumberValue(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  const raw = String(value).trim();
  if (!raw) {
    return null;
  }

  const negativeByParens = /^\((.*)\)$/.test(raw);
  const core = negativeByParens ? raw.slice(1, -1) : raw;
  const cleaned = core
    .replace(/[%,$₩€£]/g, "")
    .replace(/\s+/g, "")
    .replace(/,/g, "");

  if (!cleaned || cleaned === "-" || cleaned === ".") {
    return null;
  }

  const number = Number(cleaned);
  if (!Number.isFinite(number)) {
    return null;
  }

  const signed = negativeByParens ? -number : number;
  return Number.isFinite(signed) ? signed : null;
}

function parseDateValue(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const formatDate = (year, month, day) =>
    `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(
      day
    ).padStart(2, "0")}`;
  const fromDateObject = (date) =>
    formatDate(date.getFullYear(), date.getMonth() + 1, date.getDate());

  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return fromDateObject(value);
  }

  const raw = String(value).trim();
  if (!raw) {
    return null;
  }

  const normalizedIso = raw.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:$|[T\s])/);
  if (normalizedIso) {
    return formatDate(
      Number(normalizedIso[1]),
      Number(normalizedIso[2]),
      Number(normalizedIso[3])
    );
  }

  const m = raw.match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2,4})$/);
  if (m) {
    let first = Number(m[1]);
    let second = Number(m[2]);
    let year = Number(m[3]);
    if (year < 100) {
      year += year >= 70 ? 1900 : 2000;
    }

    let month = first;
    let day = second;
    if (first > 12 && second <= 12) {
      month = second;
      day = first;
    }

    const date = new Date(year, month - 1, day);
    if (!Number.isFinite(date.getTime())) {
      return null;
    }

    return fromDateObject(date);
  }

  const parsed = new Date(raw);
  if (Number.isFinite(parsed.getTime())) {
    return fromDateObject(parsed);
  }

  return null;
}

function isNullLike(value) {
  if (value === null || value === undefined) {
    return true;
  }
  const text = String(value).trim().toLowerCase();
  return text === "" || text === "n/a" || text === "na" || text === "none" || text === "null" || text === "-" || text === "--";
}

function applyModelTransformPlan(rows, plan) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const workingRows = sourceRows.map((row) => ({ ...row }));
  const operations = Array.isArray(plan?.operations) ? plan.operations : [];

  let modifiedCells = 0;
  let removedColumns = 0;

  for (const operation of operations) {
    if (!operation || typeof operation !== "object") {
      continue;
    }

    if (operation.type === "rename_column") {
      for (const row of workingRows) {
        if (!(operation.from in row)) {
          continue;
        }
        const nextValue = row[operation.from];
        const currentValue = row[operation.to];
        if (currentValue === undefined || currentValue === null || String(currentValue).trim() === "") {
          row[operation.to] = nextValue;
          modifiedCells += 1;
        }
        delete row[operation.from];
      }
      continue;
    }

    if (operation.type === "trim_whitespace") {
      const targetColumns = operation.columns?.length > 0 ? operation.columns : collectColumns(workingRows);
      for (const row of workingRows) {
        for (const column of targetColumns) {
          if (!(column in row)) {
            continue;
          }
          const value = row[column];
          if (typeof value !== "string") {
            continue;
          }
          const trimmed = value.trim().replace(/\s+/g, " ");
          if (trimmed !== value) {
            row[column] = trimmed;
            modifiedCells += 1;
          }
        }
      }
      continue;
    }

    if (operation.type === "normalize_empty_to_null") {
      const targetColumns = operation.columns?.length > 0 ? operation.columns : collectColumns(workingRows);
      for (const row of workingRows) {
        for (const column of targetColumns) {
          if (!(column in row)) {
            continue;
          }
          const value = row[column];
          if (value !== null && isNullLike(value)) {
            row[column] = null;
            modifiedCells += 1;
          }
        }
      }
      continue;
    }

    if (operation.type === "value_map") {
      const map = new Map();
      for (const [rawKey, rawValue] of Object.entries(operation.mappings || {})) {
        const key = operation.caseInsensitive
          ? String(rawKey || "").trim().toLowerCase()
          : String(rawKey || "").trim();
        const value = String(rawValue || "").trim();
        if (key && value) {
          map.set(key, value);
        }
      }

      for (const row of workingRows) {
        if (!(operation.column in row)) {
          continue;
        }
        const source = row[operation.column];
        if (source === null || source === undefined) {
          continue;
        }
        const key = operation.caseInsensitive
          ? String(source).trim().toLowerCase()
          : String(source).trim();
        if (!map.has(key)) {
          continue;
        }
        const nextValue = map.get(key);
        if (String(source) !== String(nextValue)) {
          row[operation.column] = nextValue;
          modifiedCells += 1;
        }
      }
      continue;
    }

    if (operation.type === "parse_number") {
      for (const row of workingRows) {
        if (!(operation.column in row)) {
          continue;
        }
        const current = row[operation.column];
        const next = parseNumberValue(current);
        if (next === null) {
          continue;
        }
        if (typeof current !== "number" || current !== next) {
          row[operation.column] = next;
          modifiedCells += 1;
        }
      }
      continue;
    }

    if (operation.type === "normalize_date") {
      for (const row of workingRows) {
        if (!(operation.column in row)) {
          continue;
        }
        const current = row[operation.column];
        const next = parseDateValue(current);
        if (!next) {
          continue;
        }
        if (String(current) !== String(next)) {
          row[operation.column] = next;
          modifiedCells += 1;
        }
      }
      continue;
    }

    if (operation.type === "drop_columns") {
      const columns = Array.isArray(operation.columns) ? operation.columns : [];
      for (const row of workingRows) {
        for (const column of columns) {
          if (column in row) {
            delete row[column];
            removedColumns += 1;
          }
        }
      }
    }
  }

  const cleanedRows = [];
  let droppedRows = 0;

  for (const row of workingRows) {
    const hasData = Object.values(row).some((value) => !isNullLike(value));
    if (!hasData) {
      droppedRows += 1;
      continue;
    }
    cleanedRows.push(row);
  }

  const columns = collectColumns(cleanedRows);
  return {
    rows: cleanedRows,
    columns,
    stats: {
      inputRows: sourceRows.length,
      outputRows: cleanedRows.length,
      modifiedCells,
      droppedRows,
      removedColumns,
      normalizedColumns: columns.length,
      operationsApplied: operations.length,
    },
  };
}

function toCsvValue(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function escapeCsvCell(value) {
  const text = toCsvValue(value);
  if (/[,"\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function rowsToCsv(rows, preferredColumns = null) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const columns = Array.isArray(preferredColumns) && preferredColumns.length > 0
    ? preferredColumns
    : collectColumns(sourceRows);

  const header = columns.map((column) => escapeCsvCell(column)).join(",");
  const body = sourceRows
    .map((row) => columns.map((column) => escapeCsvCell(row?.[column])).join(","))
    .join("\n");

  return `${header}\n${body}`;
}

function toDiffComparableValue(value) {
  if (value === undefined) {
    return "__undefined__";
  }
  if (value === null) {
    return null;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function toDiffDisplayValue(value, maxLength = 120) {
  if (value === null || value === undefined) {
    return "(empty)";
  }
  const text = toCellString(value, maxLength);
  return text ? text : "(empty)";
}

function buildRowsDiffPreview(beforeRows, afterRows, options = {}) {
  const sourceRows = Array.isArray(beforeRows) ? beforeRows : [];
  const targetRows = Array.isArray(afterRows) ? afterRows : [];
  const maxCells = clampInt(options.maxCells, 120, 1);
  const maxRows = clampInt(options.maxRows, 80, 1);
  const maxValueLength = clampInt(options.maxValueLength, 120, 16);

  const minLength = Math.min(sourceRows.length, targetRows.length);
  const changedCells = [];
  const changedRowSet = new Set();
  let totalChangedCells = 0;

  for (let rowIndex = 0; rowIndex < minLength; rowIndex += 1) {
    const before = sourceRows[rowIndex] && typeof sourceRows[rowIndex] === "object"
      ? sourceRows[rowIndex]
      : {};
    const after = targetRows[rowIndex] && typeof targetRows[rowIndex] === "object"
      ? targetRows[rowIndex]
      : {};

    const columns = [...new Set([...Object.keys(before), ...Object.keys(after)])];
    let rowChanged = false;

    for (const column of columns) {
      const beforeValue = before[column];
      const afterValue = after[column];
      if (toDiffComparableValue(beforeValue) === toDiffComparableValue(afterValue)) {
        continue;
      }

      rowChanged = true;
      totalChangedCells += 1;
      if (changedCells.length < maxCells && changedRowSet.size < maxRows) {
        changedCells.push({
          rowIndex,
          column,
          before: toDiffDisplayValue(beforeValue, maxValueLength),
          after: toDiffDisplayValue(afterValue, maxValueLength),
        });
      }
    }

    if (rowChanged) {
      changedRowSet.add(rowIndex);
    }
  }

  const droppedRows = Math.max(0, sourceRows.length - targetRows.length);
  const addedRows = Math.max(0, targetRows.length - sourceRows.length);
  const totalChangedRows = changedRowSet.size + droppedRows + addedRows;

  return {
    changedRowCount: totalChangedRows,
    changedCellCount: totalChangedCells,
    changedCells,
    droppedRowsEstimate: droppedRows,
    addedRowsEstimate: addedRows,
    truncated: totalChangedCells > changedCells.length,
  };
}

module.exports = {
  buildModelInputSummary,
  buildTablePreview,
  buildRowsDiffPreview,
  collectColumns,
  deriveActionFromCommand,
  parseModelTransformPlan,
  applyModelTransformPlan,
  rowsToCsv,
  sanitizeExecutionRows,
};
