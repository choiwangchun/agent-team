function normalizeText(value) {
  if (value === null || value === undefined) {
    return "";
  }

  return String(value)
    .normalize("NFKC")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
}

function slugify(value) {
  const normalized = normalizeText(value).replace(/[^a-z0-9가-힣 ]/g, "");
  const slug = normalized.trim().replace(/\s+/g, "_");
  return slug || "field";
}

function normalizeScope(value) {
  const text = normalizeText(value);
  return text.length === 0 ? "*" : text;
}

function normalizeColumnName(value) {
  return normalizeText(value);
}

function isEmpty(value) {
  return (
    value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim().length === 0)
  );
}

function cleanCell(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length === 0 ? null : trimmed;
  }
  return value;
}

const DEFAULT_FIELDS = [
  {
    key: "gender",
    label: "Gender",
    aliases: [
      "gender",
      "sex",
      "gender type",
      "boy/girl",
      "성별",
      "male",
      "female",
      "boy",
      "girl",
      "man",
      "woman",
    ],
    valueAliases: {
      male: ["male", "m", "man", "boy", "남", "남성", "남자", "소년"],
      female: ["female", "f", "woman", "girl", "여", "여성", "여자", "소녀"],
      unknown: ["unknown", "other", "na", "n/a", "none", "미상", "기타"],
    },
  },
  {
    key: "name",
    label: "Name",
    aliases: ["name", "full name", "person name", "customer name", "이름", "성명"],
  },
  {
    key: "age",
    label: "Age",
    aliases: ["age", "years", "age years", "연령", "나이"],
  },
  {
    key: "company",
    label: "Company",
    aliases: ["company", "company name", "organization", "org", "회사", "회사명"],
  },
  {
    key: "email",
    label: "Email",
    aliases: ["email", "email address", "mail", "e-mail", "이메일"],
  },
];

class OntologyService {
  constructor() {
    this.fields = new Map();
    this.headerAliasIndex = new Map();
    this.columnOverrides = new Map();
    this.registerFields(DEFAULT_FIELDS);
  }

  getDefaultFields() {
    return DEFAULT_FIELDS.map((field) => this.serializeField(this.normalizeFieldDefinition(field)));
  }

  hasField(fieldKey) {
    return this.fields.has(String(fieldKey || "").trim());
  }

  loadFields(fields = []) {
    this.fields.clear();
    this.headerAliasIndex.clear();

    if (!Array.isArray(fields) || fields.length === 0) {
      this.registerFields(DEFAULT_FIELDS);
      return this.listFields();
    }

    this.registerFields(fields);
    return this.listFields();
  }

  registerFields(fields = []) {
    if (!Array.isArray(fields) || fields.length === 0) {
      throw new Error("fields must be a non-empty array");
    }

    const normalizedFields = fields.map((field) => this.normalizeFieldDefinition(field));

    for (const field of normalizedFields) {
      this.assertAliasConflicts(field);
      this.fields.set(field.key, field);
    }

    this.rebuildHeaderAliasIndex();
    return normalizedFields.map((field) => this.serializeField(field));
  }

  listFields() {
    return Array.from(this.fields.values()).map((field) => this.serializeField(field));
  }

  loadOverrides(overrides = []) {
    this.columnOverrides.clear();

    if (!Array.isArray(overrides)) {
      return [];
    }

    for (const override of overrides) {
      const companyScope = normalizeScope(override.companyScope || override.companyName);
      const sourceColumn = String(override.sourceColumn || "").trim();
      const sourceColumnNorm = normalizeColumnName(
        override.sourceColumnNorm || override.sourceColumn
      );
      const canonicalField = String(override.canonicalField || "").trim();

      if (!sourceColumnNorm || !canonicalField) {
        continue;
      }

      const key = this.overrideKey(companyScope, sourceColumnNorm);
      this.columnOverrides.set(key, {
        id: override.id || null,
        companyScope,
        sourceColumn,
        sourceColumnNorm,
        canonicalField,
        createdAt: override.createdAt || null,
        updatedAt: override.updatedAt || null,
      });
    }

    return this.listOverrides();
  }

  listOverrides() {
    return Array.from(this.columnOverrides.values()).map((entry) => ({ ...entry }));
  }

  upsertOverride({ companyName, sourceColumn, canonicalField }) {
    const companyScope = normalizeScope(companyName);
    const sourceColumnNorm = normalizeColumnName(sourceColumn);

    if (!sourceColumnNorm) {
      throw new Error("sourceColumn is required");
    }

    if (!this.fields.has(canonicalField)) {
      throw new Error(`canonical field not found: ${canonicalField}`);
    }

    const key = this.overrideKey(companyScope, sourceColumnNorm);
    const next = {
      id: null,
      companyScope,
      sourceColumn: String(sourceColumn || "").trim(),
      sourceColumnNorm,
      canonicalField,
      createdAt: null,
      updatedAt: null,
    };

    this.columnOverrides.set(key, next);
    return { ...next };
  }

  mapRows({ companyName, rows }) {
    if (!Array.isArray(rows) || rows.length === 0) {
      throw new Error("rows must be a non-empty array");
    }

    const safeRows = rows.map((row) => (row && typeof row === "object" ? row : {}));
    const columnMapping = this.buildColumnMapping(safeRows, companyName);
    const normalizedRows = this.normalizeRows(safeRows, columnMapping);

    return {
      rowCount: normalizedRows.length,
      columnMapping,
      normalizedRows,
      sampleRows: normalizedRows.slice(0, 10),
    };
  }

  mergeDatasets({ datasets, limit = 5000 } = {}) {
    if (!Array.isArray(datasets) || datasets.length === 0) {
      return {
        merged: {
          selectedDatasets: [],
          columns: [],
          totalRows: 0,
          returnedRows: 0,
          truncated: false,
          rows: [],
        },
      };
    }

    const selected = datasets.map((dataset) => ({
      id: dataset.id,
      companyName: dataset.companyName,
      sourceName: dataset.sourceName,
      rowCount: dataset.rowCount,
    }));

    const allRows = [];
    const allColumns = new Set();

    for (const dataset of datasets) {
      const rows = Array.isArray(dataset.normalizedRows) ? dataset.normalizedRows : [];
      for (const row of rows) {
        const mergedRow = {
          _datasetId: dataset.id,
          _companyName: dataset.companyName,
          ...row,
        };
        allRows.push(mergedRow);
        Object.keys(mergedRow).forEach((key) => allColumns.add(key));
      }
    }

    const safeLimit = Math.max(1, Number(limit) || 5000);
    const truncated = allRows.length > safeLimit;
    const rows = truncated ? allRows.slice(0, safeLimit) : allRows;

    return {
      merged: {
        selectedDatasets: selected,
        columns: Array.from(allColumns),
        totalRows: allRows.length,
        returnedRows: rows.length,
        truncated,
        rows,
      },
    };
  }

  buildColumnMapping(rows, companyName) {
    const sourceColumns = this.collectSourceColumns(rows);
    const companyScope = normalizeScope(companyName);

    return sourceColumns.map((sourceColumn) => {
      const sourceColumnNorm = normalizeColumnName(sourceColumn);

      const override =
        this.columnOverrides.get(this.overrideKey(companyScope, sourceColumnNorm)) ||
        this.columnOverrides.get(this.overrideKey("*", sourceColumnNorm));

      if (override) {
        return {
          sourceColumn,
          canonicalField: override.canonicalField,
          confidence: 1,
          reason: "override",
          companyScope: override.companyScope,
        };
      }

      const directMatch = this.headerAliasIndex.get(normalizeText(sourceColumn));
      if (directMatch) {
        return {
          sourceColumn,
          canonicalField: directMatch,
          confidence: 1,
          reason: "header-alias",
        };
      }

      const values = rows.map((row) => row[sourceColumn]).filter((value) => !isEmpty(value));
      const inferred = this.inferFieldByValues(values);

      if (inferred) {
        return {
          sourceColumn,
          canonicalField: inferred.fieldKey,
          confidence: inferred.confidence,
          reason: "value-pattern",
        };
      }

      return {
        sourceColumn,
        canonicalField: `custom.${slugify(sourceColumn)}`,
        confidence: 0.5,
        reason: "fallback-custom",
      };
    });
  }

  normalizeRows(rows, columnMapping) {
    return rows.map((row) => this.normalizeSingleRow(row, columnMapping));
  }

  normalizeSingleRow(row, columnMapping) {
    const normalized = {};
    const conflicts = [];

    for (const mapping of columnMapping) {
      const rawValue = cleanCell(row[mapping.sourceColumn]);
      if (rawValue === null) {
        continue;
      }

      const canonicalField = mapping.canonicalField;
      const normalizedValue = this.normalizeValue(canonicalField, rawValue);

      if (normalized[canonicalField] === undefined || normalized[canonicalField] === null) {
        normalized[canonicalField] = normalizedValue;
        continue;
      }

      if (normalized[canonicalField] !== normalizedValue) {
        conflicts.push({
          field: canonicalField,
          existingValue: normalized[canonicalField],
          ignoredValue: normalizedValue,
          sourceColumn: mapping.sourceColumn,
        });
      }
    }

    if (conflicts.length > 0) {
      normalized._conflicts = conflicts;
    }

    return normalized;
  }

  normalizeValue(fieldKey, value) {
    const cleanValue = cleanCell(value);
    if (cleanValue === null) {
      return null;
    }

    const field = this.fields.get(fieldKey);
    if (!field || !field.valueLookup) {
      return cleanValue;
    }

    const normalized = normalizeText(cleanValue);
    if (field.valueLookup.has(normalized)) {
      return field.valueLookup.get(normalized);
    }

    return cleanValue;
  }

  inferFieldByValues(values) {
    if (!Array.isArray(values) || values.length === 0) {
      return null;
    }

    let best = null;

    for (const field of this.fields.values()) {
      if (!field.valueLookup || field.valueLookup.size === 0) {
        continue;
      }

      let matched = 0;
      let total = 0;
      const sample = values.slice(0, 50);

      for (const value of sample) {
        const normalized = normalizeText(value);
        if (!normalized) {
          continue;
        }
        total += 1;
        if (field.valueLookup.has(normalized)) {
          matched += 1;
        }
      }

      if (total === 0) {
        continue;
      }

      const ratio = matched / total;
      if (matched >= 2 && ratio >= 0.6) {
        const candidate = {
          fieldKey: field.key,
          confidence: Number(ratio.toFixed(3)),
        };
        if (!best || candidate.confidence > best.confidence) {
          best = candidate;
        }
      }
    }

    return best;
  }

  collectSourceColumns(rows) {
    const columns = new Set();
    for (const row of rows) {
      Object.keys(row).forEach((key) => columns.add(String(key).trim()));
    }
    return Array.from(columns).filter((key) => key.length > 0);
  }

  rebuildHeaderAliasIndex() {
    this.headerAliasIndex.clear();
    for (const field of this.fields.values()) {
      for (const alias of field.aliases) {
        this.headerAliasIndex.set(normalizeText(alias), field.key);
      }
      this.headerAliasIndex.set(normalizeText(field.key), field.key);
    }
  }

  assertAliasConflicts(nextField) {
    for (const existingField of this.fields.values()) {
      if (existingField.key === nextField.key) {
        continue;
      }

      for (const alias of nextField.aliases) {
        if (
          existingField.aliases.some(
            (existingAlias) => normalizeText(existingAlias) === normalizeText(alias)
          )
        ) {
          throw new Error(
            `alias conflict: "${alias}" is already used by field "${existingField.key}"`
          );
        }
      }
    }
  }

  normalizeFieldDefinition(field) {
    if (!field || typeof field !== "object") {
      throw new Error("each field must be an object");
    }

    const key = String(field.key || "").trim();
    if (!key) {
      throw new Error("field.key is required");
    }

    const aliases = Array.isArray(field.aliases) ? field.aliases : [];
    const normalizedAliases = Array.from(
      new Set(
        [key, ...aliases]
          .map((alias) => String(alias || "").trim())
          .filter((alias) => alias.length > 0)
      )
    );

    const normalizedField = {
      key,
      label: String(field.label || key),
      aliases: normalizedAliases,
      valueAliases: {},
      valueLookup: new Map(),
    };

    const sourceValueAliases =
      field.valueAliases && typeof field.valueAliases === "object"
        ? field.valueAliases
        : {};

    for (const [canonicalValue, synonyms] of Object.entries(sourceValueAliases)) {
      const canonical = String(canonicalValue || "").trim();
      if (!canonical) {
        continue;
      }

      const list = Array.isArray(synonyms) ? synonyms : [];
      const merged = Array.from(
        new Set(
          [canonical, ...list]
            .map((entry) => String(entry || "").trim())
            .filter((entry) => entry.length > 0)
        )
      );

      normalizedField.valueAliases[canonical] = merged;

      for (const alias of merged) {
        normalizedField.valueLookup.set(normalizeText(alias), canonical);
      }
    }

    return normalizedField;
  }

  serializeField(field) {
    return {
      key: field.key,
      label: field.label,
      aliases: field.aliases,
      valueAliases: field.valueAliases,
    };
  }

  overrideKey(companyScope, sourceColumnNorm) {
    return `${companyScope}::${sourceColumnNorm}`;
  }
}

module.exports = {
  OntologyService,
  normalizeScope,
  normalizeColumnName,
};
