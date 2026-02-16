#!/usr/bin/env python3
import json
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Tuple


NULL_LIKE_VALUES = {"", "n/a", "na", "none", "null", "-", "--"}


def collect_columns(rows: List[Dict[str, Any]]) -> List[str]:
    seen = set()
    columns = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            normalized = str(key or "").strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                columns.append(normalized)
    return columns


def is_null_like(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip().lower()
    return text in NULL_LIKE_VALUES


def parse_number_value(value: Any):
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value

    raw = str(value).strip()
    if not raw:
        return None

    negative_by_parens = raw.startswith("(") and raw.endswith(")")
    core = raw[1:-1] if negative_by_parens else raw
    cleaned = (
        core.replace("%", "")
        .replace("$", "")
        .replace("₩", "")
        .replace("€", "")
        .replace("£", "")
        .replace(",", "")
        .replace(" ", "")
    )

    if cleaned in {"", "-", "."}:
        return None

    try:
        number = float(cleaned)
    except Exception:
        return None

    if negative_by_parens:
        number = -number

    if number.is_integer():
        return int(number)
    return number


def parse_date_value(value: Any):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    raw = str(value).strip()
    if not raw:
        return None

    iso_match = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:$|[T\s])", raw)
    if iso_match:
        y, m, d = int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3))
        try:
            return datetime(y, m, d).strftime("%Y-%m-%d")
        except Exception:
            return None

    md_match = re.match(r"^(\d{1,2})[\/\.-](\d{1,2})[\/\.-](\d{2,4})$", raw)
    if md_match:
        first = int(md_match.group(1))
        second = int(md_match.group(2))
        year = int(md_match.group(3))
        if year < 100:
            year += 1900 if year >= 70 else 2000

        month = first
        day = second
        if first > 12 and second <= 12:
            month = second
            day = first

        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except Exception:
            return None

    format_candidates = (
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
    )
    for fmt in format_candidates:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue

    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return None


def normalize_columns_input(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def normalize_mappings(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in value.items():
        key = str(k).strip()
        mapped = str(v).strip()
        if key and mapped:
            out[key] = mapped
    return out


def apply_operations(rows: List[Dict[str, Any]], operations: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    working_rows: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            working_rows.append(dict(row))

    modified_cells = 0
    removed_columns = 0

    for operation in operations:
        if not isinstance(operation, dict):
            continue
        op_type = str(operation.get("type", "")).strip().lower()
        if not op_type:
            continue

        if op_type == "rename_column":
            source = str(operation.get("from", "")).strip()
            target = str(operation.get("to", "")).strip()
            if not source or not target or source == target:
                continue
            for row in working_rows:
                if source not in row:
                    continue
                next_value = row.get(source)
                current_value = row.get(target)
                if current_value is None or str(current_value).strip() == "":
                    row[target] = next_value
                    modified_cells += 1
                row.pop(source, None)
            continue

        if op_type == "trim_whitespace":
            columns = normalize_columns_input(operation.get("columns"))
            for row in working_rows:
                target_columns = columns if columns else list(row.keys())
                for column in target_columns:
                    if column not in row:
                        continue
                    value = row.get(column)
                    if not isinstance(value, str):
                        continue
                    trimmed = re.sub(r"\s+", " ", value.strip())
                    if trimmed != value:
                        row[column] = trimmed
                        modified_cells += 1
            continue

        if op_type == "normalize_empty_to_null":
            columns = normalize_columns_input(operation.get("columns"))
            for row in working_rows:
                target_columns = columns if columns else list(row.keys())
                for column in target_columns:
                    if column not in row:
                        continue
                    value = row.get(column)
                    if value is not None and is_null_like(value):
                        row[column] = None
                        modified_cells += 1
            continue

        if op_type == "value_map":
            column = str(operation.get("column", "")).strip()
            mappings = normalize_mappings(operation.get("mappings"))
            if not column or not mappings:
                continue
            case_insensitive = operation.get("caseInsensitive", True) is not False
            mapped = {}
            for src, dst in mappings.items():
                key = src.strip().lower() if case_insensitive else src.strip()
                if key:
                    mapped[key] = dst
            for row in working_rows:
                if column not in row:
                    continue
                source_value = row.get(column)
                if source_value is None:
                    continue
                source_key = str(source_value).strip()
                lookup_key = source_key.lower() if case_insensitive else source_key
                if lookup_key not in mapped:
                    continue
                next_value = mapped[lookup_key]
                if str(source_value) != str(next_value):
                    row[column] = next_value
                    modified_cells += 1
            continue

        if op_type == "parse_number":
            column = str(operation.get("column", "")).strip()
            if not column:
                continue
            for row in working_rows:
                if column not in row:
                    continue
                current = row.get(column)
                next_value = parse_number_value(current)
                if next_value is None:
                    continue
                if current != next_value:
                    row[column] = next_value
                    modified_cells += 1
            continue

        if op_type == "normalize_date":
            column = str(operation.get("column", "")).strip()
            if not column:
                continue
            for row in working_rows:
                if column not in row:
                    continue
                current = row.get(column)
                next_value = parse_date_value(current)
                if not next_value:
                    continue
                if str(current) != str(next_value):
                    row[column] = next_value
                    modified_cells += 1
            continue

        if op_type == "drop_columns":
            columns = normalize_columns_input(operation.get("columns"))
            if not columns:
                continue
            for row in working_rows:
                for column in columns:
                    if column in row:
                        row.pop(column, None)
                        removed_columns += 1
            continue

    cleaned_rows = []
    dropped_rows = 0
    for row in working_rows:
        has_data = any(not is_null_like(value) for value in row.values())
        if not has_data:
            dropped_rows += 1
            continue
        cleaned_rows.append(row)

    columns = collect_columns(cleaned_rows)
    stats = {
        "inputRows": len(rows),
        "outputRows": len(cleaned_rows),
        "modifiedCells": modified_cells,
        "droppedRows": dropped_rows,
        "removedColumns": removed_columns,
        "normalizedColumns": len(columns),
        "operationsApplied": len(operations),
    }
    return cleaned_rows, stats


def main():
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
        rows = payload.get("rows")
        operations = payload.get("operations")
        if not isinstance(rows, list):
            raise ValueError("rows must be an array")
        if not isinstance(operations, list):
            operations = []
        result_rows, stats = apply_operations(rows, operations)
        sys.stdout.write(
            json.dumps(
                {"rows": result_rows, "stats": stats},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    except Exception as error:
        sys.stderr.write(str(error))
        sys.exit(1)


if __name__ == "__main__":
    main()
