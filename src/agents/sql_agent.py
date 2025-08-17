# filepath: /sql-generator/sql-generator/src/agents/sql_agent.py
import os
import json
from typing import Dict, Any

from openai import OpenAI
import json as _json
import re
import logging

logger = logging.getLogger(__name__)


def load_tables(json_path: str) -> Dict[str, Any]:
    """Load table metadata from a JSON file.

    The JSON should contain an object with table names as keys and per-table
    metadata including columns and human meanings.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_messages(question: str, tables: Dict[str, Any]) -> list:
    """Build the chat messages to send to the Kimi model.

    The system message forces the assistant to only return SQL statements
    with no explanations or markdown. The user message includes the tables
    metadata (serialized) and the user's NL question.
    """
    system = {
        "role": "system",
        "content": (
            "You are an expert SQL generator.\n"
            "Rules: Return ONLY the SQL statements required to answer the\n"
            "user's request.\n"
            "Do NOT return explanations, commentary, markdown, backticks, or\n"
            "code fences.\n"
            "Use the table and column names exactly as provided. If the\n"
            "request is ambiguous, choose a reasonable, minimal\n"
            "interpretation and produce safe SQL.\n"
            "Target Oracle SQL (Oracle 12c+). Use Oracle-compatible syntax\n"
            "and data types where relevant. Do NOT invent columns or\n"
            "tables. Ensure any attributes returned exist in the provided\n"
            "table metadata; the assistant must not return SQL that refers\n"
            "to unknown columns or tables."
        ),
    }

    tables_serialized = json.dumps(tables, ensure_ascii=False, indent=2)

    user = {
        "role": "user",
        "content": (
            "I will provide database table metadata in JSON.\n"
            "Using only that metadata, generate SQL that answers the\n"
            "question.\n"
            "Do NOT invent columns or tables.\n\n"
            "TABLES JSON:\n" + tables_serialized + "\n\n"
            "QUESTION: " + question + "\n\n"
            "Output rules reminder: ONLY SQL. No comments, no explanation,\n"
            "no markdown."
        ),
    }

    return [system, user]


def summarize_tables(
    tables: Dict[str, Any], max_chars: int = 8000
) -> Dict[str, Any]:
    """Create a compact representation of the tables for prompting.

    If the full serialized JSON would be too large (token-heavy), return a
    compact dict that lists table names and column names (and types when
    available). This helps avoid large max_tokens usage and 402 errors.
    """
    compact = {}
    for tname, meta in tables.items():
        # Prefer explicit 'columns' key when present
        if isinstance(meta, dict) and "columns" in meta:
            cols_raw = meta.get("columns")
            if isinstance(cols_raw, dict):
                cols = {
                    k: (v if isinstance(v, (str, int, float)) else
                        str(type(v).__name__))
                    for k, v in cols_raw.items()
                }
                compact[tname] = {"columns": cols}
            elif isinstance(cols_raw, (list, tuple)):
                compact[tname] = {"columns": list(cols_raw)}
            else:
                compact[tname] = {"info": str(cols_raw)}
        elif isinstance(meta, dict) and meta:
            # Heuristic: if the values are simple (strings/numbers), treat
            # the dict as a mapping column->meaning
            if all(
                isinstance(v, (str, int, float, type(None)))
                for v in meta.values()
            ):
                cols = {k: str(v) for k, v in meta.items()}
                compact[tname] = {"columns": cols}
            else:
                compact[tname] = {"info": str(meta)}
        else:
            compact[tname] = {"info": str(meta)}

    serial = _json.dumps(compact, ensure_ascii=False)
    if len(serial) <= max_chars:
        return compact

    # If still too big, reduce to only column names and truncate long lists
    compact2 = {}
    for tname, meta in compact.items():
        cols = meta.get("columns") or {}
        col_names = list(cols.keys())
        # keep first 50 columns if very large
        if len(col_names) > 50:
            col_names = col_names[:50]
            compact2[tname] = {"columns": col_names, "truncated": True}
        else:
            compact2[tname] = {"columns": col_names}

    # If still too large, keep only table names
    serial2 = _json.dumps(compact2, ensure_ascii=False)
    if len(serial2) <= max_chars:
        return compact2

    # Fallback: just return table names
    return {t: {"columns": []} for t in list(tables.keys())}


def generate_sql(
    question: str,
    tables_json_path: str = "data/tables.json",
    model: str = "moonshotai/kimi-k2",
    base_url: str | None = None,
    api_key: str | None = None,
) -> str:
    """Generate SQL from a natural language question using OpenRouter / Kimi.

    Returns the model's raw text output (expected to be SQL only).
    """
    api_key = api_key or os.getenv("OPENROUTER_API_KEY")
    base_url = base_url or os.getenv(
        "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"
    )

    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is required. Set the environment variable."
        )

    client = OpenAI(base_url=base_url, api_key=api_key)

    # Load the full tables metadata for validation, but supply a
    # compact/summarized version to the model to avoid huge prompts.
    orig_tables = load_tables(tables_json_path)
    prompt_tables = summarize_tables(orig_tables, max_chars=8_000)
    messages = build_messages(question, prompt_tables)

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=4_000,
        extra_headers={
            "HTTP-Referer": os.getenv("OPENROUTER_REFERER", ""),
            "X-Title": os.getenv("OPENROUTER_X_TITLE", "sql-agent"),
        },
        extra_body={},
    )

    content = resp.choices[0].message.content
    sql = content.strip()

    # Validate the SQL references against the original table metadata
    # to ensure the model did not invent columns or tables. If validation
    # fails, ask the model once to rewrite the SQL using the full schema
    # and Oracle-compatible syntax. If the repair still fails, raise.
    try:
        validate_sql(sql, orig_tables)
    except RuntimeError as err:
        # Log the validation error and attempt one repair pass. If repair
        # fails, log and return the original SQL rather than raising.
        logger.warning("Initial SQL validation failed: %s", err)

        repair_user = {
            "role": "user",
            "content": (
                "The SQL you returned references unknown columns or tables:\n"
                f"{err}\n\n"
                "Here is the full tables metadata (do NOT invent columns or\n"
                "tables). Rewrite the SQL for Oracle using ONLY the provided\n"
                "tables/columns. If the question cannot be answered with the\n"
                "available schema, return a safe, no-rows query such as\n"
                "SELECT NULL FROM DUAL WHERE 1=0 and nothing else.\n\n"
                "TABLES JSON:\n"
                + _json.dumps(orig_tables, ensure_ascii=False, indent=2)
            ),
        }

        repair_messages = messages + [repair_user]
        try:
            resp2 = client.chat.completions.create(
                model=model,
                messages=repair_messages,
                max_tokens=4_000,
                extra_headers={
                    "HTTP-Referer": os.getenv("OPENROUTER_REFERER", ""),
                    "X-Title": os.getenv("OPENROUTER_X_TITLE", "sql-agent"),
                },
                extra_body={},
            )
            sql2 = resp2.choices[0].message.content.strip()
            try:
                validate_sql(sql2, orig_tables)
                sql = sql2
            except RuntimeError as err2:
                logger.warning("Repaired SQL still invalid: %s", err2)
                # keep original sql
        except Exception as call_err:
            logger.error("Error requesting SQL repair: %s", call_err)

    return sql


def _collect_table_columns(table_meta: Any) -> set:
    """Return a set of column names (upper-cased) for a table metadata entry.

    Handles different metadata shapes used in tests and data files: either
    {'columns': ['c1','c2']} or {'columns': {'C1': 'meaning', ...}}.
    """
    cols = set()
    if not isinstance(table_meta, dict):
        return cols
    # try .get('columns') first
    c = table_meta.get("columns") if "columns" in table_meta else None
    if isinstance(c, dict):
        cols.update(k.upper() for k in c.keys())
    elif isinstance(c, (list, tuple)):
        cols.update(str(x).upper() for x in c)
    else:
        # If the table_meta itself is a mapping of col->meaning, treat keys
        cols.update(k.upper() for k in table_meta.keys())
    return cols


def _split_top_level(s: str) -> list:
    """Split string by commas at top-level (ignore commas inside
    parentheses or quotes). Returns list of parts.
    """
    parts = []
    cur = []
    depth = 0
    in_sq = False
    in_dq = False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
            cur.append(ch)
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
            cur.append(ch)
        elif not in_sq and not in_dq:
            if ch == "(":
                depth += 1
                cur.append(ch)
            elif ch == ")":
                if depth > 0:
                    depth -= 1
                cur.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(cur).strip())
                cur = []
            else:
                cur.append(ch)
        else:
            cur.append(ch)
        i += 1
    if cur:
        parts.append("".join(cur).strip())
    return parts


def validate_sql(sql: str, tables: Dict[str, Any]) -> None:
    """Lightweight validation of SQL: ensure referenced tables and columns
    exist in the provided tables metadata.

    This is not a full SQL parser. It handles common SELECT/FROM/JOIN
    patterns and column lists. On unknown references it raises RuntimeError.
    """
    s = sql.strip().rstrip(";")

    # find SELECT ... FROM region
    sel_match = re.search(r"\bSELECT\b(.*?)\bFROM\b", s, flags=re.I | re.S)

    # extract table names and aliases from FROM/JOIN
    alias_map = {}
    tables_found = set()
    tbl_pattern = (
        r"\b(?:FROM|JOIN)\s+(\"?[A-Za-z0-9_\.\"]+\"?)"
        r"(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?"
    )
    for m in re.finditer(tbl_pattern, s, flags=re.I):
        tbl_token = m.group(1).strip().strip('"')
        alias = m.group(2)
        # skip tokens that look like function calls or contain parens or
        # other invalid characters
        if "(" in tbl_token or ")" in tbl_token:
            logger.debug(
                "Skipping non-table token in FROM/JOIN: %s", tbl_token
            )
            continue
        tbl_token = re.sub(r"[;,]+$", "", tbl_token)
        # allow only identifier + optional dots (schema.table). Otherwise skip
        if not re.match(r"^[A-Za-z0-9_\.]+$", tbl_token):
            logger.debug(
                "Skipping non-identifier token in FROM/JOIN: %s", tbl_token
            )
            continue
        tbl_name = tbl_token.split(".")[-1].upper()
        tables_found.add(tbl_name)
        if alias:
            alias_map[alias.upper()] = tbl_name

    # build available columns map
    available = {t.upper(): _collect_table_columns(meta)
                 for t, meta in tables.items()}

    cols_used = set()
    if sel_match:
        cols_part = sel_match.group(1)
        pieces = _split_top_level(cols_part)
        for piece in [p for p in pieces if p]:
            inner = piece
            # strip AS alias and bare alias
            inner = re.sub(r"\s+AS\s+[A-Za-z0-9_]+$", "", inner, flags=re.I)
            inner = re.sub(r"\s+[A-Za-z0-9_]+$", "", inner)

            # unwrap functions iteratively, picking first top-level arg
            unwrap_re = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$",
                                   flags=re.I | re.S)
            prev = None
            while True:
                mm = unwrap_re.match(inner.strip())
                if not mm:
                    break
                args = _split_top_level(mm.group(2).strip())
                if not args:
                    break
                inner = args[0].strip()
                if inner == prev:
                    break
                prev = inner

            # now extract final identifier (last part after dots)
            part = inner.split(".")[-1].strip().strip('"')
            # remove non-identifier chars
            col_name = re.sub(r"[^A-Za-z0-9_]", "", part).upper()
            if not col_name or col_name == "*":
                continue
            cols_used.add(col_name)

    # If no FROM found, validate columns against all available tables
    search_tables = tables_found or set(available.keys())

    # verify tables exist
    unknown_tables = [t for t in tables_found if t not in available]
    if unknown_tables:
        raise RuntimeError(
            "SQL references unknown table(s): %s" % (unknown_tables,)
        )

    # verify columns exist in at least one of the search tables
    missing = []
    for col in cols_used:
        ok = False
        for t in search_tables:
            if col in available.get(t, set()):
                ok = True
                break
        if not ok:
            missing.append(col)
    if missing:
        raise RuntimeError("SQL references unknown column(s): %s" % (missing,))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the SQL generator agent locally"
    )
    parser.add_argument(
        "question",
        nargs="+",
        help="Question to ask the agent (in French or English).",
    )
    parser.add_argument(
        "--tables",
        default="data/tables.json",
        help="Path to tables JSON file",
    )
    parser.add_argument(
        "--model",
        default="moonshotai/kimi-k2",
        help="Model to use",
    )
    args = parser.parse_args()

    q = " ".join(args.question)
    # Prefer using OPENROUTER_API_KEY env var. Do not hard-code API keys.
    # Passing api_key=None allows the function to read the env var.
    sql = generate_sql(
        q,
        tables_json_path=args.tables,
        model=args.model,
        api_key=None,
    )
    print(sql)
