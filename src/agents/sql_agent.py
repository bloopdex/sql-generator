from __future__ import annotations

import os
import json
from typing import Dict, Any, Optional

import asyncio
import logging
import re

from sqlalchemy import text, create_engine as create_sync_engine
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from openai import OpenAI
import json as _json

logger = logging.getLogger(__name__)


# -----------------------------
# DB helpers (multi-dialect)
# -----------------------------

def infer_dialect_from_url(db_url: str) -> str:
    try:
        url = make_url(db_url)
    except Exception:
        prefix = db_url.split(":", 1)[0].lower()
        return "postgresql" if "postgres" in prefix else (
            "mysql" if "mysql" in prefix else (
                "oracle" if "oracle" in prefix or "cx_oracle" in prefix or "oracledb" in prefix else "other"
            )
        )

    backend = (url.get_backend_name() or "").lower()
    if "postgres" in backend:
        return "postgresql"
    if "mysql" in backend or "mariadb" in backend:
        return "mysql"
    if "oracle" in backend or "cx_oracle" in backend or "oracledb" in backend:
        return "oracle"
    return backend or "other"


def normalize_async_db_url(db_url: str) -> tuple[str, bool]:
    try:
        url = make_url(db_url)
    except Exception:
        return db_url, False

    backend = url.get_backend_name().lower() if url.get_backend_name() else ""
    driver = url.get_driver_name().lower() if url.get_driver_name() else ""

    # PostgreSQL
    if "postgres" in backend:
        if not driver or driver in {"psycopg2", "psycopg", "pg8000"}:
            url = url.set(drivername="postgresql+asyncpg")
        return str(url), "asyncpg" in (url.get_driver_name() or "").lower()

    # MySQL / MariaDB
    if "mysql" in backend or "mariadb" in backend:
        if not driver or driver in {"pymysql", "mysqldb", "mysqlconnector"}:
            url = url.set(drivername="mysql+aiomysql")
            return str(url), True
        return str(url), driver in {"aiomysql", "asyncmy"}

    # Oracle: sync-only via thread
    if "oracle" in backend or "cx_oracle" in backend or "oracledb" in backend:
        return str(url), False

    return str(url), False


def _maybe_init_oracle_thick(engine_url: str) -> None:
    try:
        url = make_url(engine_url)
    except Exception:
        return

    backend = (url.get_backend_name() or "").lower()
    if "oracle" not in backend and "oracledb" not in backend and "cx_oracle" not in backend:
        return

    try:
        import oracledb  # type: ignore
    except Exception:
        return

    try:
        is_thin = getattr(oracledb, "is_thin_mode", None)
        if callable(is_thin) and not oracledb.is_thin_mode():
            return
    except Exception:
        pass

    lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR") or os.getenv("ORACLE_HOME")
    if not lib_dir:
        logger.warning(
            "Oracle thick mode may be required but ORACLE_CLIENT_LIB_DIR/ORACLE_HOME is not set. "
            "If you encounter DPY-3010, install Oracle Instant Client and set ORACLE_CLIENT_LIB_DIR."
        )
        return

    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)  # type: ignore
        logger.info("Initialized Oracle thick mode using client libraries at: %s", lib_dir)
    except Exception as e:
        logger.warning("Failed to initialize Oracle thick mode with lib_dir=%s: %s", lib_dir, e)


def _augment_oracle_errors(e: Exception) -> str:
    """Append helpful hints for common Oracle driver errors."""
    msg = str(e)
    if "DPY-3010" in msg:
        msg += (
            "\nHint: The Oracle DB server version isn't supported in python-oracledb thin mode."
            "\nInstall Oracle Instant Client and set ORACLE_CLIENT_LIB_DIR to its directory, or set ORACLE_HOME."
            "\nExample (Linux/macOS): export ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_21_10"
            "\nThen re-run the command."
            "\nDocs: https://python-oracledb.readthedocs.io/en/latest/user_guide/troubleshooting.html#dpy-3010"
        )
    if "ORA-01017" in msg:
        msg += (
            "\nHint: ORA-01017 indicates the database rejected the credentials."
            "\n- Verify username/password (case-sensitive) and that the account is not LOCKED or EXPIRED."
            "\n- If your password has special characters (@/:?#& etc.), they must be URL-encoded in the DB URL."
            "\n  Example: oracle+oracledb://user:%40b%2Fc%3F@host:1521/?service_name=XE"
            "\n- Ensure you're connecting to the correct container/service:"
            "\n  Many XE installs use service_name=XEPDB1 rather than XE. Try:"
            "\n    oracle+oracledb://USER:PASS@HOST:1521/?service_name=XEPDB1"
            "\n  Or, if your DB uses a SID, try '?sid=XE' instead of '?service_name=XE'."
            "\n- Test with SQL*Plus to confirm: sqlplus USER/PASS@//HOST:1521/XEPDB1"
            "\nDocs: https://docs.oracle.com/error-help/db/ora-01017/"
        )
    return msg


async def _execute_sql_async(engine_url: str, sql: str) -> list:
    engine = create_async_engine(engine_url, echo=False, future=True)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql))
            if result.returns_rows:
                rows = (await result.mappings().all()) if hasattr(result, "mappings") else result.fetchall()
                return [dict(r) for r in rows]
            else:
                return [{"rows_affected": result.rowcount}]
    except SQLAlchemyError as e:
        emsg = _augment_oracle_errors(e)
        raise RuntimeError(f"DB execution error: {emsg}") from e
    finally:
        await engine.dispose()


def _execute_sql_sync(engine_url: str, sql: str) -> list:
    _maybe_init_oracle_thick(engine_url)

    engine = create_sync_engine(engine_url, echo=False, future=True)
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            if result.returns_rows:
                try:
                    rows = result.mappings().all()
                    return [dict(r) for r in rows]
                except Exception:
                    keys = result.keys()
                    rows = result.fetchall()
                    return [dict(zip(keys, row)) for row in rows]
            else:
                return [{"rows_affected": result.rowcount}]
    except SQLAlchemyError as e:
        emsg = _augment_oracle_errors(e)
        raise RuntimeError(f"DB execution error: {emsg}") from e
    finally:
        engine.dispose()


async def execute_sql_on_db(sql: str, db_url: str) -> list:
    normalized_url, is_async = normalize_async_db_url(db_url)
    if is_async:
        return await _execute_sql_async(normalized_url, sql)
    return await asyncio.to_thread(_execute_sql_sync, normalized_url, sql)


# -----------------------------
# SQL generation (multi-dialect)
# -----------------------------

def load_tables(json_path: str) -> Dict[str, Any]:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _dialect_instruction(target_dialect: str) -> str:
    d = (target_dialect or "oracle").strip().lower()
    if d in {"postgres", "postgresql"}:
        return (
            "Target PostgreSQL (13+). Use PostgreSQL-compatible syntax and types. "
            "Do NOT invent columns or tables. Only use the provided schema."
        )
    if d in {"mysql", "mariadb"}:
        return (
            "Target MySQL (8.0+). Use MySQL-compatible syntax and types. "
            "Do NOT invent columns or tables. Only use the provided schema."
        )
    return (
        "Target Oracle SQL (Oracle 12c+). Use Oracle-compatible syntax and "
        "data types. Prefer limiting rows with ROWNUM (e.g., WHERE ROWNUM <= "
        "10) instead of FETCH FIRST ... ROWS ONLY. Do NOT invent columns or "
        "tables. Only use the provided schema."
    )


def build_messages(
    question: str,
    tables: Dict[str, Any],
    target_dialect: str = "oracle",
) -> list:
    system = {
        "role": "system",
        "content": (
            "You are an expert SQL generator.\n"
            "Rules: Return ONLY the SQL statements required to answer the "
            "user's request.\n"
            "Do NOT return explanations, commentary, markdown, backticks, or "
            "code fences.\n"
            "Use the table and column names exactly as provided. If the "
            "request is ambiguous, choose a reasonable, minimal "
            "interpretation and produce safe SQL.\n"
            f"{_dialect_instruction(target_dialect)}"
        ),
    }

    tables_serialized = json.dumps(tables, ensure_ascii=False, indent=2)

    user = {
        "role": "user",
        "content": (
            "I will provide database table metadata in JSON.\n"
            "Using only that metadata, generate SQL that answers the "
            "question.\n"
            "Do NOT invent columns or tables.\n\n"
            "TABLES JSON:\n" + tables_serialized + "\n\n"
            "QUESTION: " + question + "\n\n"
            "Output rules reminder: ONLY SQL. No comments, no explanation, "
            "no markdown."
        ),
    }

    return [system, user]


def summarize_tables(
    tables: Dict[str, Any], max_chars: int = 8000
) -> Dict[str, Any]:
    compact = {}
    for tname, meta in tables.items():
        if isinstance(meta, dict) and "columns" in meta:
            cols_raw = meta.get("columns")
            if isinstance(cols_raw, dict):
                cols = {
                    k: (
                        v
                        if isinstance(v, (str, int, float))
                        else str(type(v).__name__)
                    )
                    for k, v in cols_raw.items()
                }
                compact[tname] = {"columns": cols}
            elif isinstance(cols_raw, (list, tuple)):
                compact[tname] = {"columns": list(cols_raw)}
            else:
                compact[tname] = {"info": str(cols_raw)}
        elif isinstance(meta, dict) and meta:
            if all(
                isinstance(v, (str, int, float, type(None))) for v in meta.values()
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

    compact2 = {}
    for tname, meta in compact.items():
        cols = meta.get("columns") or {}
        col_names = list(cols.keys())
        if len(col_names) > 50:
            col_names = col_names[:50]
            compact2[tname] = {"columns": col_names, "truncated": True}
        else:
            compact2[tname] = {"columns": col_names}

    serial2 = _json.dumps(compact2, ensure_ascii=False)
    if len(serial2) <= max_chars:
        return compact2

    return {t: {"columns": []} for t in list(tables.keys())}


def generate_sql(
    question: str,
    tables_json_path: str = "data/tables.json",
    model: str = "moonshotai/kimi-k2:free",
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    target_dialect: str = "oracle",
) -> str:
    api_key = api_key or os.getenv("OPENROUTER_API_KEY")
    base_url = base_url or os.getenv(
        "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"
    )

    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is required. Set the environment variable."
        )

    client = OpenAI(base_url=base_url, api_key=api_key)

    orig_tables = load_tables(tables_json_path)
    prompt_tables = summarize_tables(orig_tables, max_chars=8_000)
    messages = build_messages(
        question, prompt_tables, target_dialect=target_dialect
    )

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
    if target_dialect.lower() == "oracle":
        sql = _prefer_rownum_limit(sql)

    try:
        validate_sql(sql, orig_tables)
    except RuntimeError as err:
        logger.warning("Initial SQL validation failed: %s", err)

        # choose a dialect-specific safe, no-rows SQL fallback
        if target_dialect in {"postgresql", "mysql"}:
            no_rows_sql = "SELECT NULL WHERE 1=0"
        elif target_dialect == "oracle":
            no_rows_sql = "SELECT NULL FROM DUAL WHERE 1=0"
        else:
            no_rows_sql = "SELECT NULL WHERE 1=0"

        repair_user = {
            "role": "user",
            "content": (
                "The SQL you returned references unknown columns or tables:\n"
                f"{err}\n\n"
                "Here is the full tables metadata (do NOT invent columns or "
                "tables). "
                f"Rewrite the SQL for {target_dialect} using ONLY the "
                "provided tables/columns. If the question cannot be answered "
                "with the available schema, return a safe, no-rows query such "
                f"as {no_rows_sql} and nothing else.\n\n"
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
            if target_dialect.lower() == "oracle":
                sql2 = _prefer_rownum_limit(sql2)
            try:
                validate_sql(sql2, orig_tables)
                sql = sql2
            except RuntimeError as err2:
                logger.warning("Repaired SQL still invalid: %s", err2)
        except Exception as call_err:
            logger.error("Error requesting SQL repair: %s", call_err)

    return sql


def _collect_table_columns(table_meta: Any) -> set:
    cols = set()
    if not isinstance(table_meta, dict):
        return cols
    c = table_meta.get("columns") if "columns" in table_meta else None
    if isinstance(c, dict):
        cols.update(k.upper() for k in c.keys())
    elif isinstance(c, (list, tuple)):
        cols.update(str(x).upper() for x in c)
    else:
        cols.update(k.upper() for k in table_meta.keys())
    return cols


def _prefer_rownum_limit(sql: str) -> str:
    """Rewrite trailing FETCH FIRST n ROWS ONLY to WHERE ROWNUM <= n.

    Simple heuristic: single SELECT, no existing ROWNUM limit handling.
    """
    s = sql.strip().rstrip(";")
    pattern = r"\bFETCH\s+FIRST\s+(\d+)\s+ROWS\s+ONLY\b"
    m = re.search(pattern, s, flags=re.I)
    if not m:
        return sql
    n = m.group(1)
    before = s[: m.start()].rstrip()
    # If there's already a WHERE, append AND ROWNUM <= n; otherwise add WHERE
    if re.search(r"\bWHERE\b", before, flags=re.I):
        new = re.sub(pattern, "", s, flags=re.I)
        new = new.rstrip()
        if not re.search(r"\bROWNUM\b", new, flags=re.I):
            new += f" AND ROWNUM <= {n}"
    else:
        new = re.sub(pattern, "", s, flags=re.I).rstrip()
        if not re.search(r"\bROWNUM\b", new, flags=re.I):
            new += f" WHERE ROWNUM <= {n}"
    return new


def _split_top_level(s: str) -> list:
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


def _strip_parentheses_regions(s: str) -> str:
    out = []
    depth = 0
    in_sq = False
    in_dq = False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
            out.append(ch)
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
            out.append(ch)
        else:
            if not in_sq and not in_dq:
                if ch == "(":
                    depth += 1
                    out.append(ch)
                elif ch == ")":
                    if depth > 0:
                        depth -= 1
                    out.append(ch)
                else:
                    out.append(ch if depth == 0 else " ")
            else:
                out.append(ch)
        i += 1
    return "".join(out)


def validate_sql(sql: str, tables: Dict[str, Any]) -> None:
    s = sql.strip().rstrip(";")

    sel_match = re.search(r"\bSELECT\b(.*?)\bFROM\b", s, flags=re.I | re.S)

    alias_map = {}
    tables_found = set()
    s_top = _strip_parentheses_regions(s)

    tbl_pattern = (
        r"\b(?:FROM|JOIN)\s+(\"?[A-Za-z0-9_\.\"]+\"?)"
        r"(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?"
    )
    for m in re.finditer(tbl_pattern, s_top, flags=re.I):
        tbl_token = m.group(1).strip().strip('"')
        alias = m.group(2)
        if "(" in tbl_token or ")" in tbl_token:
            continue
        tbl_token = re.sub(r"[;,]+$", "", tbl_token)
        if not re.match(r"^[A-Za-z0-9_\.]+$", tbl_token):
            continue
        tbl_name = tbl_token.split(".")[-1].upper()
        tables_found.add(tbl_name)
        if alias:
            alias_map[alias.upper()] = tbl_name

    available = {
        t.upper(): _collect_table_columns(meta) for t, meta in tables.items()
    }

    cols_used = set()
    if sel_match:
        cols_part = sel_match.group(1)
        pieces = _split_top_level(cols_part)
        for piece in [p for p in pieces if p]:
            inner = piece
            inner = re.sub(r"\s+AS\s+[A-Za-z0-9_]+$", "", inner, flags=re.I)
            inner = re.sub(r"\s+[A-Za-z0-9_]+$", "", inner)

            unwrap_re = re.compile(
                r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$",
                flags=re.I | re.S,
            )
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

            part = inner.split(".")[-1].strip().strip('"')
            col_name = re.sub(r"[^A-Za-z0-9_]", "", part).upper()
            if not col_name or col_name == "*":
                continue
            cols_used.add(col_name)

    search_tables = tables_found or set(available.keys())

    unknown_tables = [t for t in tables_found if t not in available]
    if unknown_tables:
        raise RuntimeError(
            "SQL references unknown table(s): %s" % (unknown_tables,)
        )

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
        "--model", default="moonshotai/kimi-k2:free", help="Model to use"
    )
    parser.add_argument(
        "--dialect",
        choices=["oracle", "postgresql", "mysql"],
        default="oracle",
        help="Target SQL dialect for generation.",
    )
    args = parser.parse_args()

    q = " ".join(args.question)
    sql = generate_sql(
        q,
        tables_json_path=args.tables,
        model=args.model,
        api_key=None,  # read from env
        target_dialect=args.dialect,
    )
    print(sql)
