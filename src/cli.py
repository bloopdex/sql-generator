import argparse
import logging
import asyncio
import uvicorn

from src.agents.sql_agent import (
    generate_sql,
    execute_sql_on_db,
    infer_dialect_from_url,
)
from src.api import app

logger = logging.getLogger("sql_generator.cli")


def main():
    parser = argparse.ArgumentParser(description="Command-line interface for the SQL generator.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate-sql subcommand
    gen_parser = subparsers.add_parser("generate-sql", help="Generate SQL from a question.")
    gen_parser.add_argument("question", nargs="*", help="Natural language question.")
    gen_parser.add_argument("--tables", default="data/tables.json", help="Path to tables JSON.")
    gen_parser.add_argument("--model", default="moonshotai/kimi-k2:free", help="Model to use.")
    gen_parser.add_argument(
        "--dialect",
        choices=["oracle", "postgresql", "mysql"],
        default="oracle",
        help="Target SQL dialect for generation.",
    )

    # execute-sql subcommand
    exec_parser = subparsers.add_parser("execute-sql", help="Generate and execute SQL on a DB.")
    exec_parser.add_argument("question", nargs="*", help="Natural language question.")
    exec_parser.add_argument("--tables", default="data/tables.json", help="Path to tables JSON.")
    exec_parser.add_argument("--model", default="moonshotai/kimi-k2:free", help="Model to use.")
    exec_parser.add_argument(
        "--db-url",
        required=True,
        help=(
            "SQLAlchemy DB URL (e.g. postgresql+asyncpg://user:pass@host/db, "
            "mysql+aiomysql://user:pass@host/db, oracle+oracledb://user:pass@host:1521/?service_name=orclpdb1)"
        ),
    )
    exec_parser.add_argument(
        "--dialect",
        choices=["oracle", "postgresql", "mysql"],
        default=None,
        help="Target SQL dialect for generation. If omitted, inferred from --db-url.",
    )

    # serve subcommand
    serve_parser = subparsers.add_parser("serve", help="Run FastAPI server (dev)")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)

    args = parser.parse_args()

    if args.command == "serve":
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if args.command == "generate-sql":
        if not args.question:
            parser.error("question is required for generate-sql")
        question = " ".join(args.question)
        sql = generate_sql(
            question,
            tables_json_path=args.tables,
            model=args.model,
            target_dialect=args.dialect,
        )
        print(sql)
        return

    if args.command == "execute-sql":
        if not args.question:
            parser.error("question is required for execute-sql")
        question = " ".join(args.question)

        # Infer dialect from db-url if not explicitly provided
        target_dialect = args.dialect or infer_dialect_from_url(args.db_url)
        # Normalize to expected set
        if target_dialect not in {"oracle", "postgresql", "mysql"}:
            # Default to 'oracle' if unknown
            target_dialect = "oracle"

        sql = generate_sql(
            question,
            tables_json_path=args.tables,
            model=args.model,
            target_dialect=target_dialect,
        )
        print(f"\033[36m[SQL]\033[0m {sql}")
        try:
            result = asyncio.run(execute_sql_on_db(sql, args.db_url))
            print(f"\033[32m[Result]\033[0m {result}")
        except Exception as e:
            print(f"\033[31m[DB Error]\033[0m {e}")
        return


if __name__ == "__main__":
    main()