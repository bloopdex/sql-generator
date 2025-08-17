import argparse
import logging

import uvicorn

from src.agents.sql_agent import generate_sql
from src.api import app

logger = logging.getLogger("sql_generator.cli")


def main():
    parser = argparse.ArgumentParser(
        description="Command-line interface for the SQL generator."
    )
    parser.add_argument(
        "question",
        nargs="*",
        help="Natural language question to generate SQL for.",
    )
    parser.add_argument(
        "--tables",
        default="data/tables.json",
        help="Path to the JSON file containing table metadata.",
    )
    parser.add_argument(
        "--model",
        default="moonshotai/kimi-k2",
        help="Model to use for SQL generation.",
    )
    parser.add_argument(
        "--serve", action="store_true", help="Run FastAPI server (uvicorn)"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)

    args = parser.parse_args()

    if args.serve:
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if not args.question:
        parser.error("question is required when not serving")

    question = " ".join(args.question)
    sql = generate_sql(
        question, tables_json_path=args.tables, model=args.model
    )
    print(sql)


if __name__ == "__main__":
    main()
