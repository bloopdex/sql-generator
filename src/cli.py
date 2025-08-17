import argparse
from agents.sql_agent import generate_sql


def main():
    parser = argparse.ArgumentParser(
        description="Command-line interface for the SQL generator."
    )
    parser.add_argument(
        "question",
        nargs="+",
        help="Natural language question to generate SQL for."
    )
    parser.add_argument(
        "--tables",
        default="data/tables.json",
        help="Path to the JSON file containing table metadata."
    )
    parser.add_argument(
        "--model",
        default="moonshotai/kimi-k2",
        help="Model to use for SQL generation."
    )
    args = parser.parse_args()

    question = " ".join(args.question)
    sql = generate_sql(question, tables_json_path=args.tables, model=args.model)
    print(sql)


if __name__ == "__main__":
    main()
