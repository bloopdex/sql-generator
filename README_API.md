# SQL Generator API

This repository provides a FastAPI wrapper around the SQL generator agent.

Run locally:

```bash
# install deps
pip install -r requirements.txt

# start server
python -m src.cli --serve --host 0.0.0.0 --port 8000

# POST example
curl -X POST http://127.0.0.1:8000/sql-generator \
  -H 'Content-Type: application/json' \
  -d '{"question":"Chiffre d\'affaire par client", "tables_path":"data/tables.json"}'
```

Endpoints:

- POST /sql-generator -> returns {"sql": "..."}
- GET /health -> health check
- GET / -> minimal status

Logging:
Configure logging in your environment to see agent warnings and repair attempts.
