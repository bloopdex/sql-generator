# SQL Generator API

This repository provides a FastAPI wrapper around the SQL generator agent.

## Getting Started

### Installation

```bash
# install deps
pip install -r requirements.txt
```

### Starting the Server

```bash
# start server locally
python -m src.cli serve --host 0.0.0.0 --port 8000
```

The API will be available at `http://127.0.0.1:8000`

## API Endpoints

### POST /sql-generator

Generate SQL queries from natural language questions.

#### Request

**Content-Type:** `application/json`

**Request Body:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `question` | string | Yes | - | Natural language question to convert to SQL |
| `tables_path` | string | No | `"data/tables.json"` | Path to the JSON file containing table metadata |
| `model` | string | No | `"moonshotai/kimi-k2:free"` | AI model to use for SQL generation |
| `stream` | boolean | No | `false` | If `true`, returns a streaming response (text/event-stream) |

**Example Request (Regular Mode):**

```bash
curl -X POST http://127.0.0.1:8000/sql-generator \
  -H 'Content-Type: application/json' \
  -d '{
    "question": "Chiffre d'\''affaire par client",
    "tables_path": "data/tables.json",
    "model": "moonshotai/kimi-k2:free",
    "stream": false
  }'
```

**Example Request (Streaming Mode):**

```bash
curl -X POST http://127.0.0.1:8000/sql-generator \
  -H 'Content-Type: application/json' \
  -d '{
    "question": "Show all sales from 2024",
    "stream": true
  }'
```

#### Response

**Status Code:** `200 OK` (Success)

**Content-Type:** `application/json` (when `stream=false`)

**Response Body (Regular Mode):**

| Field | Type | Description |
|-------|------|-------------|
| `sql` | string | The generated SQL query |

**Example Response (Regular Mode):**

```json
{
  "sql": "SELECT VNT_TER_ID, SUM(NET_HT) AS chiffre_affaire FROM ventes_ca_ia WHERE VNT_ANNULE_FLAG = 0 AND VNT_AVOIR_FLAG = 0 GROUP BY VNT_TER_ID"
}
```

**Content-Type:** `text/event-stream` (when `stream=true`)

**Response Body (Streaming Mode):**

Server-Sent Events (SSE) format:
```
data: SELECT * FROM ventes_ca_ia WHERE VNT_ANNEE = 2024

```

#### Error Responses

**Status Code:** `500 Internal Server Error`

**Response Body:**

```json
{
  "detail": "Error message describing what went wrong"
}
```

**Status Code:** `422 Unprocessable Entity`

**Response Body:**

```json
{
  "detail": [
    {
      "loc": ["body", "question"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

---

### GET /health

Health check endpoint to verify the API is running.

#### Request

No parameters required.

**Example Request:**

```bash
curl -X GET http://127.0.0.1:8000/health
```

#### Response

**Status Code:** `200 OK`

**Content-Type:** `application/json`

**Response Body:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Health status of the service (always "healthy") |

**Example Response:**

```json
{
  "status": "healthy"
}
```

---

### GET /

Minimal status endpoint providing basic service information.

#### Request

No parameters required.

**Example Request:**

```bash
curl -X GET http://127.0.0.1:8000/
```

#### Response

**Status Code:** `200 OK`

**Content-Type:** `application/json`

**Response Body:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Status of the service (always "ok") |
| `service` | string | Name of the service (always "sql-generator") |

**Example Response:**

```json
{
  "status": "ok",
  "service": "sql-generator"
}
```

---

## Additional Information

### Authentication

Currently, this API does not require authentication.

### Logging

Configure logging in your environment to see agent warnings and repair attempts. The API uses Python's standard logging module with the following loggers:

- `sql_generator.api` - API-level logs
- `sql_generator.cli` - CLI-level logs
- Agent-specific logs for SQL generation

### Content Types

- **Request:** `application/json`
- **Response:** `application/json` (regular mode) or `text/event-stream` (streaming mode)

### Rate Limiting

Currently, no rate limiting is implemented.

### API Documentation

Interactive API documentation is available via FastAPI's built-in Swagger UI:

- Swagger UI: `http://127.0.0.1:8000/docs`
- ReDoc: `http://127.0.0.1:8000/redoc`
