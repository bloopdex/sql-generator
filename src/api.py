from typing import Optional, Dict, Any
import logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from src.agents.sql_agent import generate_sql

logger = logging.getLogger("sql_generator.api")

app = FastAPI(title="SQL Generator API", version="0.1")


class SQLRequest(BaseModel):
    question: str
    tables_path: Optional[str] = "data/tables.json"
    model: Optional[str] = "moonshotai/kimi-k2"
    stream: Optional[bool] = False


class SQLResponse(BaseModel):
    sql: str


@app.get("/", include_in_schema=False)
async def root() -> Dict[str, Any]:
    return {"status": "ok", "service": "sql-generator"}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "healthy"}


@app.post("/sql-generator", response_model=SQLResponse)
async def sql_generator(req: SQLRequest):
    """Generate SQL for the provided natural language question.

    This endpoint runs the existing agent (`generate_sql`) in a
    threadpool (since it can block). If `stream` is True the endpoint
    returns a streaming response (text/event-stream) with the final
    SQL when available.
    """
    try:
        if req.stream:
            async def event_stream():
                try:
                    sql = await run_in_threadpool(
                        generate_sql,
                        req.question,
                        req.tables_path,
                        req.model,
                    )
                    yield f"data: {sql}\n\n"
                except Exception as e:
                    logger.exception("Error generating SQL (stream)")
                    yield f"event: error\ndata: {str(e)}\n\n"

            return StreamingResponse(
                event_stream(), media_type="text/event-stream"
            )

        sql = await run_in_threadpool(
            generate_sql, req.question, req.tables_path, req.model
        )
        return JSONResponse(content={"sql": sql})
    except Exception as e:
        logger.exception("Error generating SQL")
        raise HTTPException(status_code=500, detail=str(e))
