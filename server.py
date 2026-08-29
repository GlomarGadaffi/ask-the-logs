"""FastAPI web server for BigQuery SQL Agent with OAuth support."""

import os
import json
import time
import asyncio
import hashlib
import warnings
import logging
from typing import Optional

# Suppress Google ADK warnings
warnings.filterwarnings("ignore", message=r".*EXPERIMENTAL.*feature.*", category=UserWarning)
warnings.filterwarnings("ignore", message=r".*non-text parts.*", category=UserWarning)
warnings.filterwarnings("ignore", message=r".*GOOGLE_API_KEY.*GEMINI_API_KEY.*", category=UserWarning)
logging.getLogger("google.genai").setLevel(logging.ERROR)
logging.getLogger("google.adk").setLevel(logging.ERROR)

from dotenv import load_dotenv

load_dotenv()

if os.environ.get("GOOGLE_API_KEY") and os.environ.get("GEMINI_API_KEY"):
    del os.environ["GEMINI_API_KEY"]

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, JSONResponse
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from bigquery_agent.agent import create_bigquery_agent
from bigquery_agent.sources import list_sources


def _rate_limit_key(request: Request) -> str:
    """Rate-limit by authenticated Bearer token (hashed) rather than raw IP:
    users behind the same NAT/corporate egress shouldn't share one bucket,
    and a stolen token from a fresh IP should still be limited. Falls back
    to remote address for requests with no Bearer token."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return hashlib.sha256(auth[7:].encode()).hexdigest()[:16]
    return get_remote_address(request)


limiter = Limiter(key_func=_rate_limit_key)

app = FastAPI(title="BigQuery SQL Agent")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Per-user session state
session_service = InMemorySessionService()
active_runners: dict[str, dict] = {}


def _extract_token(request: Request) -> str:
    """Extract Bearer token from Authorization header."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    return auth[7:]


@app.get("/api/config")
async def get_config():
    """Serve public frontend config. Lets the OAuth client ID live in .env
    instead of a hard-coded placeholder in static/index.html."""
    return {"client_id": os.environ.get("OAUTH_CLIENT_ID", "")}


@app.get("/api/sources")
async def get_sources():
    """Return the catalog of supported log sources with schema metadata."""
    return {"sources": list_sources()}


@app.get("/api/projects")
async def list_projects(request: Request):
    """List GCP projects accessible to the authenticated user."""
    token = _extract_token(request)

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://cloudresourcemanager.googleapis.com/v1/projects",
            params={"filter": "lifecycleState:ACTIVE", "pageSize": 100},
            headers={"Authorization": f"Bearer {token}"},
        )

    if resp.status_code == 403:
        raise HTTPException(
            status_code=403,
            detail="Token lacks project listing permissions. Ensure cloudresourcemanager.readonly scope.",
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    data = resp.json()
    projects = [
        {"id": p["projectId"], "name": p.get("name", p["projectId"]), "number": p.get("projectNumber")}
        for p in data.get("projects", [])
    ]
    projects.sort(key=lambda p: p["name"].lower())
    return {"projects": projects}


@app.get("/api/datasets")
async def list_datasets(request: Request, project_id: str):
    """List BigQuery datasets in a project."""
    token = _extract_token(request)

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets",
            headers={"Authorization": f"Bearer {token}"},
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    data = resp.json()
    datasets = [d["datasetReference"]["datasetId"] for d in data.get("datasets", [])]
    datasets.sort()
    return {"datasets": datasets}


@app.post("/api/query")
@limiter.limit("10/minute")
async def query(request: Request):
    """Stream agent response via SSE."""
    token = _extract_token(request)
    body = await request.json()
    user_message = body.get("message", "").strip()
    project_id = body.get("project_id")
    dataset = body.get("dataset")  # optional
    source_key = body.get("source_key")  # optional — selects domain context
    session_id = body.get("session_id")

    if not user_message:
        return JSONResponse({"error": "Empty message"}, status_code=400)
    if not project_id:
        return JSONResponse({"error": "No project_id specified"}, status_code=400)

    # Get or create runner + session for this project + source context
    if session_id and session_id in active_runners:
        entry = active_runners[session_id]
        runner = entry["runner"]
        session = entry["session"]
    else:
        agent = create_bigquery_agent(
            access_token=token,
            project_id=project_id,
            default_dataset=dataset,
            source_key=source_key,
        )
        runner = Runner(
            agent=agent,
            app_name="bigquery_agent",
            session_service=session_service,
        )
        session = await session_service.create_session(
            app_name="bigquery_agent",
            user_id="web_user",
        )
        session_id = session.id
        active_runners[session_id] = {
            "runner": runner,
            "session": session,
            "project_id": project_id,
            "source_key": source_key,
        }

    content = types.Content(
        role="user",
        parts=[types.Part(text=user_message)],
    )

    async def event_stream():
        start_time = time.time()
        input_tokens = 0
        output_tokens = 0

        yield f"data: {json.dumps({'type': 'session', 'session_id': session_id})}\n\n"

        try:
            async for event in runner.run_async(
                user_id="web_user",
                session_id=session.id,
                new_message=content,
            ):
                if hasattr(event, "usage_metadata"):
                    usage = event.usage_metadata
                    if hasattr(usage, "prompt_token_count"):
                        input_tokens = usage.prompt_token_count or 0
                    if hasattr(usage, "candidates_token_count"):
                        output_tokens = usage.candidates_token_count or 0

                if hasattr(event, "content") and event.content:
                    for part in event.content.parts:
                        if hasattr(part, "text") and part.text:
                            yield f"data: {json.dumps({'type': 'text', 'content': part.text})}\n\n"

                if hasattr(event, "content") and event.content:
                    for part in event.content.parts:
                        if hasattr(part, "function_call") and part.function_call:
                            fn = part.function_call
                            tool_name = fn.name if hasattr(fn, "name") else str(fn)
                            yield f"data: {json.dumps({'type': 'tool', 'name': tool_name})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

        duration = time.time() - start_time
        yield f"data: {json.dumps({'type': 'done', 'duration': round(duration, 2), 'input_tokens': input_tokens, 'output_tokens': output_tokens})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/api/reset")
async def reset_session(request: Request):
    """Clear a session so the user can start fresh."""
    body = await request.json()
    session_id = body.get("session_id")
    if session_id and session_id in active_runners:
        del active_runners[session_id]
    return {"ok": True}


# Serve static files (must be last — catches all unmatched routes)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
