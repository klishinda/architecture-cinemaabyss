import os
import hashlib
import threading
from typing import Optional
from fastapi import FastAPI, Request, Response, HTTPException
import httpx

app = FastAPI()

MONOLITH_MOVIES_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
NEW_MOVIES_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")

try:
    MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))
except ValueError:
    MIGRATION_PERCENT = 0

def should_route_to_new(user_id: Optional[str]) -> bool:
    if user_id:
        digest = hashlib.sha256(user_id.encode()).digest()
        v = int.from_bytes(digest, byteorder="big") % 100
        return v < MIGRATION_PERCENT

    if not hasattr(should_route_to_new, "_lock"):
        should_route_to_new._lock = threading.Lock()
        should_route_to_new._counter = 0

    with should_route_to_new._lock:
        v = should_route_to_new._counter % 100
        should_route_to_new._counter += 1

    return v < MIGRATION_PERCENT

async def proxy_request(request: Request, target_base: str):
    url = target_base + request.url.path
    if request.url.query:
        url += "?" + request.url.query

    headers = dict(request.headers)
    headers.pop("host", None)

    body = await request.body()
    method = request.method

    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.request(method, url, headers=headers, content=body)
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=str(e))

    filtered_headers = {
        k: v for k, v in resp.headers.items()
        if k.lower() not in ("content-length", "transfer-encoding", "connection", "date")
    }

    return Response(content=resp.content, status_code=resp.status_code, headers=filtered_headers)

@app.api_route("/api/movies", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def movies_entry(request: Request):
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        user_id = None

    route_new = should_route_to_new(user_id)
    target = NEW_MOVIES_URL if route_new else MONOLITH_MOVIES_URL
    return await proxy_request(request, target)

@app.api_route("/api/users", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def movies_entry(request: Request):
    target = MONOLITH_MOVIES_URL
    return await proxy_request(request, target)

@app.get("/health")
async def health():
    return {"status": "ok", "migration_percent": MIGRATION_PERCENT}
