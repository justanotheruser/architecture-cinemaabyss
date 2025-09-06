from contextlib import asynccontextmanager
import random
import os

from fastapi import FastAPI, Request, Response
from httpx import AsyncClient

PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() in {"1", "true", "yes", "on"}
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


monolyth_client: AsyncClient = None  # type: ignore
movies_service_client: AsyncClient = None  # type: ignore

@asynccontextmanager
async def lifespan(_: FastAPI):
    global monolyth_client
    global movies_service_client
    monolyth_client = AsyncClient(base_url=MONOLITH_URL)
    movies_service_client = AsyncClient(base_url=MOVIES_SERVICE_URL)
    yield


app = FastAPI(lifespan=lifespan)


def choose_backend(path: str) -> AsyncClient:
    if path.startswith("api/movies") or path.startswith("api/events"):
        if GRADUAL_MIGRATION and MOVIES_MIGRATION_PERCENT > 0:
            if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
                return movies_service_client
    return monolyth_client


def _clean_headers(headers: dict) -> dict:
    return {
        k: v
        for k, v in headers.items()
        if k.lower() not in HOP_BY_HOP_HEADERS
        and k.lower() not in {"host", "content-length"}
    }


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def catch_all(full_path: str, request: Request):    
    client = choose_backend(full_path)
    url = request.url.path
    if request.url.query:
        url += f"?{request.url.query}"
    response = await client.request(request.method, url, 
                         content=await request.body(),
                         headers=_clean_headers(dict(request.headers)))
    resp_headers = _clean_headers(dict(response.headers))
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=resp_headers,
        media_type=response.headers.get("content-type"),
    )

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "gradual_migration": GRADUAL_MIGRATION,
        "movies_migration_percent": MOVIES_MIGRATION_PERCENT,
        "monolith_url": MONOLITH_URL,
        "movies_url": MOVIES_SERVICE_URL,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)