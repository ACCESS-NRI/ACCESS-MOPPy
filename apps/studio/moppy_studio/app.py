"""Authenticated API for a single user's ARE session."""

import getpass
import os
import secrets
import subprocess
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import __version__
from .configuration import dump, parse_yaml, template
from .service import Service

WEB_ROOT = Path(__file__).resolve().parent / "web"


class ConfigText(BaseModel):
    yaml: str = Field(max_length=512000)


class ConfigBody(BaseModel):
    config: dict
    name: str = Field(default="", max_length=160)
    module: str = "conda/analysis3"


class PathBody(BaseModel):
    path: str = Field(max_length=4096)
    name: str = Field(default="", max_length=160)


class SubmitBody(BaseModel):
    sha256: str


def create_app(service=None, token=None):
    api = FastAPI(title="MOPPy Studio", docs_url=None, redoc_url=None, openapi_url=None)
    token = (
        token
        or os.environ.get("MOPPY_STUDIO_ACCESS_TOKEN")
        or secrets.token_urlsafe(32)
    )
    if len(token) < 32:
        raise ValueError("Session token must have at least 32 characters")
    svc = service or Service(
        os.environ.get(
            "MOPPY_STUDIO_STATE_DIR", Path.home() / ".local/state/moppy-studio"
        )
    )
    api.state.token = token
    api.state.service = svc

    @api.middleware("http")
    async def auth(request: Request, call_next):
        path = request.scope["path"].removeprefix(request.scope.get("root_path", ""))
        if path.startswith("/api/"):
            if not secrets.compare_digest(
                request.headers.get("X-MOPPY-Token", ""), token
            ):
                return JSONResponse(
                    {"detail": "Open through ARE, or enter the local session token."},
                    status_code=401,
                )
            if (
                request.method not in ("GET", "HEAD")
                and os.environ.get("MOPPY_STUDIO_REQUIRE_XSRF") == "1"
            ):
                cookie, header = (
                    request.cookies.get("_xsrf", ""),
                    request.headers.get("X-XSRFToken", ""),
                )
                if not cookie or not secrets.compare_digest(cookie, header):
                    return JSONResponse(
                        {"detail": "Missing or invalid session XSRF token"},
                        status_code=403,
                    )
            if int(request.headers.get("content-length", "0")) > 2_000_000:
                return JSONResponse({"detail": "Request is too large"}, status_code=413)
        response = await call_next(request)
        response.headers.update(
            {
                "Cache-Control": "no-store",
                "Referrer-Policy": "no-referrer",
                "X-Content-Type-Options": "nosniff",
            }
        )
        return response

    @api.exception_handler(ValueError)
    async def value_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @api.exception_handler(OSError)
    async def os_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @api.exception_handler(RuntimeError)
    async def runtime_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=503)

    @api.exception_handler(subprocess.TimeoutExpired)
    async def timeout_error(request, exc):
        return JSONResponse(
            {"detail": "Runtime inspection timed out. No job was submitted."},
            status_code=504,
        )

    @api.get("/api/health")
    def health():
        return {
            "version": __version__,
            "user": getpass.getuser(),
            "state_dir": str(svc.state),
            "on_gadi": Path("/g/data/xp65").is_dir(),
        }

    @api.get("/api/template")
    def new_template():
        c = template(getpass.getuser())
        return {"config": c, "yaml": dump(c)}

    @api.post("/api/config/parse")
    def parse(body: ConfigText):
        c = parse_yaml(body.yaml)
        return {"config": c, "yaml": dump(c)}

    @api.post("/api/config/render")
    def render(body: ConfigBody):
        return {"yaml": dump(body.config)}

    @api.post("/api/config/load")
    def load(body: PathBody):
        return svc.load(body.path)

    @api.post("/api/plans")
    def plan(body: ConfigBody):
        return svc.plan(body.config, body.name, body.module)

    @api.get("/api/runs")
    def runs():
        return svc.list(live=True)

    @api.post("/api/runs/attach")
    def attach(body: PathBody):
        return svc.attach(body.path, body.name)

    @api.get("/api/runs/{key}")
    def run(key: str):
        return svc.refresh(key)

    @api.post("/api/runs/{key}/submit")
    def submit(key: str, body: SubmitBody):
        return svc.submit(key, body.sha256)

    @api.post("/api/runs/{key}/reconcile")
    def reconcile(key: str):
        return svc.reconcile(key)

    @api.get("/api/runs/{key}/logs")
    def logs(key: str, kind: str = "monitor_out", variable: str | None = None):
        return svc.logs(key, kind, variable)

    @api.get("/")
    def index():
        return FileResponse(WEB_ROOT / "index.html")

    @api.get("/{asset}")
    def static(asset: str):
        if asset not in (
            "app.js",
            "style.css",
            "moppy-logo.png",
            "favicon.png",
            "access-logo.png",
        ):
            raise HTTPException(404)
        return FileResponse(WEB_ROOT / asset)

    return api


app = create_app()
