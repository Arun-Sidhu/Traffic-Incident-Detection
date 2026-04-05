# __init__.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from prometheus_fastapi_instrumentator import Instrumentator

from .routes.health import router as health_router
from .routes.incidents import router as incidents_router
from .routes.ui import router as ui_router
from .settings import CORS_ALLOW_CREDENTIALS, CORS_ALLOW_ORIGINS, STATIC_DIR

app = FastAPI(title="Traffic Incident Detection API")

Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(health_router)
app.include_router(ui_router)
app.include_router(incidents_router)