# ui.py
from fastapi import APIRouter
from fastapi.responses import FileResponse

from ..settings import STATIC_DIR

router = APIRouter(tags=["ui"])


@router.get("/ui", response_class=FileResponse)
def ui() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")