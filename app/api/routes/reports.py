"""Reports API routes."""

from typing import List
from uuid import uuid5, NAMESPACE_URL

import structlog
from fastapi import APIRouter, HTTPException

from app.core.config import settings
from app.models.schemas import ErrorResponse, ReportInfo

logger = structlog.get_logger()
router = APIRouter()


@router.get(
    "",
    response_model=List[ReportInfo],
    responses={500: {"model": ErrorResponse}},
    summary="List reports",
)
async def list_reports():
    """Return configured reports with public embed links and workspace dataset IDs."""
    try:
        reports = settings.get_reports()
        return [
            ReportInfo(
                id=str(uuid5(NAMESPACE_URL, r["url"])),
                name=r["name"],
                embed_url=r["url"],
                web_url=r["url"],
                dataset_id=r.get("dataset_id") or settings.powerbi_dataset_id,
                workspace_id=r.get("workspace_id") or settings.powerbi_workspace_id,
                powerbi_report_id=r.get("report_id") or settings.powerbi_report_id,
                embed_token=None,
                embed_token_expiry=None,
            )
            for r in reports
        ]
    except Exception as e:
        logger.exception("Failed to list reports", error=str(e))
        raise HTTPException(
            status_code=500,
            detail={"error": "Failed to list reports", "details": [{"code": "CONFIG_ERROR", "message": str(e)}]},
        )
