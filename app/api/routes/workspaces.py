"""
Workspaces API routes.
"""

import structlog
from fastapi import APIRouter, Depends, HTTPException

from app.connectors.powerbi.client import PowerBIClient, get_powerbi_client
from app.models.schemas import (
    DatasetsResponse,
    DatasetInfo,
    ErrorResponse,
    WorkspaceInfo,
    WorkspacesResponse,
)

logger = structlog.get_logger()

router = APIRouter()


@router.get(
    "",
    response_model=WorkspacesResponse,
    responses={500: {"model": ErrorResponse}},
    summary="List workspaces",
    description="Get all Power BI workspaces accessible to the service principal"
)
async def list_workspaces(
    client: PowerBIClient = Depends(get_powerbi_client)
):
    """
    List all Power BI workspaces the authenticated user/service principal has access to.
    """
    try:
        workspaces_data = await client.list_workspaces()
        
        workspaces = [
            WorkspaceInfo(
                id=ws["id"],
                name=ws["name"],
                description=ws.get("description"),
                type=ws.get("type", "Workspace"),
                is_read_only=ws.get("isReadOnly", False)
            )
            for ws in workspaces_data
        ]
        
        logger.info("Listed workspaces", count=len(workspaces))
        
        return WorkspacesResponse(
            workspaces=workspaces,
            total=len(workspaces)
        )
        
    except Exception as e:
        logger.exception("Failed to list workspaces", error=str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to list workspaces",
                "details": [{"code": "POWERBI_ERROR", "message": str(e)}]
            }
        )


@router.get(
    "/{workspace_id}",
    response_model=WorkspaceInfo,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    summary="Get workspace",
    description="Get details of a specific workspace"
)
async def get_workspace(
    workspace_id: str,
    client: PowerBIClient = Depends(get_powerbi_client)
):
    """Get a specific workspace by ID."""
    try:
        workspaces_data = await client.list_workspaces()
        
        workspace = next(
            (ws for ws in workspaces_data if ws["id"] == workspace_id),
            None
        )
        
        if not workspace:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Workspace not found",
                    "details": [{"code": "NOT_FOUND", "message": f"No workspace with ID: {workspace_id}"}]
                }
            )
        
        return WorkspaceInfo(
            id=workspace["id"],
            name=workspace["name"],
            description=workspace.get("description"),
            type=workspace.get("type", "Workspace"),
            is_read_only=workspace.get("isReadOnly", False)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get workspace", workspace_id=workspace_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to get workspace",
                "details": [{"code": "POWERBI_ERROR", "message": str(e)}]
            }
        )


@router.get(
    "/{workspace_id}/datasets",
    response_model=DatasetsResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    summary="List datasets in workspace",
    description="Get all datasets in a specific workspace"
)
async def list_workspace_datasets(
    workspace_id: str,
    client: PowerBIClient = Depends(get_powerbi_client)
):
    """List all datasets in a workspace."""
    try:
        datasets_data = await client.list_datasets(workspace_id)
        
        datasets = [
            DatasetInfo(
                id=ds["id"],
                name=ds["name"],
                description=ds.get("description"),
                workspace_id=workspace_id,
                configured_by=ds.get("configuredBy")
            )
            for ds in datasets_data
        ]
        
        logger.info(
            "Listed datasets",
            workspace_id=workspace_id,
            count=len(datasets)
        )
        
        return DatasetsResponse(
            workspace_id=workspace_id,
            datasets=datasets,
            total=len(datasets)
        )
        
    except Exception as e:
        logger.exception(
            "Failed to list datasets",
            workspace_id=workspace_id,
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to list datasets",
                "details": [{"code": "POWERBI_ERROR", "message": str(e)}]
            }
        )
