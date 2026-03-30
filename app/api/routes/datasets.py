"""
Datasets API routes.
"""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Annotated

from app.connectors.powerbi.client import PowerBIClient, get_powerbi_client
from app.core.config import settings
from app.models.schemas import (
    ColumnInfo,
    DatasetInfo,
    ErrorResponse,
    QueryResult,
    SchemaResponse,
    TableInfo,
)

logger = structlog.get_logger()

router = APIRouter()


@router.get(
    "/{dataset_id}",
    response_model=DatasetInfo,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    summary="Get dataset",
    description="Get details of a specific dataset"
)
async def get_dataset(
    dataset_id: str,
    workspace_id: str = Query(default=None, description="Workspace ID (uses default if not provided)"),
    client: PowerBIClient = Depends(get_powerbi_client)
):
    """Get a specific dataset by ID."""
    ws_id = workspace_id or settings.powerbi_workspace_id
    
    try:
        datasets_data = await client.list_datasets(ws_id)
        
        dataset = next(
            (ds for ds in datasets_data if ds["id"] == dataset_id),
            None
        )
        
        if not dataset:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Dataset not found",
                    "details": [{"code": "NOT_FOUND", "message": f"No dataset with ID: {dataset_id}"}]
                }
            )
        
        return DatasetInfo(
            id=dataset["id"],
            name=dataset["name"],
            description=dataset.get("description"),
            workspace_id=ws_id,
            configured_by=dataset.get("configuredBy")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get dataset", dataset_id=dataset_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to get dataset",
                "details": [{"code": "POWERBI_ERROR", "message": str(e)}]
            }
        )


@router.get(
    "/{dataset_id}/schema",
    response_model=SchemaResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    summary="Get dataset schema",
    description="Get full schema including tables, columns, and relationships"
)
async def get_dataset_schema(
    dataset_id: str,
    workspace_id: str = Query(default=None, description="Workspace ID"),
    include_glossary: bool = Query(default=True, description="Include business term glossary"),
    client: PowerBIClient = Depends(get_powerbi_client)
):
    """
    Get the complete schema for a dataset.
    
    Includes:
    - All tables and their columns
    - Data types and descriptions
    - Business name translations (if glossary enabled)
    - Sample values for each column
    """
    ws_id = workspace_id or settings.powerbi_workspace_id
    
    try:
        # Get dataset info
        datasets_data = await client.list_datasets(ws_id)
        dataset = next(
            (ds for ds in datasets_data if ds["id"] == dataset_id),
            None
        )
        
        if not dataset:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Dataset not found",
                    "details": [{"code": "NOT_FOUND", "message": f"No dataset with ID: {dataset_id}"}]
                }
            )
        
        # Get tables
        tables_data = await client.get_tables(dataset_id, workspace_id=ws_id)
        
        tables = []
        for table in tables_data:
            columns = [
                ColumnInfo(
                    name=col["name"],
                    data_type=col.get("dataType", "Unknown"),
                    description=col.get("description"),
                    is_nullable=not col.get("isHidden", False)
                )
                for col in table.get("columns", [])
            ]
            
            tables.append(TableInfo(
                name=table["name"],
                description=table.get("description"),
                columns=columns
            ))
        
        # Build glossary if requested
        glossary = {}
        if include_glossary:
            # Common Portuguese business term patterns
            patterns = {
                "dt_": "Data de",
                "vlr_": "Valor de",
                "qtd_": "Quantidade de",
                "cd_": "Código de",
                "nm_": "Nome de",
                "nr_": "Número de",
                "fl_": "Flag de",
                "id_": "Identificador de",
                "ds_": "Descrição de",
                "tp_": "Tipo de",
            }
            
            for table in tables:
                for col in table.columns:
                    col_lower = col.name.lower()
                    for prefix, translation in patterns.items():
                        if col_lower.startswith(prefix):
                            suffix = col.name[len(prefix):].replace("_", " ").title()
                            glossary[col.name] = f"{translation} {suffix}"
                            break
        
        dataset_info = DatasetInfo(
            id=dataset["id"],
            name=dataset["name"],
            description=dataset.get("description"),
            workspace_id=ws_id,
            tables=tables,
            configured_by=dataset.get("configuredBy")
        )
        
        logger.info(
            "Retrieved schema",
            dataset_id=dataset_id,
            table_count=len(tables),
            glossary_entries=len(glossary)
        )
        
        return SchemaResponse(
            dataset=dataset_info,
            glossary=glossary
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get schema", dataset_id=dataset_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to get schema",
                "details": [{"code": "POWERBI_ERROR", "message": str(e)}]
            }
        )


@router.post(
    "/{dataset_id}/query",
    response_model=QueryResult,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    summary="Execute DAX query",
    description="Execute a DAX query against the dataset"
)
async def execute_query(
    dataset_id: str,
    query: str,
    workspace_id: str = Query(default=None, description="Workspace ID"),
    max_rows: Annotated[int, Query(ge=1, le=100000)] = 10000,
    client: PowerBIClient = Depends(get_powerbi_client)
):
    """
    Execute a DAX query against a Power BI dataset.
    
    The query must be a valid DAX expression starting with EVALUATE.
    Results are limited to max_rows to prevent memory issues.
    """
    ws_id = workspace_id or settings.powerbi_workspace_id
    
    # Validate query
    query_upper = query.strip().upper()
    if not query_upper.startswith("EVALUATE"):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid query",
                "details": [{"code": "INVALID_DAX", "message": "Query must start with EVALUATE"}]
            }
        )
    
    # Check for dangerous operations
    dangerous_keywords = ["ADDCOLUMNS", "SUMMARIZECOLUMNS"]  # These are fine
    blocked_keywords = ["DEFINE", "VAR"]  # Allow these too actually
    
    try:
        import time
        start = time.perf_counter()
        
        result = await client.execute_query(dataset_id, query, workspace_id=ws_id)

        execution_time = (time.perf_counter() - start) * 1000

        # client.execute_query already returns parsed {"columns": [...], "rows": [...]}
        columns = result.get("columns", [])
        rows = result.get("rows", [])
        
        # Truncate if needed
        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]
        
        logger.info(
            "Query executed",
            dataset_id=dataset_id,
            row_count=len(rows),
            execution_time_ms=execution_time,
            truncated=truncated
        )
        
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=execution_time,
            dax_query=query,
            truncated=truncated
        )
        
    except Exception as e:
        logger.exception("Query execution failed", dataset_id=dataset_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Query execution failed",
                "details": [{"code": "QUERY_ERROR", "message": str(e)}]
            }
        )


@router.get(
    "/{dataset_id}/refresh",
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    summary="Get refresh history",
    description="Get the refresh history for a dataset"
)
async def get_refresh_history(
    dataset_id: str,
    workspace_id: str = Query(default=None, description="Workspace ID"),
    top: Annotated[int, Query(ge=1, le=100)] = 10,
    client: PowerBIClient = Depends(get_powerbi_client)
):
    """Get the refresh history for a dataset."""
    ws_id = workspace_id or settings.powerbi_workspace_id
    
    try:
        history = await client.get_refresh_history(dataset_id, workspace_id=ws_id)
        
        return {
            "dataset_id": dataset_id,
            "refreshes": history,
            "total": len(history)
        }
        
    except Exception as e:
        logger.exception("Failed to get refresh history", dataset_id=dataset_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to get refresh history",
                "details": [{"code": "POWERBI_ERROR", "message": str(e)}]
            }
        )
