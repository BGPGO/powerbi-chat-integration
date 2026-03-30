"""
FastAPI application entry point.
"""

import time
import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import chat, datasets, export, health, measures, reports, workspaces
from app.core.config import settings

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def _warm_schema_cache() -> None:
    """Aquece o cache de schema para todos os reports configurados."""
    import asyncio
    try:
        from app.connectors.powerbi.schema_extractor import DynamicSchemaExtractor
        from app.connectors.powerbi.client import get_powerbi_client

        client = get_powerbi_client()
        extractor = DynamicSchemaExtractor(client)
        reports = settings.get_reports()

        seen = set()
        tasks = []
        for r in reports:
            key = (r.get("workspace_id"), r.get("dataset_id"))
            if key in seen or not all(key):
                continue
            seen.add(key)
            tasks.append(extractor.extract_full_schema(key[0], key[1]))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                    logger.warning("Schema warmup falhou", error=str(res))
                else:
                    logger.info("Schema aquecido", dataset_id=list(seen)[i][1], tables=len(res.tables))
    except Exception as e:
        logger.warning("Schema warmup erro geral", error=str(e))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    import asyncio
    logger.info(
        "Starting application",
        app_name=settings.app_name,
        environment=settings.environment,
        debug=settings.debug
    )
    # Aquece schema cache em background sem bloquear o startup
    asyncio.ensure_future(_warm_schema_cache())

    yield

    logger.info("Shutting down application")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="Chat interface for Power BI with AI-powered query generation",
    version="1.0.0",
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Middleware
# ============================================================================

@app.middleware("http")
async def request_middleware(request: Request, call_next):
    """Add request ID and timing to all requests."""
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    
    start_time = time.perf_counter()
    
    # Add request context to logs
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        path=request.url.path,
        method=request.method
    )
    
    try:
        response = await call_next(request)
        
        # Add timing header
        process_time = (time.perf_counter() - start_time) * 1000
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Process-Time-Ms"] = f"{process_time:.2f}"
        
        logger.info(
            "Request completed",
            status_code=response.status_code,
            process_time_ms=process_time
        )
        
        return response
        
    except Exception as e:
        logger.exception("Request failed", error=str(e))
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "request_id": request_id
            }
        )


# ============================================================================
# Exception Handlers
# ============================================================================

@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    """Handle validation errors."""
    return JSONResponse(
        status_code=400,
        content={
            "error": "Validation error",
            "details": [{"code": "VALIDATION_ERROR", "message": str(exc)}],
            "request_id": getattr(request.state, "request_id", None)
        }
    )


@app.exception_handler(PermissionError)
async def permission_error_handler(request: Request, exc: PermissionError):
    """Handle authorization errors."""
    return JSONResponse(
        status_code=403,
        content={
            "error": "Access denied",
            "details": [{"code": "FORBIDDEN", "message": str(exc)}],
            "request_id": getattr(request.state, "request_id", None)
        }
    )


# ============================================================================
# Routes
# ============================================================================

# Include API routes
app.include_router(
    health.router,
    prefix=f"{settings.api_prefix}/health",
    tags=["Health"]
)

app.include_router(
    chat.router,
    prefix=f"{settings.api_prefix}/chat",
    tags=["Chat"]
)

app.include_router(
    workspaces.router,
    prefix=f"{settings.api_prefix}/workspaces",
    tags=["Workspaces"]
)

app.include_router(
    datasets.router,
    prefix=f"{settings.api_prefix}/datasets",
    tags=["Datasets"]
)

app.include_router(
    reports.router,
    prefix=f"{settings.api_prefix}/reports",
    tags=["Reports"]
)

app.include_router(
    measures.router,
    prefix=f"{settings.api_prefix}/measures",
    tags=["Measures"]
)

app.include_router(
    export.router,
    prefix=f"{settings.api_prefix}/export",
    tags=["Export"]
)


# ============================================================================
# Root endpoint
# ============================================================================

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": settings.app_name,
        "version": "1.0.0",
        "docs": "/docs" if settings.debug else None,
        "health": f"{settings.api_prefix}/health"
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug
    )
