"""
Health check API routes.
"""

import asyncio
import time
from typing import Literal, Optional

import httpx
import structlog
from fastapi import APIRouter
from pydantic import BaseModel

from app.core.config import settings

logger = structlog.get_logger()
router = APIRouter()


class ServiceHealth(BaseModel):
    name: str
    status: Literal["healthy", "unhealthy", "degraded"]
    latency_ms: Optional[float] = None
    message: Optional[str] = None


class HealthResponse(BaseModel):
    status: Literal["healthy", "unhealthy", "degraded"]
    version: str
    environment: str
    services: list[ServiceHealth]


async def check_powerbi_health() -> ServiceHealth:
    """Verifica conectividade com a API do Power BI."""
    try:
        start = time.perf_counter()
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.powerbi.com/v1.0/myorg",
                timeout=5.0,
            )
        latency = (time.perf_counter() - start) * 1000
        # 401/403/404 são esperados sem auth — significa que a API está acessível
        if response.status_code in [200, 401, 403, 404]:
            return ServiceHealth(name="powerbi_api", status="healthy", latency_ms=latency)
        return ServiceHealth(
            name="powerbi_api",
            status="degraded",
            latency_ms=latency,
            message=f"Status inesperado: {response.status_code}",
        )
    except Exception as e:
        logger.warning("Power BI health check failed", error=str(e))
        return ServiceHealth(name="powerbi_api", status="unhealthy", message=str(e))


async def check_anthropic_health() -> ServiceHealth:
    """Verifica se a Anthropic API key está configurada."""
    try:
        key = settings.anthropic_api_key.get_secret_value()
        if key and key.startswith("sk-ant-"):
            return ServiceHealth(name="anthropic_api", status="healthy")
        return ServiceHealth(name="anthropic_api", status="unhealthy", message="API key inválida")
    except Exception as e:
        return ServiceHealth(name="anthropic_api", status="unhealthy", message=str(e))


@router.get("", response_model=HealthResponse, summary="Health check")
async def health_check():
    """Verifica saúde da aplicação e dependências."""
    services = await asyncio.gather(
        check_powerbi_health(),
        check_anthropic_health(),
        return_exceptions=True,
    )

    service_results = []
    names = ["powerbi_api", "anthropic_api"]
    for i, service in enumerate(services):
        if isinstance(service, Exception):
            service_results.append(
                ServiceHealth(name=names[i], status="unhealthy", message=str(service))
            )
        else:
            service_results.append(service)

    statuses = [s.status for s in service_results]
    if all(s == "healthy" for s in statuses):
        overall = "healthy"
    elif any(s == "unhealthy" for s in statuses):
        overall = "unhealthy"
    else:
        overall = "degraded"

    return HealthResponse(
        status=overall,
        version="1.0.0",
        environment=settings.environment,
        services=service_results,
    )


@router.get("/live", summary="Liveness probe")
async def liveness():
    return {"status": "alive"}


@router.get("/ready", summary="Readiness probe")
async def readiness():
    return {"status": "ready"}
