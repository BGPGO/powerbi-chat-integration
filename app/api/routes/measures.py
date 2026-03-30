"""
API para gerenciamento das medidas DAX customizadas do cliente.
Permite recarregar o measures.json sem reiniciar o serviço.
"""

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.custom_measures import load_custom_measures, invalidate_cache, get_custom_measures_prompt

router = APIRouter()


class MeasuresResponse(BaseModel):
    count: int
    measures: list[dict]


@router.get("", response_model=MeasuresResponse, summary="Lista medidas customizadas")
async def list_measures():
    """Retorna as medidas DAX customizadas carregadas do measures.json."""
    measures = load_custom_measures()
    return MeasuresResponse(count=len(measures), measures=measures)


@router.post("/reload", summary="Recarrega measures.json e invalida catálogo dinâmico")
async def reload_measures():
    """
    Invalida o cache do measures.json e do catálogo dinâmico de medidas.
    No próximo request, o sistema redescobre as medidas do dataset via API.
    Use após editar o measures.json ou republicar o dataset Power BI.
    """
    invalidate_cache()
    measures = load_custom_measures()

    # Invalida também o catálogo dinâmico (força refresh no próximo request)
    try:
        from app.core.measure_catalog import MeasureCatalog
        MeasureCatalog.get_instance().invalidate()
        catalog_status = "invalidated"
    except Exception as e:
        catalog_status = f"error: {e}"

    return {
        "status": "reloaded",
        "count": len(measures),
        "measures": [m.get("name") for m in measures],
        "catalog": catalog_status,
    }
