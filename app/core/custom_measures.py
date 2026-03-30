"""
Carregador de medidas DAX customizadas e nativas por cliente.

measures.json (raiz do projeto) define duas categorias:
  - type "native"  → medida já existe no dataset Power BI; referenciar como [NomeMedida]
  - type "custom"  → DAX definido aqui, para casos que não existem no modelo Power BI

Para adaptar a um novo cliente: editar measures.json e chamar POST /api/v1/measures/reload.
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MEASURES_FILE = Path(__file__).parent.parent.parent / "measures.json"


def load_custom_measures() -> list[dict[str, Any]]:
    """Carrega as medidas do measures.json. Retorna lista vazia em caso de erro."""
    if not _MEASURES_FILE.exists():
        logger.warning("measures.json não encontrado em %s", _MEASURES_FILE)
        return []
    try:
        with open(_MEASURES_FILE, encoding="utf-8") as f:
            data = json.load(f)
        measures = data.get("measures", [])
        logger.info("Carregadas %d medidas de measures.json", len(measures))
        return measures
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Erro ao carregar measures.json: %s", exc)
        return []


def build_measures_prompt(measures: list[dict[str, Any]]) -> str:
    """
    Formata as medidas como bloco de texto para injeção no prompt do agente.

    Medidas 'native': o agente deve usar CALCULATE([NomeMedida], filtros).
    Medidas 'custom': o agente deve expandir o DAX fornecido com os filtros.
    """
    if not measures:
        return ""

    native = [m for m in measures if m.get("type") == "native"]
    custom = [m for m in measures if m.get("type") == "custom"]

    lines = ["## MEDIDAS DO CLIENTE\n"]

    if native:
        lines.append("### Medidas nativas (existem no dataset Power BI)")
        lines.append(
            "Para estas medidas, use `CALCULATE([NomeMedida], filtros)`. "
            "NÃO reescreva a lógica — a medida já está correta no modelo.\n"
        )
        for m in native:
            name = m.get("name", "")
            description = m.get("description", "")
            aliases = m.get("aliases", [])
            lines.append(f"**[{name}]** — {description}")
            if aliases:
                lines.append(f"  O usuário pode chamar de: {', '.join(aliases)}")
            lines.append(
                f"  → DAX: `EVALUATE CALCULATE(ROW(\"{name}\", [{name}]), <filtros>)`\n"
            )

    if custom:
        lines.append("### Medidas customizadas (DAX definido aqui)")
        lines.append(
            "Para estas medidas, use o DAX abaixo aplicando os filtros do usuário via CALCULATE.\n"
        )
        for m in custom:
            name = m.get("name", "")
            description = m.get("description", "")
            dax = m.get("dax", "")
            lines.append(f"**[{name}]** — {description}")
            lines.append(f"  ```\n  {dax}\n  ```\n")

    lines.append(
        "**Exemplo com filtro de período para medida nativa:**\n"
        "```dax\n"
        "EVALUATE\n"
        "CALCULATE(\n"
        "    ROW(\"EBITDA\", [EBITDA]),\n"
        "    'data'[Ano ] = \"2026\",\n"
        "    'data'[Nome mês] = \"Março\"\n"
        ")\n"
        "```\n"
    )

    return "\n".join(lines)


# Cache em memória
_cached_prompt: str | None = None


def get_custom_measures_prompt() -> str:
    """Retorna o bloco formatado das medidas, com cache em memória."""
    global _cached_prompt
    if _cached_prompt is None:
        measures = load_custom_measures()
        _cached_prompt = build_measures_prompt(measures)
    return _cached_prompt


def invalidate_cache() -> None:
    """Força recarregamento do measures.json na próxima chamada."""
    global _cached_prompt
    _cached_prompt = None
