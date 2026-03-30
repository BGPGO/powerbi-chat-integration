"""
KPI Shortcuts — Atalhos determinísticos para perguntas KPI comuns.

Evita o LLM para queries simples e frequentes, usando regex pré-validado.
Retorna DAX direto e confiável para: faturamento, receita, despesa, resultado, EBITDA.
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Mapeamento de nomes de meses (com/sem acento) → nome oficial (maiúscula)
_MONTH_MAP = {
    "janeiro": "Janeiro", "jan": "Janeiro",
    "fevereiro": "Fevereiro", "fev": "Fevereiro",
    "marco": "Março", "março": "Março", "mar": "Março",
    "abril": "Abril", "abr": "Abril",
    "maio": "Maio", "mai": "Maio",
    "junho": "Junho", "jun": "Junho",
    "julho": "Julho", "jul": "Julho",
    "agosto": "Agosto", "ago": "Agosto",
    "setembro": "Setembro", "set": "Setembro",
    "outubro": "Outubro", "out": "Outubro",
    "novembro": "Novembro", "nov": "Novembro",
    "dezembro": "Dezembro", "dez": "Dezembro",
}

_DEFAULT_YEAR = "2026"


@dataclass
class KPIMatch:
    dax_query: str
    explanation: str
    label: str


def _extract_year(text: str) -> Optional[str]:
    """Extrai ano de 4 dígitos (2020-2030) do texto."""
    m = re.search(r'\b(20[2-3]\d)\b', text)
    return m.group(1) if m else None


def _extract_month(text: str) -> Optional[str]:
    """Extrai nome do mês do texto (com ou sem acento)."""
    # Normaliza: minúsculas, remove acentos simples para lookup
    normalized = text.lower()
    # Remove acento de 'ç' → 'c' para "marco"/"março"
    normalized = normalized.replace('ç', 'c').replace('ã', 'a').replace('é', 'e').replace('ê', 'e')
    for key in sorted(_MONTH_MAP.keys(), key=len, reverse=True):
        if key in normalized:
            return _MONTH_MAP[key]
    return None


def _build_filters(year: Optional[str], month: Optional[str]) -> str:
    """Gera filtros DAX para ano/mês."""
    parts = []
    if year:
        parts.append(f"'data'[Ano ] = \"{year}\"")
    if month:
        parts.append(f"'data'[Nome mês] = \"{month}\"")
    return ", ".join(parts) if parts else ""


def _build_filter_block(year: Optional[str], month: Optional[str]) -> str:
    """Gera bloco FILTER(ALL('data'), ...) para SUMMARIZECOLUMNS."""
    parts = []
    if year:
        parts.append(f"'data'[Ano ] = \"{year}\"")
    if month:
        parts.append(f"'data'[Nome mês] = \"{month}\"")
    if not parts:
        return ""
    condition = " && ".join(parts)
    return f"FILTER(ALL('data'), {condition})"


# ─── Padrões de detecção ──────────────────────────────────────────────────────

_FATURAMENTO_PATTERN = re.compile(
    r'\b(faturamento|faturou|fat\.?|receita\s+total|total\s+de\s+receita|quanto\s+(foi|é|foi)\s+(o\s+)?fatur)',
    re.IGNORECASE
)

_RECEITA_PATTERN = re.compile(
    r'\b(receita|recebimento|entradas?)\b',
    re.IGNORECASE
)

_DESPESA_PATTERN = re.compile(
    r'\b(despesas?|custos?|gastos?|pagamentos?|saídas?|saidas?)\b',
    re.IGNORECASE
)

_RESULTADO_PATTERN = re.compile(
    r'\b(resultado|saldo|lucro|prejuízo|prejuizo|líquido|liquido|valor\s+líquido|valor\s+liquido)\b',
    re.IGNORECASE
)

_EBITDA_PATTERN = re.compile(
    r'\b(ebitda|resultado\s+operacional|lucro\s+operacional)\b',
    re.IGNORECASE
)


def try_kpi_shortcut(question: str) -> Optional[KPIMatch]:
    """
    Tenta resolver a pergunta como KPI simples sem passar pelo LLM.
    Retorna KPIMatch com DAX pré-validado, ou None se não reconhecer.
    """
    year = _extract_year(question) or _DEFAULT_YEAR
    month = _extract_month(question)
    filters = _build_filters(year, month)
    filter_block = _build_filter_block(year, month)

    # EBITDA (verificar antes de resultado pois é mais específico)
    if _EBITDA_PATTERN.search(question):
        if month:
            dax = f"""EVALUATE
ROW(
    "EBITDA", CALCULATE(
        [EBITDA],
        'data'[Ano ] = "{year}",
        'data'[Nome mês] = "{month}"
    )
)"""
            label = f"EBITDA — {month}/{year}"
        else:
            dax = f"""EVALUATE
ROW(
    "EBITDA", CALCULATE(
        [EBITDA],
        'data'[Ano ] = "{year}"
    )
)"""
            label = f"EBITDA — {year}"
        return KPIMatch(dax_query=dax, explanation=label, label=label)

    # FATURAMENTO / RECEITA
    if _FATURAMENTO_PATTERN.search(question) or _RECEITA_PATTERN.search(question):
        if month:
            dax = f"""EVALUATE
ROW(
    "Receita", CALCULATE(
        SUM('data'[receita]),
        'data'[Ano ] = "{year}",
        'data'[Nome mês] = "{month}"
    )
)"""
            label = f"Receita — {month}/{year}"
        else:
            dax = f"""EVALUATE
ROW(
    "Receita", CALCULATE(
        SUM('data'[receita]),
        'data'[Ano ] = "{year}"
    )
)"""
            label = f"Receita — {year}"
        return KPIMatch(dax_query=dax, explanation=label, label=label)

    # DESPESA
    if _DESPESA_PATTERN.search(question):
        if month:
            dax = f"""EVALUATE
ROW(
    "Despesa", CALCULATE(
        SUM('data'[despesas]),
        'data'[Ano ] = "{year}",
        'data'[Nome mês] = "{month}"
    )
)"""
            label = f"Despesa — {month}/{year}"
        else:
            dax = f"""EVALUATE
ROW(
    "Despesa", CALCULATE(
        SUM('data'[despesas]),
        'data'[Ano ] = "{year}"
    )
)"""
            label = f"Despesa — {year}"
        return KPIMatch(dax_query=dax, explanation=label, label=label)

    # RESULTADO / SALDO / LUCRO
    if _RESULTADO_PATTERN.search(question):
        if month:
            dax = f"""EVALUATE
ROW(
    "Resultado", CALCULATE(
        SUM('data'[receita]) - SUM('data'[despesas]),
        'data'[Ano ] = "{year}",
        'data'[Nome mês] = "{month}"
    )
)"""
            label = f"Resultado — {month}/{year}"
        else:
            dax = f"""EVALUATE
ROW(
    "Resultado", CALCULATE(
        SUM('data'[receita]) - SUM('data'[despesas]),
        'data'[Ano ] = "{year}"
    )
)"""
            label = f"Resultado — {year}"
        return KPIMatch(dax_query=dax, explanation=label, label=label)

    return None
