"""
PowerBI Report Filter Extractor — Extrai filtros hard do relatório via API.

Baixa o PBIX (via Export API) e parseia o Layout JSON para extrair:
  - Filtros a nível de relatório
  - Filtros a nível de página
  - Filtros a nível de visual

Converte para cláusulas DAX que são injetadas em todas as queries geradas,
garantindo que a IA respeite os mesmos filtros que o usuário vê no relatório.
"""

import html
import io
import json
import logging
import time
import traceback
import zipfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# TTL do cache de filtros: 30 minutos (o layout raramente muda)
_FILTER_CACHE_TTL = 1800

# ─────────────────────────────────────────────────────────────
# Modelos
# ─────────────────────────────────────────────────────────────

@dataclass
class PBIFilter:
    """Representa um filtro extraído do relatório Power BI."""
    table: str
    column: str
    filter_type: int          # 1=Basic, 2=Advanced, 3=TopN, 4=Date, 8=RelativeDate
    operator: Optional[str] = None
    values: List[Any] = field(default_factory=list)
    conditions: List[Dict] = field(default_factory=list)  # Para Advanced (filterType=2)
    logical_operator: Optional[str] = None  # "And" | "Or"
    source: str = "report"    # "report" | "page:{name}" | "visual:{id}"
    dax: Optional[str] = None  # Cláusula DAX pré-calculada


@dataclass
class ReportFilters:
    """Container com todos os filtros extraídos de um relatório."""
    workspace_id: str
    report_id: str
    report_filters: List[PBIFilter] = field(default_factory=list)
    page_filters: Dict[str, List[PBIFilter]] = field(default_factory=dict)
    visual_filters: Dict[str, List[PBIFilter]] = field(default_factory=dict)
    extracted_at: float = field(default_factory=time.time)

    @property
    def is_fresh(self) -> bool:
        return (time.time() - self.extracted_at) < _FILTER_CACHE_TTL

    def all_report_and_page_filters(self, page_name: Optional[str] = None) -> List[PBIFilter]:
        """Retorna filtros de relatório + página específica (se fornecida)."""
        filters = list(self.report_filters)
        if page_name and page_name in self.page_filters:
            filters.extend(self.page_filters[page_name])
        elif not page_name:
            # Se não especificou página, inclui filtros de TODAS as páginas
            # (só os que são comuns, ou seja, se alguma página tem filtros que
            #  precisam ser aplicados globalmente)
            # Na prática, preferimos não aplicar page filters sem saber a página
            pass
        return filters


# ─────────────────────────────────────────────────────────────
# Mapeamento de operadores Power BI → DAX
# ─────────────────────────────────────────────────────────────

# filterType 2 (Advanced) — operadores de condição
_CONDITION_OPERATOR_MAP = {
    "LessThan": "<",
    "LessThanOrEqual": "<=",
    "GreaterThan": ">",
    "GreaterThanOrEqual": ">=",
    "Contains": None,           # Sem equivalente simples em DAX de filtro
    "DoesNotContain": None,
    "StartsWith": None,
    "Is": "= BLANK()",
    "IsNot": "<> BLANK()",
    "In": "IN",
    "NotIn": "NOT IN",
    # Numéricos
    "Equals": "=",
    "NotEquals": "<>",
}

# filterType 1 (Basic) — condição única
_BASIC_CONDITION_MAP = {
    "Is": "=",
    "IsNot": "<>",
    "In": "IN",
    "NotIn": "NOT IN",
    "LessThan": "<",
    "LessThanOrEqual": "<=",
    "GreaterThan": ">",
    "GreaterThanOrEqual": ">=",
}


# ─────────────────────────────────────────────────────────────
# Extrator principal
# ─────────────────────────────────────────────────────────────

class PowerBIReportFilterExtractor:
    """
    Extrai filtros hard de relatórios Power BI via download do PBIX.

    Uso:
        extractor = PowerBIReportFilterExtractor(client)
        filters = await extractor.extract_all_filters(workspace_id, report_id)
        dax_clauses = extractor.merge_filters_to_dax(filters)
        # dax_clauses = ["'data'[cNatureza] = \"R\"", "'data'[Ano ] = \"2025\""]
    """

    # Cache singleton em memória: (workspace_id, report_id) → ReportFilters
    # asyncio é single-threaded — não precisa de Lock para acessar dict
    _cache: Dict[str, "ReportFilters"] = {}

    def __init__(self, client) -> None:
        self._client = client

    # ── API pública ───────────────────────────────────────────

    async def extract_all_filters(
        self,
        workspace_id: str,
        report_id: str,
    ) -> ReportFilters:
        """
        Extrai todos os filtros do relatório (com cache de 30 min).
        Não lança exceção — retorna ReportFilters vazio se falhar.
        """
        cache_key = f"{workspace_id}:{report_id}"

        cached = self._cache.get(cache_key)
        if cached and cached.is_fresh:
            logger.debug("FilterExtractor: usando cache de filtros (%s)", cache_key)
            return cached

        try:
            pbix_bytes = await self._download_pbix(workspace_id, report_id)
            layout = await self._parse_layout_from_pbix(pbix_bytes)

            report_filters = self._extract_report_filters(layout)
            page_filters = self._extract_page_filters(layout)
            visual_filters = self._extract_visual_filters(layout)

            result = ReportFilters(
                workspace_id=workspace_id,
                report_id=report_id,
                report_filters=report_filters,
                page_filters=page_filters,
                visual_filters=visual_filters,
            )

            self._cache[cache_key] = result

            total = (
                len(report_filters)
                + sum(len(v) for v in page_filters.values())
                + sum(len(v) for v in visual_filters.values())
            )
            logger.info(
                "FilterExtractor: %d filtros extraídos do relatório %s "
                "(%d report, %d em páginas, %d em visuais)",
                total, report_id,
                len(report_filters),
                sum(len(v) for v in page_filters.values()),
                sum(len(v) for v in visual_filters.values()),
            )
            return result

        except Exception as e:
            logger.warning(
                "FilterExtractor: falha ao extrair filtros do relatório %s — %s (%s).\n%s",
                report_id, e, type(e).__name__, traceback.format_exc(),
            )
            return ReportFilters(workspace_id=workspace_id, report_id=report_id)

    def merge_filters_to_dax(
        self,
        report_filters: ReportFilters,
        page_name: Optional[str] = None,
    ) -> List[str]:
        """
        Converte filtros de relatório (+ página opcional) em cláusulas DAX.
        Retorna lista de strings prontas para usar em CALCULATE(..., <filtros>).
        """
        filters = report_filters.all_report_and_page_filters(page_name)
        dax_clauses = []
        for f in filters:
            clause = f.dax or self._pbi_filter_to_dax(f)
            if clause:
                dax_clauses.append(clause)
        return dax_clauses

    def invalidate_cache(self, workspace_id: str, report_id: str) -> None:
        """Invalida o cache para um relatório específico."""
        cache_key = f"{workspace_id}:{report_id}"
        self._cache.pop(cache_key, None)

    # ── Download ──────────────────────────────────────────────

    async def _download_pbix(self, workspace_id: str, report_id: str) -> bytes:
        """Baixa o arquivo PBIX via GET /groups/{wid}/reports/{rid}/Export."""
        return await self._client.download_report_pbix(
            report_id=report_id,
            workspace_id=workspace_id,
        )

    # ── Parse do Layout ───────────────────────────────────────

    async def _parse_layout_from_pbix(self, pbix_bytes: bytes) -> Dict[str, Any]:
        """
        Abre o ZIP em memória e extrai Report/Layout.
        O Layout é um JSON com encoding UTF-16-LE (padrão do Power BI Desktop).
        """
        with zipfile.ZipFile(io.BytesIO(pbix_bytes)) as zf:
            # O arquivo de layout pode estar em Report/Layout ou Report/Layout.json
            layout_names = [n for n in zf.namelist() if n.lower().startswith("report/layout")]
            if not layout_names:
                raise ValueError("PBIX não contém Report/Layout — arquivo pode estar protegido ou ser Premium-only")

            layout_bytes = zf.read(layout_names[0])

        # Power BI Desktop salva com BOM UTF-16-LE
        for encoding in ("utf-16-le", "utf-16", "utf-8-sig", "utf-8"):
            try:
                raw = layout_bytes.decode(encoding).lstrip("\ufeff")
                return json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue

        raise ValueError("Não foi possível decodificar Report/Layout do PBIX")

    # ── Extração de filtros ───────────────────────────────────

    def _parse_filters_string(self, filters_raw: Any) -> List[Dict]:
        """
        Parseia o campo 'filters' do Layout — que é um JSON-string duplamente encodado.
        Retorna lista de objetos de filtro ou [] se falhar.
        """
        if not filters_raw:
            return []
        if isinstance(filters_raw, list):
            return filters_raw
        if isinstance(filters_raw, str):
            try:
                parsed = json.loads(filters_raw)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
        return []

    def _extract_report_filters(self, layout: Dict[str, Any]) -> List[PBIFilter]:
        """Extrai filtros a nível de relatório."""
        raw = self._parse_filters_string(layout.get("filters"))
        result = []
        for f in raw:
            pbi = self._parse_single_filter(f, source="report")
            if pbi:
                result.append(pbi)
        return result

    def _extract_page_filters(self, layout: Dict[str, Any]) -> Dict[str, List[PBIFilter]]:
        """Extrai filtros a nível de página, indexados pelo displayName da página."""
        page_filters: Dict[str, List[PBIFilter]] = {}
        for section in layout.get("sections", []):
            page_name = section.get("displayName") or section.get("name", "unknown")
            raw = self._parse_filters_string(section.get("filters"))
            filters = []
            for f in raw:
                pbi = self._parse_single_filter(f, source=f"page:{page_name}")
                if pbi:
                    filters.append(pbi)
            if filters:
                page_filters[page_name] = filters
        return page_filters

    def _extract_visual_filters(self, layout: Dict[str, Any]) -> Dict[str, List[PBIFilter]]:
        """Extrai filtros a nível de visual."""
        visual_filters: Dict[str, List[PBIFilter]] = {}
        for section in layout.get("sections", []):
            page_name = section.get("displayName") or section.get("name", "")
            for vc in section.get("visualContainers", []):
                # O config do visual é também JSON-string
                config_raw = vc.get("config", "{}")
                if isinstance(config_raw, str):
                    try:
                        config = json.loads(config_raw)
                    except json.JSONDecodeError:
                        config = {}
                else:
                    config = config_raw

                visual_id = config.get("name", "") or vc.get("id", "")
                raw = self._parse_filters_string(vc.get("filters"))
                filters = []
                for f in raw:
                    pbi = self._parse_single_filter(f, source=f"visual:{page_name}:{visual_id}")
                    if pbi:
                        filters.append(pbi)
                if filters:
                    visual_filters[f"{page_name}:{visual_id}"] = filters
        return visual_filters

    def _parse_single_filter(self, filter_obj: Dict, source: str) -> Optional[PBIFilter]:
        """
        Converte um objeto de filtro do Power BI em PBIFilter.

        O Power BI usa o formato Power BI Query Language (PBQL) para filtros internos:
          {
            "expression": {"Column": {"Expression": {"SourceRef": {"Entity": "data"}}, "Property": "coluna"}},
            "filter": {"Where": [{"Condition": {"In": {"Values": [...]}}}]},
            "type": "Categorical"
          }

        Retorna None se não conseguir parsear.
        """
        if not isinstance(filter_obj, dict):
            return None

        # Extrai tabela e coluna do campo "expression"
        expr = filter_obj.get("expression", {})
        col_expr = expr.get("Column", {})
        table = col_expr.get("Expression", {}).get("SourceRef", {}).get("Entity", "")
        column = col_expr.get("Property", "")

        if not table or not column:
            return None

        # Só processa tabelas da fonte principal de dados
        # (ignora slicers e filtros sem filtro ativo)
        flt = filter_obj.get("filter", {})

        pbi = PBIFilter(
            table=table,
            column=column,
            filter_type=0,   # será preenchido abaixo
            source=source,
        )

        # Pré-calcula o DAX a partir da estrutura PBQL
        pbi.dax = self._pbql_filter_to_dax(table, column, flt)
        return pbi if pbi.dax else None

    # ── Conversão PBQL → DAX ──────────────────────────────────

    def _pbql_filter_to_dax(
        self,
        table: str,
        column: str,
        flt: Dict,
    ) -> Optional[str]:
        """
        Converte o campo 'filter' do formato Power BI Query Language para DAX.

        Suporta:
          - In (IN {...})
          - Not > In (NOT IN {...})
          - Comparison (=, <>, <, <=, >, >=)
        """
        if not flt:
            return None

        col_ref = f"'{table}'[{column}]"
        where = flt.get("Where", [])
        parts = []

        for w in where:
            cond = w.get("Condition", {})
            clause = self._parse_pbql_condition(col_ref, cond)
            if clause:
                parts.append(clause)

        if not parts:
            return None
        return " && ".join(parts)

    def _parse_pbql_condition(self, col_ref: str, cond: Dict) -> Optional[str]:
        """Parseia uma condição PBQL e retorna cláusula DAX."""

        # NOT { In: {...} } → NOT IN
        if "Not" in cond:
            inner_expr = cond["Not"].get("Expression", {})
            if "In" in inner_expr:
                vals = self._extract_pbql_in_values(inner_expr["In"])
                if vals:
                    vals_str = ", ".join(f'"{v}"' for v in vals)
                    return f"{col_ref} NOT IN {{{vals_str}}}"

        # In: {...} → IN
        if "In" in cond:
            vals = self._extract_pbql_in_values(cond["In"])
            if vals:
                if len(vals) == 1:
                    return f'{col_ref} = "{vals[0]}"'
                vals_str = ", ".join(f'"{v}"' for v in vals)
                return f"{col_ref} IN {{{vals_str}}}"

        # Comparison: { ComparisonKind, Left, Right }
        if "Comparison" in cond:
            c = cond["Comparison"]
            kind = c.get("ComparisonKind", 0)
            ops = {0: "=", 1: "<>", 2: "<", 3: "<=", 4: ">", 5: ">="}
            op = ops.get(kind, "=")
            right_lit = c.get("Right", {}).get("Literal", {}).get("Value", "")
            if right_lit.startswith("'") and right_lit.endswith("'"):
                right_lit = right_lit[1:-1]
            if right_lit:
                return f'{col_ref} {op} "{html.unescape(right_lit)}"'

        return None

    def _extract_pbql_in_values(self, in_expr: Dict) -> List[str]:
        """Extrai lista de valores de um IN expression PBQL."""
        vals = []
        for row in in_expr.get("Values", []):
            for item in row:
                lit = item.get("Literal", {}).get("Value", "")
                if lit.startswith("'") and lit.endswith("'"):
                    lit = lit[1:-1]
                if lit:
                    vals.append(html.unescape(lit))
        return vals

    # ── Conversão legada (filterType format) ─────────────────
    # Mantido para compatibilidade com relatórios que usam o formato antigo

    def _pbi_filter_to_dax(self, f: PBIFilter) -> Optional[str]:
        """Converte PBIFilter (formato legado filterType) para DAX."""
        col_ref = f"'{f.table}'[{f.column}]"
        try:
            if f.filter_type == 1:
                return self._basic_filter_to_dax(col_ref, f.operator, f.values)
            elif f.filter_type == 2:
                return self._advanced_filter_to_dax(col_ref, f.conditions, f.logical_operator)
            elif f.filter_type == 4:
                return self._date_filter_to_dax(col_ref, f.values)
        except Exception as e:
            logger.debug("Não foi possível converter filtro para DAX: %s — %s", f, e)
        return None

    def _basic_filter_to_dax(self, col_ref, operator, values):
        if not values:
            return None
        op = operator or "In"
        non_null = [v for v in values if v is not None]
        def _q(v):
            return f'"{v}"' if isinstance(v, str) else str(v)
        if op in ("In", "NotIn"):
            if not non_null:
                return None
            vals_str = ", ".join(_q(v) for v in non_null)
            clause = f"{col_ref} IN {{{vals_str}}}"
            return f"NOT ({clause})" if op == "NotIn" else clause
        if len(non_null) == 1:
            dax_op = _BASIC_CONDITION_MAP.get(op)
            if dax_op:
                return f"{col_ref} {dax_op} {_q(non_null[0])}"
        return None

    def _advanced_filter_to_dax(self, col_ref, conditions, logical_operator):
        if not conditions:
            return None
        parts = []
        for cond in conditions:
            op = cond.get("operator", "")
            value = cond.get("value")
            dax_op = _CONDITION_OPERATOR_MAP.get(op)
            if dax_op and value is not None:
                val_str = f'"{value}"' if isinstance(value, str) else str(value)
                parts.append(f"{col_ref} {dax_op} {val_str}")
        if not parts:
            return None
        joiner = " && " if (logical_operator or "And") == "And" else " || "
        return parts[0] if len(parts) == 1 else "(" + joiner.join(parts) + ")"

    def _date_filter_to_dax(self, col_ref, values):
        lower = values[0] if len(values) > 0 else None
        upper = values[1] if len(values) > 1 else None
        if lower and upper:
            return f'({col_ref} >= "{lower}" && {col_ref} <= "{upper}")'
        if lower:
            return f'{col_ref} >= "{lower}"'
        if upper:
            return f'{col_ref} <= "{upper}"'
        return None
