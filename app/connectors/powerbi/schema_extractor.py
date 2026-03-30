"""
DynamicSchemaExtractor — Extrai metadados de datasets Power BI Pro via API REST + DAX.

Compatível com Power BI Pro (sem DMV/XMLA). Estratégia:
  1. Descobre tabelas via REST API (páginas do relatório, relatórios do workspace).
  2. Para cada tabela executa EVALUATE TOPN(1, 'Tabela') → colunas + valores de amostra.
  3. Descobre medidas via tabelas de medidas comuns.
  4. Busca relacionamentos via REST API /relationships.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# TTL do cache de schema: 30 minutos
_SCHEMA_CACHE_TTL = 1800

# Máximo de colunas para amostrar (evita flood de API calls)
_MAX_SAMPLE_COLUMNS = 40

# Colunas com alta relevância de negócio — sempre amostradas primeiro
_PRIORITY_COLUMN_PATTERNS = {
    "cgrupo", "cnatureza", "cstatus", "previsto", "caregoria", "categoria",
    "ano ", "ano_mes", "nome mes", "nome mês", "receita", "despesa",
    "resultado", "grupo", "natureza", "status", "situacao", "situação",
    "tipo", "origem", "centro", "departamento",
}

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _infer_dtype(value: Any) -> str:
    """Infere tipo de dado Power BI a partir de um valor Python de amostra."""
    if value is None:
        return "TEXT"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DECIMAL"
    # Datas chegam como string ISO no JSON do Power BI
    if isinstance(value, str):
        import re as _re
        if _re.match(r'^\d{4}-\d{2}-\d{2}', value):
            return "DATETIME"
        return "TEXT"
    return "TEXT"


# ─────────────────────────────────────────────────────────────
# Modelos
# ─────────────────────────────────────────────────────────────

@dataclass
class ColumnInfo:
    table_id: int
    name: str
    data_type: str
    is_hidden: bool = False
    description: str = ""
    expression: Optional[str] = None   # Para colunas calculadas
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class MeasureInfo:
    table_id: int
    name: str
    expression: str
    description: str = ""
    display_folder: str = ""
    is_hidden: bool = False


@dataclass
class RelationshipInfo:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: str = "ManyToOne"     # "ManyToOne" | "OneToMany" | "OneToOne"
    cross_filter: str = "OneDirection"


@dataclass
class HierarchyInfo:
    table_name: str
    name: str
    description: str = ""


@dataclass
class TableInfo:
    id: int
    name: str
    is_hidden: bool = False
    description: str = ""
    columns: List[ColumnInfo] = field(default_factory=list)
    measures: List[MeasureInfo] = field(default_factory=list)


@dataclass
class DatasetSchema:
    dataset_id: str
    workspace_id: str
    tables: List[TableInfo] = field(default_factory=list)
    relationships: List[RelationshipInfo] = field(default_factory=list)
    hierarchies: List[HierarchyInfo] = field(default_factory=list)
    extracted_at: float = field(default_factory=time.time)

    @property
    def is_fresh(self) -> bool:
        return (time.time() - self.extracted_at) < _SCHEMA_CACHE_TTL

    def get_table(self, name: str) -> Optional[TableInfo]:
        for t in self.tables:
            if t.name.lower() == name.lower():
                return t
        return None

    def get_all_measures(self, include_hidden: bool = False) -> List[MeasureInfo]:
        measures = []
        for t in self.tables:
            for m in t.measures:
                if include_hidden or not m.is_hidden:
                    measures.append(m)
        return measures

    def to_schema_dict(self) -> Dict[str, Any]:
        """Converte para o formato de schema dict usado pelo orchestrator."""
        tables = []
        for t in self.tables:
            if t.is_hidden:
                continue
            columns = [
                {
                    "name": c.name,
                    "dataType": c.data_type,
                    "isHidden": c.is_hidden,
                    "description": c.description,
                    "expression": c.expression,
                    "sampleValues": c.sample_values[:5],
                }
                for c in t.columns
            ]
            measures = [
                {
                    "name": m.name,
                    "expression": m.expression,
                    "isHidden": m.is_hidden,
                    "description": m.description,
                    "displayFolder": m.display_folder,
                }
                for m in t.measures
            ]
            tables.append({
                "name": t.name,
                "isHidden": t.is_hidden,
                "description": t.description,
                "columns": columns,
                "measures": measures,
            })

        relationships = [
            {
                "fromTable": r.from_table,
                "fromColumn": r.from_column,
                "toTable": r.to_table,
                "toColumn": r.to_column,
                "cardinality": r.cardinality,
            }
            for r in self.relationships
        ]

        return {
            "dataset_id": self.dataset_id,
            "tables": tables,
            "relationships": relationships,
            "extracted_at": datetime.fromtimestamp(self.extracted_at).isoformat(),
        }


# ─────────────────────────────────────────────────────────────
# Cache singleton
# ─────────────────────────────────────────────────────────────

class SchemaCache:
    """Cache em memória com TTL para DatasetSchema.
    asyncio é single-threaded — dict operations são atômicas, Lock desnecessário.
    """

    _cache: Dict[str, DatasetSchema] = {}

    @classmethod
    async def get(cls, workspace_id: str, dataset_id: str) -> Optional[DatasetSchema]:
        key = f"{workspace_id}:{dataset_id}"
        schema = cls._cache.get(key)
        if schema and schema.is_fresh:
            return schema
        return None

    @classmethod
    async def set(cls, schema: DatasetSchema) -> None:
        key = f"{schema.workspace_id}:{schema.dataset_id}"
        cls._cache[key] = schema

    @classmethod
    async def invalidate(cls, workspace_id: str, dataset_id: str) -> None:
        key = f"{workspace_id}:{dataset_id}"
        cls._cache.pop(key, None)


# ─────────────────────────────────────────────────────────────
# Extrator principal
# ─────────────────────────────────────────────────────────────

class DynamicSchemaExtractor:
    """
    Extrai o schema completo de um dataset Power BI Pro via REST API + DAX TOPN.

    Uso:
        extractor = DynamicSchemaExtractor(client)
        schema = await extractor.extract_full_schema(workspace_id, dataset_id)
        schema_dict = schema.to_schema_dict()  # compatível com o orchestrator
    """

    def __init__(self, client) -> None:
        self._client = client

    async def extract_full_schema(
        self,
        workspace_id: str,
        dataset_id: str,
        use_cache: bool = True,
        report_id: Optional[str] = None,
    ) -> DatasetSchema:
        """
        Extrai schema completo (com cache) via estratégia Pro-compatível:
          a. Descobre nomes de tabelas via REST API + páginas do relatório.
          b. Para cada tabela executa EVALUATE TOPN(1, 'Tabela') → colunas + amostras.
          c. Descobre medidas via tabelas de medidas comuns.
          d. Busca relacionamentos via REST API.

        Não lança exceção — retorna DatasetSchema vazio se a descoberta falhar.
        """
        if use_cache:
            cached = await SchemaCache.get(workspace_id, dataset_id)
            if cached:
                logger.debug("SchemaExtractor: usando cache (%s)", dataset_id)
                return cached

        schema = await self._try_pro_extraction(workspace_id, dataset_id, report_id)
        await SchemaCache.set(schema)
        logger.info(
            "SchemaExtractor (Pro): %d tabelas, %d medidas, %d relacionamentos — dataset %s",
            len(schema.tables),
            sum(len(t.measures) for t in schema.tables),
            len(schema.relationships),
            dataset_id,
        )
        return schema

    # ─────────────────────────────────────────────────────────────
    # Estratégia Pro-compatível (TOPN + REST API)
    # ─────────────────────────────────────────────────────────────

    # Semáforo: máximo 2 queries DAX simultâneas (evita throttling do Power BI Pro)
    _DAX_SEMAPHORE = asyncio.Semaphore(2)

    async def _try_pro_extraction(
        self,
        workspace_id: str,
        dataset_id: str,
        report_id: Optional[str],
    ) -> DatasetSchema:
        """
        Extrai schema sem DMV, compatível com Power BI Pro.

        A tabela principal é sempre 'data' — não há descoberta de tabelas.
        Foca em:
          1. Colunas + amostras de 'data' via TOPN(1).
          2. Hard filters do relatório via pages API.
          3. Relacionamentos via REST API.

        Nunca lança exceção — retorna DatasetSchema (possivelmente parcial).
        """
        tables_list: List[TableInfo] = []

        # ── 1. Tabela 'data' — sempre presente ────────────────────
        data_table = await self._probe_data_table(workspace_id, dataset_id)
        if data_table.columns:
            tables_list.append(data_table)
            logger.info(
                "SchemaExtractor Pro: 'data' — %d colunas — dataset %s",
                len(data_table.columns), dataset_id,
            )
        else:
            logger.warning("SchemaExtractor Pro: tabela 'data' vazia ou inacessivel — dataset %s", dataset_id)

        # ── 2. Hard filters das páginas do relatório ───────────────
        page_filters: List[str] = []
        if report_id:
            page_filters = await self._get_report_page_filters(workspace_id, report_id)
            if page_filters:
                logger.info("SchemaExtractor Pro: %d hard filter(s) extraidos do relatorio", len(page_filters))

        # ── 3. Relacionamentos via REST API ────────────────────────
        rel_objs = await self._get_relationships_rest(workspace_id, dataset_id)

        schema = DatasetSchema(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            tables=tables_list,
            relationships=rel_objs,
        )
        schema.report_filters = page_filters  # type: ignore[attr-defined]
        return schema

    async def _probe_data_table(self, workspace_id: str, dataset_id: str) -> TableInfo:
        """Extrai colunas e amostras da tabela 'data' via EVALUATE TOPN(1, 'data')."""
        ti = TableInfo(id=0, name="data")
        try:
            async with self._DAX_SEMAPHORE:
                result = await self._client.execute_query(
                    dataset_id=dataset_id,
                    dax_query="EVALUATE TOPN(1, 'data')",
                    workspace_id=workspace_id,
                )
            col_names = result.get("columns", [])
            rows = result.get("rows", [])
            sample_row = rows[0] if rows else {}
            for col_name in col_names:
                sample_val = sample_row.get(col_name)
                ti.columns.append(ColumnInfo(
                    table_id=0,
                    name=col_name,
                    data_type=_infer_dtype(sample_val),
                    sample_values=[sample_val] if sample_val is not None else [],
                ))
        except Exception as e:
            logger.debug("SchemaExtractor Pro: TOPN('data') falhou — %s", e)
        return ti

    async def _get_report_page_filters(
        self, workspace_id: str, report_id: str
    ) -> List[str]:
        """
        Extrai filtros hard das páginas do relatório via REST API.
        Retorna lista de strings descritivas dos filtros (para contexto do LLM).
        """
        import json
        filters_desc: List[str] = []
        try:
            pages_data = await self._client._get(
                f"/groups/{workspace_id}/reports/{report_id}/pages"
            )
            pages = pages_data.get("value", [])
            seen: set = set()
            for page in pages:
                raw = page.get("filters", "")
                if not raw:
                    continue
                try:
                    items = json.loads(raw) if isinstance(raw, str) else raw
                    for flt in (items if isinstance(items, list) else []):
                        expr = flt.get("expression", {})
                        col_expr = expr.get("Column", {})
                        entity = col_expr.get("Expression", {}).get("SourceRef", {}).get("Entity", "")
                        prop = col_expr.get("Property", "")
                        values = [c.get("litteral", {}).get("value", "") for c in flt.get("filter", {}).get("Conditions", [])]
                        if entity and prop:
                            key = f"{entity}[{prop}]"
                            if key not in seen:
                                seen.add(key)
                                v_str = f" IN {values}" if values else ""
                                filters_desc.append(f"'{entity}'[{prop}]{v_str}")
                except Exception:
                    pass
        except Exception as e:
            logger.debug("SchemaExtractor Pro: GET pages/filters falhou — %s", e)
        return filters_desc

    async def _get_relationships_rest(
        self,
        workspace_id: str,
        dataset_id: str,
    ) -> List[RelationshipInfo]:
        """Busca relacionamentos via REST API."""
        try:
            data = await self._client._get(
                f"/groups/{workspace_id}/datasets/{dataset_id}/relationships"
            )
            rels = []
            for r in data.get("value", []):
                from_t = r.get("fromTable", "")
                to_t = r.get("toTable", "")
                if from_t and to_t:
                    rels.append(RelationshipInfo(
                        from_table=from_t,
                        from_column=r.get("fromColumn", ""),
                        to_table=to_t,
                        to_column=r.get("toColumn", ""),
                        cardinality=r.get("crossFilteringBehavior", "OneDirection"),
                    ))
            return rels
        except Exception as e:
            logger.debug("SchemaExtractor Pro: GET relationships falhou — %s", e)
            return []

    async def extract_from_pbix(
        self,
        pbix_bytes: bytes,
        workspace_id: str,
        dataset_id: str,
    ) -> "DatasetSchema":
        """
        Extrai schema a partir do PBIX — apenas parsing do Report/Layout + REST API.
        Não executa nenhuma query DAX contra o dataset.

        Descobre tabelas, colunas e medidas a partir das prototypeQuery dos visuais
        e das expressões de filtro do relatório.
        """
        import io
        import json
        import zipfile

        columns_by_table: Dict[str, set] = {}   # table_name → set[col_name]
        measures_by_table: Dict[str, set] = {}  # table_name → set[measure_name]

        def _add_col(table: str, col: str) -> None:
            if table and col:
                columns_by_table.setdefault(table, set()).add(col)

        def _add_meas(table: str, meas: str) -> None:
            if table and meas:
                measures_by_table.setdefault(table, set()).add(meas)

        def _parse_filter_list(raw) -> None:
            """Extrai tabela+coluna de uma lista de filtros PBQL."""
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    return
            if not isinstance(raw, list):
                return
            for flt in raw:
                if not isinstance(flt, dict):
                    continue
                expr = flt.get("expression", {})
                col_expr = expr.get("Column", {})
                entity = (
                    col_expr.get("Expression", {})
                    .get("SourceRef", {})
                    .get("Entity", "")
                )
                prop = col_expr.get("Property", "")
                _add_col(entity, prop)

        try:
            with zipfile.ZipFile(io.BytesIO(pbix_bytes)) as zf:
                members = zf.namelist()
                layout_file = next(
                    (m for m in members if m == "Report/Layout"),
                    next((m for m in members if m.lower().startswith("report/layout")), None),
                )
                if not layout_file:
                    logger.warning("extract_from_pbix: Report/Layout não encontrado no PBIX")
                    return DatasetSchema(dataset_id=dataset_id, workspace_id=workspace_id)

                raw = zf.read(layout_file)

            layout = None
            for enc in ("utf-16-le", "utf-16", "utf-8-sig", "utf-8"):
                try:
                    layout = json.loads(raw.decode(enc).lstrip("\ufeff"))
                    break
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
            if layout is None:
                logger.warning("extract_from_pbix: falha ao decodificar Report/Layout")
                return DatasetSchema(dataset_id=dataset_id, workspace_id=workspace_id)

            # Filtros de relatório
            _parse_filter_list(layout.get("filters", []))

            for section in layout.get("sections", []):
                # Filtros de página
                _parse_filter_list(section.get("filters", []))

                for vc in section.get("visualContainers", []):
                    # Filtros de visual
                    _parse_filter_list(vc.get("filters", []))

                    config_raw = vc.get("config", "{}")
                    if isinstance(config_raw, str):
                        try:
                            config = json.loads(config_raw)
                        except Exception:
                            continue
                    else:
                        config = config_raw

                    sv = config.get("singleVisual", {})
                    pq = sv.get("prototypeQuery", {})
                    if not pq:
                        continue

                    # Alias → entity lookup
                    alias_map = {
                        f.get("Name", ""): f.get("Entity", "")
                        for f in pq.get("From", [])
                    }

                    def _resolve(source: str) -> str:
                        return alias_map.get(source, source)

                    for sel in pq.get("Select", []):
                        # Column reference
                        col = sel.get("Column", {})
                        if col:
                            src = col.get("Expression", {}).get("SourceRef", {}).get("Source", "")
                            _add_col(_resolve(src), col.get("Property", ""))

                        # Measure reference
                        meas = sel.get("Measure", {})
                        if meas:
                            src = meas.get("Expression", {}).get("SourceRef", {}).get("Source", "")
                            _add_meas(_resolve(src), meas.get("Property", ""))

                        # Aggregation (SUM, COUNT, etc. of a column)
                        agg = sel.get("Aggregation", {})
                        if agg:
                            inner = agg.get("Expression", {}).get("Column", {})
                            if inner:
                                src = inner.get("Expression", {}).get("SourceRef", {}).get("Source", "")
                                _add_col(_resolve(src), inner.get("Property", ""))

                    # Where conditions (additional column refs)
                    for where in pq.get("Where", []):
                        cond = where.get("Condition", {})
                        in_expr = cond.get("In", {})
                        if in_expr:
                            for expr_item in in_expr.get("Expressions", []):
                                col = expr_item.get("Column", {})
                                if col:
                                    src = col.get("Expression", {}).get("SourceRef", {}).get("Source", "")
                                    _add_col(_resolve(src), col.get("Property", ""))

        except Exception as e:
            logger.error("extract_from_pbix: falha ao parsear PBIX — %s", e)
            return DatasetSchema(dataset_id=dataset_id, workspace_id=workspace_id)

        all_table_names = {
            t for t in (set(columns_by_table) | set(measures_by_table))
            if t.strip()
        }

        logger.info(
            "extract_from_pbix: %d tabelas descobertas no Layout: %s",
            len(all_table_names),
            sorted(all_table_names),
        )

        # Monta TableInfo sem executar nenhuma query
        tables_list: List[TableInfo] = []
        for i, tname in enumerate(sorted(all_table_names)):
            ti = TableInfo(id=i, name=tname)
            for col_name in sorted(columns_by_table.get(tname, set())):
                ti.columns.append(ColumnInfo(table_id=i, name=col_name, data_type="TEXT"))
            for mname in sorted(measures_by_table.get(tname, set())):
                ti.measures.append(MeasureInfo(table_id=i, name=mname, expression=""))
            tables_list.append(ti)

        # Relacionamentos via REST API (sem queries DAX)
        rel_objs: List[RelationshipInfo] = []
        try:
            data = await self._client._get(
                f"/groups/{workspace_id}/datasets/{dataset_id}/relationships"
            )
            for r in data.get("value", []):
                from_t = r.get("fromTable", "")
                to_t = r.get("toTable", "")
                if from_t and to_t:
                    rel_objs.append(RelationshipInfo(
                        from_table=from_t,
                        from_column=r.get("fromColumn", ""),
                        to_table=to_t,
                        to_column=r.get("toColumn", ""),
                        cardinality=r.get("crossFilteringBehavior", "OneDirection"),
                    ))
        except Exception as e:
            logger.debug("extract_from_pbix: relacionamentos via REST — %s", e)

        schema = DatasetSchema(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            tables=tables_list,
            relationships=rel_objs,
        )
        await SchemaCache.set(schema)

        logger.info(
            "extract_from_pbix: schema final — %d tabelas, %d colunas, %d medidas, %d relacionamentos",
            len(tables_list),
            sum(len(t.columns) for t in tables_list),
            sum(len(t.measures) for t in tables_list),
            len(rel_objs),
        )
        return schema

    # ── Sampling ──────────────────────────────────────────────

    def _select_columns_for_sampling(
        self, tables: List[TableInfo]
    ) -> List[tuple]:
        """
        Seleciona até _MAX_SAMPLE_COLUMNS colunas TEXT não ocultas para amostrar.
        Prioriza: (1) colunas de alta relevância de negócio, (2) tabelas principais,
        (3) exclui colunas que parecem IDs técnicos.
        """
        id_pattern = {"app_key", "app_secret", "secret", "chave", "guid",
                      "pk", "fk", "_id", "aux_", "nCod", "cCod", "pagina"}
        priority: List[tuple] = []
        regular: List[tuple] = []

        # Coloca tabelas principais primeiro (data, depois outras)
        sorted_tables = sorted(
            [t for t in tables if not t.is_hidden],
            key=lambda t: (0 if t.name.lower() == "data" else 1, t.name),
        )

        for t in sorted_tables:
            for c in t.columns:
                if c.is_hidden or c.data_type != "TEXT" or c.expression:
                    continue
                name_lower = c.name.lower()
                # Exclui IDs técnicos
                if any(p in name_lower for p in id_pattern):
                    continue
                tup = (t.name, c.name, c)
                # Prioriza colunas de negócio
                if any(p in name_lower for p in _PRIORITY_COLUMN_PATTERNS):
                    priority.append(tup)
                else:
                    regular.append(tup)

        combined = priority + regular
        return combined[:_MAX_SAMPLE_COLUMNS]

    async def _fill_sample_values(
        self,
        workspace_id: str,
        dataset_id: str,
        tasks: List[tuple],
    ) -> None:
        """Busca amostras de valores em paralelo para as colunas selecionadas."""
        async def _sample(table_name: str, col_name: str, col_obj: ColumnInfo) -> None:
            try:
                dax = (
                    f"EVALUATE TOPN(10, "
                    f"SUMMARIZE('{table_name}', '{table_name}'[{col_name}]), "
                    f"'{table_name}'[{col_name}], ASC)"
                )
                async with self._DAX_SEMAPHORE:
                    result = await self._client.execute_query(
                        dataset_id=dataset_id,
                        dax_query=dax,
                        workspace_id=workspace_id,
                    )
                rows = result.get("rows", [])
                col_obj.sample_values = [
                    list(r.values())[0]
                    for r in rows
                    if r and list(r.values())[0] is not None
                ][:10]
            except Exception as e:
                logger.debug("Sample falhou para '%s'[%s]: %s", table_name, col_name, e)

        await asyncio.gather(*[_sample(t, c, obj) for t, c, obj in tasks])
