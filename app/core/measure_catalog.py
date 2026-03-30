"""
MeasureCatalog — Auto-descoberta de medidas do dataset Power BI via DMV.

Executa EVALUATE INFO.MEASURES() via executeQueries (disponível no Power BI Pro)
para descobrir automaticamente TODAS as medidas nativas do dataset.

Zero configuração manual: qualquer medida publicada no Power BI fica disponível
para o chat imediatamente no próximo refresh do catálogo (TTL: 30 minutos).
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# TTL padrão: 30 minutos
_DEFAULT_TTL = 1800


@dataclass
class CatalogMeasure:
    """Representa uma medida descoberta do dataset Power BI."""
    name: str             # Nome exato: "CAPEX", "EBITDA"
    table_name: str       # Tabela pai (pode ser vazio se DMV não retornar)
    expression: str       # Expressão DAX (pode ser vazio)
    is_hidden: bool = False

    @property
    def dax_ref(self) -> str:
        """Referência DAX para usar em CALCULATE: [CAPEX]"""
        return f"[{self.name}]"


class MeasureCatalog:
    """
    Catálogo dinâmico de medidas do Power BI.

    Descobre medidas via INFO.MEASURES() DMV usando o mesmo endpoint
    executeQueries já configurado no PowerBIClient.

    Uso típico:
        catalog = MeasureCatalog.get_instance()
        await catalog.ensure_fresh(client, dataset_id)
        names = catalog.get_measure_names()
        block = catalog.build_prompt_block()
    """

    _instance: Optional["MeasureCatalog"] = None
    _lock = asyncio.Lock()

    def __init__(self, ttl_seconds: int = _DEFAULT_TTL):
        self._measures: Dict[str, CatalogMeasure] = {}  # name.lower() → measure
        self._ttl = ttl_seconds
        self._last_refresh: float = 0.0
        self._refresh_lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "MeasureCatalog":
        """Retorna instância singleton do catálogo."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ── Propriedades de estado ────────────────────────────────────

    @property
    def is_stale(self) -> bool:
        """True se o cache expirou ou nunca foi carregado."""
        return (time.time() - self._last_refresh) > self._ttl or not self._measures

    @property
    def is_loaded(self) -> bool:
        """True se o catálogo tem pelo menos uma medida."""
        return bool(self._measures)

    # ── Carregamento ──────────────────────────────────────────────

    async def ensure_fresh(
        self,
        client,
        dataset_id: str,
        workspace_id: Optional[str] = None,
    ) -> None:
        """Garante que o catálogo está carregado e atualizado."""
        if self.is_stale:
            await self.refresh(client, dataset_id, workspace_id)

    async def refresh(
        self,
        client,
        dataset_id: str,
        workspace_id: Optional[str] = None,
    ) -> int:
        """
        Atualiza o catálogo via INFO.MEASURES() DMV.
        Thread-safe via asyncio.Lock.
        Retorna número de medidas descobertas.
        """
        async with self._refresh_lock:
            # Double-check após obter o lock
            if not self.is_stale:
                return len(self._measures)

            logger.info("MeasureCatalog: iniciando refresh via INFO.MEASURES()...")
            try:
                # Busca tabelas (para mapear TableID → nome)
                table_map = await self._fetch_table_map(client, dataset_id, workspace_id)

                # Busca medidas via DMV
                rows = await self._fetch_measures_rows(client, dataset_id, workspace_id)

                measures: Dict[str, CatalogMeasure] = {}
                for row in rows:
                    # A API do Power BI limpa os prefixos de coluna
                    # mas pode retornar com prefixo "INFO.MEASURES"
                    name = (
                        row.get("Name")
                        or row.get("[Name]")
                        or row.get("MeasureName")
                        or ""
                    ).strip()
                    if not name:
                        continue

                    table_id = row.get("TableID") or row.get("TableId")
                    table_name = table_map.get(table_id, "") if table_id else ""

                    expression = (
                        row.get("Expression") or row.get("[Expression]") or ""
                    )
                    is_hidden = bool(
                        row.get("IsHidden") or row.get("[IsHidden]", False)
                    )

                    measures[name.lower()] = CatalogMeasure(
                        name=name,
                        table_name=table_name,
                        expression=expression,
                        is_hidden=is_hidden,
                    )

                self._measures = measures
                self._last_refresh = time.time()
                logger.info(
                    "MeasureCatalog: %d medidas descobertas (TTL=%ds)",
                    len(measures),
                    self._ttl,
                )
                return len(measures)

            except Exception as e:
                logger.error("MeasureCatalog: falha no refresh — %s", e)
                # Mantém cache anterior se existir
                return len(self._measures)

    async def _fetch_table_map(
        self,
        client,
        dataset_id: str,
        workspace_id: Optional[str],
    ) -> Dict:
        """Busca mapeamento TableID → nome via INFO.TABLES()."""
        try:
            result = await client.execute_query(
                dataset_id=dataset_id,
                dax_query=(
                    "EVALUATE SELECTCOLUMNS(INFO.TABLES(), "
                    '"ID", [ID], "Name", [Name])'
                ),
                workspace_id=workspace_id,
            )
            table_map = {}
            for row in result.get("rows", []):
                tid = row.get("ID")
                tname = row.get("Name", "")
                if tid is not None:
                    table_map[tid] = tname
            return table_map
        except Exception as e:
            logger.debug("MeasureCatalog: INFO.TABLES() não disponível — %s", e)
            return {}

    async def _fetch_measures_rows(
        self,
        client,
        dataset_id: str,
        workspace_id: Optional[str],
    ) -> List[Dict]:
        """Busca medidas brutas via INFO.MEASURES()."""
        # Tenta SELECTCOLUMNS primeiro (mais limpo)
        try:
            result = await client.execute_query(
                dataset_id=dataset_id,
                dax_query=(
                    "EVALUATE SELECTCOLUMNS("
                    "INFO.MEASURES(), "
                    '"Name", [Name], '
                    '"TableID", [TableID], '
                    '"Expression", [Expression], '
                    '"IsHidden", [IsHidden])'
                ),
                workspace_id=workspace_id,
            )
            rows = result.get("rows", [])
            if rows:
                return rows
        except Exception as e:
            logger.debug("SELECTCOLUMNS INFO.MEASURES() falhou: %s", e)

        # Fallback: EVALUATE INFO.MEASURES() sem SELECTCOLUMNS
        try:
            result = await client.execute_query(
                dataset_id=dataset_id,
                dax_query="EVALUATE INFO.MEASURES()",
                workspace_id=workspace_id,
            )
            return result.get("rows", [])
        except Exception as e:
            logger.error("INFO.MEASURES() falhou completamente: %s", e)
            return []

    # ── Acesso ao catálogo ────────────────────────────────────────

    def get_measure_names(self, include_hidden: bool = False) -> List[str]:
        """Retorna lista de nomes de medidas (sem as ocultas por padrão)."""
        return [
            m.name
            for m in self._measures.values()
            if include_hidden or not m.is_hidden
        ]

    def get_all_measures(self, include_hidden: bool = False) -> List[CatalogMeasure]:
        """Retorna todas as medidas como objetos CatalogMeasure."""
        return [
            m for m in self._measures.values()
            if include_hidden or not m.is_hidden
        ]

    def find_by_name(self, name: str) -> Optional[CatalogMeasure]:
        """Busca medida por nome exato (case-insensitive)."""
        return self._measures.get(name.lower())

    # ── Integração com MeasureMatcher ────────────────────────────

    def to_matcher_args(self) -> tuple:
        """
        Retorna (measure_names, aliases) prontos para criar MeasureMatcher.
        Aliases vêm do measures.json (enriquecimento manual opcional).
        """
        names = self.get_measure_names()
        aliases = self._load_aliases_from_json()
        return names, aliases

    def _load_aliases_from_json(self) -> Dict[str, List[str]]:
        """Carrega aliases manuais do measures.json (opcional)."""
        try:
            # Tenta caminhos comuns
            for candidate in [
                Path("measures.json"),
                Path("app/measures.json"),
                Path(__file__).parent.parent.parent / "measures.json",
            ]:
                if candidate.exists():
                    data = json.loads(candidate.read_text(encoding="utf-8"))
                    return {
                        entry["name"]: entry.get("aliases", [])
                        for entry in data
                        if entry.get("name")
                    }
        except Exception as e:
            logger.debug("measures.json não disponível: %s", e)
        return {}

    # ── Prompt para o LLM ─────────────────────────────────────────

    def build_prompt_block(self) -> str:
        """
        Gera bloco de texto com todas as medidas para injeção no prompt do LLM.
        O LLM recebe a lista real de medidas e não precisa adivinhar nomes.
        """
        measures = self.get_all_measures(include_hidden=False)
        if not measures:
            return ""

        lines = [
            "## MEDIDAS NATIVAS DO DATASET (descobertas automaticamente)",
            "Use CALCULATE([NomeMedida], filtros). NUNCA reescreva a lógica inline.\n",
        ]
        for m in sorted(measures, key=lambda x: x.name):
            desc = f" — {m.table_name}" if m.table_name else ""
            lines.append(f"- [{m.name}]{desc}")

        lines.append(
            "\nREGRA ABSOLUTA: Se o usuário pedir uma métrica desta lista, "
            "use CALCULATE([NomeMedida], 'data'[Ano ] = \"YYYY\"). "
            "Nunca invente nomes de colunas ou medidas."
        )
        return "\n".join(lines)

    def load_from_schema(self, schema) -> int:
        """
        Carrega medidas a partir de um DatasetSchema já extraído,
        evitando chamadas DMV redundantes.

        Args:
            schema: instância de DatasetSchema (app.connectors.powerbi.schema_extractor)

        Returns:
            Número de medidas carregadas.
        """
        try:
            from app.connectors.powerbi.schema_extractor import DatasetSchema
            if not isinstance(schema, DatasetSchema):
                return len(self._measures)

            measures: Dict[str, CatalogMeasure] = {}
            for table in schema.tables:
                for m in table.measures:
                    if not m.name:
                        continue
                    measures[m.name.lower()] = CatalogMeasure(
                        name=m.name,
                        table_name=table.name,
                        expression=m.expression or "",
                        is_hidden=m.is_hidden,
                    )

            self._measures = measures
            self._last_refresh = time.time()
            logger.info(
                "MeasureCatalog: %d medidas carregadas via DatasetSchema",
                len(measures),
            )
            return len(measures)
        except Exception as e:
            logger.error("MeasureCatalog.load_from_schema falhou: %s", e)
            return len(self._measures)

    # ── Invalidação manual ────────────────────────────────────────

    def invalidate(self) -> None:
        """Força o catálogo a ser considerado stale no próximo acesso."""
        self._last_refresh = 0.0
        logger.info("MeasureCatalog: cache invalidado manualmente")
