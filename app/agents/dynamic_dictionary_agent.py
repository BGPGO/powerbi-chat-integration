"""
DynamicDictionaryAgent — Gera automaticamente descrições de negócio para schemas Power BI.

Usa Claude para auto-descrever tabelas, colunas e medidas descobertas via API,
substituindo a necessidade de manter o omie_dictionary.py manualmente.
Funciona para qualquer cliente sem configuração prévia.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)


@dataclass
class ColumnDescription:
    original_name: str
    business_name: str
    description: str
    business_domain: str = ""
    is_dimension: bool = False
    is_measure: bool = False


@dataclass
class TableDescription:
    original_name: str
    business_name: str
    description: str
    domain: str = ""
    key_columns: List[str] = field(default_factory=list)
    columns: List[ColumnDescription] = field(default_factory=list)


@dataclass
class MeasureDescription:
    original_name: str
    business_name: str
    description: str
    formula_plain: str = ""   # Explicação em português da expressão DAX
    unit: str = ""            # "R$", "%", "unidades", etc.


@dataclass
class BusinessRule:
    rule: str
    applies_to: str   # nome da tabela/coluna
    example: str = ""


@dataclass
class KPIInfo:
    measure_name: str
    display_name: str
    description: str
    related_dimensions: List[str] = field(default_factory=list)


@dataclass
class QueryHint:
    """Dica de como formular perguntas sobre este dataset."""
    example_question: str
    maps_to: str    # medida ou coluna que responde à pergunta


@dataclass
class DynamicDictionary:
    dataset_id: str
    tables: List[TableDescription] = field(default_factory=list)
    measures: List[MeasureDescription] = field(default_factory=list)
    business_rules: List[BusinessRule] = field(default_factory=list)
    kpis: List[KPIInfo] = field(default_factory=list)
    query_hints: List[QueryHint] = field(default_factory=list)
    # Glossário plano: nome_original → nome_negócio
    glossary: Dict[str, str] = field(default_factory=dict)

    def build_context_prompt(self, max_chars: int = 4000) -> str:
        """
        Gera um bloco de contexto compacto para injetar no prompt do LLM.
        Prioriza KPIs, regras de negócio e hints de query.
        """
        parts = []

        if self.kpis:
            parts.append("## KPIs DISPONÍVEIS")
            for kpi in self.kpis[:15]:
                dims = ", ".join(kpi.related_dimensions[:3]) if kpi.related_dimensions else ""
                dim_str = f" (dimensões: {dims})" if dims else ""
                parts.append(f"- [{kpi.measure_name}]: {kpi.description}{dim_str}")

        if self.business_rules:
            parts.append("\n## REGRAS DE NEGÓCIO")
            for rule in self.business_rules[:10]:
                parts.append(f"- {rule.rule}")

        if self.query_hints:
            parts.append("\n## PERGUNTAS TÍPICAS → MEDIDA")
            for hint in self.query_hints[:10]:
                parts.append(f'- "{hint.example_question}" → {hint.maps_to}')

        text = "\n".join(parts)
        if len(text) > max_chars:
            text = text[:max_chars] + "\n...[truncado]"
        return text


class DynamicDictionaryAgent:
    """
    Gera um dicionário de negócio dinâmico a partir do schema do dataset.

    Uso:
        agent = DynamicDictionaryAgent()
        schema = await extractor.extract_full_schema(wid, did)
        dictionary = await agent.generate_dynamic_dictionary(
            schema,
            client_context="ERP financeiro Omie — gestão de receitas e despesas"
        )
        context_block = dictionary.build_context_prompt()
    """

    # Máximo de tabelas/medidas por batch para não exceder context window
    _BATCH_SIZE = 8

    def __init__(self) -> None:
        from app.core.config import get_settings
        _s = get_settings()
        self._llm = ChatAnthropic(
            model="claude-sonnet-4-6",
            temperature=0.1,
            api_key=_s.anthropic_api_key.get_secret_value(),
        )

    async def generate_dynamic_dictionary(
        self,
        schema: "DatasetSchema",  # type: ignore
        client_context: Optional[str] = None,
    ) -> DynamicDictionary:
        """
        Gera dicionário de negócio completo para o dataset.
        Executa descrições de tabelas, medidas, regras e hints em paralelo.
        """
        from app.connectors.powerbi.schema_extractor import DatasetSchema

        if not isinstance(schema, DatasetSchema):
            logger.warning("DynamicDictionaryAgent: schema inválido, retornando dicionário vazio")
            return DynamicDictionary(dataset_id="")

        context = client_context or "sistema de gestão empresarial"

        # Executa todas as etapas em paralelo
        try:
            tables_desc, measures_desc, rules, kpis, hints = await asyncio.gather(
                self._describe_tables_batch(schema.tables, context),
                self._describe_measures_batch(schema.get_all_measures(), context),
                self._infer_business_rules(schema, context),
                self._detect_kpis(schema.get_all_measures(), context),
                self._build_query_hints(schema, context),
                return_exceptions=True,
            )

            tables_desc = tables_desc if not isinstance(tables_desc, Exception) else []
            measures_desc = measures_desc if not isinstance(measures_desc, Exception) else []
            rules = rules if not isinstance(rules, Exception) else []
            kpis = kpis if not isinstance(kpis, Exception) else []
            hints = hints if not isinstance(hints, Exception) else []

        except Exception as e:
            logger.error("DynamicDictionaryAgent: falha na geração — %s", e)
            return DynamicDictionary(dataset_id=schema.dataset_id)

        # Constrói glossário plano
        glossary: Dict[str, str] = {}
        for t in (tables_desc or []):
            glossary[t.original_name] = t.business_name
            for c in t.columns:
                glossary[c.original_name] = c.business_name

        dictionary = DynamicDictionary(
            dataset_id=schema.dataset_id,
            tables=tables_desc or [],
            measures=measures_desc or [],
            business_rules=rules or [],
            kpis=kpis or [],
            query_hints=hints or [],
            glossary=glossary,
        )

        logger.info(
            "DynamicDictionaryAgent: %d tabelas, %d medidas, %d KPIs, %d regras descritos",
            len(dictionary.tables),
            len(dictionary.measures),
            len(dictionary.kpis),
            len(dictionary.business_rules),
        )
        return dictionary

    # ── Descrição de tabelas ──────────────────────────────────

    async def _describe_tables_batch(
        self, tables: List[Any], context: str
    ) -> List[TableDescription]:
        """Descreve tabelas em batches para não sobrecarregar o LLM."""
        visible = [t for t in tables if not t.is_hidden]
        results = []

        for i in range(0, len(visible), self._BATCH_SIZE):
            batch = visible[i : i + self._BATCH_SIZE]
            batch_result = await self._describe_tables_single_batch(batch, context)
            results.extend(batch_result)

        return results

    async def _describe_tables_single_batch(
        self, tables: List[Any], context: str
    ) -> List[TableDescription]:
        """Descreve um batch de tabelas via Claude."""
        tables_info = []
        for t in tables:
            cols = [
                f"  - {c.name} ({c.data_type})"
                + (f" — amostras: {c.sample_values[:3]}" if c.sample_values else "")
                for c in t.columns[:20]
                if not c.is_hidden
            ]
            measures = [f"  - [{m.name}]" for m in t.measures[:10] if not m.is_hidden]
            tables_info.append(
                f"Tabela: {t.name}\n"
                + ("Colunas:\n" + "\n".join(cols) + "\n" if cols else "")
                + ("Medidas:\n" + "\n".join(measures) + "\n" if measures else "")
            )

        prompt = f"""Você é um especialista em Business Intelligence analisando um dataset Power BI.
Contexto do cliente: {context}

Para cada tabela abaixo, gere em português:
1. Um nome de negócio amigável (ex: "Tabela de Transações Financeiras")
2. Uma descrição de 1-2 linhas do que a tabela representa
3. Para cada coluna: nome de negócio + descrição curta
4. O domínio de negócio (ex: "financeiro", "vendas", "rh")

TABELAS:
{'---'.join(tables_info)}

Responda em JSON com esta estrutura exata:
{{
  "tables": [
    {{
      "original_name": "nome_original",
      "business_name": "Nome Amigável",
      "description": "O que representa",
      "domain": "financeiro",
      "key_columns": ["col1", "col2"],
      "columns": [
        {{"original_name": "col", "business_name": "Nome Col", "description": "O que é", "is_dimension": true}}
      ]
    }}
  ]
}}"""

        try:
            response = await self._llm.ainvoke([
                SystemMessage(content="Você é um analista de BI especialista em modelagem de dados."),
                HumanMessage(content=prompt),
            ])
            import json
            content = response.content
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(content[start:end])
                result = []
                for td in data.get("tables", []):
                    cols = [
                        ColumnDescription(
                            original_name=c.get("original_name", ""),
                            business_name=c.get("business_name", c.get("original_name", "")),
                            description=c.get("description", ""),
                            is_dimension=c.get("is_dimension", False),
                        )
                        for c in td.get("columns", [])
                    ]
                    result.append(TableDescription(
                        original_name=td.get("original_name", ""),
                        business_name=td.get("business_name", td.get("original_name", "")),
                        description=td.get("description", ""),
                        domain=td.get("domain", ""),
                        key_columns=td.get("key_columns", []),
                        columns=cols,
                    ))
                return result
        except Exception as e:
            logger.debug("Falha ao descrever tabelas: %s", e)

        # Fallback: retorna descrição mínima
        return [
            TableDescription(
                original_name=t.name,
                business_name=t.name,
                description=t.description or f"Tabela {t.name}",
            )
            for t in tables
        ]

    # ── Descrição de medidas ──────────────────────────────────

    async def _describe_measures_batch(
        self, measures: List[Any], context: str
    ) -> List[MeasureDescription]:
        """Descreve medidas DAX em batches."""
        results = []
        for i in range(0, len(measures), self._BATCH_SIZE):
            batch = measures[i : i + self._BATCH_SIZE]
            batch_result = await self._describe_measures_single_batch(batch, context)
            results.extend(batch_result)
        return results

    async def _describe_measures_single_batch(
        self, measures: List[Any], context: str
    ) -> List[MeasureDescription]:
        """Descreve um batch de medidas via Claude."""
        measures_info = "\n".join(
            f"- [{m.name}]: {m.expression[:200] if m.expression else '(sem expressão)'}"
            for m in measures
        )

        prompt = f"""Contexto: {context}

Para cada medida DAX abaixo, gere:
1. Nome de negócio amigável
2. Descrição em português do que calcula
3. Explicação simples da fórmula (sem jargão técnico)
4. Unidade (R$, %, unidades, dias, etc.)

MEDIDAS:
{measures_info}

Responda em JSON:
{{
  "measures": [
    {{
      "original_name": "[Nome]",
      "business_name": "Nome Amigável",
      "description": "O que calcula",
      "formula_plain": "Soma os valores pagos e recebidos",
      "unit": "R$"
    }}
  ]
}}"""

        try:
            response = await self._llm.ainvoke([
                SystemMessage(content="Você é um analista financeiro especialista em DAX."),
                HumanMessage(content=prompt),
            ])
            import json
            content = response.content
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(content[start:end])
                return [
                    MeasureDescription(
                        original_name=m.get("original_name", "").strip("[]"),
                        business_name=m.get("business_name", m.get("original_name", "")),
                        description=m.get("description", ""),
                        formula_plain=m.get("formula_plain", ""),
                        unit=m.get("unit", ""),
                    )
                    for m in data.get("measures", [])
                ]
        except Exception as e:
            logger.debug("Falha ao descrever medidas: %s", e)

        return [
            MeasureDescription(
                original_name=m.name,
                business_name=m.name,
                description=m.description or f"Medida {m.name}",
            )
            for m in measures
        ]

    # ── Regras de negócio ─────────────────────────────────────

    async def _infer_business_rules(self, schema: Any, context: str) -> List[BusinessRule]:
        """Infere regras de negócio a partir do schema e expressões DAX."""
        measures_with_expr = [
            m for t in schema.tables for m in t.measures if m.expression
        ]
        if not measures_with_expr:
            return []

        # Pega as 10 medidas mais importantes (visíveis, com expressão longa)
        sample = sorted(
            [m for m in measures_with_expr if not m.is_hidden],
            key=lambda m: len(m.expression or ""),
            reverse=True,
        )[:10]

        measures_text = "\n".join(
            f"[{m.name}] = {m.expression[:300]}" for m in sample
        )

        prompt = f"""Contexto: {context}

Analisando as expressões DAX das medidas abaixo, identifique regras de negócio implícitas.
Por exemplo: "Receita considera apenas status PAGO e RECEBIDO", "Cancelados são excluídos de todos os cálculos", etc.

MEDIDAS:
{measures_text}

Retorne em JSON (máximo 8 regras):
{{
  "rules": [
    {{
      "rule": "Descrição da regra em português",
      "applies_to": "nome_da_medida_ou_tabela",
      "example": "Exemplo de aplicação"
    }}
  ]
}}"""

        try:
            response = await self._llm.ainvoke([
                SystemMessage(content="Você é um analista de BI descobrindo regras de negócio."),
                HumanMessage(content=prompt),
            ])
            import json
            content = response.content
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(content[start:end])
                return [
                    BusinessRule(
                        rule=r.get("rule", ""),
                        applies_to=r.get("applies_to", ""),
                        example=r.get("example", ""),
                    )
                    for r in data.get("rules", [])
                    if r.get("rule")
                ]
        except Exception as e:
            logger.debug("Falha ao inferir regras: %s", e)

        return []

    # ── KPIs ──────────────────────────────────────────────────

    async def _detect_kpis(self, measures: List[Any], context: str) -> List[KPIInfo]:
        """Identifica quais medidas são KPIs principais do negócio."""
        visible = [m for m in measures if not m.is_hidden]
        if not visible:
            return []

        measures_list = "\n".join(f"- [{m.name}]" + (f": {m.description}" if m.description else "") for m in visible[:30])

        prompt = f"""Contexto: {context}

Das medidas abaixo, identifique os principais KPIs de negócio.
Para cada KPI, liste as dimensões naturais para análise (ex: por período, por departamento, etc.).

MEDIDAS:
{measures_list}

Responda em JSON (máximo 15 KPIs):
{{
  "kpis": [
    {{
      "measure_name": "NomeMedida",
      "display_name": "Nome para o usuário",
      "description": "O que esse KPI mede",
      "related_dimensions": ["por mês", "por departamento", "por cliente"]
    }}
  ]
}}"""

        try:
            response = await self._llm.ainvoke([
                SystemMessage(content="Você é um CFO identificando KPIs estratégicos."),
                HumanMessage(content=prompt),
            ])
            import json
            content = response.content
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(content[start:end])
                return [
                    KPIInfo(
                        measure_name=k.get("measure_name", ""),
                        display_name=k.get("display_name", k.get("measure_name", "")),
                        description=k.get("description", ""),
                        related_dimensions=k.get("related_dimensions", []),
                    )
                    for k in data.get("kpis", [])
                    if k.get("measure_name")
                ]
        except Exception as e:
            logger.debug("Falha ao detectar KPIs: %s", e)

        return []

    # ── Query hints ───────────────────────────────────────────

    async def _build_query_hints(self, schema: Any, context: str) -> List[QueryHint]:
        """Gera exemplos de perguntas naturais mapeadas para medidas."""
        measures = schema.get_all_measures()[:15]
        if not measures:
            return []

        measures_list = ", ".join(f"[{m.name}]" for m in measures)

        prompt = f"""Contexto: {context}

Para um assistente de BI em português, gere perguntas naturais que um empresário faria,
mapeadas para as medidas disponíveis: {measures_list}

Retorne 10-15 exemplos em JSON:
{{
  "hints": [
    {{
      "example_question": "Quanto faturamos em janeiro de 2025?",
      "maps_to": "[Receita] filtrado por mês/ano"
    }}
  ]
}}"""

        try:
            response = await self._llm.ainvoke([
                SystemMessage(content="Você é um empresário fazendo perguntas ao seu BI."),
                HumanMessage(content=prompt),
            ])
            import json
            content = response.content
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(content[start:end])
                return [
                    QueryHint(
                        example_question=h.get("example_question", ""),
                        maps_to=h.get("maps_to", ""),
                    )
                    for h in data.get("hints", [])
                    if h.get("example_question")
                ]
        except Exception as e:
            logger.debug("Falha ao gerar hints: %s", e)

        return []
