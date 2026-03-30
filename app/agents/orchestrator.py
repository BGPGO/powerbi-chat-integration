"""
Orquestrador Principal — LangGraph State Machine
Coordena os sub-agentes: dictionary, datasource, query_builder
"""

import asyncio
import logging
import re
import time
import unicodedata as _unicodedata
import re as _re
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, StateGraph
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from app.agents.dictionary_agent import DictionaryAgent
from app.agents.datasource_agent import DataSourceAgent
from app.agents.query_builder_agent import QueryBuilderAgent
from app.agents.omie_dictionary import get_omie_schema, get_omie_context
from app.agents.measure_matcher import MeasureMatcher
from app.agents.filter_extractor import FilterExtractor
from app.agents.dax_template_engine import TemplatedDaxPipeline
from app.core.measure_catalog import MeasureCatalog

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Cache do dicionário dinâmico (TTL 30 min — gerado em background)
# ─────────────────────────────────────────────────────────────

_DICT_CACHE: Dict[str, Any] = {}  # dataset_id → {"dict": DynamicDictionary, "ts": float}
_DICT_CACHE_TTL = 1800
_DICT_REFRESH_LOCK: Dict[str, bool] = {}  # dataset_id → True se refresh em andamento


def _get_cached_dynamic_dict(dataset_id: str) -> Optional[Any]:
    """Retorna DynamicDictionary do cache se ainda fresco, ou None."""
    entry = _DICT_CACHE.get(dataset_id)
    if entry and (time.time() - entry["ts"]) < _DICT_CACHE_TTL:
        return entry["dict"]
    return None


async def _refresh_dynamic_dict_bg(dynamic_schema_obj: Any) -> None:
    """Gera DynamicDictionary em background e armazena no cache."""
    did = dynamic_schema_obj.dataset_id
    if _DICT_REFRESH_LOCK.get(did):
        return  # Já está sendo gerado
    _DICT_REFRESH_LOCK[did] = True
    try:
        from app.agents.dynamic_dictionary_agent import DynamicDictionaryAgent
        agent = DynamicDictionaryAgent()
        dyn_dict = await agent.generate_dynamic_dictionary(
            dynamic_schema_obj,
            client_context="sistema de gestão financeira empresarial",
        )
        _DICT_CACHE[did] = {"dict": dyn_dict, "ts": time.time()}
        logger.info(
            "DynamicDict background: %d KPIs, %d regras, %d hints gerados para dataset %s",
            len(dyn_dict.kpis), len(dyn_dict.business_rules),
            len(dyn_dict.query_hints), did,
        )
    except Exception as e:
        logger.warning("DynamicDict background falhou: %s", e)
    finally:
        _DICT_REFRESH_LOCK.pop(did, None)


# ─────────────────────────────────────────────────────────────
# Estado compartilhado (compatível com chat.py)
# ─────────────────────────────────────────────────────────────

class OrchestratorState(TypedDict):
    messages: List[Dict[str, str]]
    current_query: str
    intent: Optional[str]
    dataset_id: Optional[str]
    workspace_id: Optional[str]
    schema_context: Dict[str, Any]
    translation_context: Dict[str, Any]
    query_result: Optional[Dict[str, Any]]
    agents_called: List[str]
    agent_outputs: Dict[str, Any]
    final_response: Optional[str]
    suggestions: List[str]
    error: Optional[str]
    template_dax: Optional[str]       # DAX pre-built by template engine
    resolved_measure: Optional[str]   # Measure name resolved (for LLM enrichment)
    powerbi_filters: Optional[Dict[str, Any]]  # Filters to apply to the Power BI iframe URL
    hard_filters: Optional[List[str]]  # Filtros hard extraídos do relatório PBI (cláusulas DAX)
    report_id: Optional[str]  # ID real do relatório Power BI (para extração de hard filters)


# ─────────────────────────────────────────────────────────────
# Dimensões não integradas no BI
# ─────────────────────────────────────────────────────────────

_UNSUPPORTED_DIMENSIONS = {
    "projeto": "Projeto",
    "projetos": "Projeto",
    "obra": "Obra",
    "obras": "Obras",
    "contrato": "Contrato",
    "contratos": "Contrato",
    "filial": "Filial",
    "filiais": "Filial",
    "produto": "Produto",
    "produtos": "Produto",
    "item": "Produto",
    "sku": "Produto",
    "vendedor": "Vendedor",
    "vendedores": "Vendedor",
    "funcionario": "Funcionário",
    "funcionarios": "Funcionário",
    "colaborador": "Colaborador",
    "colaboradores": "Colaborador",
    "regiao": "Região",
    "regioes": "Região",
    "cidade": "Cidade",
    "estado": "Estado",
    "canal": "Canal de Venda",
    "canais": "Canal de Venda",
}


# ─────────────────────────────────────────────────────────────
# Helper: qualidade do input
# ─────────────────────────────────────────────────────────────

def _classify_input_quality(text: str) -> Optional[str]:
    """
    Detecta inputs problemáticos ANTES de passar para o LLM.
    Retorna: "GIBBERISH" | "UNCLEAR" | None (input ok)

    - GIBBERISH: sequência aleatória de caracteres sem sentido algum
    - UNCLEAR: texto em português mas ininteligível / muito mal escrito
    - None: input suficientemente legível para processar
    """
    text = text.strip()
    if not text:
        return "GIBBERISH"

    # Remove pontuação e normaliza
    words = text.split()
    clean = _re.sub(r'[^a-záéíóúàâêôãõüçñ\s]', '',
                    _unicodedata.normalize("NFKD", text.lower()))
    clean = "".join(c for c in clean if not _unicodedata.combining(c))
    letters_only = _re.sub(r'\s+', '', clean)

    # Critério 1: muito curto e sem sentido (< 3 chars)
    if len(letters_only) < 3:
        return "GIBBERISH"

    # Critério 2: sem espaço, muito longo, poucos vogais → sequência aleatória
    if len(words) == 1 and len(text) > 7:
        vowels = sum(1 for c in letters_only if c in 'aeiouáéíóúàâêôãõü')
        ratio = vowels / max(len(letters_only), 1)
        if ratio < 0.15:
            return "GIBBERISH"

    # Critério 3: maioria das "palavras" são sequências sem vogal (> 60%)
    no_vowel_words = [w for w in words
                      if len(w) > 2 and not _re.search(r'[aeiouáéíóúàâêôãõü]', w.lower())]
    if len(words) >= 2 and len(no_vowel_words) / len(words) > 0.6:
        return "GIBBERISH"

    # Critério 4: string com alta proporção de caracteres repetidos ou aleatórios
    unique_ratio = len(set(letters_only)) / max(len(letters_only), 1)
    if len(letters_only) > 10 and unique_ratio < 0.25:
        return "GIBBERISH"

    return None  # Input parece legível


# ─────────────────────────────────────────────────────────────
# Nós do grafo
# ─────────────────────────────────────────────────────────────

class _OrchestratorNodes:
    """Contém os nós do grafo LangGraph"""

    def __init__(self) -> None:
        from app.core.config import get_settings
        _s = get_settings()
        self.llm = ChatAnthropic(
            model="claude-sonnet-4-6",
            temperature=0.0,
            api_key=_s.anthropic_api_key.get_secret_value(),
        )
        self.dictionary_agent = DictionaryAgent()
        self.datasource_agent = DataSourceAgent()
        self.query_builder_agent = QueryBuilderAgent()

    # ── 1. Classificar intenção ────────────────────────────────

    async def classify_intent(self, state: OrchestratorState) -> OrchestratorState:
        question = state["current_query"]

        # 1. Verificação determinística de input inválido
        quality = _classify_input_quality(question)
        if quality == "GIBBERISH":
            return {**state, "intent": "GIBBERISH"}

        # 2. Classificação via LLM (agora inclui OUT_OF_CONTEXT e UNCLEAR)
        prompt = f"""Você analisa perguntas enviadas para um assistente de BI financeiro de uma empresa.
Classifique a mensagem abaixo em uma das categorias.

CATEGORIAS:
- DATA_QUERY: Quer ver números, valores, totais, análises financeiras (use para a maioria das perguntas)
- SCHEMA_QUERY: Pergunta sobre quais dados existem no sistema
- TRANSLATION: Quer entender o que significa um indicador ou termo
- EXPLORATION: Quer saber o que pode perguntar ao assistente
- OUT_OF_CONTEXT: Pergunta válida, mas completamente fora do contexto financeiro/empresarial (ex: receita de bolo, previsão do tempo, futebol, política)
- UNCLEAR: Texto em português mas tão mal escrito ou fragmentado que é impossível entender a intenção (ex: "qual minha mwior catgeoria" pode ser corrigido → DATA_QUERY; mas "qual isso aqui meu pq nao sei la" → UNCLEAR)

REGRA IMPORTANTE para UNCLEAR vs DATA_QUERY:
- Erros ortográficos simples (troca de letras, acento errado) → DATA_QUERY (o sistema corrige automaticamente)
- Texto completamente sem sentido lógico mesmo sem erros → UNCLEAR
- Só use UNCLEAR quando for impossível entender a intenção mesmo tentando

EXEMPLOS DATA_QUERY:
- "quanto recebi em fevereiro do ano passado?"
- "quais minha mwior catgeoria de receita" (erro tipográfico → DATA_QUERY)
- "qual meu top 10 clinte" (erro tipográfico → DATA_QUERY)
- "faturamento 2025"
- "resultado mensal"

EXEMPLOS OUT_OF_CONTEXT:
- "receita de bolo de cenoura"
- "como está o tempo hoje"
- "quem ganhou o jogo ontem"
- "me explique a teoria da relatividade"

EXEMPLOS UNCLEAR:
- "qual isso meu la nao sei pq"
- "oque la aqui isso tb"
- "sim nao talvez isso"

MENSAGEM: {question}

Responda APENAS com a categoria em maiúsculas."""

        response = await self.llm.ainvoke([
            SystemMessage(content="Você é um classificador de intenções de BI financeiro."),
            HumanMessage(content=prompt),
        ])

        intent = response.content.strip().upper()
        valid_intents = {"DATA_QUERY", "SCHEMA_QUERY", "TRANSLATION", "EXPLORATION",
                         "OUT_OF_CONTEXT", "UNCLEAR", "GIBBERISH"}
        if intent not in valid_intents:
            intent = "DATA_QUERY"

        logger.info(f"Intent classificado: {intent}")
        return {**state, "intent": intent}

    # ── 2. Buscar schema ───────────────────────────────────────

    async def fetch_schema(self, state: OrchestratorState) -> OrchestratorState:
        from app.core.config import get_settings
        _s = get_settings()
        dataset_id = state.get("dataset_id") or getattr(_s, "powerbi_dataset_id", None)
        workspace_id = state.get("workspace_id") or getattr(_s, "powerbi_workspace_id", None)

        schema_info = "Schema estático"
        dynamic_schema_obj = None  # DatasetSchema instance

        # Tenta extrair schema dinâmico — com timeout para não travar a requisição
        try:
            from app.connectors.powerbi.schema_extractor import DynamicSchemaExtractor, SchemaCache
            from app.connectors.powerbi.client import get_powerbi_client
            import asyncio as _asyncio

            _client = get_powerbi_client()
            _extractor = DynamicSchemaExtractor(_client)

            if workspace_id and dataset_id:
                report_id = state.get("report_id") or getattr(_s, "powerbi_report_id", None)

                # Verifica cache primeiro (sem timeout)
                cached = await SchemaCache.get(workspace_id, dataset_id)
                if cached:
                    dynamic_schema_obj = cached
                else:
                    # Cache vazio: tenta extrair com timeout de 8s para não travar
                    try:
                        dynamic_schema_obj = await _asyncio.wait_for(
                            _extractor.extract_full_schema(
                                workspace_id, dataset_id, report_id=report_id
                            ),
                            timeout=8.0,
                        )
                    except _asyncio.TimeoutError:
                        logger.warning("fetch_schema: timeout na extração de schema — prosseguindo sem schema")
                        dynamic_schema_obj = None

                if dynamic_schema_obj and dynamic_schema_obj.tables:
                    schema_info = (
                        f"Schema dinâmico: {len(dynamic_schema_obj.tables)} tabelas, "
                        f"{sum(len(t.measures) for t in dynamic_schema_obj.tables)} medidas"
                    )
                    logger.info("fetch_schema: %s", schema_info)
        except Exception as e:
            logger.warning("fetch_schema: schema dinâmico falhou — %s", e)

        # Sempre usa schema dinâmico — nunca fallback estático
        if dynamic_schema_obj and dynamic_schema_obj.tables:
            dyn_dict = dynamic_schema_obj.to_schema_dict()
            schema_context = {
                "dataset_id": dataset_id or dyn_dict.get("dataset_id", ""),
                "tables": dyn_dict["tables"],
                "relationships": dyn_dict.get("relationships", []),
                "extracted_at": dyn_dict.get("extracted_at", ""),
                "report_filters": getattr(dynamic_schema_obj, "report_filters", []),
                "_dynamic_schema": dynamic_schema_obj,
            }
        else:
            # Schema ainda não disponível (cache aquecendo) — contexto mínimo
            schema_context = {
                "dataset_id": dataset_id or "",
                "tables": [],
                "relationships": [],
                "_dynamic_schema": None,
            }

        return {
            **state,
            "schema_context": schema_context,
            "agents_called": state["agents_called"] + ["datasource"],
            "agent_outputs": {
                **state["agent_outputs"],
                "datasource": {"content": schema_info},
            },
        }

    # ── 3. Traduzir schema ─────────────────────────────────────

    async def translate_schema(self, state: OrchestratorState) -> OrchestratorState:
        schema = state.get("schema_context", {})
        dynamic_schema_obj = schema.get("_dynamic_schema")

        # Constrói o bloco de contexto para o LLM
        context_blocks = []

        if dynamic_schema_obj and dynamic_schema_obj.tables:
            # Colunas técnicas que não ajudam o LLM a gerar DAX
            _SKIP_COL_PATTERNS = {
                "app_key", "app_secret", "pagina", "ncod", "ccod", "auxcateg",
                "auxcli", "aux_caixa", "aux_comp", "data_extra", "dataaux",
                "data auxiliar", "indice", "semana", "semana comp",
                "nvalormovcc", "ncodmovcc", "ncod", "cnum", "chora",
                "ddtconc", "ddtcred", "ddtreg",
            }
            # Tabelas auxiliares de apoio visual (geralmente 1-2 colunas de label)
            _SKIP_TABLE_PATTERNS = {
                "última data", "valor liquido competencia", "ebitda competencia",
                "resultado operacional competencia", "valor liquido",
                "ebitda", "resultado operacional",
            }

            # Schema dinâmico real — filtra ruído técnico
            lines = ["## SCHEMA DO DATASET (descoberto dinamicamente via API)\n"]
            for t in dynamic_schema_obj.tables:
                if t.is_hidden:
                    continue
                # Pula tabelas auxiliares de apoio visual
                if any(p in t.name.lower() for p in _SKIP_TABLE_PATTERNS):
                    continue

                measures_str = ""
                if t.measures:
                    mnames = ", ".join(f"[{m.name}]" for m in t.measures if not m.is_hidden)
                    if mnames:
                        measures_str = f"\n  Medidas: {mnames}"

                col_lines = []
                for c in t.columns:
                    if c.is_hidden:
                        continue
                    # Filtra colunas técnicas desnecessárias
                    cname_lower = c.name.lower()
                    if any(p in cname_lower for p in _SKIP_COL_PATTERNS):
                        continue
                    samples = ""
                    if c.sample_values:
                        sample_list = [str(v) for v in c.sample_values[:5] if v is not None]
                        if sample_list:
                            samples = f" — ex: {', '.join(sample_list)}"
                    col_lines.append(f"  - [{c.name}] ({c.data_type}){samples}")

                # Limita a 40 colunas por tabela para controlar tamanho do prompt
                if len(col_lines) > 40:
                    col_lines = col_lines[:40] + [f"  ... (+{len(col_lines)-40} colunas)"]

                cols_str = "\n".join(col_lines) if col_lines else "  (sem colunas visíveis)"
                lines.append(f"### '{t.name}'\n{cols_str}{measures_str}\n")

            if dynamic_schema_obj.relationships:
                lines.append("## RELACIONAMENTOS")
                for r in dynamic_schema_obj.relationships:
                    lines.append(
                        f"  '{r.from_table}'[{r.from_column}] → '{r.to_table}'[{r.to_column}]"
                        f" ({r.cardinality})"
                    )
                lines.append("")

            # Hard filters do relatório (regras de negócio que o DAX deve respeitar)
            report_filters = getattr(dynamic_schema_obj, "report_filters", [])
            if report_filters:
                lines.append("## FILTROS FIXOS DO RELATÓRIO (sempre aplicar no CALCULATEFILTER ou FILTER)")
                for f in report_filters:
                    lines.append(f"  - {f}")
                lines.append("")

            context_blocks.append("\n".join(lines))

            # Enriquece com dicionário dinâmico (apenas se já estiver em cache — não bloqueia)
            try:
                cached_dict = _get_cached_dynamic_dict(dynamic_schema_obj.dataset_id)
                if cached_dict:
                    dict_block = cached_dict.build_context_prompt()
                    if dict_block:
                        context_blocks.append(dict_block)
                        logger.info("translate_schema: dicionário dinâmico (cache) aplicado")
                else:
                    # Gera em background para próximas requests
                    asyncio.ensure_future(
                        _refresh_dynamic_dict_bg(dynamic_schema_obj)
                    )
            except Exception as e:
                logger.debug("translate_schema: dicionário dinâmico não disponível — %s", e)

        else:
            context_blocks.append("Schema ainda sendo carregado. Informe ao usuário que o sistema está inicializando.")

        schema_with_context = {
            **schema,
            "omie_context": "\n\n".join(context_blocks),
        }
        return {
            **state,
            "translation_context": schema_with_context,
            "agents_called": state["agents_called"] + ["dictionary"],
            "agent_outputs": {
                **state["agent_outputs"],
                "dictionary": {
                    "content": (
                        f"Schema dinâmico: {len(dynamic_schema_obj.tables)} tabelas"
                        if dynamic_schema_obj and dynamic_schema_obj.tables
                        else "Dicionário Omie estático"
                    )
                },
            },
        }

    # ── 3.5 Resolve medida e tenta gerar DAX via template ─────────

    async def execute_template_dax(self, state: OrchestratorState) -> OrchestratorState:
        """Executa DAX pré-construído pelo template engine — sem LLM."""
        dax_query = state.get("template_dax")
        if not dax_query:
            return {**state, "error": "template_dax não encontrado no state"}

        schema = state.get("translation_context") or state.get("schema_context", {})
        dataset_id = schema.get("dataset_id")
        if not dataset_id:
            from app.core.config import get_settings
            dataset_id = getattr(get_settings(), "powerbi_dataset_id", None)

        if not dataset_id:
            logger.warning("execute_template_dax: dataset_id não encontrado, fallback para LLM")
            return {**state, "template_dax": None}

        try:
            result = await self.query_builder_agent.client.execute_query(dataset_id, dax_query)
            query_result = {
                "columns": result.get("columns", []),
                "rows": result.get("rows", []),
                "row_count": result.get("row_count", 0),
                "execution_time_ms": 0,
                "dax_query": dax_query,
                "truncated": False,
            }
            return {
                **state,
                "query_result": query_result,
                "agents_called": state["agents_called"] + ["query_builder"],
                "agent_outputs": {
                    **state["agent_outputs"],
                    "query_builder": {
                        "content": f"Template DAX executado: {state.get('resolved_measure', '')}",
                        "data": {"success": True, "dax_query": dax_query},
                    },
                },
                "error": None,
            }
        except Exception as e:
            logger.error(f"execute_template_dax falhou: {e} — fallback para LLM")
            return {**state, "template_dax": None, "query_result": None, "error": None}

    # ── 4. Executar query ──────────────────────────────────────

    async def execute_query(self, state: OrchestratorState) -> OrchestratorState:
        try:
            schema = state.get("translation_context") or state.get("schema_context", {})
            # Passa medida já resolvida para o LLM não precisar adivinhar
            if state.get("resolved_measure"):
                schema = {**schema, "resolved_measure": state["resolved_measure"]}
            # Passa filtros hard do relatório para o query builder aplicar
            hard_filters = state.get("hard_filters") or []
            if hard_filters:
                schema = {**schema, "hard_filters": hard_filters}
            result = await self.query_builder_agent.execute_question(
                question=state["current_query"],
                schema=schema,
            )

            query_result = None
            if result.get("success"):
                raw = result.get("results", {})
                query_result = {
                    "columns": raw.get("columns", []),
                    "rows": raw.get("rows", []),
                    "row_count": raw.get("row_count", 0),
                    "execution_time_ms": result.get("execution_time_ms", 0),
                    "dax_query": result.get("dax_query"),
                    "truncated": raw.get("truncated", False),
                }

            return {
                **state,
                "query_result": query_result,
                "agents_called": state["agents_called"] + ["query_builder"],
                "agent_outputs": {
                    **state["agent_outputs"],
                    "query_builder": {
                        "content": result.get("explanation", ""),
                        "data": result,
                    },
                },
                "error": None if result.get("success") else result.get("error"),
            }
        except Exception as e:
            logger.error(f"Erro no query builder agent: {e}")
            return {
                **state,
                "agents_called": state["agents_called"] + ["query_builder"],
                "error": str(e),
            }

    # ── 5. Gerar resposta final ────────────────────────────────

    async def generate_response(self, state: OrchestratorState) -> OrchestratorState:
        # Se já temos uma resposta pré-definida (ex: dimensão não integrada), retorna direto
        if state.get("final_response") and not state.get("query_result"):
            suggestions = await self._generate_suggestions(state)
            return {
                **state,
                "suggestions": suggestions,
            }

        # Resposta amigável para inputs inválidos / fora de contexto
        intent = state.get("intent", "DATA_QUERY")
        if intent == "GIBBERISH":
            return {
                **state,
                "final_response": (
                    "Não consegui identificar uma pergunta aí. "
                    "Tente digitar algo como: *Qual o faturamento do mês passado?* "
                    "ou *Quais foram minhas maiores despesas esse ano?*"
                ),
                "suggestions": [
                    "Quanto faturamos esse ano?",
                    "Qual foi o resultado do mês passado?",
                    "Quem são meus top 10 clientes?",
                ],
            }

        if intent == "UNCLEAR":
            return {
                **state,
                "final_response": (
                    "Não consegui entender bem essa pergunta. "
                    "Pode reformular de forma mais direta? "
                    "Por exemplo: *Qual a receita de janeiro?* ou *Quanto gastamos esse mês?*"
                ),
                "suggestions": [
                    "Quanto faturamos esse ano?",
                    "Qual foi o resultado do mês passado?",
                    "Onde estou gastando mais?",
                ],
            }

        if intent == "OUT_OF_CONTEXT":
            return {
                **state,
                "final_response": (
                    "Essa pergunta está fora do que consigo analisar aqui. "
                    "Estou conectado ao seu painel financeiro e posso responder sobre "
                    "receitas, despesas, resultados, clientes, departamentos e fluxo de caixa. "
                    "O que quer saber sobre o financeiro da empresa?"
                ),
                "suggestions": [
                    "Quanto faturamos esse ano?",
                    "Qual foi o resultado do mês passado?",
                    "Quem são meus top 10 clientes?",
                ],
            }

        query_result = state.get("query_result")
        error = state.get("error")
        pbi_filters = state.get("powerbi_filters")
        filter_desc = pbi_filters.get("description", "") if pbi_filters else ""

        context_parts = []
        if query_result and query_result.get("row_count", 0) > 0:
            rows_preview = str(query_result.get("rows", [])[:10])
            context_parts.append(
                f"Registros retornados: {query_result['row_count']}\n"
                f"Dados: {rows_preview}"
            )
        elif query_result:
            context_parts.append("A query foi executada mas não retornou registros.")
        if error:
            context_parts.append(f"Erro ao buscar dados: {error}")

        system_content = """Você é o assistente financeiro da empresa, integrado ao painel de gestão.
Fale diretamente com o empresário, como se fosse um analista de confiança ao lado dele.

COMO RESPONDER:
- Use linguagem clara, direta e natural — como uma conversa profissional
- Vá direto ao ponto: apresente o número ou informação principal na primeira frase
- Se tiver dados, destaque o número mais importante primeiro, depois complemente
- Se a lista tiver muitos itens, mostre os mais relevantes e diga quantos existem no total
- Use frases curtas. Nada de parágrafos longos
- Formate valores sempre como R$ X.XXX,XX (ex: R$ 1.250.000,00)
- Para percentuais, use X,X% (ex: 12,5%)
- Se não houver dados para o período consultado, diga isso diretamente e sugira outro período
- Nunca mencione nomes técnicos: DAX, Power BI, query, dataset, API, tabela, coluna, banco de dados
- Nunca use emojis
- Nunca peça desculpas, não use "infelizmente", "lamentavelmente", "me desculpe"
- Nunca comece com "Claro!", "Com certeza!", "Ótima pergunta!" ou frases de enchimento
- Se os dados vierem de um período que não foi solicitado, deixe claro qual período está sendo mostrado

FORMATO PARA LISTAS (quando houver múltiplos itens):
Use este formato limpo:
1. Nome — R$ valor
2. Nome — R$ valor
...

FORMATO PARA COMPARAÇÕES:
Período A: R$ valor
Período B: R$ valor
Variação: +X% (ou -X%)"""

        period_hint = f"\nPeríodo identificado na pergunta: {filter_desc}" if filter_desc else ""
        prompt = f"""PERGUNTA DO USUÁRIO: {state['current_query']}
{period_hint}
{'DADOS ENCONTRADOS:' + chr(10) + chr(10).join(context_parts) if context_parts else 'Nenhum dado retornado — pode ser que o período não tenha registros ou que o acesso ao banco de dados não esteja configurado. Informe isso ao usuário de forma direta e sugira verificar o relatório visual filtrado.'}

Responda em português de forma direta e profissional."""

        response = await self.llm.ainvoke([
            SystemMessage(content=system_content),
            HumanMessage(content=prompt),
        ])

        # ── Revisão interna antes de responder ────────────────────
        draft = response.content
        if query_result and query_result.get("row_count", 0) > 0:
            draft = await self._review_response(
                question=state["current_query"],
                draft=draft,
                query_result=query_result,
            )

        # Gera sugestões de perguntas
        suggestions = await self._generate_suggestions(state)

        return {
            **state,
            "final_response": draft,
            "suggestions": suggestions,
        }

    async def _review_response(
        self,
        question: str,
        draft: str,
        query_result: Dict[str, Any],
    ) -> str:
        """Revisão interna: verifica se a resposta é consistente com os dados retornados."""
        try:
            rows_str = str(query_result.get("rows", [])[:5])
            review_prompt = f"""Você é um revisor financeiro rigoroso. Verifique se a resposta abaixo está correta e consistente com os dados retornados pela query.

PERGUNTA DO USUÁRIO: {question}

DADOS RETORNADOS PELA QUERY:
{rows_str}
(Total de registros: {query_result.get('row_count', 0)})

RESPOSTA GERADA (RASCUNHO):
{draft}

INSTRUÇÕES DE REVISÃO:
- Verifique se os valores numéricos na resposta batem com os dados
- Verifique se o período mencionado está correto
- Se a resposta estiver correta, retorne ela EXATAMENTE como está (sem modificar nada)
- Se houver erro de valor, período ou interpretação, corrija SOMENTE o trecho errado
- NUNCA adicione explicações sobre a revisão — retorne apenas a resposta final
- Mantenha o mesmo estilo e formato da resposta original"""

            review = await self.llm.ainvoke([HumanMessage(content=review_prompt)])
            return review.content
        except Exception:
            return draft

    async def _generate_suggestions(self, state: OrchestratorState) -> List[str]:
        try:
            schema_info = ""
            for t in state.get("schema_context", {}).get("tables", [])[:3]:
                schema_info += f"\n- {t['name']}"

            prompt = f"""Um CEO acaba de perguntar: "{state['current_query']}"
Com base nisso, sugira 3 perguntas de acompanhamento que um executivo faria naturalmente.
Use linguagem direta e coloquial em português (como "quanto faturamos?", "quem são os maiores?", "como foi mês a mês?").
Retorne apenas as 3 perguntas, uma por linha, sem numeração, sem explicações."""

            response = await self.llm.ainvoke([HumanMessage(content=prompt)])
            lines = [s.strip() for s in response.content.split("\n") if s.strip()]
            return lines[:3]
        except Exception:
            return []

    # ── Roteamento condicional ─────────────────────────────────

    def should_execute_query(self, state: OrchestratorState) -> str:
        intent = state.get("intent", "DATA_QUERY")
        if intent in {"DATA_QUERY", "EXPLORATION"}:
            return "execute_query"
        return "generate_response"

    async def resolve_and_route(self, state: OrchestratorState) -> OrchestratorState:
        """
        Extrai filtros temporais da pergunta e prepara os filtros para o iframe do Power BI.
        Nao executa queries DAX — usa apenas o modelo estatico Omie + FilterExtractor.
        """
        intent = state.get("intent", "DATA_QUERY")
        if intent not in {"DATA_QUERY", "EXPLORATION"}:
            return {**state, "template_dax": None, "resolved_measure": None, "powerbi_filters": None}

        question = state["current_query"]

        # Detecta dimensões não integradas
        import unicodedata
        question_lower = question.lower()
        question_norm = "".join(
            c for c in unicodedata.normalize("NFKD", question_lower)
            if not unicodedata.combining(c)
        )
        for dim_key, dim_name in _UNSUPPORTED_DIMENSIONS.items():
            if re.search(r'\b' + re.escape(dim_key) + r'\b', question_norm):
                logger.info(f"Dimensao nao integrada detectada: {dim_name}")
                unsupported_msg = (
                    f"A dimensao **{dim_name}** nao esta integrada no BI no momento. "
                    f"Caso queira visualizar essa informacao, entre em contato com a equipe de BI."
                )
                return {
                    **state,
                    "final_response": unsupported_msg,
                    "template_dax": None,
                    "resolved_measure": None,
                    "powerbi_filters": None,
                }

        # Extrai filtros temporais
        extractor = FilterExtractor()
        temporal = extractor.extract(question)
        logger.info(
            f"Filtros extraidos: year={temporal.year}, month={temporal.month}, "
            f"quarter={temporal.quarter}, rolling={temporal.rolling_window}, "
            f"is_all_time={temporal.is_all_time}"
        )

        # Constroi powerbi_filters para o iframe
        powerbi_filters = None
        if temporal.rolling_window:
            rw = temporal.rolling_window
            powerbi_filters = {
                "year": None,
                "month": None,
                "months_in_range": None,
                "quarter": None,
                "rolling_window_days": rw.n,
                "description": f"Ultimos {rw.n} dias",
                "has_filter": False,  # URL filter nao suporta TODAY()
            }
        elif not temporal.is_all_time and (temporal.year or temporal.month or temporal.months_in_range):
            desc_parts = []
            if temporal.month and not temporal.months_in_range:
                desc_parts.append(temporal.month)
            elif temporal.quarter:
                desc_parts.append(f"T{temporal.quarter}")
            elif temporal.months_in_range:
                desc_parts.append(f"{temporal.months_in_range[0]} a {temporal.months_in_range[-1]}")
            if temporal.year:
                desc_parts.append(temporal.year)
            powerbi_filters = {
                "year": temporal.year,
                "month": temporal.month if not temporal.months_in_range else None,
                "months_in_range": temporal.months_in_range,
                "quarter": temporal.quarter,
                "rolling_window_days": None,
                "description": " de ".join(desc_parts) if desc_parts else temporal.year or "",
                "has_filter": True,
            }

        hard_filters: List[str] = []

        return {
            **state,
            "template_dax": None,
            "resolved_measure": None,
            "powerbi_filters": powerbi_filters,
            "hard_filters": hard_filters,
        }

    def should_use_template(self, state: OrchestratorState) -> str:
        """Decide o proximo no apos resolve_and_route."""
        if state.get("final_response"):
            return "generate_response"
        intent = state.get("intent", "DATA_QUERY")
        if intent in {"GIBBERISH", "OUT_OF_CONTEXT", "UNCLEAR"}:
            return "generate_response"
        if intent not in {"DATA_QUERY", "EXPLORATION"}:
            return "generate_response"
        # Tenta executar query para retornar o valor real
        return "execute_query"


# ─────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────

def create_orchestrator():
    """
    Cria e compila o grafo LangGraph do orquestrador.
    Chamado como dependência pelo FastAPI.
    """
    nodes = _OrchestratorNodes()
    workflow = StateGraph(OrchestratorState)

    # Nós
    workflow.add_node("classify_intent", nodes.classify_intent)
    workflow.add_node("fetch_schema", nodes.fetch_schema)
    workflow.add_node("translate_schema", nodes.translate_schema)
    workflow.add_node("resolve_and_route", nodes.resolve_and_route)
    workflow.add_node("execute_template_dax", nodes.execute_template_dax)
    workflow.add_node("execute_query", nodes.execute_query)
    workflow.add_node("generate_response", nodes.generate_response)

    # Fluxo
    workflow.set_entry_point("classify_intent")
    workflow.add_edge("classify_intent", "fetch_schema")
    workflow.add_edge("fetch_schema", "translate_schema")
    workflow.add_edge("translate_schema", "resolve_and_route")

    workflow.add_conditional_edges(
        "resolve_and_route",
        nodes.should_use_template,
        {
            "execute_template_dax": "execute_template_dax",
            "execute_query": "execute_query",
            "generate_response": "generate_response",
        },
    )

    workflow.add_edge("execute_template_dax", "generate_response")
    workflow.add_edge("execute_query", "generate_response")
    workflow.add_edge("generate_response", END)

    return workflow.compile()
