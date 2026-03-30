"""
Query Builder Agent - Construção e Execução de DAX
Responsável por converter linguagem natural em queries DAX e executá-las

Capacidades:
- Interpretar perguntas em linguagem natural
- Gerar queries DAX otimizadas
- Executar consultas via API
- Formatar resultados para visualização
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig
from app.core.config import get_settings
from app.agents.omie_dictionary import get_omie_context
from app.core.custom_measures import get_custom_measures_prompt


logger = logging.getLogger(__name__)


def _infer_business_rules_from_tables(tables: List[Dict]) -> str:
    """
    Infere regras de negócio automaticamente a partir das colunas descobertas no schema.
    Detecta padrões por presença de coluna — não depende de valores de amostra.
    """
    # Identifica tabela principal de dados
    main_table = None
    col_names: set = set()  # col_name.lower()

    for t in tables:
        tname = t.get("name", "").lower()
        if tname == "data" or (main_table is None and len(t.get("columns", [])) > 5):
            main_table = t.get("name", "data")
            col_names = {c.get("name", "").lower() for c in t.get("columns", [])}

    if not main_table:
        return ""

    rules = [f"## REGRAS DE NEGÓCIO (inferidas automaticamente do schema da tabela '{main_table}')"]

    col_names_lower = {c.lower() for c in col_names}  # lowercase para comparar
    # Mapeia col_name_lower → sample_value (primeiro disponível)
    _sample_map: Dict[str, Any] = {}
    for t in tables:
        if (t.get("name", "").lower() == (main_table or "").lower()):
            for c in t.get("columns", []):
                sv = c.get("sampleValues", [])
                if sv:
                    _sample_map[c.get("name", "").lower()] = sv[0]

    def _orig(target_lower: str) -> str:
        for c in col_names:
            if c.lower() == target_lower:
                return c
        return target_lower

    def _year_filter(col_original: str) -> str:
        """Retorna cláusula de filtro de ano baseado no formato do sample value."""
        sample = _sample_map.get(col_original.lower(), "")
        if isinstance(sample, str) and len(sample) >= 4:
            if sample[:4].isdigit():  # formato "2024janeiro"
                return f"LEFT('{main_table}'[{col_original}], 4) = \"XXXX\""
            else:  # formato "janeiro2024"
                return f"RIGHT('{main_table}'[{col_original}], 4) = \"XXXX\""
        return f"RIGHT('{main_table}'[{col_original}], 4) = \"XXXX\""

    def _month_filter(col_original: str) -> str:
        """Retorna exemplo de filtro de mês baseado no formato do sample value."""
        sample = _sample_map.get(col_original.lower(), "")
        if isinstance(sample, str) and len(sample) >= 4:
            if sample[:4].isdigit():  # formato "2024janeiro"
                return f"'{main_table}'[{col_original}] = \"ANOmes\" — ex: \"2024janeiro\", \"2025dezembro\""
            else:  # formato "janeiro2024"
                return f"'{main_table}'[{col_original}] = \"mesANO\" — ex: \"janeiro2024\", \"dezembro2025\""
        return f"'{main_table}'[{col_original}] = \"mesANO\" — ex: \"janeiro2024\""

    # ── Flags de presença de colunas ──────────────────────────────
    has_receita_comp  = "receita competencia" in col_names_lower
    has_despesa_comp  = "despesas competencia" in col_names_lower
    has_ano_mes_comp  = "ano_mes_competencia" in col_names_lower or "ano_mes competencia" in col_names_lower
    has_cgrupo        = "cgrupo" in col_names_lower
    has_previsto      = "previsto/realizado" in col_names_lower
    has_cnatureza     = "cnatureza" in col_names_lower
    has_cstatus       = "cstatus" in col_names_lower
    has_receita       = "receita" in col_names_lower
    has_despesas      = "despesas" in col_names_lower
    has_rec_desp      = "receita/despesa" in col_names_lower  # Conta Azul
    has_situacao      = "situação" in col_names_lower or "situacao" in col_names_lower
    has_ano           = "ano " in col_names_lower  # "Ano " com espaço (Omie)
    has_ano_mes_cx    = "ano_mes" in col_names_lower or "ano_mes_caixa" in col_names_lower
    has_nome_mes      = "nome mês" in col_names_lower or "nome mes" in col_names_lower

    # Nomes originais das colunas de período (variam por sistema)
    ano_mes_comp_col  = _orig("ano_mes competencia") if "ano_mes competencia" in col_names_lower else _orig("ano_mes_competencia")
    ano_mes_cx_col    = _orig("ano_mes") if "ano_mes" in col_names_lower else _orig("ano_mes_caixa")

    # ── Regras de receita ──────────────────────────────────────────
    if has_receita_comp and has_cgrupo and has_previsto:
        # Omie: receita de competência com filtro de grupo e realizado
        rules.append(
            f"- RECEITA/FATURAMENTO (competência) = "
            f"CALCULATE(SUM('{main_table}'[{_orig('receita competencia')}]), "
            f"'{main_table}'[{_orig('cgrupo')}] = \"CONTA_A_RECEBER\", "
            f"'{main_table}'[{_orig('previsto/realizado')}] = \"Realizado\")"
        )
    elif has_receita and has_rec_desp:
        # Conta Azul: filtra pela coluna Receita/Despesa
        rules.append(
            f"- RECEITA = CALCULATE(SUM('{main_table}'[{_orig('receita')}]), "
            f"'{main_table}'[{_orig('receita/despesa')}] = \"Receita\")"
        )
    elif has_receita and has_cnatureza:
        # Genérico com cNatureza
        rules.append(
            f"- RECEITA = CALCULATE(SUM('{main_table}'[{_orig('receita')}]), "
            f"'{main_table}'[{_orig('cnatureza')}] = \"R\")"
        )
    elif has_receita:
        rules.append(f"- RECEITA = SUM('{main_table}'[{_orig('receita')}])")

    # ── Regras de despesas ─────────────────────────────────────────
    if has_despesa_comp and has_cgrupo and has_previsto:
        rules.append(
            f"- DESPESAS (competência) = "
            f"CALCULATE(SUM('{main_table}'[{_orig('despesas competencia')}]), "
            f"'{main_table}'[{_orig('cgrupo')}] = \"CONTA_A_PAGAR\", "
            f"'{main_table}'[{_orig('previsto/realizado')}] = \"Realizado\")"
        )
    elif has_despesas and has_rec_desp:
        rules.append(
            f"- DESPESAS = CALCULATE(SUM('{main_table}'[{_orig('despesas')}]), "
            f"'{main_table}'[{_orig('receita/despesa')}] = \"Despesa\")"
        )
    elif has_despesas and has_cnatureza:
        rules.append(
            f"- DESPESAS = CALCULATE(SUM('{main_table}'[{_orig('despesas')}]), "
            f"'{main_table}'[{_orig('cnatureza')}] = \"P\")"
        )
    elif has_despesas:
        rules.append(f"- DESPESAS = SUM('{main_table}'[{_orig('despesas')}])")

    # ── Regras de resultado ────────────────────────────────────────
    if has_receita_comp and has_despesa_comp and has_cgrupo and has_previsto:
        rules.append(
            f"- RESULTADO/LUCRO = RECEITA (competência) - DESPESAS (competência) — use as fórmulas acima"
        )
    elif has_receita and has_despesas:
        rules.append(f"- RESULTADO/LUCRO = SUM receita - SUM despesas (com filtros apropriados acima)")

    # ── Caixa / recebimentos ───────────────────────────────────────
    if has_receita and has_cstatus:
        rules.append(
            f"- RECEBIMENTOS CAIXA = CALCULATE(SUM('{main_table}'[{_orig('receita')}]), "
            f"'{main_table}'[{_orig('cstatus')}] IN {{\"PAGO\", \"RECEBIDO\"}})"
        )

    # ── Filtros de cancelado/status ────────────────────────────────
    if has_previsto:
        rules.append(
            f"- EXCLUIR CANCELADOS = '{main_table}'[{_orig('previsto/realizado')}] <> \"Cancelado\""
        )
    if has_situacao:
        situ_col = _orig("situação") if "situação" in col_names_lower else _orig("situacao")
        rules.append(f"- STATUS do registro: coluna [{situ_col}] — ex: \"Em aberto\", \"Pago\", \"Vencido\"")
    if has_cnatureza:
        rules.append(f"- [{_orig('cnatureza')}]: \"R\"=receita, \"P\"=despesa — NUNCA \"D\"")

    # ── Filtros de período ─────────────────────────────────────────
    if has_ano_mes_comp:
        rules.append(f"- FILTRO ANO (competência): {_year_filter(ano_mes_comp_col)}")
        rules.append(f"- FILTRO MÊS (competência): {_month_filter(ano_mes_comp_col)}")
    if has_ano:
        rules.append(
            f"- FILTRO ANO (caixa): '{main_table}'[{_orig('ano ')}] = \"XXXX\" — coluna tem ESPAÇO no final"
        )
    if has_ano_mes_cx and not has_ano:
        rules.append(f"- FILTRO ANO (caixa): {_year_filter(ano_mes_cx_col)}")
    if has_nome_mes:
        nome_mes_col = _orig('nome mês') if 'nome mês' in col_names_lower else _orig('nome mes')
        rules.append(
            f"- FILTRO MÊS (caixa): '{main_table}'[{nome_mes_col}] = \"MêsCapitalizado\" — ex: \"Janeiro\""
        )

    rules.append("")
    return "\n".join(rules)


@dataclass
class DAXQuery:
    """Representa uma query DAX gerada"""
    query: str
    explanation: str
    confidence: float
    tables_used: List[str]
    measures_used: List[str]


@dataclass
class QueryResult:
    """Resultado de uma query executada"""
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: int
    query: str


class QueryBuilderAgent:
    """
    Agente especializado em construção e execução de queries DAX
    
    Exemplo de uso:
        agent = QueryBuilderAgent()
        result = await agent.execute_question(
            question="Qual o total de vendas de janeiro?",
            schema=schema
        )
    """
    
    SYSTEM_PROMPT = """
Você é um especialista em DAX (Data Analysis Expressions) para Power BI.
Gera queries DAX corretas a partir de perguntas em linguagem natural, usando APENAS
as colunas e tabelas presentes no schema fornecido abaixo.

REGRAS DE DAX:
- TODA query começa com EVALUATE
- Use ROW() para retornar um único valor agregado
- Use SUMMARIZECOLUMNS para agregações com agrupamento
- Use TOPN para rankings
- Sempre especifique a tabela: 'NomeTabela'[Coluna]
- Use variáveis (VAR/RETURN) para queries complexas

REGRA ABSOLUTA: use SOMENTE os nomes de colunas e tabelas listados no SCHEMA REAL abaixo.
Se a coluna não existir no schema, NÃO a invente — use a coluna mais próxima disponível.

{omie_context}

{custom_measures}

REGRA CRÍTICA DE FORMATO JSON:
- Responda APENAS com o JSON solicitado — sem markdown, sem texto extra
- Toda query DAX vai dentro de uma string JSON — escape TODAS as aspas duplas como \"
- Quebras de linha dentro da query: use \\n ou escreva tudo em uma linha
- NUNCA coloque aspas duplas sem escape dentro do valor dax_query
"""

    DAX_GENERATION_PROMPT = """
    Com base no modelo de dados Omie e na pergunta do usuário, gere uma query DAX precisa.

    PERGUNTA: {question}

    HISTÓRICO DE CONVERSA:
    {history}

    SCHEMA ADICIONAL (se disponível via API):
    {schema}

    Responda SOMENTE com este objeto JSON (sem markdown, sem texto antes ou depois):
    {{
        "dax_query": "EVALUATE ...",
        "explanation": "Explicação em português do que a query faz",
        "confidence": 0.0,
        "tables_used": ["tabela1"],
        "measures_used": []
    }}

    ATENÇÃO: escape todas as aspas duplas dentro de dax_query como \\"
    """
    
    # Funções DAX potencialmente perigosas
    BLOCKED_FUNCTIONS = [
        "PATHITEM",  # Pode ser lenta
        "LOOKUPVALUE",  # Pode ser lenta em grandes volumes
    ]
    
    @staticmethod
    def _build_dynamic_system_prompt(schema: Optional[Dict[str, Any]]) -> str:
        """
        Constrói o system prompt dinamicamente a partir do schema real descoberto via API.
        Quando há schema dinâmico, injeta os nomes de colunas/tabelas reais.
        Quando não há, usa o contexto Omie estático como fallback.
        """
        tables = schema.get("tables", []) if schema else []
        omie_context = schema.get("omie_context", "") if schema else ""
        hard_filters = schema.get("hard_filters", []) if schema else []

        # Constrói bloco de schema dinâmico se disponível
        schema_block = ""
        if tables:
            lines = ["## SCHEMA REAL DO DATASET (use APENAS estes nomes — nenhum outro):"]
            for t in tables[:15]:  # Limita para não exceder context window
                tname = t.get("name", "")
                if not tname or t.get("isHidden"):
                    continue
                col_parts = []
                for c in t.get("columns", [])[:30]:
                    if c.get("isHidden"):
                        continue
                    cname = c.get("name", "")
                    dtype = c.get("dataType", "")
                    samples = c.get("sampleValues", [])
                    sample_str = ""
                    if samples:
                        sample_list = [str(s) for s in samples[:5] if s is not None]
                        if sample_list:
                            sample_str = f" (ex: {', '.join(sample_list)})"
                    col_parts.append(f"[{cname}]{sample_str}")

                measures = [
                    f"[{m.get('name', '')}]"
                    for m in t.get("measures", [])
                    if not m.get("isHidden") and m.get("name")
                ]

                col_str = ", ".join(col_parts) if col_parts else "(sem colunas)"
                meas_str = f"\n    Medidas: {', '.join(measures)}" if measures else ""
                lines.append(f"  '{tname}': {col_str}{meas_str}")

            lines.append("")
            lines.append("REGRA ABSOLUTA: use SOMENTE os nomes de tabela/coluna listados acima.")
            lines.append("Nunca invente nomes. Se não encontrar a coluna, pergunte ao usuário.")
            schema_block = "\n".join(lines)

        # Bloco de hard filters
        hard_filters_block = ""
        if hard_filters:
            hf_lines = ["## FILTROS OBRIGATÓRIOS DO RELATÓRIO POWER BI"]
            hf_lines.append("Estes filtros DEVEM estar presentes em TODA query DAX gerada:")
            for hf in hard_filters:
                hf_lines.append(f"  - {hf}")
            hf_lines.append("")
            hard_filters_block = "\n".join(hf_lines)

        from app.core.custom_measures import get_custom_measures_prompt
        custom_measures = get_custom_measures_prompt()

        # Base: sempre inclui as regras invioláveis + contexto Omie estático
        base_prompt = QueryBuilderAgent.SYSTEM_PROMPT.format(
            omie_context=omie_context or get_omie_context(),
            custom_measures=custom_measures or "",
        )

        # Quando temos schema dinâmico, adiciona contexto extra sem remover regras base
        if tables:
            # Regras adicionais inferidas do schema (se colunas-chave estiverem presentes)
            business_rules = _infer_business_rules_from_tables(tables)

            return (
                base_prompt
                + ("\n\n" + business_rules if business_rules else "")
                + ("\n\n" + schema_block if schema_block else "")
                + ("\n\n" + hard_filters_block if hard_filters_block else "")
            )

        # Fallback: apenas prompt estático com hard filters
        if hard_filters_block:
            return base_prompt + "\n\n" + hard_filters_block
        return base_prompt

    def __init__(self):
        settings = get_settings()
        
        self.config = PowerBIConfig.from_env()
        self.client = PowerBIClient(self.config)
        
        self.llm = ChatAnthropic(
            model="claude-sonnet-4-6",
            temperature=0.0,
            api_key=settings.anthropic_api_key.get_secret_value(),
        )
        
        self._max_rows = 10000
        self._timeout_seconds = 60
    
    async def execute_question(
        self,
        question: str,
        schema: Dict[str, Any],
        history: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """
        Processa uma pergunta e retorna resultados
        
        Args:
            question: Pergunta em linguagem natural
            schema: Schema do dataset
            history: Histórico de conversa
            
        Returns:
            Resultados da query com metadados
        """
        
        # 1. Gera query DAX
        dax_query = await self.generate_dax(question, schema, history)
        
        if dax_query.confidence < 0.5:
            logger.warning(f"Low confidence DAX query: {dax_query.confidence}")
            return {
                "success": False,
                "error": "Não foi possível gerar uma query confiável para esta pergunta",
                "confidence": dax_query.confidence,
                "suggestion": "Tente reformular sua pergunta de forma mais específica"
            }
        
        # 2. Valida a query
        validation = self._validate_dax(dax_query.query)
        if not validation["valid"]:
            return {
                "success": False,
                "error": validation["error"],
                "dax_query": dax_query.query
            }
        
        # 3. Executa a query
        try:
            dataset_id = schema.get("dataset_id")
            if not dataset_id:
                raise QueryError("Dataset ID não encontrado no schema")
            
            import time
            start_time = time.time()
            
            result = await self.client.execute_query(dataset_id, dax_query.query)
            
            execution_time = int((time.time() - start_time) * 1000)
            
            # 4. Formata resultados
            formatted = self._format_results(result)
            
            return {
                "success": True,
                "dax_query": dax_query.query,
                "explanation": dax_query.explanation,
                "results": formatted,
                "row_count": len(formatted.get("rows", [])),
                "execution_time_ms": execution_time,
                "tables_used": dax_query.tables_used,
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "dax_query": dax_query.query
            }
    
    async def generate_dax(
        self,
        question: str,
        schema: Dict[str, Any],
        history: Optional[List[Dict]] = None
    ) -> DAXQuery:
        """Gera query DAX a partir de linguagem natural"""

        # Injeta catálogo dinâmico de medidas (descoberto do dataset via DMV)
        discovered_block = ""
        try:
            from app.core.measure_catalog import MeasureCatalog
            catalog = MeasureCatalog.get_instance()
            if catalog.is_loaded:
                discovered_block = catalog.build_prompt_block()
        except Exception:
            pass

        # Se há uma medida já resolvida pelo orchestrator, adiciona dica explícita
        resolved_hint = ""
        resolved_measure = schema.get("resolved_measure") if schema else None
        if resolved_measure:
            resolved_hint = (
                f"\n\nMEDIDA JÁ IDENTIFICADA PELO SISTEMA: [{resolved_measure}]\n"
                f"Use CALCULATE([{resolved_measure}], filtros). "
                f"NÃO reescreva a lógica desta medida."
            )

        system_prompt = self._build_dynamic_system_prompt(schema)

        # Appends discovered measures and resolved hint regardless of prompt path
        if discovered_block or resolved_hint:
            system_prompt = system_prompt + "\n\n" + discovered_block + resolved_hint

        # Prepara schema adicional (pode vir da API do Power BI)
        schema_summary = self._summarize_schema(schema) if schema else "Nenhum schema adicional."

        # Prepara histórico
        history_text = ""
        if history:
            history_text = "\n".join([
                f"- {h.get('question', '')}: {h.get('summary', '')}"
                for h in history[-3:]  # Últimas 3 interações
            ])

        prompt = self.DAX_GENERATION_PROMPT.format(
            schema=schema_summary,
            question=question,
            history=history_text or "Nenhum histórico"
        )

        response = await self.llm.ainvoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=prompt)
        ])
        
        # Parse da resposta — estratégia múltipla robusta
        import json as _json
        content = response.content

        # Estratégia 1: JSON padrão
        data = None
        try:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                data = _json.loads(content[json_start:json_end])
        except (_json.JSONDecodeError, ValueError):
            pass

        # Estratégia 2: pré-processa para escapar aspas internas no dax_query
        if data is None:
            try:
                def _fix_dax_quotes(m):
                    prefix = m.group(1)   # "dax_query": "
                    inner  = m.group(2)   # conteúdo cru da query
                    suffix = m.group(3)   # fechamento: ", ou "}
                    # escapa aspas que ainda não estão escapadas
                    inner_fixed = re.sub(r'(?<!\\)"', '\\"', inner)
                    return prefix + inner_fixed + suffix

                fixed = re.sub(
                    r'("dax_query"\s*:\s*")(EVALUATE[\s\S]*?)("(?:\s*[,}]))',
                    _fix_dax_quotes,
                    content,
                    flags=re.DOTALL | re.IGNORECASE,
                )
                json_start = fixed.find("{")
                json_end = fixed.rfind("}") + 1
                if json_start >= 0 and json_end > json_start:
                    data = _json.loads(fixed[json_start:json_end])
            except Exception:
                pass

        if data is not None:
            raw_query = data.get("dax_query", "")
            # Unescape \\n → \n se o LLM escapou quebras de linha
            raw_query = raw_query.replace("\\n", "\n").replace("\\t", "\t")
            fixed_query = self._fix_known_hallucinations(raw_query)
            if fixed_query != raw_query:
                logger.warning(f"DAX hallucination fixed")
            return DAXQuery(
                query=fixed_query,
                explanation=data.get("explanation", ""),
                confidence=float(data.get("confidence", 0.5)),
                tables_used=data.get("tables_used", []),
                measures_used=data.get("measures_used", []),
            )

        # Estratégia 3: extrai bloco EVALUATE, para ANTES do próximo campo JSON
        logger.warning("JSON parse failed for DAX response — using EVALUATE block extraction")
        eval_match = re.search(
            r'(EVALUATE\b[\s\S]+?)(?=\n\s*"(?:explanation|confidence|tables_used|measures_used)"|\Z)',
            content, re.DOTALL | re.IGNORECASE
        )
        if eval_match:
            raw_query = eval_match.group(1).strip()
            # Remove artefatos de JSON no final (aspas, vírgulas, chaves)
            raw_query = re.sub(r'[",\s}]+$', '', raw_query).strip()
            return DAXQuery(
                query=self._fix_known_hallucinations(raw_query),
                explanation="Query extraída automaticamente",
                confidence=0.6,
                tables_used=[],
                measures_used=[],
            )

        logger.error("Could not extract any DAX query from LLM response")
        return DAXQuery(
            query="",
            explanation="Falha ao gerar query",
            confidence=0.0,
            tables_used=[],
            measures_used=[],
        )
    
    def _summarize_schema(self, schema: Dict[str, Any]) -> str:
        """Resume schema para uso no prompt"""
        
        lines = []
        
        for table in schema.get("tables", []):
            if table.get("isHidden"):
                continue
                
            table_name = table["name"]
            lines.append(f"\nTabela: {table_name}")
            
            # Colunas
            columns = [
                c["name"] for c in table.get("columns", [])
                if not c.get("isHidden")
            ]
            if columns:
                lines.append(f"  Colunas: {', '.join(columns[:10])}")
                if len(columns) > 10:
                    lines.append(f"  ... e mais {len(columns) - 10} colunas")
            
            # Medidas
            measures = [m["name"] for m in table.get("measures", [])]
            if measures:
                lines.append(f"  Medidas: {', '.join(measures)}")
        
        # Relacionamentos
        relationships = schema.get("relationships", [])
        if relationships:
            lines.append("\nRelacionamentos:")
            for rel in relationships[:5]:
                lines.append(f"  - {rel.get('fromTable')} -> {rel.get('toTable')}")
        
        return "\n".join(lines)
    
    def _validate_dax(self, query: str) -> Dict[str, Any]:
        """Valida uma query DAX antes de executar"""
        
        if not query or not query.strip():
            return {"valid": False, "error": "Query vazia"}
        
        # Verifica se começa com EVALUATE
        if not query.strip().upper().startswith("EVALUATE"):
            return {"valid": False, "error": "Query deve começar com EVALUATE"}
        
        # Verifica funções bloqueadas
        query_upper = query.upper()
        for func in self.BLOCKED_FUNCTIONS:
            if func in query_upper:
                return {
                    "valid": False, 
                    "error": f"Função {func} não é permitida por motivos de performance"
                }
        
        # Verifica complexidade (níveis de aninhamento)
        open_parens = query.count("(")
        close_parens = query.count(")")
        
        if open_parens != close_parens:
            return {"valid": False, "error": "Parênteses não balanceados"}
        
        if open_parens > 30:
            return {
                "valid": False,
                "error": "Query muito complexa (muitos níveis de aninhamento)"
            }
        
        return {"valid": True}

    def _fix_known_hallucinations(self, query: str) -> str:
        """Corrige alucinações conhecidas do LLM antes de executar a query."""
        import re
        # data[valor] → data[receita] (alucinação mais comum)
        query = re.sub(r"data\[(?:V|v)alor\]", "data[receita]", query)
        query = re.sub(r"'data'\[(?:V|v)alor\]", "'data'[receita]", query)
        # data[total] → data[receita]
        query = re.sub(r"data\[(?:T|t)otal\]", "data[receita]", query)
        query = re.sub(r"'data'\[(?:T|t)otal\]", "'data'[receita]", query)
        # cNatureza = "D" → cNatureza = "P"
        query = re.sub(r'cNatureza\s*=\s*"D"', 'cNatureza = "P"', query)
        return query

    def _format_results(self, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Formata resultados para exibição"""
        
        columns = raw_result.get("columns", [])
        rows = raw_result.get("rows", [])
        
        # Limita número de linhas
        if len(rows) > self._max_rows:
            rows = rows[:self._max_rows]
            truncated = True
        else:
            truncated = False
        
        # Formata valores numéricos
        formatted_rows = []
        for row in rows:
            formatted_row = {}
            for key, value in row.items():
                if isinstance(value, float):
                    # Formata números com 2 casas decimais
                    formatted_row[key] = round(value, 2)
                else:
                    formatted_row[key] = value
            formatted_rows.append(formatted_row)
        
        return {
            "columns": columns,
            "rows": formatted_rows,
            "row_count": len(formatted_rows),
            "truncated": truncated,
        }
    
    # ─────────────────────────────────────────────────────────────
    # MÉTODOS AUXILIARES
    # ─────────────────────────────────────────────────────────────
    
    async def suggest_visualizations(
        self, 
        results: Dict[str, Any],
        question: str
    ) -> List[Dict[str, Any]]:
        """Sugere tipos de visualização para os resultados"""
        
        columns = results.get("columns", [])
        row_count = results.get("row_count", 0)
        
        suggestions = []
        
        # Analisa tipos de dados nas colunas
        has_numeric = any(
            isinstance(results["rows"][0].get(col), (int, float))
            for col in columns
        ) if results.get("rows") else False
        
        has_date = any("date" in col.lower() or "data" in col.lower() for col in columns)
        has_category = len(columns) > 1 and row_count < 50
        
        if has_date and has_numeric:
            suggestions.append({
                "type": "line_chart",
                "reason": "Dados temporais com valores numéricos",
                "config": {"x_axis": "date_column", "y_axis": "numeric_column"}
            })
        
        if has_category and has_numeric:
            suggestions.append({
                "type": "bar_chart",
                "reason": "Categorias com valores numéricos",
                "config": {"categories": "category_column", "values": "numeric_column"}
            })
        
        if row_count == 1 and has_numeric:
            suggestions.append({
                "type": "card",
                "reason": "Valor único/métrica",
                "config": {"value": columns[0]}
            })
        
        if row_count <= 100:
            suggestions.append({
                "type": "table",
                "reason": "Dados tabulares",
                "config": {"columns": columns}
            })
        
        return suggestions
    
    async def explain_query(self, query: str) -> str:
        """Explica uma query DAX em linguagem natural"""
        
        prompt = f"""
        Explique a seguinte query DAX em linguagem simples:
        
        {query}
        
        Inclua:
        1. O que a query faz
        2. Quais tabelas e colunas usa
        3. Quais filtros aplica
        4. O que cada função faz
        """
        
        response = await self.llm.ainvoke([
            SystemMessage(content=self.SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ])
        
        return response.content
    
    async def optimize_query(self, query: str, schema: Dict[str, Any]) -> str:
        """Sugere otimizações para uma query DAX"""
        
        prompt = f"""
        Analise e otimize a seguinte query DAX:
        
        QUERY ORIGINAL:
        {query}
        
        SCHEMA:
        {self._summarize_schema(schema)}
        
        Sugira otimizações considerando:
        1. Uso de variáveis VAR/RETURN
        2. Substituição de funções lentas
        3. Uso de medidas existentes
        4. Filtros mais eficientes
        
        Retorne a query otimizada.
        """
        
        response = await self.llm.ainvoke([
            SystemMessage(content=self.SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ])
        
        return response.content
    
    async def close(self):
        """Fecha conexões"""
        await self.client.close()


class QueryError(Exception):
    """Erro na construção ou execução de query"""
    pass
