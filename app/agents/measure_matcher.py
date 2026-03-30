"""
MeasureMatcher — Intent-to-Measure Matching hibrido.

Estrategia em 3 camadas:
  1. Fuzzy match (rapidfuzz ou difflib) com normalizacao + sinonimos pt-BR
  2. Dicionario de sinonimos empresariais (determinisico, zero latencia)
  3. LLM fallback (Claude) para casos ambiguos

Retorna a medida mais provavel + score de confianca + raciocinio.
"""

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Configuracao
# ─────────────────────────────────────────────────────────────────────────────

# Threshold minimo para fuzzy match ser considerado confiavel (0-100)
FUZZY_HIGH_CONFIDENCE = 80
# Threshold para "talvez" — abaixo disso, vai para LLM
FUZZY_LOW_CONFIDENCE = 55
# Threshold para sinonimo direto (match exato no dicionario)
SYNONYM_CONFIDENCE = 92


# ─────────────────────────────────────────────────────────────────────────────
# Dicionario de sinonimos empresariais pt-BR → termo canonico
# O termo canonico e comparado fuzzy com os nomes das medidas.
# ─────────────────────────────────────────────────────────────────────────────

BUSINESS_SYNONYMS: Dict[str, List[str]] = {
    # ── RECEITA ──────────────────────────────────────────────────
    "receita": [
        # Formal
        "receita", "receita bruta", "receita total", "faturamento",
        "total de receita", "total de receitas", "receitas",
        "receita de vendas", "vendas",
        # Verbos / coloquial CEO
        "recebi", "recebemos", "recebeu",
        "faturamos", "faturei", "faturou",
        "quanto entrou", "o que entrou", "quanto entramos", "o que entramos",
        "quanto ganhamos", "quanto ganhei", "o que ganhamos",
        "quanto vendemos", "quanto vendeu", "vendemos",
        "entrou na conta", "caiu na conta",
        "entradas", "recebimento", "recebimentos",
        "fechamos em vendas",
    ],
    # ── DESPESA ──────────────────────────────────────────────────
    "despesa": [
        # Formal
        "despesa", "despesas", "custos", "custo", "gastos", "gasto",
        "pagamentos", "pagamento", "saídas", "saidas", "despesa total",
        "total de despesa", "total de despesas",
        # Verbos / coloquial CEO
        "gastamos", "gastei", "gastou",
        "pagamos", "pagei", "pagou",
        "o que saiu", "quanto saiu", "quanto gastamos", "o que gastamos",
        "quanto pagamos", "o que pagamos",
        "o que desembolsamos", "desembolsamos",
        "estamos gastando", "tô gastando", "estou gastando",
        "quanto sai por mês",
    ],
    # ── RESULTADO ─────────────────────────────────────────────────
    "resultado": [
        # Formal
        "resultado", "lucro", "prejuízo", "prejuizo", "saldo",
        "valor líquido", "valor liquido", "resultado líquido", "resultado liquido",
        "bottom line", "net income", "resultado final", "saldo final",
        # Verbos / coloquial CEO
        "sobrou", "o que sobrou", "quanto sobrou",
        "ficou", "o que ficou", "quanto ficou",
        "como fechamos", "fechamos bem", "como foi o resultado",
        "tô no lucro", "tô no prejuízo", "estamos no lucro",
        "o que restou", "quanto restou",
        "ganhamos ou perdemos", "positivo ou negativo",
    ],
    # ── EBITDA ───────────────────────────────────────────────────
    "ebitda": [
        "ebitda", "resultado operacional", "lucro operacional",
        "lajida", "resultado antes de juros",
        "geração de caixa operacional",
    ],
    # ── CAPEX ────────────────────────────────────────────────────
    "capex": [
        "capex", "investimentos de capital", "investimento de capital",
        "investimentos capex", "gastos de capital", "capital expenditure",
        "investimentos fixos", "imobilizado", "ativo fixo",
        "quanto investimos", "o que investimos", "investimentos",
    ],
    # ── OPEX ─────────────────────────────────────────────────────
    "opex": [
        "opex", "despesas operacionais", "despesa operacional",
        "custos operacionais", "custo operacional", "operational expenditure",
        "gastos operacionais", "custo de operação",
    ],
    # ── MARGEM BRUTA ─────────────────────────────────────────────
    "margem bruta": [
        "margem bruta", "margem de lucro bruto", "gross margin",
        "margem bruta percentual", "nossa margem",
    ],
    # ── MARGEM LÍQUIDA ───────────────────────────────────────────
    "margem liquida": [
        "margem liquida", "margem líquida", "margem de lucro líquido", "margem de lucro liquido",
        "net margin", "margem liquida percentual",
    ],
    # ── MARGEM EBITDA ────────────────────────────────────────────
    "margem ebitda": [
        "margem ebitda", "margem operacional", "operating margin",
    ],
    # ── INADIMPLÊNCIA ────────────────────────────────────────────
    "inadimplencia": [
        "inadimplencia", "inadimplência", "inadimplentes", "atraso", "atrasados",
        "títulos atrasados", "titulos atrasados", "contas atrasadas",
        "default rate", "taxa de inadimplência", "taxa de inadimplencia",
        "quem não pagou", "quem nao pagou", "o que está atrasado",
    ],
    # ── TICKET MÉDIO ─────────────────────────────────────────────
    "ticket medio": [
        "ticket medio", "ticket médio", "valor medio por venda",
        "valor médio", "valor medio", "average ticket", "ticket",
        "quanto cada cliente gasta em média", "média por cliente",
    ],
    # ── ROI ──────────────────────────────────────────────────────
    "roi": [
        "roi", "retorno sobre investimento", "return on investment",
        "retorno do investimento", "retorno",
    ],
    # ── CMV ──────────────────────────────────────────────────────
    "cmv": [
        "cmv", "custo da mercadoria vendida", "custo dos produtos vendidos",
        "cpv", "cogs", "cost of goods sold",
    ],
    # ── ROL ──────────────────────────────────────────────────────
    "rol": [
        "rol", "receita operacional liquida", "receita operacional líquida",
        "net revenue", "receita líquida", "receita liquida",
    ],
    # ── FLUXO DE CAIXA ───────────────────────────────────────────
    "fluxo de caixa": [
        "fluxo de caixa", "cash flow", "caixa", "posição de caixa", "posicao de caixa",
        "como está o caixa", "como ta o caixa", "dinheiro em caixa",
        "quanto tem no caixa", "o que tem no caixa",
    ],
    # ── BURN RATE ────────────────────────────────────────────────
    "burn rate": [
        "burn rate", "taxa de queima", "queima de caixa",
        "quanto estamos queimando", "quanto queimamos por mês",
    ],
    # ── CONTAS A RECEBER ─────────────────────────────────────────
    "contas a receber": [
        "contas a receber", "a receber", "accounts receivable",
        "recebíveis", "recebiveis", "quanto vamos receber",
        "o que vamos receber", "o que está para entrar",
    ],
    # ── CONTAS A PAGAR ───────────────────────────────────────────
    "contas a pagar": [
        "contas a pagar", "a pagar", "accounts payable",
        "quanto devemos", "quanto temos a pagar",
        "o que precisamos pagar", "compromissos financeiros",
    ],
    # ── DEPRECIAÇÃO ──────────────────────────────────────────────
    "depreciacao": [
        "depreciacao", "depreciação", "depreciation",
        "amortizacao", "amortização",
    ],
    # ── IMPOSTOS ─────────────────────────────────────────────────
    "impostos": [
        "impostos", "tributos", "carga tributária", "carga tributaria", "tax",
        "imposto", "icms", "pis", "cofins", "iss", "ir", "irrf",
        "imposto de renda", "quanto pagamos de imposto", "quanto foi de imposto",
    ],
}

# Inverte o dicionario: forma_do_usuario → termo_canonico
_USER_TERM_TO_CANONICAL: Dict[str, str] = {}
for canonical, variants in BUSINESS_SYNONYMS.items():
    for variant in variants:
        _USER_TERM_TO_CANONICAL[_normalize(variant) if callable(globals().get('_normalize')) else variant.lower()] = canonical


# ─────────────────────────────────────────────────────────────────────────────
# Dataclasses de resultado
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MeasureMatch:
    """Resultado do matching de medida."""
    measure_name: str           # Nome exato da medida no Power BI (ex: "[Total CAPEX]")
    confidence: float           # 0.0 a 1.0
    method: str                 # "fuzzy", "synonym", "llm", "none"
    reasoning: str              # Explicacao legivel
    canonical_term: Optional[str] = None  # Termo canonico intermediario (se usou sinonimo)
    alternatives: List[str] = field(default_factory=list)  # Outras medidas possiveis


# ─────────────────────────────────────────────────────────────────────────────
# Funcoes de normalizacao
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """
    Normaliza texto para comparacao:
    - Lowercase
    - Remove acentos (unicodedata NFKD)
    - Remove caracteres especiais exceto espaco
    - Colapsa espacos multiplos
    """
    text = text.lower().strip()
    # Remove acentos
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    # Remove tudo que nao e alfanumerico ou espaco
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    # Colapsa espacos
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_measure_name(raw: str) -> str:
    """
    Extrai o nome limpo de uma medida.
    '[Total CAPEX]' → 'total capex'
    'Valor liquido' → 'valor liquido'
    """
    # Remove colchetes
    name = raw.strip("[] ")
    return _normalize(name)


# Reconstruir o dicionario invertido agora que _normalize esta definida
_USER_TERM_TO_CANONICAL = {}
for _canonical, _variants in BUSINESS_SYNONYMS.items():
    for _variant in _variants:
        _USER_TERM_TO_CANONICAL[_normalize(_variant)] = _canonical


# ─────────────────────────────────────────────────────────────────────────────
# Fuzzy matching
# ─────────────────────────────────────────────────────────────────────────────

def _fuzzy_ratio(a: str, b: str) -> int:
    """
    Retorna score de similaridade 0-100.
    Usa rapidfuzz se disponivel, senao difflib.
    """
    try:
        from rapidfuzz import fuzz
        return int(fuzz.ratio(a, b))
    except ImportError:
        return int(SequenceMatcher(None, a, b).ratio() * 100)


def _fuzzy_partial_ratio(a: str, b: str) -> int:
    """
    Partial ratio: detecta se 'a' e substring fuzzy de 'b' ou vice-versa.
    Util para "capex" dentro de "Total CAPEX".
    """
    try:
        from rapidfuzz import fuzz
        return int(fuzz.partial_ratio(a, b))
    except ImportError:
        # Fallback: testa se o menor e substring do maior
        if len(a) > len(b):
            a, b = b, a
        # Sliding window
        best = 0
        for i in range(len(b) - len(a) + 1):
            score = SequenceMatcher(None, a, b[i:i+len(a)]).ratio() * 100
            if score > best:
                best = score
        return int(best)


def _fuzzy_token_sort_ratio(a: str, b: str) -> int:
    """
    Token sort ratio: ordena tokens antes de comparar.
    "Total CAPEX" vs "CAPEX Total" → 100.
    """
    try:
        from rapidfuzz import fuzz
        return int(fuzz.token_sort_ratio(a, b))
    except ImportError:
        sa = " ".join(sorted(a.split()))
        sb = " ".join(sorted(b.split()))
        return int(SequenceMatcher(None, sa, sb).ratio() * 100)


def _best_fuzzy_score(query: str, measure: str) -> int:
    """
    Retorna o melhor score entre ratio, partial_ratio e token_sort_ratio.
    """
    return max(
        _fuzzy_ratio(query, measure),
        _fuzzy_partial_ratio(query, measure),
        _fuzzy_token_sort_ratio(query, measure),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Extracao de termos de medida da pergunta do usuario
# ─────────────────────────────────────────────────────────────────────────────

def _extract_measure_intent(question: str) -> List[str]:
    """
    Extrai termos candidatos da pergunta que provavelmente referem medidas.
    Remove stopwords e filtros temporais para isolar o 'o que' da pergunta.
    """
    normalized = _normalize(question)

    # Remove filtros temporais e preposicoes comuns
    temporal_patterns = [
        r"\b(?:em|de|do|da|no|na|nos|nas|para|pelo|pela)\b",
        r"\b(?:janeiro|fevereiro|marco|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\b",
        r"\b(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\b",
        r"\b(?:q[1-4]|primeiro|segundo|terceiro|quarto)\s*trimestre\b",
        r"\b20[2-3]\d\b",
        r"\b(?:ano passado|ano anterior|ano retrasado|esse ano|este ano|ano atual)\b",
        r"\b(?:ultimo|ultima|ultimos|ultimas|proximo|proxima)\b",
        r"\b(?:mes passado|mes anterior|mes atual|este mes)\b",
    ]
    cleaned = normalized
    for pat in temporal_patterns:
        cleaned = re.sub(pat, " ", cleaned)

    # Remove palavras interrogativas e verbos auxiliares
    stopwords = {
        "qual", "quais", "quanto", "quanta", "quantos", "quantas",
        "como", "onde", "quando", "porque", "por que",
        "foi", "e", "era", "sao", "foram", "sera", "serao",
        "esta", "estamos", "estao", "estava", "indo", "vai", "vao",
        "meu", "minha", "meus", "minhas", "nosso", "nossa", "nossos", "nossas",
        "total", "valor", "me", "mostra", "mostre", "diga", "informe",
        "o", "a", "os", "as", "um", "uma", "uns", "umas",
        "que", "se", "com", "por", "sem", "das", "dos",
        "hoje", "agora", "ontem", "amanha", "aqui",
        "clima", "tempo", "dia", "noite",
    }
    tokens = cleaned.split()
    # Filtra stopwords e tokens muito curtos (< 3 chars sao quase sempre ruido)
    filtered = [t for t in tokens if t not in stopwords and len(t) >= 3]

    # Retorna tanto a frase completa filtrada quanto tokens individuais
    result = []
    full_phrase = " ".join(filtered)
    if full_phrase:
        result.append(full_phrase)
    # Adiciona cada token individualmente tambem
    for t in filtered:
        if t not in result:
            result.append(t)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Classe principal: MeasureMatcher
# ─────────────────────────────────────────────────────────────────────────────

class MeasureMatcher:
    """
    Intent-to-Measure Matching hibrido.

    Uso:
        matcher = MeasureMatcher(measures=["EBITDA", "Total CAPEX", "Margem Bruta %"])
        result = matcher.match("qual foi meu capex total em 2025?")
        # result.measure_name = "Total CAPEX", result.confidence = 0.95

        # Se fuzzy nao resolver com confianca suficiente:
        result = await matcher.match_with_llm_fallback(
            question="quanto investimos em ativo fixo?",
            llm=my_llm_instance,
        )
    """

    def __init__(self, measures: List[str], aliases: Optional[Dict[str, List[str]]] = None):
        """
        Args:
            measures: Lista de nomes de medidas do Power BI (ex: ["EBITDA", "Total CAPEX"])
            aliases: Aliases adicionais do measures.json (ex: {"EBITDA": ["ebitda", "lucro operacional"]})
        """
        self.measures = measures
        self.aliases = aliases or {}

        # Pre-computa versoes normalizadas
        self._normalized_measures: Dict[str, str] = {}
        for m in measures:
            self._normalized_measures[m] = _extract_measure_name(m)

        # Adiciona aliases ao mapa de lookup
        self._alias_to_measure: Dict[str, str] = {}
        for measure_name, alias_list in self.aliases.items():
            for alias in alias_list:
                self._alias_to_measure[_normalize(alias)] = measure_name

    @classmethod
    def from_measures_json(cls, measures_data: List[Dict[str, Any]]) -> "MeasureMatcher":
        """
        Cria MeasureMatcher a partir do formato de measures.json.

        Args:
            measures_data: Lista de dicts com "name", "aliases", etc.
        """
        names = [m["name"] for m in measures_data if m.get("name")]
        aliases = {}
        for m in measures_data:
            if m.get("aliases"):
                aliases[m["name"]] = m["aliases"]
        return cls(measures=names, aliases=aliases)

    @classmethod
    def from_schema_and_measures(
        cls,
        schema: Dict[str, Any],
        custom_measures: List[Dict[str, Any]],
    ) -> "MeasureMatcher":
        """
        Cria MeasureMatcher combinando medidas do schema Power BI + measures.json.
        """
        names = []
        # Medidas do schema
        for table in schema.get("tables", []):
            for m in table.get("measures", []):
                if m.get("name"):
                    names.append(m["name"])

        # Medidas customizadas
        aliases = {}
        for m in custom_measures:
            name = m.get("name", "")
            if name and name not in names:
                names.append(name)
            if m.get("aliases"):
                aliases[name] = m["aliases"]

        return cls(measures=names, aliases=aliases)

    # ── Matching sincronico (fuzzy + sinonimos) ──────────────────

    def match(self, question: str) -> MeasureMatch:
        """
        Tenta match usando fuzzy + sinonimos (sem LLM).
        Retorna MeasureMatch com confidence indicando se LLM fallback e necessario.
        """
        if not self.measures:
            return MeasureMatch(
                measure_name="",
                confidence=0.0,
                method="none",
                reasoning="Nenhuma medida disponivel no catalogo.",
            )

        intents = _extract_measure_intent(question)
        if not intents:
            return MeasureMatch(
                measure_name="",
                confidence=0.0,
                method="none",
                reasoning="Nao foi possivel extrair intent de medida da pergunta.",
            )

        # 1. Verifica aliases diretos (measures.json)
        normalized_q = _normalize(question)
        for alias_norm, measure_name in self._alias_to_measure.items():
            if alias_norm in normalized_q:
                return MeasureMatch(
                    measure_name=measure_name,
                    confidence=SYNONYM_CONFIDENCE / 100,
                    method="synonym",
                    reasoning=f"Alias direto '{alias_norm}' encontrado em measures.json -> [{measure_name}]",
                    canonical_term=alias_norm,
                )

        # 2. Verifica sinonimos empresariais
        for intent in intents:
            if intent in _USER_TERM_TO_CANONICAL:
                canonical = _USER_TERM_TO_CANONICAL[intent]
                # Agora faz fuzzy do canonico contra as medidas
                best_measure, best_score = self._best_fuzzy_against_measures(canonical)
                if best_score >= FUZZY_LOW_CONFIDENCE:
                    return MeasureMatch(
                        measure_name=best_measure,
                        confidence=min(SYNONYM_CONFIDENCE, best_score + 10) / 100,
                        method="synonym",
                        reasoning=(
                            f"Sinonimo '{intent}' -> canonico '{canonical}' "
                            f"-> fuzzy match [{best_measure}] (score={best_score})"
                        ),
                        canonical_term=canonical,
                    )

        # 3. Verifica sinonimos como substring da pergunta completa
        for user_term, canonical in _USER_TERM_TO_CANONICAL.items():
            if len(user_term) > 3 and user_term in normalized_q:
                best_measure, best_score = self._best_fuzzy_against_measures(canonical)
                if best_score >= FUZZY_LOW_CONFIDENCE:
                    return MeasureMatch(
                        measure_name=best_measure,
                        confidence=min(SYNONYM_CONFIDENCE, best_score + 5) / 100,
                        method="synonym",
                        reasoning=(
                            f"Sinonimo '{user_term}' encontrado na pergunta "
                            f"-> canonico '{canonical}' -> [{best_measure}] (score={best_score})"
                        ),
                        canonical_term=canonical,
                    )

        # 4. Fuzzy direto: cada intent contra cada medida
        overall_best_measure = ""
        overall_best_score = 0
        overall_best_intent = ""

        for intent in intents:
            best_measure, best_score = self._best_fuzzy_against_measures(intent)
            if best_score > overall_best_score:
                overall_best_score = best_score
                overall_best_measure = best_measure
                overall_best_intent = intent

        if overall_best_score >= FUZZY_HIGH_CONFIDENCE:
            return MeasureMatch(
                measure_name=overall_best_measure,
                confidence=overall_best_score / 100,
                method="fuzzy",
                reasoning=(
                    f"Fuzzy match direto: '{overall_best_intent}' "
                    f"-> [{overall_best_measure}] (score={overall_best_score})"
                ),
            )
        elif overall_best_score >= FUZZY_LOW_CONFIDENCE:
            # Match parcial — confianca media, LLM pode confirmar
            return MeasureMatch(
                measure_name=overall_best_measure,
                confidence=overall_best_score / 100,
                method="fuzzy",
                reasoning=(
                    f"Fuzzy match parcial: '{overall_best_intent}' "
                    f"-> [{overall_best_measure}] (score={overall_best_score}). "
                    f"Confianca media — LLM fallback recomendado."
                ),
                alternatives=self._get_alternatives(overall_best_intent, exclude=overall_best_measure),
            )
        else:
            return MeasureMatch(
                measure_name=overall_best_measure if overall_best_score > 30 else "",
                confidence=overall_best_score / 100,
                method="fuzzy",
                reasoning=(
                    f"Fuzzy match fraco: melhor candidato '{overall_best_intent}' "
                    f"-> [{overall_best_measure}] (score={overall_best_score}). "
                    f"LLM fallback necessario."
                ),
                alternatives=self._get_alternatives(overall_best_intent),
            )

    def _best_fuzzy_against_measures(self, query_term: str) -> Tuple[str, int]:
        """Retorna (nome_medida, score) do melhor fuzzy match."""
        best_measure = ""
        best_score = 0

        query_norm = _normalize(query_term)

        for original_name, normalized_name in self._normalized_measures.items():
            score = _best_fuzzy_score(query_norm, normalized_name)
            if score > best_score:
                best_score = score
                best_measure = original_name

        return best_measure, best_score

    def _get_alternatives(self, intent: str, exclude: str = "", top_n: int = 3) -> List[str]:
        """Retorna top-N medidas alternativas."""
        query_norm = _normalize(intent)
        scored = []
        for original_name, normalized_name in self._normalized_measures.items():
            if original_name == exclude:
                continue
            score = _best_fuzzy_score(query_norm, normalized_name)
            scored.append((original_name, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in scored[:top_n]]

    # ── Matching assincrono com LLM fallback ─────────────────────

    async def match_with_llm_fallback(
        self,
        question: str,
        llm: Any,
        confidence_threshold: float = 0.75,
    ) -> MeasureMatch:
        """
        Match hibrido: tenta fuzzy+sinonimos primeiro, LLM se insuficiente.

        Args:
            question: Pergunta do usuario
            llm: Instancia do LLM (ChatAnthropic ou similar com .ainvoke)
            confidence_threshold: Abaixo disso, usa LLM
        """
        # Tenta fuzzy primeiro
        result = self.match(question)

        if result.confidence >= confidence_threshold:
            logger.info(
                f"MeasureMatcher: fuzzy/synonym resolveu — "
                f"[{result.measure_name}] conf={result.confidence:.2f} method={result.method}"
            )
            return result

        # Fallback: LLM
        logger.info(
            f"MeasureMatcher: fuzzy insuficiente (conf={result.confidence:.2f}), "
            f"usando LLM fallback"
        )
        return await self._llm_match(question, llm, fuzzy_hint=result)

    async def _llm_match(
        self,
        question: str,
        llm: Any,
        fuzzy_hint: Optional[MeasureMatch] = None,
    ) -> MeasureMatch:
        """Usa o LLM para identificar a medida correta."""
        from langchain_core.messages import SystemMessage, HumanMessage
        import json

        measures_list = "\n".join(f"- [{m}]" for m in self.measures)

        hint_text = ""
        if fuzzy_hint and fuzzy_hint.measure_name:
            alts = ", ".join(f"[{a}]" for a in fuzzy_hint.alternatives[:3])
            hint_text = (
                f"\nDica do fuzzy match: melhor candidato [{fuzzy_hint.measure_name}] "
                f"(confianca {fuzzy_hint.confidence:.0%}). "
                f"Alternativas: {alts}"
            )

        system = (
            "Voce e um especialista em Power BI e financas empresariais. "
            "Sua tarefa e identificar qual medida do catalogo corresponde "
            "a pergunta do usuario. Responda APENAS com JSON valido."
        )

        prompt = f"""Catalogo de medidas disponiveis no Power BI:
{measures_list}

Pergunta do usuario: "{question}"
{hint_text}

Identifique qual medida do catalogo o usuario esta pedindo.
Se NENHUMA medida corresponde, retorne measure_name vazio.

Responda APENAS com este JSON (sem markdown):
{{
    "measure_name": "Nome exato da medida do catalogo (ou vazio)",
    "confidence": 0.0 a 1.0,
    "reasoning": "Explicacao curta"
}}"""

        try:
            response = await llm.ainvoke([
                SystemMessage(content=system),
                HumanMessage(content=prompt),
            ])

            content = response.content
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                data = json.loads(content[json_start:json_end])
            else:
                raise ValueError("No JSON in LLM response")

            measure_name = data.get("measure_name", "")
            # Valida que a medida existe no catalogo
            if measure_name and measure_name not in self.measures:
                # Tenta match case-insensitive
                for m in self.measures:
                    if m.lower() == measure_name.lower():
                        measure_name = m
                        break
                else:
                    logger.warning(
                        f"LLM retornou medida '{measure_name}' que nao existe no catalogo"
                    )
                    measure_name = ""

            return MeasureMatch(
                measure_name=measure_name,
                confidence=float(data.get("confidence", 0.5)),
                method="llm",
                reasoning=data.get("reasoning", "Classificado pelo LLM"),
            )

        except Exception as e:
            logger.error(f"LLM fallback failed: {e}")
            # Retorna o melhor resultado do fuzzy como ultimo recurso
            if fuzzy_hint and fuzzy_hint.measure_name:
                return MeasureMatch(
                    measure_name=fuzzy_hint.measure_name,
                    confidence=fuzzy_hint.confidence * 0.8,  # Penaliza por falha LLM
                    method="fuzzy",
                    reasoning=f"LLM fallback falhou ({e}). Usando fuzzy: {fuzzy_hint.reasoning}",
                    alternatives=fuzzy_hint.alternatives,
                )
            return MeasureMatch(
                measure_name="",
                confidence=0.0,
                method="none",
                reasoning=f"Todas as estrategias falharam. Erro LLM: {e}",
            )

    # ── Metodo de conveniencia ───────────────────────────────────

    def needs_llm_fallback(self, question: str, threshold: float = 0.75) -> bool:
        """Verifica se a pergunta precisa de LLM fallback (util para decisao no orchestrator)."""
        result = self.match(question)
        return result.confidence < threshold
