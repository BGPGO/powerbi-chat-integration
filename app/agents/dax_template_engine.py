"""
Templated DAX Generation Engine — Geração determinística de DAX sem LLM.

Elimina alucinações (data[valor], colunas inexistentes, parênteses desbalanceados)
ao usar templates pré-validados com placeholders tipados.

Cobertura alvo: 95%+ das perguntas KPI do usuário.

Arquitetura:
    pergunta → MeasureMatcher → PatternDetector → DaxTemplateEngine → DAX confiável
                   ↓                  ↓                    ↓
              measure name      pattern + params      string determinística
"""

import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. ENUMS E TIPOS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class QueryPattern(Enum):
    """Taxonomia completa de padrões de query DAX."""
    KPI_SIMPLE = auto()           # "Qual o CAPEX de 2025?"
    KPI_BY_DIMENSION = auto()     # "CAPEX por departamento em 2025"
    KPI_COMPARISON = auto()       # "Compare CAPEX 2024 vs 2025"
    KPI_TOP_N = auto()            # "Top 5 departamentos por CAPEX"
    KPI_TREND = auto()            # "CAPEX mês a mês em 2025"
    KPI_MULTI_METRIC = auto()     # "Receita e despesa por mês em 2025"
    KPI_ROLLING = auto()          # "receita da última semana"
    KPI_ROLLING_AVERAGE = auto()  # "média de receita das últimas 3 semanas"
    KPI_MONTHLY_AVERAGE = auto()  # "média mensal de receita desse ano"
    UNKNOWN = auto()              # Fallback para LLM


@dataclass
class DimensionMapping:
    """Mapeamento de termo pt-BR para coluna(s) DAX."""
    keywords: List[str]
    dax_columns: List[str]  # ex: ["'Departamentos'[Centro de Custo]"]
    display_name: str


@dataclass
class TemporalFilter:
    """Filtro temporal extraído da pergunta."""
    year: Optional[str] = None
    month: Optional[str] = None
    year2: Optional[str] = None   # Para comparações (vs)
    month2: Optional[str] = None
    rolling_days: Optional[int] = None                          # ex: 7, 14, 21
    rolling_is_average: bool = False
    rolling_date_col: str = "'data'[DataPagamento]"             # ou 'data'[dDtVenc]
    rolling_status_filter: bool = True
    rolling_is_forward: bool = False  # True quando janela e futura (proximos N dias)
    is_all_time: bool = False  # True quando pergunta nao especifica periodo ou pede "todo o periodo"


@dataclass
class StatusFilter:
    """Filtro de status/regime contábil."""
    statuses: Optional[List[str]] = None  # ex: ["PAGO", "RECEBIDO"]
    regime: Optional[str] = None          # "caixa" ou "competencia"


@dataclass
class ParsedQuery:
    """Resultado completo da análise de uma pergunta."""
    pattern: QueryPattern
    measure_name: Optional[str] = None
    measure_expression: Optional[str] = None  # Para medidas inline (SUM(...))
    measure_label: Optional[str] = None
    temporal: TemporalFilter = field(default_factory=TemporalFilter)
    dimensions: List[DimensionMapping] = field(default_factory=list)
    top_n: Optional[int] = None
    status_filter: Optional[StatusFilter] = None
    extra_measures: List[Tuple[str, str]] = field(default_factory=list)  # [(label, expr)]
    order_by: Optional[str] = None
    order_desc: bool = True
    original_question: str = ""
    hard_filters: List[str] = field(default_factory=list)  # Filtros hard do relatório PBI


@dataclass
class TemplateResult:
    """Resultado da renderização de template."""
    dax_query: str
    explanation: str
    label: str
    pattern: QueryPattern
    confidence: float = 1.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. CONSTANTES DO MODELO
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_DEFAULT_YEAR = "2026"

_ALL_TIME_RE = re.compile(
    r'\b(todo\s+o\s+per[ií]odo|todo\s+per[ií]odo|todos\s+os\s+tempos|'
    r'hist[oó]rico\s+completo|hist[oó]rico\s+total|desde\s+o\s+in[ií]cio|'
    r'desde\s+sempre|de\s+todos\s+os\s+anos|total\s+geral|'
    r'sem\s+filtro\s+de\s+per[ií]odo|todos\s+os\s+per[ií]odos|'
    r'acumulado\s+total|todo\s+o\s+hist[oó]rico)\b',
    re.IGNORECASE,
)

_HAS_TEMPORAL_HINT = re.compile(
    r'\b(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|'
    r'janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro|'
    r'esse\s+ano|este\s+ano|ano\s+passado|ano\s+atual|mes\s+passado|esse\s+mes|'
    r'ultimo|[uú]ltim[ao]|semana|dias?|semestre|trimestre|'
    r'20[2-3]\d|ytd|mtd|acumulado)\b',
    re.IGNORECASE,
)

# Mapeamento meses pt-BR → nome oficial (inicial maiúscula, com acento)
MONTH_MAP: Dict[str, str] = {
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

# Mapeamento de dimensões pt-BR → colunas DAX
DIMENSION_REGISTRY: List[DimensionMapping] = [
    DimensionMapping(
        keywords=[
            "departamento", "departamentos", "centro de custo", "centros de custo",
            "setor", "setores", "area", "área", "areas", "áreas",
            "time", "times", "equipe", "equipes", "unidade", "unidades",
        ],
        dax_columns=["'Departamentos'[Centro de Custo]"],
        display_name="Departamento",
    ),
    DimensionMapping(
        keywords=[
            "categoria", "categorias", "tipo de despesa", "tipo de receita",
            "tipo", "tipos", "natureza", "classificação", "classificacao",
        ],
        dax_columns=["'Categorias'[Categorias]"],
        display_name="Categoria",
    ),
    DimensionMapping(
        keywords=["grupo", "grupo de categoria", "grupos"],
        dax_columns=["'Categorias'[Grupo]"],
        display_name="Grupo",
    ),
    DimensionMapping(
        keywords=[
            "cliente", "clientes", "fornecedor", "fornecedores",
            "empresa", "empresas", "parceiro", "parceiros",
            "quem me paga", "quem me pagou", "quem paga mais",
        ],
        dax_columns=["'Clientes'[razao_social]"],
        display_name="Cliente",
    ),
    DimensionMapping(
        keywords=[
            "conta", "contas", "banco", "bancos",
            "conta corrente", "conta bancária", "conta bancaria",
            "qual banco", "em qual conta", "em que banco",
        ],
        dax_columns=["'Conta Corrente'[descricao]"],
        display_name="Conta",
    ),
    DimensionMapping(
        keywords=["dre", "natureza dre", "linha dre"],
        dax_columns=["'dre'[Natureza]"],
        display_name="DRE",
    ),
    DimensionMapping(
        keywords=["status", "situação", "situacao"],
        dax_columns=["'data'[cStatus]"],
        display_name="Status",
    ),
    DimensionMapping(
        keywords=[
            "mês", "mes", "mensal", "mês a mês", "mes a mes", "por mês", "por mes",
            "evolução mensal", "evolucao mensal", "tendência", "tendencia",
            "ao longo do ano", "ao longo dos meses", "como foi cada mês",
            "todo mês", "cada mês", "cada mes",
        ],
        dax_columns=["'data'[Nome mês]", "'data'[Ano ]"],
        display_name="Mês",
    ),
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. MEASURE MATCHER — Resolve medida a partir da pergunta
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class MeasureDefinition:
    """Uma medida conhecida do modelo."""
    name: str                           # Nome no Power BI: "EBITDA"
    measure_type: str                   # "native" | "custom" | "inline"
    expression: Optional[str] = None    # DAX expression (para inline/custom)
    aliases: List[str] = field(default_factory=list)
    description: str = ""


# Medidas inline (derivadas das colunas calculadas do modelo Omie)
_BUILTIN_MEASURES: List[MeasureDefinition] = [
    MeasureDefinition(
        name="Receita",
        measure_type="inline",
        expression="SUM('data'[receita])",
        aliases=[
            # Substantivos formais
            "faturamento", "receita", "receitas", "receita total", "receita bruta",
            "total de receita", "total de receitas",
            # Verbos / formas coloquiais CEO
            "faturamos", "faturei", "faturou",
            "recebi", "recebemos", "recebeu",
            "o que entrou", "o que entramos", "quanto entrou", "quanto entramos",
            "quanto ganhamos", "quanto ganhei", "o que ganhamos",
            "quanto vendemos", "quanto vendeu", "vendemos",
            "o que faturamos", "o que faturou",
            "entrou na conta", "caiu na conta", "entradas", "recebimento", "recebimentos",
        ],
        description="Receita total (com rateio aplicado)",
    ),
    MeasureDefinition(
        name="Despesa",
        measure_type="inline",
        expression="SUM('data'[despesas])",
        aliases=[
            # Substantivos formais
            "despesa", "despesas", "custos", "custo", "gastos", "gasto",
            "pagamentos", "pagamento", "saídas", "saidas", "despesa total",
            # Verbos / formas coloquiais CEO
            "gastamos", "gastei", "gastou",
            "pagamos", "pagou", "pagamos",
            "o que saiu", "quanto saiu", "o que gastamos", "quanto gastamos",
            "o que pagamos", "quanto pagamos",
            "o que desembolsamos", "desembolsamos",
            "estou gastando", "estamos gastando", "tô gastando",
        ],
        description="Despesa total (com duplo rateio aplicado)",
    ),
    MeasureDefinition(
        name="Resultado",
        measure_type="inline",
        expression="SUM('data'[receita]) - SUM('data'[despesas])",
        aliases=[
            # Substantivos formais
            "resultado", "saldo", "lucro", "prejuízo", "prejuizo",
            "valor líquido", "valor liquido", "líquido", "liquido",
            "superávit", "superavit", "déficit", "deficit",
            "resultado financeiro", "resultado final", "saldo final",
            # Verbos / formas coloquiais CEO
            "sobrou", "o que sobrou", "quanto sobrou",
            "ficou", "o que ficou", "quanto ficou",
            "como fechamos", "fechamos", "como foi o resultado",
            "ganhamos ou perdemos", "tô no lucro", "tô no prejuízo",
            "o que restou", "quanto restou",
        ],
        description="Resultado financeiro (receita - despesa)",
    ),
]


class MeasureMatcher:
    """
    Resolve uma pergunta em linguagem natural para uma MeasureDefinition.

    Fontes de medidas (em ordem de prioridade):
    1. measures.json (nativas do Power BI e customizadas pelo cliente)
    2. Medidas inline builtin (receita, despesa, resultado)
    """

    def __init__(self, measures_file: Optional[Path] = None):
        self._measures: List[MeasureDefinition] = []
        self._load_measures(measures_file)

    def _load_measures(self, measures_file: Optional[Path] = None) -> None:
        """Carrega medidas do measures.json + builtins."""
        # 1. Medidas do measures.json (nativas e custom)
        path = measures_file or Path(__file__).parent.parent.parent / "measures.json"
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                for m in data.get("measures", []):
                    self._measures.append(MeasureDefinition(
                        name=m["name"],
                        measure_type=m.get("type", "native"),
                        expression=m.get("dax"),
                        aliases=[a.lower() for a in m.get("aliases", [])],
                        description=m.get("description", ""),
                    ))
                logger.info("MeasureMatcher: carregadas %d medidas de measures.json", len(self._measures))
            except (json.JSONDecodeError, OSError) as exc:
                logger.error("MeasureMatcher: erro ao carregar measures.json: %s", exc)

        # 2. Medidas inline builtin (sempre presentes)
        self._measures.extend(_BUILTIN_MEASURES)

    def match(self, question: str) -> Optional[MeasureDefinition]:
        """
        Retorna a medida que melhor corresponde à pergunta.

        Prioridade: medidas do measures.json primeiro (nativas/custom),
        depois inline builtins. Dentro de cada grupo, aliases mais longos
        são preferidos (match mais específico).
        """
        q_lower = question.lower()
        # Remove acentos comuns para matching flexível
        q_norm = self._normalize(q_lower)

        best_match: Optional[MeasureDefinition] = None
        best_len = 0
        best_priority = 999  # menor = melhor

        for measure in self._measures:
            priority = 0 if measure.measure_type in ("native", "custom") else 1

            # Testa o nome da medida
            candidates = [measure.name.lower()] + measure.aliases
            for alias in candidates:
                alias_norm = self._normalize(alias)
                if alias_norm in q_norm:
                    # Prefere match mais longo e maior prioridade
                    if (priority < best_priority) or \
                       (priority == best_priority and len(alias_norm) > best_len):
                        best_match = measure
                        best_len = len(alias_norm)
                        best_priority = priority

        return best_match

    @staticmethod
    def _normalize(text: str) -> str:
        """Normaliza texto removendo acentos comuns."""
        replacements = {
            'ç': 'c', 'ã': 'a', 'á': 'a', 'â': 'a', 'à': 'a',
            'é': 'e', 'ê': 'e', 'í': 'i', 'ó': 'o', 'ô': 'o',
            'õ': 'o', 'ú': 'u', 'ü': 'u',
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text

    def get_all_measure_names(self) -> List[str]:
        """Retorna todos os nomes de medidas conhecidos."""
        return [m.name for m in self._measures]

    def reload(self, measures_file: Optional[Path] = None) -> None:
        """Recarrega medidas do measures.json."""
        self._measures.clear()
        self._load_measures(measures_file)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. PATTERN DETECTOR — Classifica o padrão da pergunta
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class PatternDetector:
    """
    Analisa uma pergunta em linguagem natural e extrai:
    - Padrão (QueryPattern)
    - Filtros temporais (ano, mês)
    - Dimensões de agrupamento
    - Top N
    - Filtro de status/regime
    """

    # Regex patterns
    _YEAR_RE = re.compile(r'\b(20[2-3]\d)\b')
    _TOP_N_RE = re.compile(r'\btop\s*(\d+)\b', re.IGNORECASE)
    _MAIORES_N_RE = re.compile(
        r'\b(?:(?:top|maiores?|principais?|melhores?)\s+(\d+)|(\d+)\s+(?:maiores?|principais?|melhores?))\b',
        re.IGNORECASE,
    )
    _VS_RE = re.compile(
        r'\b(?:compar[ae]|versus|vs\.?|contra|x)\b.*\b(20[2-3]\d)\b.*\b(20[2-3]\d)\b',
        re.IGNORECASE
    )
    _VS_SIMPLE_RE = re.compile(
        r'\b(20[2-3]\d)\b.*\b(?:vs\.?|versus|x|contra)\b.*\b(20[2-3]\d)\b',
        re.IGNORECASE
    )
    _TREND_RE = re.compile(
        r'\b(mês\s+a\s+mês|mes\s+a\s+mes|mensal|evolução|evolucao|tendência|tendencia|'
        r'por\s+mês|por\s+mes|ao\s+longo\s+dos?\s+meses?|ao\s+longo\s+do\s+ano|'
        r'cada\s+mês|cada\s+mes|todo\s+(?:mês|mes)|como\s+foi\s+cada\s+mês|'
        r'mês\s+a\s+mês|progressão|progressao)\b',
        re.IGNORECASE,
    )
    _CAIXA_RE = re.compile(
        r'\b(caixa|pago|pagos?|recebido|recebidos?|efetivo|efetivamente|realizado)\b',
        re.IGNORECASE,
    )
    _ABERTO_RE = re.compile(
        r'\b(aberto|a\s+vencer|atrasado|pendente|em\s+aberto|a\s+receber|a\s+pagar)\b',
        re.IGNORECASE,
    )
    _MULTI_METRIC_RE = re.compile(
        r'\b(receita\s+e\s+despesa|despesa\s+e\s+receita|receitas?\s+e\s+despesas?|'
        r'faturamento\s+e\s+(?:despesa|custo|gasto)|'
        r'o\s+que\s+entrou\s+e\s+(?:saiu|gastamos)|'
        r'quanto\s+(?:entrou|ganhamos)\s+e\s+(?:saiu|gastamos)|'
        r'recebi\s+e\s+gastei|ganhamos\s+e\s+gastamos)\b',
        re.IGNORECASE,
    )
    _MONTHLY_AVG_RE = re.compile(
        r'\b(m[eé]dia\s+mensal|m[eé]dia\s+por\s+m[eê]s|m[eé]dia\s+ao\s+m[eê]s|'
        r'por\s+m[eê]s\s+em\s+m[eé]dia|m[eé]dia\s+de\s+cada\s+m[eê]s|'
        r'quanto\s+(?:recebo|faturamos|gastamos|saiu|entrou)\s+por\s+m[eê]s\s+em\s+m[eé]dia)\b',
        re.IGNORECASE,
    )
    _FORWARD_ROLLING_RE = re.compile(
        r'\b(pr[oó]ximos?\s+(\d+)\s+dias?|'
        r'pr[oó]ximas?\s+(\d+)\s+semanas?|'
        r'pr[oó]ximos?\s+(\d+)\s+meses?|'
        r'pr[oó]xima\s+semana|'
        r'pr[oó]ximo\s+m[eê]s|'
        r'nos?\s+pr[oó]ximos?\s+(\d+)\s+dias?|'
        r'nos?\s+pr[oó]ximas?\s+(\d+)\s+semanas?)\b',
        re.IGNORECASE,
    )

    def detect(self, question: str, measure: Optional[MeasureDefinition] = None) -> ParsedQuery:
        """Analisa a pergunta e retorna ParsedQuery com todos os parâmetros extraídos."""

        parsed = ParsedQuery(
            pattern=QueryPattern.UNKNOWN,
            original_question=question,
        )

        # Preenche a medida
        if measure:
            if measure.measure_type == "native":
                parsed.measure_name = f"[{measure.name}]"
                parsed.measure_label = measure.name
            elif measure.measure_type == "custom" and measure.expression:
                parsed.measure_expression = measure.expression
                parsed.measure_label = measure.name
            else:  # inline
                parsed.measure_expression = measure.expression
                parsed.measure_label = measure.name

        # Extrai filtros temporais
        parsed.temporal = self._extract_temporal(question)

        # Extrai filtro de status/regime
        parsed.status_filter = self._extract_status_filter(question)

        # Detecta multi-metric (receita e despesa)
        is_multi = bool(self._MULTI_METRIC_RE.search(question))
        if is_multi:
            parsed.extra_measures = [
                ("Receita", "SUM('data'[receita])"),
                ("Despesa", "SUM('data'[despesas])"),
                ("Resultado", "SUM('data'[receita]) - SUM('data'[despesas])"),
            ]
            parsed.measure_label = "Receita e Despesa"

        # Detecta padrão (ordem importa: mais específico primeiro)

        # 0. Janela rolling (KPI_ROLLING / KPI_ROLLING_AVERAGE)
        if parsed.temporal.rolling_days is not None:
            # Verifica se a pergunta tambem e multi-metric ("entrou e saiu essa semana")
            is_multi_rolling = bool(self._MULTI_METRIC_RE.search(question))
            if is_multi_rolling:
                parsed.extra_measures = [
                    ("Receita", "SUM('data'[receita])"),
                    ("Despesa", "SUM('data'[despesas])"),
                    ("Resultado", "SUM('data'[receita]) - SUM('data'[despesas])"),
                ]
                parsed.measure_label = "Receita e Despesa"
            if parsed.temporal.rolling_is_average:
                parsed.pattern = QueryPattern.KPI_ROLLING_AVERAGE
            else:
                parsed.pattern = QueryPattern.KPI_ROLLING
            return parsed

        # 1. Comparação temporal (vs)
        if self._detect_comparison(question):
            parsed.pattern = QueryPattern.KPI_COMPARISON
            years = self._extract_comparison_years(question)
            if years:
                parsed.temporal.year = years[0]
                parsed.temporal.year2 = years[1]
            return parsed

        # 2. Top N
        top_n = self._extract_top_n(question)
        if top_n:
            parsed.top_n = top_n
            parsed.pattern = QueryPattern.KPI_TOP_N
            dims = self._extract_dimensions(question)
            if not dims:
                dims = self._extract_dimensions_bare(question)
            parsed.dimensions = dims if dims else [DIMENSION_REGISTRY[0]]  # default: departamento
            # "pior" ou "menor" → ordenação ASC (menor valor primeiro)
            if re.search(r'\b(pior|menor|mais\s+baixo|menos)\b', question, re.IGNORECASE):
                parsed.order_desc = False
            return parsed

        # 3. Média mensal (detectar ANTES de tendência para evitar captura pelo _TREND_RE)
        if self._MONTHLY_AVG_RE.search(question):
            # Não faz sentido "média mensal" de um mês específico → fallback para KPI_SIMPLE
            if parsed.temporal.month:
                parsed.pattern = QueryPattern.KPI_SIMPLE
            elif measure:
                parsed.pattern = QueryPattern.KPI_MONTHLY_AVERAGE
            return parsed

        # 4. Tendência mensal (explícita)
        if self._TREND_RE.search(question):
            parsed.pattern = QueryPattern.KPI_TREND
            return parsed

        # 5. Por dimensão
        dims = self._extract_dimensions(question)
        if dims:
            # Se a dimensão é "mês" → padrão de tendência
            if dims[0].display_name == "Mês":
                parsed.pattern = QueryPattern.KPI_TREND
            elif is_multi:
                parsed.pattern = QueryPattern.KPI_MULTI_METRIC
                parsed.dimensions = dims
            else:
                parsed.pattern = QueryPattern.KPI_BY_DIMENSION
                parsed.dimensions = dims
            return parsed

        # 6. Multi-metric sem dimensão (ex: "receita e despesa em 2025")
        if is_multi:
            parsed.pattern = QueryPattern.KPI_SIMPLE
            # Sobrescreve a medida para gerar ROW com múltiplas colunas
            return parsed

        # 7. KPI simples (valor único)
        if measure:
            parsed.pattern = QueryPattern.KPI_SIMPLE
            return parsed

        # 8. Não reconhecido
        parsed.pattern = QueryPattern.UNKNOWN
        return parsed

    def _detect_rolling(self, text: str) -> Optional[dict]:
        """
        Detecta janelas rolling na pergunta.

        Retorna dict com chaves:
            - days (int): número de dias da janela
            - is_average (bool): True se for média
            - date_col (str): coluna de data a usar
            - status_filter (bool): True se aplicar filtro de status PAGO/RECEBIDO
        Ou None se nenhuma janela rolling for detectada.
        """
        t = text.lower()

        # Detecta se é competência/vencimento (sem filtro de status)
        is_competencia = bool(re.search(
            r'\b(vencimento|venc\.?|competencia|competência|a\s+vencer|ddtvenc)\b', t
        ))
        date_col = "'data'[dDtVenc]" if is_competencia else "'data'[DataPagamento]"
        status_filter = not is_competencia

        # Detecta se é média
        is_average = bool(re.search(r'\b(media|média|médio|medio|average)\b', t))

        days: Optional[int] = None

        # "última semana" / "semana passada" / "essa semana" / "esta semana" (sem número → 7 dias)
        if re.search(r'\b(ultima\s+semana|última\s+semana|semana\s+passada|essa\s+semana|esta\s+semana|nessa\s+semana|nesta\s+semana)\b', t):
            days = 7

        # "últimas N semanas"
        if days is None:
            m = re.search(r'\b(?:ultimas?|últimas?)\s+(\d+)\s+semanas?\b', t)
            if m:
                days = int(m.group(1)) * 7

        # "últimos N dias"
        if days is None:
            m = re.search(r'\b(?:ultimos?|últimos?)\s+(\d+)\s+dias?\b', t)
            if m:
                days = int(m.group(1))

        # "último mês" / "mês passado" — apenas quando combinado com média
        if days is None:
            if re.search(r'\b(ultimo\s+mes|último\s+mês|mes\s+passado|mês\s+passado)\b', t) and is_average:
                days = 30

        # "últimos N meses"
        if days is None:
            m = re.search(r'\b(?:ultimos?|últimos?)\s+(\d+)\s+m[eê]ses?\b', t)
            if m:
                days = int(m.group(1)) * 30

        if days is None:
            # ── Janelas FUTURAS (proximos N dias/semanas/meses) ──────────────
            forward_days: Optional[int] = None
            forward_col = "'data'[dDtVenc]"

            # "proximos N dias" / "nos proximos N dias"
            m = re.search(r'\b(?:nos?\s+)?pr[oó]ximos?\s+(\d+)\s+dias?\b', t)
            if m:
                forward_days = int(m.group(1))

            # "proximas N semanas" / "nas proximas N semanas"
            if forward_days is None:
                m = re.search(r'\b(?:nas?\s+)?pr[oó]ximas?\s+(\d+)\s+semanas?\b', t)
                if m:
                    forward_days = int(m.group(1)) * 7

            # "proximos N meses"
            if forward_days is None:
                m = re.search(r'\b(?:nos?\s+)?pr[oó]ximos?\s+(\d+)\s+m[eê]ses?\b', t)
                if m:
                    forward_days = int(m.group(1)) * 30

            # "proxima semana" (singular)
            if forward_days is None and re.search(r'\bpr[oó]xima\s+semana\b', t):
                forward_days = 7

            # "proximo mes" (singular)
            if forward_days is None and re.search(r'\bpr[oó]ximo\s+m[eê]s\b', t):
                forward_days = 30

            if forward_days is not None:
                return {
                    "days": forward_days,
                    "is_average": is_average,
                    "date_col": forward_col,
                    "status_filter": False,
                    "is_forward": True,
                }

            return None

        return {
            "days": days,
            "is_average": is_average,
            "date_col": date_col,
            "status_filter": status_filter,
            "is_forward": False,
        }

    def _extract_temporal(self, question: str) -> TemporalFilter:
        """Extrai ano e mês da pergunta, incluindo expressões relativas."""
        import datetime
        tf = TemporalFilter()
        current_year = datetime.datetime.now().year
        q_lower = question.lower()

        # Detecção de janelas rolling (verificar ANTES do ano/mês)
        rolling = self._detect_rolling(q_lower)
        if rolling:
            tf.rolling_days = rolling["days"]
            tf.rolling_is_average = rolling.get("is_average", False)
            tf.rolling_date_col = rolling.get("date_col", "'data'[DataPagamento]")
            tf.rolling_status_filter = rolling.get("status_filter", True)
            tf.rolling_is_forward = rolling.get("is_forward", False)
            tf.year = None  # rolling não usa filtro de ano
            return tf

        # Detecção de "todo o período" — ANTES de qualquer outra extração
        if _ALL_TIME_RE.search(question):
            tf.is_all_time = True
            tf.year = None
            return tf

        # Ano — expressões relativas (verificar ANTES do regex de 4 dígitos)
        if re.search(r'\b(ano\s+passado|ano\s+anterior|ultimo\s+ano|últim[oa]\s+ano)\b', q_lower):
            tf.year = str(current_year - 1)
        elif re.search(r'\b(esse\s+ano|este\s+ano|desse\s+ano|deste\s+ano|ano\s+atual|ano\s+corrente|nesse\s+ano)\b', q_lower):
            tf.year = str(current_year)
        elif re.search(r'\b(proximo\s+ano|próximo\s+ano|ano\s+que\s+vem)\b', q_lower):
            tf.year = str(current_year + 1)
        else:
            years = self._YEAR_RE.findall(question)
            if years:
                tf.year = years[0]
            else:
                # Sem ano explícito: verifica se há algum indicador temporal
                # Se não houver, é "todo o período" (pergunta geral)
                if not _HAS_TEMPORAL_HINT.search(question):
                    tf.is_all_time = True
                    tf.year = None
                    return tf
                tf.year = _DEFAULT_YEAR

        # Mês — expressões relativas
        now = datetime.datetime.now()
        if re.search(r'\b(mes\s+passado|mês\s+passado|mes\s+anterior|mês\s+anterior|ultimo\s+mes|último\s+mês)\b', q_lower):
            prev = now.month - 1 if now.month > 1 else 12
            month_names = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                           "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
            tf.month = month_names[prev - 1]
        elif re.search(r'\b(esse\s+mes|este\s+mes|esse\s+mês|este\s+mês|mes\s+atual|mês\s+atual)\b', q_lower):
            month_names = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                           "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
            tf.month = month_names[now.month - 1]
        else:
            # Normaliza acentos para lookup de meses pelo nome
            q_norm = q_lower.replace('ç','c').replace('ã','a').replace('é','e').replace('ê','e')
            for key in sorted(MONTH_MAP.keys(), key=len, reverse=True):
                key_norm = key.replace('ç','c').replace('ã','a').replace('é','e').replace('ê','e')
                if key_norm in q_norm:
                    tf.month = MONTH_MAP[key]
                    break

        return tf

    def _extract_comparison_years(self, question: str) -> Optional[Tuple[str, str]]:
        """Extrai dois anos de uma comparação."""
        m = self._VS_RE.search(question)
        if m:
            return (m.group(1), m.group(2))
        m = self._VS_SIMPLE_RE.search(question)
        if m:
            return (m.group(1), m.group(2))
        # Tenta extrair dois anos quaisquer
        years = self._YEAR_RE.findall(question)
        if len(years) >= 2:
            return (years[0], years[1])
        return None

    def _detect_comparison(self, question: str) -> bool:
        """Detecta se a pergunta é uma comparação temporal."""
        q_lower = question.lower()
        comparison_words = [
            "compar", "versus", " vs", " vs.", " x ", "contra",
            "crescimento", "cresceu", "aumentou", "caiu", "reduziu",
            "melhorou", "piorou", "evoluiu", "em relação", "diferença",
        ]
        has_comparison_word = any(w in q_lower for w in comparison_words)
        has_two_years = len(self._YEAR_RE.findall(question)) >= 2
        has_year_passado = bool(re.search(r'\bano\s+passado\b', q_lower)) and bool(self._YEAR_RE.search(question))
        return (has_comparison_word and has_two_years) or has_year_passado

    def _extract_top_n(self, question: str) -> Optional[int]:
        """Extrai N dos padrões 'top N', 'maiores N', 'principais N'."""
        m = self._TOP_N_RE.search(question)
        if m:
            return min(int(m.group(1)), 100)
        m = self._MAIORES_N_RE.search(question)
        if m:
            n = int(m.group(1) or m.group(2))
            return min(n, 100)
        # "melhor X", "maior X", "pior X", "menor X" (singular) → top 1
        if re.search(r'\b(melhor|maior|pior|menor)\b', question, re.IGNORECASE):
            dims = self._extract_dimensions(question)
            if not dims:
                # Tenta match bare de dimensão (sem "por/de cada")
                dims = self._extract_dimensions_bare(question)
            if dims:
                return 1
        # Sem número explícito mas com "maiores/principais" → top 10 padrão
        if re.search(r'\b(maiores?|principais?|melhores?|top)\b', question, re.IGNORECASE):
            # Só aplica se há uma dimensão mencionada (evita falso positivo)
            dims = self._extract_dimensions(question)
            if not dims:
                dims = self._extract_dimensions_bare(question)
            if dims:
                return 10
        return None

    def _extract_dimensions(self, question: str) -> List[DimensionMapping]:
        """Extrai dimensões mencionadas na pergunta."""
        q_lower = question.lower()
        q_norm = MeasureMatcher._normalize(q_lower)

        # Detecta padrão "por [dimensão]", "top N [dimensão]", etc.
        found: List[Tuple[int, DimensionMapping]] = []
        for dim in DIMENSION_REGISTRY:
            for kw in dim.keywords:
                kw_norm = MeasureMatcher._normalize(kw)
                # Verifica se tem "por <dimensão>", "de cada <dimensão>",
                # ou "top N <dimensão>" (ex: "top 5 clientes por receita")
                patterns = [
                    f"por {kw_norm}",
                    f"de cada {kw_norm}",
                    f"cada {kw_norm}",
                    f"entre {kw_norm}",
                    f"dos {kw_norm}",
                    f"das {kw_norm}",
                ]
                for pat in patterns:
                    idx = q_norm.find(pat)
                    if idx >= 0:
                        found.append((idx, dim))
                        break
                else:
                    # Tenta "top N <dimensão>" pattern
                    top_dim_match = re.search(
                        rf'\btop\s*\d+\s+{re.escape(kw_norm)}', q_norm
                    )
                    if top_dim_match:
                        found.append((top_dim_match.start(), dim))
                    else:
                        continue
                break

        # Remove duplicatas mantendo ordem
        seen = set()
        result = []
        for _, dim in sorted(found, key=lambda x: x[0]):
            if dim.display_name not in seen:
                result.append(dim)
                seen.add(dim.display_name)

        return result

    def _extract_dimensions_bare(self, question: str) -> List[DimensionMapping]:
        """
        Versão simplificada de _extract_dimensions que aceita menção bare da dimensão
        (sem exigir "por/de cada/etc."). Usada para "melhor mês", "pior cliente", etc.
        """
        q_lower = question.lower()
        q_norm = MeasureMatcher._normalize(q_lower)
        found: List[Tuple[int, DimensionMapping]] = []
        for dim in DIMENSION_REGISTRY:
            for kw in dim.keywords:
                kw_norm = MeasureMatcher._normalize(kw)
                idx = q_norm.find(kw_norm)
                if idx >= 0:
                    found.append((idx, dim))
                    break
        seen: set = set()
        result: List[DimensionMapping] = []
        for _, dim in sorted(found, key=lambda x: x[0]):
            if dim.display_name not in seen:
                result.append(dim)
                seen.add(dim.display_name)
        return result

    def _extract_status_filter(self, question: str) -> Optional[StatusFilter]:
        """Detecta filtro de regime (caixa vs competência)."""
        if self._CAIXA_RE.search(question):
            return StatusFilter(
                statuses=["PAGO", "RECEBIDO"],
                regime="caixa",
            )
        if self._ABERTO_RE.search(question):
            return StatusFilter(
                statuses=["A VENCER", "ATRASADO", "VENCE HOJE"],
                regime="aberto",
            )
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. DAX TEMPLATE ENGINE — Renderiza DAX determinístico
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class DaxTemplateEngine:
    """
    Renderiza DAX a partir de ParsedQuery usando templates pré-validados.

    Garantias:
    - Parênteses sempre balanceados (validado post-render)
    - Coluna 'Ano ' com espaço no final
    - Meses com inicial maiúscula
    - NUNCA usa data[valor] — usa measure_name ou measure_expression
    - Toda query começa com EVALUATE
    """

    def render(self, parsed: ParsedQuery) -> TemplateResult:
        """Renderiza a query DAX a partir do ParsedQuery."""

        renderer = {
            QueryPattern.KPI_SIMPLE: self._render_kpi_simple,
            QueryPattern.KPI_BY_DIMENSION: self._render_kpi_by_dimension,
            QueryPattern.KPI_COMPARISON: self._render_kpi_comparison,
            QueryPattern.KPI_TOP_N: self._render_kpi_top_n,
            QueryPattern.KPI_TREND: self._render_kpi_trend,
            QueryPattern.KPI_MULTI_METRIC: self._render_kpi_multi_metric,
            QueryPattern.KPI_ROLLING: self._render_kpi_rolling,
            QueryPattern.KPI_ROLLING_AVERAGE: self._render_kpi_rolling_average,
            QueryPattern.KPI_MONTHLY_AVERAGE: self._render_kpi_monthly_average,
        }

        func = renderer.get(parsed.pattern)
        if not func:
            raise ValueError(f"Padrão {parsed.pattern} não suportado por template")

        result = func(parsed)

        # Validação post-render
        self._validate(result.dax_query)

        return result

    # ── Helpers ──────────────────────────────────────────────────

    def _get_measure_expr(self, parsed: ParsedQuery) -> str:
        """Retorna a expressão DAX da medida (nome nativo ou expressão inline)."""
        if parsed.measure_name:
            return parsed.measure_name  # ex: [EBITDA]
        if parsed.measure_expression:
            return parsed.measure_expression  # ex: SUM('data'[receita])
        raise ValueError("Nenhuma medida definida no ParsedQuery")

    def _get_label(self, parsed: ParsedQuery) -> str:
        """Retorna o label descritivo da medida."""
        return parsed.measure_label or "Valor"

    def _build_calculate_filters(self, parsed: ParsedQuery) -> List[str]:
        """Monta a lista de filtros para CALCULATE."""
        filters = []
        # Rolling usa seus próprios filtros de data — não adicionar ano/mês normais
        if parsed.temporal.rolling_days is not None:
            return filters
        # is_all_time: não adicionar filtro de ano
        if parsed.temporal.is_all_time:
            if parsed.status_filter and parsed.status_filter.statuses:
                vals = ", ".join(f'"{s}"' for s in parsed.status_filter.statuses)
                filters.append(f"'data'[cStatus] IN {{{vals}}}")
            return filters
        if parsed.temporal.year:
            filters.append(f"'data'[Ano ] = \"{parsed.temporal.year}\"")
        if parsed.temporal.month:
            filters.append(f"'data'[Nome mês] = \"{parsed.temporal.month}\"")
        if parsed.status_filter and parsed.status_filter.statuses:
            vals = ", ".join(f'"{s}"' for s in parsed.status_filter.statuses)
            filters.append(f"'data'[cStatus] IN {{{vals}}}")
        # Aplica filtros hard-coded do relatório Power BI
        filters.extend(parsed.hard_filters)
        return filters

    def _build_filter_block(self, parsed: ParsedQuery) -> str:
        """Monta FILTER(ALL('data'), ...) para SUMMARIZECOLUMNS."""
        parts = []
        # Rolling usa seus próprios filtros de data — não adicionar ano/mês normais
        if parsed.temporal.rolling_days is not None:
            return ""
        # is_all_time: não adicionar filtro de ano
        if parsed.temporal.is_all_time:
            if parsed.status_filter and parsed.status_filter.statuses:
                vals = ", ".join(f'"{s}"' for s in parsed.status_filter.statuses)
                return f"FILTER(ALL('data'), 'data'[cStatus] IN {{{vals}}})"
            return ""
        if parsed.temporal.year:
            parts.append(f"'data'[Ano ] = \"{parsed.temporal.year}\"")
        if parsed.temporal.month:
            parts.append(f"'data'[Nome mês] = \"{parsed.temporal.month}\"")
        if parsed.status_filter and parsed.status_filter.statuses:
            vals = ", ".join(f'"{s}"' for s in parsed.status_filter.statuses)
            parts.append(f"'data'[cStatus] IN {{{vals}}}")
        # Aplica filtros hard-coded do relatório Power BI
        parts.extend(parsed.hard_filters)
        if not parts:
            return ""
        condition = " && ".join(parts)
        return f"FILTER(ALL('data'), {condition})"

    def _build_filter_block_for_year(self, year: str, status_filter: Optional[StatusFilter] = None) -> str:
        """Monta FILTER(ALL('data'), ...) para um ano específico."""
        parts = [f"'data'[Ano ] = \"{year}\""]
        if status_filter and status_filter.statuses:
            vals = ", ".join(f'"{s}"' for s in status_filter.statuses)
            parts.append(f"'data'[cStatus] IN {{{vals}}}")
        condition = " && ".join(parts)
        return f"FILTER(ALL('data'), {condition})"

    # ── Template Renderers ───────────────────────────────────────

    def _render_kpi_simple(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 1: KPI simples com filtro temporal.
        "Qual o CAPEX de 2025?" → EVALUATE ROW("CAPEX", CALCULATE([CAPEX], ...))
        """
        # Caso multi-metric inline (receita e despesa como valor único)
        if parsed.extra_measures:
            filters = self._build_calculate_filters(parsed)
            filter_str = ", ".join(filters)
            rows = []
            for label, expr in parsed.extra_measures:
                if filter_str:
                    rows.append(f'    "{label}", CALCULATE({expr}, {filter_str})')
                else:
                    rows.append(f'    "{label}", {expr}')
            rows_str = ",\n".join(rows)
            period = self._period_label(parsed.temporal)
            dax = f"EVALUATE\nROW(\n{rows_str}\n)"
            return TemplateResult(
                dax_query=dax,
                explanation=f"Receita, despesa e resultado {period}",
                label=f"Receita e Despesa {period}",
                pattern=QueryPattern.KPI_SIMPLE,
            )

        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)
        filters = self._build_calculate_filters(parsed)

        if filters:
            filter_str = ",\n        ".join(filters)
            dax = f"""EVALUATE
ROW(
    "{label}", CALCULATE(
        {measure},
        {filter_str}
    )
)"""
        else:
            dax = f"""EVALUATE
ROW(
    "{label}", {measure}
)"""

        period = self._period_label(parsed.temporal)
        return TemplateResult(
            dax_query=dax,
            explanation=f"{label} {period}",
            label=f"{label} {period}",
            pattern=QueryPattern.KPI_SIMPLE,
        )

    def _render_kpi_by_dimension(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 2: KPI por dimensão.
        "CAPEX por departamento em 2025"
        → EVALUATE SUMMARIZECOLUMNS('Departamentos'[Centro de Custo], filter, "CAPEX", [CAPEX])
        """
        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)
        dim = parsed.dimensions[0] if parsed.dimensions else DIMENSION_REGISTRY[0]

        dim_cols = ",\n    ".join(dim.dax_columns)
        filter_block = self._build_filter_block(parsed)

        if filter_block:
            dax = f"""EVALUATE
SUMMARIZECOLUMNS(
    {dim_cols},
    {filter_block},
    "{label}", {measure}
)
ORDER BY [{label}] DESC"""
        else:
            dax = f"""EVALUATE
SUMMARIZECOLUMNS(
    {dim_cols},
    "{label}", {measure}
)
ORDER BY [{label}] DESC"""

        period = self._period_label(parsed.temporal)
        return TemplateResult(
            dax_query=dax,
            explanation=f"{label} por {dim.display_name} {period}",
            label=f"{label} por {dim.display_name} {period}",
            pattern=QueryPattern.KPI_BY_DIMENSION,
        )

    def _render_kpi_comparison(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 3: KPI com comparação temporal.
        "Compare CAPEX 2024 vs 2025"
        → EVALUATE ROW("CAPEX 2024", CALCULATE(..., year=2024), "CAPEX 2025", CALCULATE(..., year=2025))
        """
        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)
        year1 = parsed.temporal.year or "2025"
        year2 = parsed.temporal.year2 or "2026"

        filter1_parts = [f"'data'[Ano ] = \"{year1}\""]
        filter2_parts = [f"'data'[Ano ] = \"{year2}\""]
        if parsed.status_filter and parsed.status_filter.statuses:
            vals = ", ".join(f'"{s}"' for s in parsed.status_filter.statuses)
            sf = f"'data'[cStatus] IN {{{vals}}}"
            filter1_parts.append(sf)
            filter2_parts.append(sf)

        f1 = ", ".join(filter1_parts)
        f2 = ", ".join(filter2_parts)

        dax = f"""EVALUATE
ROW(
    "{label} {year1}", CALCULATE({measure}, {f1}),
    "{label} {year2}", CALCULATE({measure}, {f2}),
    "Variação", CALCULATE({measure}, {f2}) - CALCULATE({measure}, {f1})
)"""

        return TemplateResult(
            dax_query=dax,
            explanation=f"Comparação de {label}: {year1} vs {year2}",
            label=f"{label} {year1} vs {year2}",
            pattern=QueryPattern.KPI_COMPARISON,
        )

    def _render_kpi_top_n(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 4: Top N por KPI.
        "Top 5 departamentos por CAPEX"
        → EVALUATE TOPN(5, SUMMARIZECOLUMNS(...), [CAPEX], DESC)
        """
        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)
        n = parsed.top_n or 10
        dim = parsed.dimensions[0] if parsed.dimensions else DIMENSION_REGISTRY[0]
        sort_dir = "DESC" if parsed.order_desc else "ASC"

        dim_cols = ",\n        ".join(dim.dax_columns)
        filter_block = self._build_filter_block(parsed)

        if filter_block:
            inner = f"""SUMMARIZECOLUMNS(
        {dim_cols},
        {filter_block},
        "{label}", {measure}
    )"""
        else:
            inner = f"""SUMMARIZECOLUMNS(
        {dim_cols},
        "{label}", {measure}
    )"""

        dax = f"""EVALUATE
TOPN(
    {n},
    {inner},
    [{label}], {sort_dir}
)"""

        period = self._period_label(parsed.temporal)
        if n == 1 and not parsed.order_desc:
            rank_word = "Pior"
        elif n == 1:
            rank_word = "Melhor"
        else:
            rank_word = f"Top {n}"
        return TemplateResult(
            dax_query=dax,
            explanation=f"{rank_word} {dim.display_name} por {label} {period}",
            label=f"{rank_word} {dim.display_name} por {label} {period}",
            pattern=QueryPattern.KPI_TOP_N,
        )

    def _render_kpi_trend(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 5: Tendência mensal.
        "CAPEX mês a mês em 2025"
        → EVALUATE SUMMARIZECOLUMNS('data'[Nome mês], 'data'[Ano ], filter, "CAPEX", [CAPEX])
        """
        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)

        # Para tendência, o filtro não inclui mês (queremos todos os meses)
        filter_parts = []
        if parsed.temporal.year:
            filter_parts.append(f"'data'[Ano ] = \"{parsed.temporal.year}\"")
        if parsed.status_filter and parsed.status_filter.statuses:
            vals = ", ".join(f'"{s}"' for s in parsed.status_filter.statuses)
            filter_parts.append(f"'data'[cStatus] IN {{{vals}}}")

        if filter_parts:
            condition = " && ".join(filter_parts)
            filter_block = f"FILTER(ALL('data'), {condition})"
            dax = f"""EVALUATE
SUMMARIZECOLUMNS(
    'data'[Nome mês],
    'data'[Ano ],
    {filter_block},
    "{label}", {measure}
)"""
        else:
            dax = f"""EVALUATE
SUMMARIZECOLUMNS(
    'data'[Nome mês],
    'data'[Ano ],
    "{label}", {measure}
)"""

        period = self._period_label(parsed.temporal)
        return TemplateResult(
            dax_query=dax,
            explanation=f"{label} por mês {period}",
            label=f"{label} mensal {period}",
            pattern=QueryPattern.KPI_TREND,
        )

    def _render_kpi_multi_metric(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 6: Múltiplas métricas por dimensão.
        "Receita e despesa por departamento em 2025"
        """
        dim = parsed.dimensions[0] if parsed.dimensions else DIMENSION_REGISTRY[0]
        dim_cols = ",\n    ".join(dim.dax_columns)
        filter_block = self._build_filter_block(parsed)

        measures_str = ",\n    ".join(
            f'"{lbl}", {expr}' for lbl, expr in parsed.extra_measures
        )

        if filter_block:
            dax = f"""EVALUATE
SUMMARIZECOLUMNS(
    {dim_cols},
    {filter_block},
    {measures_str}
)
ORDER BY [Resultado] DESC"""
        else:
            dax = f"""EVALUATE
SUMMARIZECOLUMNS(
    {dim_cols},
    {measures_str}
)
ORDER BY [Resultado] DESC"""

        period = self._period_label(parsed.temporal)
        return TemplateResult(
            dax_query=dax,
            explanation=f"Receita, despesa e resultado por {dim.display_name} {period}",
            label=f"Receita e Despesa por {dim.display_name} {period}",
            pattern=QueryPattern.KPI_MULTI_METRIC,
        )

    def _render_kpi_rolling(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 7: KPI em janela rolling.
        "receita da última semana"
        → EVALUATE ROW("Receita última semana", CALCULATE(SUM(...), date >= TODAY()-7, date <= TODAY(), ...))

        Suporta:
        - Janela passada (rolling_is_forward=False): TODAY()-N a TODAY(), status PAGO/RECEBIDO
        - Janela futura (rolling_is_forward=True): TODAY() a TODAY()+N, status A VENCER/ATRASADO
        - Multi-metric (extra_measures preenchido): gera ROW com Receita, Despesa e Resultado
        """
        tf = parsed.temporal
        days = tf.rolling_days
        date_col = tf.rolling_date_col

        # ── Janela FUTURA ────────────────────────────────────────────────────
        if tf.rolling_is_forward:
            forward_lbl = self._forward_rolling_label(days)
            forward_status = "'data'[cStatus] IN {\"A VENCER\", \"ATRASADO\", \"VENCE HOJE\", \"PREVISAO\"}"

            if parsed.extra_measures:
                # Multi-metric forward rolling
                rows = []
                for lbl, expr in parsed.extra_measures:
                    rows.append(
                        f'    "{lbl} {forward_lbl}", CALCULATE(\n'
                        f'        {expr},\n'
                        f'        {date_col} >= TODAY(),\n'
                        f'        {date_col} <= TODAY() + {days},\n'
                        f'        {forward_status}\n'
                        f'    )'
                    )
                rows_str = ",\n".join(rows)
                dax = f"EVALUATE\nROW(\n{rows_str}\n)"
            else:
                measure = self._get_measure_expr(parsed)
                label = self._get_label(parsed)
                dax = f"""EVALUATE
ROW(
    "{label} {forward_lbl}", CALCULATE(
        {measure},
        {date_col} >= TODAY(),
        {date_col} <= TODAY() + {days},
        {forward_status}
    )
)"""

            result_label = parsed.measure_label or self._get_label(parsed)
            return TemplateResult(
                dax_query=dax,
                explanation=f"{result_label} — {forward_lbl}",
                label=f"{result_label} {forward_lbl}",
                pattern=QueryPattern.KPI_ROLLING,
            )

        # ── Janela PASSADA (comportamento original) ──────────────────────────
        rolling_lbl = self._rolling_label(days)

        # Multi-metric rolling (ex: "entrou e saiu essa semana")
        if parsed.extra_measures:
            rows = []
            for lbl, expr in parsed.extra_measures:
                if tf.rolling_status_filter:
                    rows.append(
                        f'    "{lbl} {rolling_lbl}", CALCULATE(\n'
                        f'        {expr},\n'
                        f'        {date_col} >= TODAY() - {days},\n'
                        f'        {date_col} <= TODAY(),\n'
                        f"        'data'[cStatus] IN {{\"PAGO\", \"RECEBIDO\"}}\n"
                        f'    )'
                    )
                else:
                    rows.append(
                        f'    "{lbl} {rolling_lbl}", CALCULATE(\n'
                        f'        {expr},\n'
                        f'        {date_col} >= TODAY() - {days},\n'
                        f'        {date_col} <= TODAY()\n'
                        f'    )'
                    )
            rows_str = ",\n".join(rows)
            dax = f"EVALUATE\nROW(\n{rows_str}\n)"
            return TemplateResult(
                dax_query=dax,
                explanation=f"Receita e Despesa — {rolling_lbl}",
                label=f"Receita e Despesa {rolling_lbl}",
                pattern=QueryPattern.KPI_ROLLING,
            )

        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)

        if tf.rolling_status_filter:
            dax = f"""EVALUATE
ROW(
    "{label} {rolling_lbl}", CALCULATE(
        {measure},
        {date_col} >= TODAY() - {days},
        {date_col} <= TODAY(),
        'data'[cStatus] IN {{"PAGO", "RECEBIDO"}}
    )
)"""
        else:
            dax = f"""EVALUATE
ROW(
    "{label} {rolling_lbl}", CALCULATE(
        {measure},
        {date_col} >= TODAY() - {days},
        {date_col} <= TODAY()
    )
)"""

        return TemplateResult(
            dax_query=dax,
            explanation=f"{label} — {rolling_lbl}",
            label=f"{label} {rolling_lbl}",
            pattern=QueryPattern.KPI_ROLLING,
        )

    def _render_kpi_rolling_average(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 8: Média em janela rolling.
        "média de receita das últimas 3 semanas"
        → EVALUATE ROW("Média Semanal Receita", DIVIDE(CALCULATE(SUM(...), ...), 3))
        """
        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)
        tf = parsed.temporal
        days = tf.rolling_days
        date_col = tf.rolling_date_col
        rolling_lbl = self._rolling_label(days)

        # Determina divisor: semanas (múltiplo de 7) ou dias
        if days % 7 == 0:
            divisor = days // 7
            avg_unit = "semanal" if divisor == 1 else f"{divisor} semanas"
        else:
            divisor = days
            avg_unit = f"{days} dias"

        if tf.rolling_status_filter:
            inner_calc = (
                f"CALCULATE(\n"
                f"        {measure},\n"
                f"        {date_col} >= TODAY() - {days},\n"
                f"        {date_col} <= TODAY(),\n"
                f"        'data'[cStatus] IN {{\"PAGO\", \"RECEBIDO\"}}\n"
                f"    )"
            )
        else:
            inner_calc = (
                f"CALCULATE(\n"
                f"        {measure},\n"
                f"        {date_col} >= TODAY() - {days},\n"
                f"        {date_col} <= TODAY()\n"
                f"    )"
            )

        dax = f"""EVALUATE
ROW(
    "Média {avg_unit} {label}", DIVIDE(
    {inner_calc},
    {divisor}
    )
)"""

        return TemplateResult(
            dax_query=dax,
            explanation=f"Média {avg_unit} de {label} — {rolling_lbl}",
            label=f"Média {avg_unit} {label} {rolling_lbl}",
            pattern=QueryPattern.KPI_ROLLING_AVERAGE,
        )

    def _render_kpi_monthly_average(self, parsed: ParsedQuery) -> TemplateResult:
        """
        Padrão 9: Média mensal de KPI.
        "média mensal de receita desse ano"
        → EVALUATE ROW("Média Mensal Receita 2026", AVERAGEX(SUMMARIZECOLUMNS(...), [Total]))
        """
        measure = self._get_measure_expr(parsed)
        label = self._get_label(parsed)
        tf = parsed.temporal

        if tf.is_all_time:
            # Média sobre todos os meses/anos únicos do histórico
            dax = f"""EVALUATE
ROW(
    "Média Mensal {label} — todo o período",
    AVERAGEX(
        SUMMARIZECOLUMNS(
            'data'[Nome mês],
            'data'[Ano ],
            "Total", {measure}
        ),
        [Total]
    )
)"""
            period_lbl = "— todo o período"
        elif tf.year:
            year = tf.year
            # Status filter
            status_parts = []
            if parsed.status_filter and parsed.status_filter.statuses:
                vals = ", ".join(f'"{s}"' for s in parsed.status_filter.statuses)
                status_parts.append(f"'data'[cStatus] IN {{{vals}}}")

            if status_parts:
                extra_filter = " && ".join(status_parts)
                filter_expr = f"FILTER(ALL('data'), 'data'[Ano ] = \"{year}\" && {extra_filter})"
            else:
                filter_expr = f"FILTER(ALL('data'), 'data'[Ano ] = \"{year}\")"

            dax = f"""EVALUATE
ROW(
    "Média Mensal {label} {year}",
    AVERAGEX(
        SUMMARIZECOLUMNS(
            'data'[Nome mês],
            {filter_expr},
            "Total", {measure}
        ),
        [Total]
    )
)"""
            period_lbl = f"— {year}"
        else:
            # Sem contexto temporal claro → delegar para KPI_SIMPLE
            return self._render_kpi_simple(parsed)

        return TemplateResult(
            dax_query=dax,
            explanation=f"Média mensal de {label} {period_lbl}",
            label=f"Média Mensal {label} {period_lbl}",
            pattern=QueryPattern.KPI_MONTHLY_AVERAGE,
        )

    def _rolling_label(self, days: Optional[int]) -> str:
        """Gera label descritivo para janela rolling passada."""
        if days is None:
            return ""
        if days == 7:
            return "última semana"
        if days == 30:
            return "último mês"
        if days % 7 == 0:
            weeks = days // 7
            return f"últimas {weeks} semanas"
        return f"últimos {days} dias"

    def _forward_rolling_label(self, days: Optional[int]) -> str:
        """Gera label descritivo para janela rolling futura (proximos N dias)."""
        if days is None:
            return ""
        if days == 7:
            return "próxima semana"
        if days == 30:
            return "próximo mês"
        if days % 7 == 0:
            weeks = days // 7
            return f"próximas {weeks} semanas"
        return f"próximos {days} dias"

    # ── Validation ───────────────────────────────────────────────

    def _validate(self, dax: str) -> None:
        """Valida invariantes do DAX gerado."""
        # 1. Deve começar com EVALUATE
        if not dax.strip().startswith("EVALUATE"):
            raise DaxTemplateError("Query não começa com EVALUATE")

        # 2. Parênteses balanceados
        open_p = dax.count("(")
        close_p = dax.count(")")
        if open_p != close_p:
            raise DaxTemplateError(
                f"Parênteses desbalanceados: {open_p} aberturas vs {close_p} fechamentos"
            )

        # 3. Chaves balanceadas
        open_b = dax.count("{")
        close_b = dax.count("}")
        if open_b != close_b:
            raise DaxTemplateError(
                f"Chaves desbalanceadas: {open_b} aberturas vs {close_b} fechamentos"
            )

        # 4. Aspas balanceadas
        quotes = dax.count('"')
        if quotes % 2 != 0:
            raise DaxTemplateError("Aspas desbalanceadas")

        # 5. NUNCA data[valor] ou data[Valor]
        if re.search(r"data\[(?:V|v)alor\]", dax):
            raise DaxTemplateError("Template gerou data[valor] — BUG no template!")

        # 6. Coluna Ano deve ter espaço
        if "'data'[Ano]" in dax:
            raise DaxTemplateError("Coluna Ano sem espaço — deve ser 'data'[Ano ]")

    def _period_label(self, temporal: TemporalFilter) -> str:
        """Gera label descritivo do período."""
        if temporal.is_all_time:
            return "— todo o período"
        parts = []
        if temporal.month:
            parts.append(temporal.month)
        if temporal.year:
            parts.append(temporal.year)
        if parts:
            return "— " + "/".join(parts)
        return ""


class DaxTemplateError(Exception):
    """Erro de validação no template DAX."""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. PIPELINE COMPLETO — TemplatedDaxPipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class PipelineResult:
    """Resultado do pipeline de geração DAX."""
    success: bool
    dax_query: Optional[str] = None
    explanation: str = ""
    label: str = ""
    pattern: Optional[QueryPattern] = None
    confidence: float = 0.0
    used_template: bool = False
    fallback_reason: Optional[str] = None


class TemplatedDaxPipeline:
    """
    Pipeline completo de geração DAX sem LLM.

    Fluxo:
        pergunta
          → MeasureMatcher.match() → medida identificada
          → PatternDetector.detect() → padrão + filtros + dimensões
          → DaxTemplateEngine.render() → DAX confiável

    Se qualquer etapa falha → retorna PipelineResult com used_template=False,
    sinalizando ao orchestrator para usar o fallback via LLM.
    """

    def __init__(self, measures_file: Optional[Path] = None):
        self.matcher = MeasureMatcher(measures_file)
        self.detector = PatternDetector()
        self.engine = DaxTemplateEngine()

    def try_generate(
        self,
        question: str,
        hard_filters: Optional[List[str]] = None,
    ) -> PipelineResult:
        """
        Tenta gerar DAX via template. Retorna PipelineResult.

        Se used_template=False, o chamador deve usar o LLM como fallback.

        Args:
            question: Pergunta em linguagem natural.
            hard_filters: Filtros DAX extraídos do relatório Power BI (obrigatórios).
        """
        # Etapa 1: Identifica a medida
        measure = self.matcher.match(question)
        if not measure:
            # Verifica se é multi-metric (receita e despesa)
            if PatternDetector._MULTI_METRIC_RE.search(question):
                measure = None  # Será tratado pelo PatternDetector
            else:
                return PipelineResult(
                    success=False,
                    fallback_reason=f"Nenhuma medida reconhecida na pergunta: '{question}'",
                )

        # Etapa 2: Detecta o padrão
        parsed = self.detector.detect(question, measure)

        # Injeta filtros hard-coded do relatório no ParsedQuery
        if hard_filters:
            parsed.hard_filters = list(hard_filters)

        if parsed.pattern == QueryPattern.UNKNOWN:
            return PipelineResult(
                success=False,
                fallback_reason=f"Padrão de query não reconhecido: '{question}'",
            )

        # Para multi-metric sem medida explícita, garantir que extra_measures está preenchido
        if not measure and parsed.extra_measures:
            pass  # OK, multi-metric
        elif not measure:
            return PipelineResult(
                success=False,
                fallback_reason="Sem medida e sem multi-metric detectado",
            )

        # Etapa 3: Renderiza o template
        try:
            result = self.engine.render(parsed)
            return PipelineResult(
                success=True,
                dax_query=result.dax_query,
                explanation=result.explanation,
                label=result.label,
                pattern=result.pattern,
                confidence=result.confidence,
                used_template=True,
            )
        except (DaxTemplateError, ValueError) as exc:
            logger.error("Template rendering failed: %s", exc)
            return PipelineResult(
                success=False,
                fallback_reason=f"Erro no template: {exc}",
            )

    def reload_measures(self, measures_file: Optional[Path] = None) -> None:
        """Recarrega medidas do measures.json."""
        self.matcher.reload(measures_file)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. SINGLETON para uso no orchestrator
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_pipeline: Optional[TemplatedDaxPipeline] = None


def get_dax_pipeline() -> TemplatedDaxPipeline:
    """Retorna singleton do pipeline."""
    global _pipeline
    if _pipeline is None:
        _pipeline = TemplatedDaxPipeline()
    return _pipeline


def invalidate_pipeline() -> None:
    """Força recriação do pipeline (após reload de measures.json)."""
    global _pipeline
    _pipeline = None
