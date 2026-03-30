"""
FilterExtractor — Extracao de filtros temporais de perguntas em linguagem natural (pt-BR).

Extrai:
- Ano: "2025", "ano passado", "ano retrasado"
- Mes: "janeiro", "jan", "marco de 2025"
- Trimestre: "Q1", "primeiro trimestre", "T3"
- Comparacao: "vs 2024", "comparado com", "crescimento"
- Date range: "de janeiro a marco", "primeiro semestre"

Padrao: se nenhum filtro temporal → ano corrente (2026).
Se pergunta e "todo o periodo" ou sem indicador temporal → retorna sem filtro (is_all_time=True).
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Regex de "todo o periodo" e indicadores temporais
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# Configuracao
# ─────────────────────────────────────────────────────────────────────────────

# Ano corrente — em producao, use datetime.now().year
CURRENT_YEAR = datetime.now().year

# ─────────────────────────────────────────────────────────────────────────────
# Mapeamentos de meses
# ─────────────────────────────────────────────────────────────────────────────

MONTH_NAMES: Dict[str, int] = {
    "janeiro": 1, "jan": 1,
    "fevereiro": 2, "fev": 2,
    "marco": 3, "março": 3, "mar": 3,
    "abril": 4, "abr": 4,
    "maio": 5, "mai": 5,
    "junho": 6, "jun": 6,
    "julho": 7, "jul": 7,
    "agosto": 8, "ago": 8,
    "setembro": 9, "set": 9,
    "outubro": 10, "out": 10,
    "novembro": 11, "nov": 11,
    "dezembro": 12, "dez": 12,
}

# Nome oficial (inicial maiuscula, como o Power BI espera)
MONTH_NUMBER_TO_NAME: Dict[int, str] = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

# Trimestres
QUARTER_MONTHS: Dict[int, List[int]] = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9],
    4: [10, 11, 12],
}

# Semestres
SEMESTER_MONTHS: Dict[int, List[int]] = {
    1: [1, 2, 3, 4, 5, 6],
    2: [7, 8, 9, 10, 11, 12],
}

# Ordinal → numero
ORDINALS: Dict[str, int] = {
    "primeiro": 1, "1o": 1, "1": 1,
    "segundo": 2, "2o": 2, "2": 2,
    "terceiro": 3, "3o": 3, "3": 3,
    "quarto": 4, "4o": 4, "4": 4,
}


# ─────────────────────────────────────────────────────────────────────────────
# Dataclasses de resultado
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TemporalFilter:
    """Resultado da extracao de filtros temporais."""
    year: Optional[str] = None          # "2025" (string para compatibilidade com DAX 'data'[Ano ])
    month: Optional[str] = None         # "Janeiro" (nome oficial, maiuscula)
    month_number: Optional[int] = None  # 1-12 (util para ranges)
    quarter: Optional[int] = None       # 1-4
    months_in_range: Optional[List[str]] = None  # ["Janeiro", "Fevereiro", "Marco"]
    is_comparison: bool = False         # True se pergunta pede comparacao
    comparison_year: Optional[str] = None  # Ano de comparacao ("2024")
    comparison_period: Optional[str] = None  # "ano passado", "mes anterior", etc
    is_ytd: bool = False                # Year-to-date
    is_mtd: bool = False                # Month-to-date
    raw_text: str = ""                  # Texto original que gerou o filtro
    default_applied: bool = False       # True se nenhum filtro foi extraido (default aplicado)
    rolling_window: Optional["RollingWindow"] = None  # Janela rolling (ultimos N dias/semanas/meses)
    is_all_time: bool = False           # True quando pergunta nao especifica periodo ou pede "todo o periodo"

    def has_filters(self) -> bool:
        """Retorna True se algum filtro temporal foi encontrado."""
        return any([
            self.year, self.month, self.quarter,
            self.months_in_range, self.is_comparison,
            self.rolling_window is not None,
        ])

    def to_dax_filters(self) -> List[str]:
        """
        Converte para clausulas DAX prontas para uso em CALCULATE/FILTER.
        Retorna lista de strings como "'data'[Ano ] = \"2025\"".
        Se is_all_time=True, retorna lista vazia (sem filtros de periodo).
        """
        if self.is_all_time:
            return []
        filters = []
        if self.year:
            filters.append(f"'data'[Ano ] = \"{self.year}\"")
        if self.month:
            filters.append(f"'data'[Nome mês] = \"{self.month}\"")
        if self.months_in_range and not self.month:
            # Range de meses: usa IN
            month_list = ", ".join(f'"{m}"' for m in self.months_in_range)
            filters.append(f"'data'[Nome mês] IN {{{month_list}}}")
        return filters

    def to_dax_filter_string(self) -> str:
        """Retorna filtros como string unica separada por virgula."""
        return ", ".join(self.to_dax_filters())

    def to_dax_rolling_filters(self) -> List[str]:
        """
        Retorna filtros DAX para janela rolling baseados em rolling_window.

        Exemplo para RollingWindow(n=7, unit="dia", status_filter=True):
            [
                "'data'[DataPagamento] >= TODAY() - 7",
                "'data'[DataPagamento] <= TODAY()",
                "'data'[cStatus] IN {\"PAGO\", \"RECEBIDO\"}"
            ]

        Returns:
            Lista de clausulas DAX, ou lista vazia se rolling_window for None.
        """
        if self.rolling_window is None:
            return []

        rw = self.rolling_window
        col = rw.date_column
        filters = [
            f"{col} >= TODAY() - {rw.n}",
            f"{col} <= TODAY()",
        ]
        if rw.status_filter:
            filters.append("'data'[cStatus] IN {\"PAGO\", \"RECEBIDO\"}")
        return filters

    def to_dax_forward_filters(self) -> List[str]:
        """
        Retorna filtros DAX para janela rolling FUTURA (proximos N dias).

        Exemplo para RollingWindow(n=30, is_forward=True):
            [
                "'data'[dDtVenc] >= TODAY()",
                "'data'[dDtVenc] <= TODAY() + 30",
                "'data'[cStatus] IN {\"A VENCER\", \"ATRASADO\", \"VENCE HOJE\", \"PREVISAO\"}"
            ]

        Returns:
            Lista de clausulas DAX, ou lista vazia se rolling_window for None ou nao for forward.
        """
        if self.rolling_window is None or not self.rolling_window.is_forward:
            return []
        rw = self.rolling_window
        col = rw.date_column
        n = rw.n
        return [
            f"{col} >= TODAY()",
            f"{col} <= TODAY() + {n}",
            "'data'[cStatus] IN {\"A VENCER\", \"ATRASADO\", \"VENCE HOJE\", \"PREVISAO\"}",
        ]


@dataclass
class RollingWindow:
    """
    Representa uma janela temporal rolling (ex: 'ultimos 7 dias').

    Atributos:
        n            — numero de dias (sempre em dias apos conversao)
        unit         — unidade original ("dia", "semana", "mes")
        is_average   — True se a pergunta pede media (ex: "media das ultimas 3 semanas")
        date_column  — coluna de data do Power BI a usar nos filtros DAX
        status_filter — True = incluir filtro de caixa PAGO/RECEBIDO na coluna cStatus
        is_forward   — True se a janela e futura (proximos N dias) em vez de passada
    """
    n: int
    unit: str
    is_average: bool = False
    date_column: str = "'data'[DataPagamento]"
    status_filter: bool = True
    is_forward: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# Classe principal: FilterExtractor
# ─────────────────────────────────────────────────────────────────────────────

class FilterExtractor:
    """
    Extrai filtros temporais de perguntas em linguagem natural pt-BR.

    Uso:
        extractor = FilterExtractor()
        f = extractor.extract("qual foi meu capex total em 2025?")
        # f.year = "2025", f.month = None

        f = extractor.extract("receita de marco de 2025")
        # f.year = "2025", f.month = "Marco"

        f = extractor.extract("faturamento do ano passado")
        # f.year = "2025" (se ano atual e 2026)

        f = extractor.extract("despesas do primeiro trimestre")
        # f.year = "2026" (default), f.quarter = 1, f.months_in_range = ["Janeiro", "Fevereiro", "Marco"]
    """

    def __init__(self, current_year: Optional[int] = None):
        self.current_year = current_year or CURRENT_YEAR

    def extract(self, text: str) -> TemporalFilter:
        """
        Extrai todos os filtros temporais de uma pergunta.
        Aplica defaults quando nenhum filtro e encontrado.
        """
        result = TemporalFilter(raw_text=text)

        # Normaliza para extracao (mas preserva original)
        normalized = self._normalize_for_extraction(text)

        # 1. Extrai ano
        year = self.extract_year(normalized)
        result.year = year

        # 2. Extrai mes
        month_name, month_num = self.extract_month(normalized)
        result.month = month_name
        result.month_number = month_num

        # 3. Extrai trimestre
        quarter = self.extract_quarter(normalized)
        if quarter:
            result.quarter = quarter
            result.months_in_range = [
                MONTH_NUMBER_TO_NAME[m] for m in QUARTER_MONTHS[quarter]
            ]

        # 4. Extrai semestre
        if not quarter:
            semester = self.extract_semester(normalized)
            if semester:
                result.months_in_range = [
                    MONTH_NUMBER_TO_NAME[m] for m in SEMESTER_MONTHS[semester]
                ]

        # 5. Extrai range de meses ("de janeiro a marco")
        if not result.months_in_range:
            month_range = self.extract_month_range(normalized)
            if month_range:
                result.months_in_range = month_range

        # 6. Extrai comparacao
        comp = self.extract_comparison(normalized)
        if comp:
            result.is_comparison = True
            result.comparison_year = comp.get("year")
            result.comparison_period = comp.get("period")

        # 7. YTD / MTD
        result.is_ytd = self._detect_ytd(normalized)
        result.is_mtd = self._detect_mtd(normalized)

        # 8. Rolling window (ultimos N dias/semanas/meses)
        rolling = self.extract_rolling_window(text)
        if rolling:
            result.rolling_window = rolling
            # Rolling window e auto-suficiente: nao aplica default de ano
            return result

        # 8.5 — Detecta "todo o periodo" (expressoes explicitas)
        if not result.year and not result.rolling_window and self.extract_all_time(normalized):
            result.is_all_time = True
            result.default_applied = False
            return result

        # 9 — Detecta "sem indicador temporal" (pergunta geral, sem periodo)
        if not result.year and not result.rolling_window:
            if not _HAS_TEMPORAL_HINT.search(normalized):
                result.is_all_time = True
                result.default_applied = False
                return result

        # 10. Default: se nenhum ano foi extraido, usa ano corrente
        if not result.year:
            result.year = str(self.current_year)
            if not result.has_filters() or (result.year and not month_name and not quarter):
                result.default_applied = True

        return result

    # ── Extratores individuais ───────────────────────────────────

    def extract_all_time(self, text: str) -> bool:
        """Retorna True se a pergunta indica 'todo o periodo' / historico completo."""
        return bool(_ALL_TIME_RE.search(text))

    def extract_year(self, text: str) -> Optional[str]:
        """
        Extrai ano de 4 digitos (2020-2039) ou expressoes relativas.

        Exemplos:
            "em 2025"        → "2025"
            "ano passado"    → "2025" (se current=2026)
            "ano retrasado"  → "2024" (se current=2026)
            "esse ano"       → "2026"
        """
        normalized = text.lower()

        # Expressoes relativas (verificar ANTES do regex de 4 digitos)
        relative_patterns = [
            (r"\b(?:esse|este|neste|nesse)\s+ano\b", 0),
            (r"\b(?:ano\s+(?:corrente|atual|vigente))\b", 0),
            (r"\b(?:ano\s+passado|ano\s+anterior|ultimo\s+ano)\b", -1),
            (r"\b(?:ano\s+retrasado|dois\s+anos\s+atras|2\s+anos\s+atras)\b", -2),
            (r"\b(?:proximo\s+ano|ano\s+que\s+vem)\b", +1),
        ]
        for pattern, offset in relative_patterns:
            if re.search(pattern, normalized):
                return str(self.current_year + offset)

        # Ano explicito de 4 digitos
        m = re.search(r'\b(20[2-3]\d)\b', text)
        if m:
            return m.group(1)

        return None

    def extract_month(self, text: str) -> tuple[Optional[str], Optional[int]]:
        """
        Extrai nome do mes e numero.

        Exemplos:
            "marco de 2025"  → ("Marco", 3)
            "em janeiro"     → ("Janeiro", 1)
            "mes passado"    → (nome_do_mes_anterior, numero)

        Returns:
            (nome_oficial, numero) ou (None, None)
        """
        normalized = text.lower()

        # "agora" / "hoje" / "este momento" → mês atual
        if re.search(r'\b(?:agora|hoje|neste\s+momento|atualmente)\b', normalized):
            now = datetime.now()
            return MONTH_NUMBER_TO_NAME[now.month], now.month

        # Mes relativo
        if re.search(r'\b(?:mes\s+passado|mes\s+anterior|ultimo\s+mes)\b', normalized):
            now = datetime.now()
            if now.month == 1:
                return MONTH_NUMBER_TO_NAME[12], 12
            return MONTH_NUMBER_TO_NAME[now.month - 1], now.month - 1

        if re.search(r'\b(?:mes\s+(?:atual|corrente|vigente)|este\s+mes|esse\s+mes)\b', normalized):
            now = datetime.now()
            return MONTH_NUMBER_TO_NAME[now.month], now.month

        # Remove acentos para lookup
        clean = self._remove_accents(normalized)

        # Busca o nome de mes mais longo primeiro (evita "mar" matchando antes de "marco")
        for key in sorted(MONTH_NAMES.keys(), key=len, reverse=True):
            key_clean = self._remove_accents(key)
            # Verifica como palavra inteira (evita "mar" dentro de "marcar")
            if re.search(r'\b' + re.escape(key_clean) + r'\b', clean):
                num = MONTH_NAMES[key]
                return MONTH_NUMBER_TO_NAME[num], num

        return None, None

    def extract_quarter(self, text: str) -> Optional[int]:
        """
        Extrai trimestre.

        Exemplos:
            "Q1"                    → 1
            "T3"                    → 3
            "primeiro trimestre"    → 1
            "3o trimestre"          → 3
            "quarto tri"            → 4
        """
        normalized = text.lower()

        # Expressões coloquiais de período
        normalized = text.lower()
        if re.search(r'\b(come[cç]o\s+do\s+ano|início\s+do\s+ano|inicio\s+do\s+ano|primeiro\s+trimestre\s+do\s+ano)\b', normalized):
            return 1
        if re.search(r'\b(fim\s+do\s+ano|final\s+do\s+ano|último\s+trimestre|ultimo\s+trimestre|fim\s+de\s+ano)\b', normalized):
            return 4

        # Q1, Q2, Q3, Q4 ou T1, T2, T3, T4
        m = re.search(r'\b[qt]([1-4])\b', normalized)
        if m:
            return int(m.group(1))

        # "primeiro trimestre", "2o trimestre", "terceiro tri"
        m = re.search(
            r'\b(primeiro|segundo|terceiro|quarto|1o|2o|3o|4o|1|2|3|4)\s*(?:trimestre|tri)\b',
            normalized,
        )
        if m:
            ordinal = m.group(1).strip()
            return ORDINALS.get(ordinal)

        return None

    def extract_semester(self, text: str) -> Optional[int]:
        """
        Extrai semestre.

        Exemplos:
            "primeiro semestre"  → 1
            "S2"                → 2
            "2o semestre"       → 2
        """
        normalized = text.lower()

        # S1, S2
        m = re.search(r'\bs([1-2])\b', normalized)
        if m:
            return int(m.group(1))

        # "primeiro semestre"
        m = re.search(
            r'\b(primeiro|segundo|1o|2o|1|2)\s*semestre\b',
            normalized,
        )
        if m:
            ordinal = m.group(1).strip()
            return ORDINALS.get(ordinal)

        return None

    def extract_month_range(self, text: str) -> Optional[List[str]]:
        """
        Extrai range de meses: "de janeiro a marco", "entre abril e junho".

        Returns:
            Lista de nomes oficiais dos meses no range, ou None.
        """
        normalized = text.lower()
        clean = self._remove_accents(normalized)

        # "de X a Y", "entre X e Y"
        m = re.search(
            r'\b(?:de|entre)\s+(\w+)\s+(?:a|e|ate|até)\s+(\w+)\b',
            clean,
        )
        if not m:
            return None

        start_str = m.group(1)
        end_str = m.group(2)

        start_num = self._month_str_to_num(start_str)
        end_num = self._month_str_to_num(end_str)

        if start_num is None or end_num is None:
            return None

        if start_num <= end_num:
            months = list(range(start_num, end_num + 1))
        else:
            # Wrap around: outubro a fevereiro → [10, 11, 12, 1, 2]
            months = list(range(start_num, 13)) + list(range(1, end_num + 1))

        return [MONTH_NUMBER_TO_NAME[m] for m in months]

    def extract_comparison(self, text: str) -> Optional[Dict[str, str]]:
        """
        Detecta se a pergunta pede comparacao temporal.

        Exemplos:
            "receita 2025 vs 2024"           → {"year": "2024", "period": "vs 2024"}
            "crescimento em relacao a 2024"   → {"year": "2024", "period": "crescimento vs 2024"}
            "comparado com ano passado"       → {"year": str(current-1), "period": "ano passado"}
        """
        normalized = text.lower()

        # "vs YYYY", "versus YYYY", "contra YYYY"
        m = re.search(r'\b(?:vs\.?|versus|contra|x)\s*(20[2-3]\d)\b', normalized)
        if m:
            return {"year": m.group(1), "period": f"vs {m.group(1)}"}

        # "comparado com YYYY", "em relacao a YYYY"
        m = re.search(
            r'\b(?:comparad[oa]\s+(?:com|a)|em\s+relacao\s+a|em\s+relação\s+a)\s*(20[2-3]\d)\b',
            normalized,
        )
        if m:
            return {"year": m.group(1), "period": f"comparado com {m.group(1)}"}

        # "crescimento", "variacao", "evolucao" (implica comparacao com periodo anterior)
        if re.search(r'\b(?:crescimento|variacao|variação|evolucao|evolução|aumento|queda|reducao|redução)\b', normalized):
            # Verifica se menciona "ano passado" / "mes passado"
            if re.search(r'\b(?:ano\s+passado|ano\s+anterior)\b', normalized):
                return {"year": str(self.current_year - 1), "period": "ano passado"}
            if re.search(r'\b(?:mes\s+passado|mes\s+anterior)\b', normalized):
                return {"year": None, "period": "mes passado"}
            # Comparacao generica — o caller decide o periodo base
            return {"year": None, "period": "periodo anterior"}

        # "comparado com ano passado"
        if re.search(r'\b(?:comparad[oa]|relacao|relação)\b', normalized):
            if re.search(r'\b(?:ano\s+passado|ano\s+anterior)\b', normalized):
                return {"year": str(self.current_year - 1), "period": "ano passado"}

        return None

    def extract_date_range(self, text: str) -> Optional[Dict]:
        """
        Extrai range de datas completo (para queries que precisam de DATE()).

        Returns:
            {"start_year": int, "start_month": int, "end_year": int, "end_month": int}
            ou None
        """
        result = self.extract(text)

        if result.months_in_range and result.year:
            year = int(result.year)
            # Encontra primeiro e ultimo mes do range
            first_month = None
            last_month = None
            for month_name in result.months_in_range:
                for num, name in MONTH_NUMBER_TO_NAME.items():
                    if name == month_name:
                        if first_month is None or num < first_month:
                            first_month = num
                        if last_month is None or num > last_month:
                            last_month = num
            if first_month and last_month:
                return {
                    "start_year": year,
                    "start_month": first_month,
                    "end_year": year,
                    "end_month": last_month,
                }

        if result.year and result.month_number:
            year = int(result.year)
            return {
                "start_year": year,
                "start_month": result.month_number,
                "end_year": year,
                "end_month": result.month_number,
            }

        if result.year:
            year = int(result.year)
            return {
                "start_year": year,
                "start_month": 1,
                "end_year": year,
                "end_month": 12,
            }

        return None

    def extract_rolling_window(self, text: str) -> Optional[RollingWindow]:
        """
        Detecta janelas temporais rolling em pt-BR e retorna um RollingWindow.

        Converte tudo para dias:
            semana  → *7
            mes     → *30

        Exemplos:
            "ultima semana"              → RollingWindow(n=7,  unit="dia")
            "ultimas 3 semanas"          → RollingWindow(n=21, unit="dia")
            "ultimo mes"                 → RollingWindow(n=30, unit="dia")
            "ultimos 30 dias"            → RollingWindow(n=30, unit="dia")
            "ultimos 2 meses"            → RollingWindow(n=60, unit="dia")
            "media das ultimas 4 semanas"→ RollingWindow(n=28, unit="dia", is_average=True)
            "semana passada"             → RollingWindow(n=7,  unit="dia")
            "semana anterior"            → RollingWindow(n=7,  unit="dia")

        Regra de coluna/status:
            - Menciona "pago", "recebido", "entrou" → DataPagamento + status_filter=True
            - Menciona "vencimento", "competencia", "a vencer" → dDtVenc + status_filter=False
            - Default: DataPagamento + status_filter=True
        """
        normalized = self._remove_accents(text.lower())

        # Mapa de numeros por extenso → inteiro
        _WORD_TO_NUM: Dict[str, int] = {
            "uma": 1, "um": 1,
            "duas": 2, "dois": 2,
            "tres": 3,
            "quatro": 4,
            "cinco": 5,
            "seis": 6,
            "sete": 7,
            "oito": 8,
            "nove": 9,
            "dez": 10,
            "onze": 11,
            "doze": 12,
        }

        def _parse_num(tok: str) -> Optional[int]:
            """Converte token numerico ou por extenso para int."""
            if tok.isdigit():
                return int(tok)
            return _WORD_TO_NUM.get(tok)

        # ── Detectar is_average ──────────────────────────────────────────────
        is_average = bool(re.search(r'\b(?:media|medio|average)\b', normalized))

        # ── Detectar coluna de data / status ────────────────────────────────
        if re.search(r'\b(?:vencimento|competencia|a\s+vencer|vencer)\b', normalized):
            date_column = "'data'[dDtVenc]"
            status_filter = False
        else:
            # Padrao: caixa (DataPagamento)
            date_column = "'data'[DataPagamento]"
            status_filter = True

        # ── Grupo de padroes de rolling ──────────────────────────────────────
        # Prefixo: ultima/ultimas/ultimo/ultimos (com ou sem acento, ja removido)
        _PREFIX = r'(?:ultima[s]?|ultimo[s]?)'

        # Quantificador: numero + unidade, ou singular (sem numero = 1)
        _NUM = r'(\d+|uma|um|duas|dois|tres|quatro|cinco|seis|sete|oito|nove|dez|onze|doze)'

        # Padrao 1: "ultimos N dias"
        m = re.search(rf'\b{_PREFIX}\s+{_NUM}\s+(?:dia[s]?)\b', normalized)
        if m:
            n = _parse_num(m.group(1))
            if n:
                return RollingWindow(n=n, unit="dia", is_average=is_average,
                                     date_column=date_column, status_filter=status_filter)

        # Padrao 2: "ultimos N meses" / "ultimo mes"
        m = re.search(rf'\b{_PREFIX}\s+{_NUM}\s+(?:mes(?:es)?)\b', normalized)
        if m:
            n = _parse_num(m.group(1))
            if n:
                return RollingWindow(n=n * 30, unit="dia", is_average=is_average,
                                     date_column=date_column, status_filter=status_filter)

        # Padrao 3: "ultima semana" (singular, sem numero)
        m = re.search(rf'\b{_PREFIX}\s+semana\b', normalized)
        if m:
            return RollingWindow(n=7, unit="dia", is_average=is_average,
                                 date_column=date_column, status_filter=status_filter)

        # Padrao 4: "ultimas N semanas"
        m = re.search(rf'\b{_PREFIX}\s+{_NUM}\s+semana[s]?\b', normalized)
        if m:
            n = _parse_num(m.group(1))
            if n:
                return RollingWindow(n=n * 7, unit="dia", is_average=is_average,
                                     date_column=date_column, status_filter=status_filter)

        # Padrao 5: "ultimo mes" (singular, sem numero)
        m = re.search(rf'\b{_PREFIX}\s+mes\b', normalized)
        if m:
            return RollingWindow(n=30, unit="dia", is_average=is_average,
                                 date_column=date_column, status_filter=status_filter)

        # Padrao 6: "semana passada" / "semana anterior"
        if re.search(r'\bsemana\s+(?:passada|anterior)\b', normalized):
            return RollingWindow(n=7, unit="dia", is_average=is_average,
                                 date_column=date_column, status_filter=status_filter)

        # Padrao 7: "mes passado" / "mes anterior" — apenas quando e rolling intent
        # (evita conflito com extract_month; so ativa se nao houver contexto de mes especifico)
        if re.search(r'\bmes\s+(?:passado|anterior)\b', normalized):
            return RollingWindow(n=30, unit="dia", is_average=is_average,
                                 date_column=date_column, status_filter=status_filter)

        # ── Padroes de janela FUTURA (proximos N dias/semanas/meses) ─────────
        # Para janelas futuras: data = dDtVenc, status_filter=False, is_forward=True
        _FORWARD_DATE_COL = "'data'[dDtVenc]"

        # "proximos N dias" / "nos proximos N dias"
        m = re.search(r'\b(?:nos?\s+)?pr[oó]ximos?\s+(\d+)\s+dias?\b', text, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            return RollingWindow(n=n, unit="dia", is_average=is_average,
                                 date_column=_FORWARD_DATE_COL, status_filter=False,
                                 is_forward=True)

        # "proximas N semanas" / "nas proximas N semanas"
        m = re.search(r'\b(?:nas?\s+)?pr[oó]ximas?\s+(\d+)\s+semanas?\b', text, re.IGNORECASE)
        if m:
            n = int(m.group(1)) * 7
            return RollingWindow(n=n, unit="dia", is_average=is_average,
                                 date_column=_FORWARD_DATE_COL, status_filter=False,
                                 is_forward=True)

        # "proximos N meses"
        m = re.search(r'\b(?:nos?\s+)?pr[oó]ximos?\s+(\d+)\s+m[eê]ses?\b', text, re.IGNORECASE)
        if m:
            n = int(m.group(1)) * 30
            return RollingWindow(n=n, unit="dia", is_average=is_average,
                                 date_column=_FORWARD_DATE_COL, status_filter=False,
                                 is_forward=True)

        # "proxima semana" (singular, sem numero)
        if re.search(r'\bpr[oó]xima\s+semana\b', text, re.IGNORECASE):
            return RollingWindow(n=7, unit="dia", is_average=is_average,
                                 date_column=_FORWARD_DATE_COL, status_filter=False,
                                 is_forward=True)

        # "proximo mes" (singular, sem numero)
        if re.search(r'\bpr[oó]ximo\s+m[eê]s\b', text, re.IGNORECASE):
            return RollingWindow(n=30, unit="dia", is_average=is_average,
                                 date_column=_FORWARD_DATE_COL, status_filter=False,
                                 is_forward=True)

        return None

    # ── Helpers privados ─────────────────────────────────────────

    def _normalize_for_extraction(self, text: str) -> str:
        """Normaliza texto preservando acentos (necessarios para lookup de meses)."""
        return text.strip()

    def _remove_accents(self, text: str) -> str:
        """Remove acentos usando unicodedata."""
        import unicodedata
        nfkd = unicodedata.normalize("NFKD", text)
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    def _month_str_to_num(self, s: str) -> Optional[int]:
        """Converte string de mes (com ou sem acento) para numero."""
        s_clean = self._remove_accents(s.lower().strip())
        for key, num in MONTH_NAMES.items():
            if self._remove_accents(key) == s_clean:
                return num
        return None

    # ── YTD / MTD ────────────────────────────────────────────────

    def _detect_ytd(self, text: str) -> bool:
        normalized = text.lower()
        return bool(re.search(
            r'\b(?:ytd|year[\s-]to[\s-]date|acumulado\s+(?:do|no)\s+ano|no\s+ano\s+ate\s+agora)\b',
            normalized,
        ))

    def _detect_mtd(self, text: str) -> bool:
        normalized = text.lower()
        return bool(re.search(
            r'\b(?:mtd|month[\s-]to[\s-]date|acumulado\s+(?:do|no)\s+mes)\b',
            normalized,
        ))
