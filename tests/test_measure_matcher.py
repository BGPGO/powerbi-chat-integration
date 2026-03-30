"""
Testes para MeasureMatcher e FilterExtractor.
Roda com: pytest tests/test_measure_matcher.py -v
"""

import pytest
from app.agents.measure_matcher import MeasureMatcher, MeasureMatch, _normalize, _best_fuzzy_score
from app.agents.filter_extractor import FilterExtractor, TemporalFilter


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_MEASURES = [
    "EBITDA",
    "Total CAPEX",
    "Margem Bruta %",
    "Receita Operacional Liquida",
    "CMV",
    "Resultado Financeiro",
    "Valor líquido",
    "Inadimplência",
    "Ticket Medio",
    "OPEX",
    "Despesa Administrativa",
    "Impostos Totais",
]

SAMPLE_ALIASES = {
    "EBITDA": ["ebitda", "resultado operacional", "lucro operacional", "lajida"],
    "Total CAPEX": ["capex", "investimentos de capital"],
    "CMV": ["custo da mercadoria vendida", "custo dos produtos"],
}


@pytest.fixture
def matcher():
    return MeasureMatcher(measures=SAMPLE_MEASURES, aliases=SAMPLE_ALIASES)


@pytest.fixture
def extractor():
    return FilterExtractor(current_year=2026)


# ─────────────────────────────────────────────────────────────────────────────
# Testes de normalizacao
# ─────────────────────────────────────────────────────────────────────────────

class TestNormalization:
    def test_remove_accents(self):
        assert _normalize("Março") == "marco"
        assert _normalize("Inadimplência") == "inadimplencia"
        assert _normalize("líquido") == "liquido"

    def test_lowercase(self):
        assert _normalize("CAPEX") == "capex"
        assert _normalize("Total CAPEX") == "total capex"

    def test_special_chars(self):
        assert _normalize("[Total CAPEX]") == "total capex"
        assert _normalize("Margem Bruta %") == "margem bruta"

    def test_multiple_spaces(self):
        assert _normalize("receita   operacional") == "receita operacional"


# ─────────────────────────────────────────────────────────────────────────────
# Testes de MeasureMatcher
# ─────────────────────────────────────────────────────────────────────────────

class TestMeasureMatcher:

    # ── Casos de match direto por alias ──

    def test_alias_exact_capex(self, matcher):
        result = matcher.match("qual foi meu capex total em 2025?")
        assert result.measure_name == "Total CAPEX"
        assert result.confidence >= 0.80
        assert result.method == "synonym"

    def test_alias_exact_ebitda(self, matcher):
        result = matcher.match("ebitda de março")
        assert result.measure_name == "EBITDA"
        assert result.confidence >= 0.90

    def test_alias_investimentos_capital(self, matcher):
        result = matcher.match("quanto foi o investimentos de capital em 2025?")
        assert result.measure_name == "Total CAPEX"
        assert result.confidence >= 0.80

    def test_alias_lajida(self, matcher):
        result = matcher.match("qual o lajida do primeiro trimestre?")
        assert result.measure_name == "EBITDA"

    def test_alias_custo_mercadoria(self, matcher):
        result = matcher.match("custo da mercadoria vendida em janeiro")
        assert result.measure_name == "CMV"

    # ── Casos de match por sinonimos empresariais ──

    def test_synonym_ativo_fixo_to_capex(self, matcher):
        result = matcher.match("quanto gastamos em ativo fixo?")
        assert result.measure_name == "Total CAPEX"
        assert result.method == "synonym"

    def test_synonym_despesas_operacionais_to_opex(self, matcher):
        result = matcher.match("quais foram as despesas operacionais?")
        assert result.measure_name == "OPEX"

    def test_synonym_impostos(self, matcher):
        result = matcher.match("total de tributos pagos em 2025")
        # "tributos" e sinonimo de "impostos" → fuzzy contra "Impostos Totais"
        assert result.measure_name == "Impostos Totais"

    # ── Casos de fuzzy match direto ──

    def test_fuzzy_margem_bruta(self, matcher):
        result = matcher.match("qual a margem bruta?")
        assert result.measure_name == "Margem Bruta %"
        assert result.confidence >= 0.60

    def test_fuzzy_resultado_financeiro(self, matcher):
        result = matcher.match("resultado financeiro de 2025")
        assert result.measure_name == "Resultado Financeiro"
        assert result.confidence >= 0.70

    def test_fuzzy_ticket_medio(self, matcher):
        result = matcher.match("qual o ticket medio?")
        assert result.measure_name == "Ticket Medio"

    # ── Casos de confianca baixa (necessitaria LLM) ──

    def test_low_confidence_vague_question(self, matcher):
        result = matcher.match("como estamos indo?")
        # "estamos" and "indo" are stopwords, so no meaningful intent extracted
        assert result.confidence < 0.80

    def test_low_confidence_unrelated(self, matcher):
        result = matcher.match("qual o clima hoje?")
        # "clima" and "hoje" are stopwords, leaving nothing meaningful
        assert result.confidence < 0.60

    # ── Factory methods ──

    def test_from_measures_json(self):
        measures_data = [
            {"name": "EBITDA", "type": "native", "aliases": ["ebitda", "lajida"]},
            {"name": "Total CAPEX", "type": "native", "aliases": ["capex"]},
        ]
        m = MeasureMatcher.from_measures_json(measures_data)
        result = m.match("qual o capex?")
        assert result.measure_name == "Total CAPEX"

    def test_from_schema_and_measures(self):
        schema = {
            "tables": [
                {
                    "name": "data",
                    "measures": [
                        {"name": "Valor líquido", "expression": "SUM(data[receita]) - SUM(data[despesas])"},
                    ],
                }
            ]
        }
        custom = [
            {"name": "EBITDA", "type": "native", "aliases": ["ebitda"]},
        ]
        m = MeasureMatcher.from_schema_and_measures(schema, custom)
        assert "Valor líquido" in m.measures
        assert "EBITDA" in m.measures

    # ── needs_llm_fallback ──

    def test_needs_llm_fallback_false(self, matcher):
        # "capex" tem alias direto → alta confianca
        assert not matcher.needs_llm_fallback("qual o capex de 2025?")

    def test_needs_llm_fallback_true(self, matcher):
        assert matcher.needs_llm_fallback("como andam nossos indicadores?")

    # ── Catalogo vazio ──

    def test_empty_catalog(self):
        m = MeasureMatcher(measures=[])
        result = m.match("capex de 2025")
        assert result.confidence == 0.0
        assert result.method == "none"


# ─────────────────────────────────────────────────────────────────────────────
# Testes de FilterExtractor
# ─────────────────────────────────────────────────────────────────────────────

class TestFilterExtractor:

    # ── Extracao de ano ──

    def test_year_explicit(self, extractor):
        f = extractor.extract("receita de 2025")
        assert f.year == "2025"

    def test_year_ano_passado(self, extractor):
        f = extractor.extract("faturamento do ano passado")
        assert f.year == "2025"  # current=2026

    def test_year_ano_retrasado(self, extractor):
        f = extractor.extract("despesas do ano retrasado")
        assert f.year == "2024"

    def test_year_este_ano(self, extractor):
        f = extractor.extract("receita deste ano")
        assert f.year == "2026"

    def test_year_default(self, extractor):
        f = extractor.extract("qual o faturamento total?")
        assert f.year == "2026"
        assert f.default_applied is True

    def test_year_proximo_ano(self, extractor):
        f = extractor.extract("previsao para proximo ano")
        assert f.year == "2027"

    # ── Extracao de mes ──

    def test_month_full_name(self, extractor):
        f = extractor.extract("receita de janeiro de 2025")
        assert f.month == "Janeiro"
        assert f.month_number == 1
        assert f.year == "2025"

    def test_month_abbreviated(self, extractor):
        f = extractor.extract("despesa em mar de 2025")
        assert f.month == "Março"

    def test_month_with_accent(self, extractor):
        f = extractor.extract("faturamento de março")
        assert f.month == "Março"

    def test_month_without_accent(self, extractor):
        f = extractor.extract("faturamento de marco")
        assert f.month == "Março"

    # ── Extracao de trimestre ──

    def test_quarter_q1(self, extractor):
        f = extractor.extract("capex no Q1 de 2025")
        assert f.quarter == 1
        assert f.months_in_range == ["Janeiro", "Fevereiro", "Março"]
        assert f.year == "2025"

    def test_quarter_primeiro_trimestre(self, extractor):
        f = extractor.extract("despesas do primeiro trimestre")
        assert f.quarter == 1

    def test_quarter_t3(self, extractor):
        f = extractor.extract("resultado do T3")
        assert f.quarter == 3

    def test_quarter_4o_tri(self, extractor):
        f = extractor.extract("receita do 4o trimestre de 2025")
        assert f.quarter == 4
        assert f.months_in_range == ["Outubro", "Novembro", "Dezembro"]

    # ── Extracao de semestre ──

    def test_semester_first(self, extractor):
        f = extractor.extract("resultado do primeiro semestre")
        assert f.months_in_range == ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho"]

    def test_semester_s2(self, extractor):
        f = extractor.extract("despesas do S2 de 2025")
        assert f.months_in_range == ["Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

    # ── Range de meses ──

    def test_month_range_jan_to_mar(self, extractor):
        f = extractor.extract("receita de janeiro a março de 2025")
        assert f.months_in_range == ["Janeiro", "Fevereiro", "Março"]

    def test_month_range_entre_abril_e_junho(self, extractor):
        f = extractor.extract("despesas entre abril e junho")
        assert f.months_in_range == ["Abril", "Maio", "Junho"]

    # ── Comparacao ──

    def test_comparison_vs(self, extractor):
        f = extractor.extract("receita 2025 vs 2024")
        assert f.is_comparison is True
        assert f.comparison_year == "2024"

    def test_comparison_crescimento(self, extractor):
        f = extractor.extract("crescimento de receita comparado com ano passado")
        assert f.is_comparison is True
        assert f.comparison_year == "2025"

    # ── DAX filter generation ──

    def test_dax_filters_year_only(self, extractor):
        f = extractor.extract("receita de 2025")
        filters = f.to_dax_filters()
        assert "'data'[Ano ] = \"2025\"" in filters

    def test_dax_filters_year_and_month(self, extractor):
        f = extractor.extract("receita de março de 2025")
        filters = f.to_dax_filters()
        assert "'data'[Ano ] = \"2025\"" in filters
        assert "'data'[Nome mês] = \"Março\"" in filters

    def test_dax_filters_quarter(self, extractor):
        f = extractor.extract("receita do Q1 de 2025")
        filters = f.to_dax_filters()
        assert "'data'[Ano ] = \"2025\"" in filters
        # Quarter gera months_in_range, que vira IN clause
        filter_str = f.to_dax_filter_string()
        assert "IN" in filter_str

    # ── extract_date_range ──

    def test_date_range_full_year(self, extractor):
        r = extractor.extract_date_range("receita de 2025")
        assert r == {"start_year": 2025, "start_month": 1, "end_year": 2025, "end_month": 12}

    def test_date_range_single_month(self, extractor):
        r = extractor.extract_date_range("receita de março de 2025")
        assert r == {"start_year": 2025, "start_month": 3, "end_year": 2025, "end_month": 3}

    def test_date_range_quarter(self, extractor):
        r = extractor.extract_date_range("receita do Q1 de 2025")
        assert r == {"start_year": 2025, "start_month": 1, "end_year": 2025, "end_month": 3}

    # ── YTD / MTD ──

    def test_ytd(self, extractor):
        f = extractor.extract("receita acumulado no ano")
        assert f.is_ytd is True

    def test_mtd(self, extractor):
        f = extractor.extract("despesas acumulado no mes")
        assert f.is_mtd is True


# ─────────────────────────────────────────────────────────────────────────────
# Testes de integracao: MeasureMatcher + FilterExtractor juntos
# ─────────────────────────────────────────────────────────────────────────────

class TestIntegration:

    def test_capex_2025_full_pipeline(self, matcher, extractor):
        question = "qual foi meu capex total em 2025?"

        measure = matcher.match(question)
        filters = extractor.extract(question)

        assert measure.measure_name == "Total CAPEX"
        assert measure.confidence >= 0.80
        assert filters.year == "2025"
        assert filters.month is None

    def test_ebitda_q1_2025(self, matcher, extractor):
        question = "ebitda do primeiro trimestre de 2025"

        measure = matcher.match(question)
        filters = extractor.extract(question)

        assert measure.measure_name == "EBITDA"
        assert filters.year == "2025"
        assert filters.quarter == 1
        assert filters.months_in_range == ["Janeiro", "Fevereiro", "Março"]

    def test_margem_bruta_ano_passado(self, matcher, extractor):
        question = "margem bruta do ano passado"

        measure = matcher.match(question)
        filters = extractor.extract(question)

        assert measure.measure_name == "Margem Bruta %"
        assert filters.year == "2025"

    def test_inadimplencia_março(self, matcher, extractor):
        question = "qual a inadimplência em março de 2025?"

        measure = matcher.match(question)
        filters = extractor.extract(question)

        assert measure.measure_name == "Inadimplência"
        assert filters.year == "2025"
        assert filters.month == "Março"
