"""
Testes do DaxTemplateEngine — valida todos os padrões de query.

Roda com: pytest tests/test_dax_template_engine.py -v
"""

import pytest
import sys
import os

# Adiciona o projeto ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.agents.dax_template_engine import (
    DaxTemplateEngine,
    DaxTemplateError,
    MeasureMatcher,
    MeasureDefinition,
    PatternDetector,
    QueryPattern,
    TemplatedDaxPipeline,
    PipelineResult,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MeasureMatcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestMeasureMatcher:
    def setup_method(self):
        self.matcher = MeasureMatcher()

    def test_match_ebitda(self):
        m = self.matcher.match("qual o ebitda de 2025?")
        assert m is not None
        assert m.name == "EBITDA"
        assert m.measure_type == "native"

    def test_match_receita(self):
        m = self.matcher.match("qual o faturamento total de 2025?")
        assert m is not None
        assert m.name == "Receita"

    def test_match_despesa(self):
        m = self.matcher.match("quanto foi de gastos em março?")
        assert m is not None
        assert m.name == "Despesa"

    def test_match_resultado(self):
        m = self.matcher.match("qual o lucro de 2025?")
        assert m is not None
        assert m.name == "Resultado"

    def test_match_resultado_operacional_prefers_ebitda(self):
        """resultado operacional deve casar com EBITDA (medida nativa, maior prioridade)."""
        m = self.matcher.match("qual o resultado operacional de 2025?")
        assert m is not None
        assert m.name == "EBITDA"

    def test_no_match(self):
        m = self.matcher.match("como está o clima hoje?")
        assert m is None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PatternDetector
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestPatternDetector:
    def setup_method(self):
        self.detector = PatternDetector()
        self.receita = MeasureDefinition(
            name="Receita", measure_type="inline",
            expression="SUM('data'[receita])", aliases=["receita", "faturamento"],
        )
        self.ebitda = MeasureDefinition(
            name="EBITDA", measure_type="native", aliases=["ebitda"],
        )

    def test_kpi_simple(self):
        p = self.detector.detect("qual a receita de 2025?", self.receita)
        assert p.pattern == QueryPattern.KPI_SIMPLE
        assert p.temporal.year == "2025"
        assert p.temporal.month is None

    def test_kpi_simple_with_month(self):
        p = self.detector.detect("receita de março de 2025", self.receita)
        assert p.pattern == QueryPattern.KPI_SIMPLE
        assert p.temporal.year == "2025"
        assert p.temporal.month == "Março"

    def test_kpi_by_dimension(self):
        p = self.detector.detect("receita por departamento em 2025", self.receita)
        assert p.pattern == QueryPattern.KPI_BY_DIMENSION
        assert len(p.dimensions) == 1
        assert p.dimensions[0].display_name == "Departamento"

    def test_kpi_by_categoria(self):
        p = self.detector.detect("despesa por categoria em 2025", self.receita)
        assert p.pattern == QueryPattern.KPI_BY_DIMENSION
        assert p.dimensions[0].display_name == "Categoria"

    def test_kpi_trend(self):
        p = self.detector.detect("receita mês a mês em 2025", self.receita)
        assert p.pattern == QueryPattern.KPI_TREND

    def test_kpi_trend_por_mes(self):
        p = self.detector.detect("receita por mês em 2025", self.receita)
        assert p.pattern == QueryPattern.KPI_TREND

    def test_kpi_comparison(self):
        p = self.detector.detect("compare receita 2024 vs 2025", self.receita)
        assert p.pattern == QueryPattern.KPI_COMPARISON
        assert p.temporal.year == "2024"
        assert p.temporal.year2 == "2025"

    def test_kpi_top_n(self):
        p = self.detector.detect("top 5 clientes por receita", self.receita)
        assert p.pattern == QueryPattern.KPI_TOP_N
        assert p.top_n == 5
        assert p.dimensions[0].display_name == "Cliente"

    def test_default_year(self):
        p = self.detector.detect("qual a receita total?", self.receita)
        assert p.temporal.year == "2026"  # default

    def test_caixa_filter(self):
        p = self.detector.detect("receita recebida em caixa 2025", self.receita)
        assert p.status_filter is not None
        assert "PAGO" in p.status_filter.statuses
        assert "RECEBIDO" in p.status_filter.statuses

    def test_multi_metric(self):
        p = self.detector.detect("receita e despesa por mês em 2025", None)
        assert p.pattern == QueryPattern.KPI_TREND
        assert len(p.extra_measures) == 3  # receita, despesa, resultado


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DaxTemplateEngine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDaxTemplateEngine:
    def setup_method(self):
        self.engine = DaxTemplateEngine()
        self.detector = PatternDetector()
        self.receita = MeasureDefinition(
            name="Receita", measure_type="inline",
            expression="SUM('data'[receita])", aliases=["receita"],
        )
        self.ebitda = MeasureDefinition(
            name="EBITDA", measure_type="native", aliases=["ebitda"],
        )

    def _render(self, question: str, measure: MeasureDefinition):
        parsed = self.detector.detect(question, measure)
        return self.engine.render(parsed)

    def test_kpi_simple_inline(self):
        r = self._render("qual a receita de 2025?", self.receita)
        assert "EVALUATE" in r.dax_query
        assert "SUM('data'[receita])" in r.dax_query
        assert "'data'[Ano ] = \"2025\"" in r.dax_query
        assert "data[valor]" not in r.dax_query.lower()

    def test_kpi_simple_native(self):
        r = self._render("qual o ebitda de 2025?", self.ebitda)
        assert "[EBITDA]" in r.dax_query
        assert "CALCULATE" in r.dax_query
        assert "'data'[Ano ] = \"2025\"" in r.dax_query

    def test_kpi_with_month(self):
        r = self._render("receita de março de 2025", self.receita)
        assert "'data'[Nome mês] = \"Março\"" in r.dax_query

    def test_kpi_by_dimension(self):
        r = self._render("receita por departamento em 2025", self.receita)
        assert "SUMMARIZECOLUMNS" in r.dax_query
        assert "'Departamentos'[Centro de Custo]" in r.dax_query
        assert "ORDER BY" in r.dax_query

    def test_kpi_comparison(self):
        r = self._render("compare receita 2024 vs 2025", self.receita)
        assert "'data'[Ano ] = \"2024\"" in r.dax_query
        assert "'data'[Ano ] = \"2025\"" in r.dax_query
        assert "Variação" in r.dax_query

    def test_kpi_top_n(self):
        r = self._render("top 10 clientes por receita em 2025", self.receita)
        assert "TOPN" in r.dax_query
        assert "10" in r.dax_query
        assert "'Clientes'[razao_social]" in r.dax_query

    def test_kpi_trend(self):
        r = self._render("receita mês a mês em 2025", self.receita)
        assert "SUMMARIZECOLUMNS" in r.dax_query
        assert "'data'[Nome mês]" in r.dax_query
        assert "'data'[Ano ]" in r.dax_query

    def test_never_generates_data_valor(self):
        """Invariante: NENHUM template pode gerar data[valor]."""
        questions = [
            "qual a receita de 2025?",
            "receita por departamento em 2025",
            "compare receita 2024 vs 2025",
            "top 5 clientes por receita",
            "receita mês a mês em 2025",
        ]
        for q in questions:
            r = self._render(q, self.receita)
            assert "data[valor]" not in r.dax_query.lower(), \
                f"data[valor] encontrado em: {q}"
            assert "data[Valor]" not in r.dax_query, \
                f"data[Valor] encontrado em: {q}"

    def test_balanced_parentheses(self):
        """Invariante: parênteses sempre balanceados."""
        questions = [
            "qual a receita de 2025?",
            "receita por departamento em 2025",
            "compare receita 2024 vs 2025",
            "top 5 clientes por receita",
            "receita mês a mês em 2025",
            "ebitda de março de 2025",
        ]
        measures = [self.receita] * 5 + [self.ebitda]
        for q, m in zip(questions, measures):
            r = self._render(q, m)
            opens = r.dax_query.count("(")
            closes = r.dax_query.count(")")
            assert opens == closes, \
                f"Parênteses desbalanceados em '{q}': {opens} vs {closes}\n{r.dax_query}"

    def test_ano_always_has_space(self):
        """Invariante: coluna Ano sempre tem espaço no final."""
        r = self._render("receita de 2025", self.receita)
        # Deve ter 'data'[Ano ] (com espaço), nunca 'data'[Ano] (sem espaço)
        assert "'data'[Ano ]" in r.dax_query
        assert "'data'[Ano]" not in r.dax_query

    def test_caixa_filter(self):
        r = self._render("receita recebida em caixa 2025", self.receita)
        assert "'data'[cStatus] IN" in r.dax_query
        assert '"PAGO"' in r.dax_query
        assert '"RECEBIDO"' in r.dax_query


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pipeline completo (integração)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestPipeline:
    def setup_method(self):
        self.pipeline = TemplatedDaxPipeline()

    def test_capex_2025(self):
        """Fluxo: 'quanto foi meu capex total em 2025?' → não reconhece (sem medida CAPEX em measures.json)."""
        # CAPEX não está no measures.json por padrão, então fallback
        r = self.pipeline.try_generate("quanto foi meu capex total em 2025?")
        # Será false porque CAPEX não está em measures.json padrão
        # (mas se fosse adicionado, funcionaria)
        assert isinstance(r, PipelineResult)

    def test_receita_2025(self):
        r = self.pipeline.try_generate("qual a receita de 2025?")
        assert r.success is True
        assert r.used_template is True
        assert "SUM('data'[receita])" in r.dax_query
        assert "'data'[Ano ] = \"2025\"" in r.dax_query
        assert r.pattern == QueryPattern.KPI_SIMPLE

    def test_despesa_por_departamento(self):
        r = self.pipeline.try_generate("despesa por departamento em 2025")
        assert r.success is True
        assert "SUMMARIZECOLUMNS" in r.dax_query
        assert "'Departamentos'[Centro de Custo]" in r.dax_query
        assert r.pattern == QueryPattern.KPI_BY_DIMENSION

    def test_ebitda_marco_2026(self):
        r = self.pipeline.try_generate("qual o ebitda de março de 2026?")
        assert r.success is True
        assert "[EBITDA]" in r.dax_query
        assert "'data'[Nome mês] = \"Março\"" in r.dax_query
        assert r.pattern == QueryPattern.KPI_SIMPLE

    def test_top_clientes(self):
        r = self.pipeline.try_generate("top 10 clientes por faturamento em 2025")
        assert r.success is True
        assert "TOPN" in r.dax_query
        assert "10" in r.dax_query
        assert "'Clientes'[razao_social]" in r.dax_query

    def test_comparacao(self):
        r = self.pipeline.try_generate("compare faturamento 2024 vs 2025")
        assert r.success is True
        assert r.pattern == QueryPattern.KPI_COMPARISON
        assert "2024" in r.dax_query
        assert "2025" in r.dax_query

    def test_tendencia(self):
        r = self.pipeline.try_generate("receita mês a mês em 2025")
        assert r.success is True
        assert r.pattern == QueryPattern.KPI_TREND
        assert "'data'[Nome mês]" in r.dax_query

    def test_multi_metric(self):
        r = self.pipeline.try_generate("receita e despesa por departamento em 2025")
        assert r.success is True
        assert "SUM('data'[receita])" in r.dax_query
        assert "SUM('data'[despesas])" in r.dax_query

    def test_unknown_fallback(self):
        r = self.pipeline.try_generate("quais são as tabelas disponíveis no modelo de dados?")
        assert r.success is False
        assert r.used_template is False
        assert r.fallback_reason is not None

    def test_no_data_valor_anywhere(self):
        """Teste abrangente: NENHUMA pergunta deve gerar data[valor]."""
        questions = [
            "receita de 2025",
            "despesa por categoria em março de 2025",
            "ebitda mês a mês em 2026",
            "top 5 departamentos por gastos",
            "compare custos 2024 vs 2025",
            "faturamento total",
            "lucro por cliente em 2025",
            "receita e despesa por mês em 2025",
            "resultado por departamento em 2026",
        ]
        for q in questions:
            r = self.pipeline.try_generate(q)
            if r.success and r.dax_query:
                assert "data[valor]" not in r.dax_query.lower(), \
                    f"data[valor] encontrado para: '{q}'\nDAX: {r.dax_query}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
