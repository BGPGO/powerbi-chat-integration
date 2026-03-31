"""
Dicionário estático do modelo de dados Omie ERP para Power BI.

Este módulo contém o mapeamento semântico completo entre:
- Termos de negócio em linguagem natural (português)
- Colunas calculadas, tabelas e filtros DAX do modelo Power BI

Fonte dos dados: API REST Omie via Power Query (M).
"""

# ─────────────────────────────────────────────────────────────────────────────
# COLUNAS CALCULADAS DO MODELO (definidas no Power BI)
# ─────────────────────────────────────────────────────────────────────────────

CALCULATED_COLUMNS = """
## COLUNAS CALCULADAS — TABELA `data`

Estas colunas já existem na tabela `data` do modelo Power BI e devem ser usadas
diretamente nas queries DAX (não recriar a lógica inline):

### `data[Valor único]`
```
IF(data[cStatus] IN {"PAGO", "RECEBIDO"}, data[nValPago], data[nValAberto])
```
- Se status é PAGO ou RECEBIDO → usa o valor efetivamente pago (`nValPago`)
- Caso contrário (A VENCER, ATRASADO, VENCE HOJE, PREVISAO, CANCELADO) → usa o valor em aberto (`nValAberto`)

### `data[pct rateio new]`
```
IF(data[categorias.nDistrPercentual] <> BLANK(), data[categorias.nDistrPercentual] / 100, 1)
```
- Percentual de rateio da categoria; se em branco, assume 100% (fator 1)

### `data[receita]`  ← COLUNA PRINCIPAL DE RECEITA
```
IF(data[cNatureza] = "R", data[Valor único], 0) * data[pct rateio new]
```
- Retorna o valor de receita já rateado; zero para despesas
- Use `SUM('data'[receita])` para somar receitas

### `data[despesas]`  ← COLUNA PRINCIPAL DE DESPESA
```
IF(data[cNatureza] = "P", data[Valor único] * (data[nDistrPercentual]/100) * (data[categorias.nDistrPercentual]/100), 0)
```
- Retorna o valor de despesa com duplo rateio aplicado; zero para receitas
- `nDistrPercentual`: percentual de distribuição do título
- `categorias.nDistrPercentual`: percentual de rateio da categoria
- Use `SUM('data'[despesas])` para somar despesas
"""

# ─────────────────────────────────────────────────────────────────────────────
# MEDIDAS DO MODELO
# ─────────────────────────────────────────────────────────────────────────────

MEASURES = """
## MEDIDAS (MEASURES) DO MODELO

### `[Valor líquido]`  ← RESULTADO / SALDO / LUCRO
```
SUM(data[receita]) - SUM(data[despesas])
```
- Use para: resultado, saldo, lucro, superávit, EBITDA (parcial — veja seção EBITDA)
- Positivo = lucro / Negativo = prejuízo
"""

# ─────────────────────────────────────────────────────────────────────────────
# DESCRIÇÃO DO MODELO DE DADOS
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_DESCRIPTION = """
## MODELO DE DADOS — OMIE ERP (Power BI)

### TABELA FATO: `data`
Fonte: API Omie `financas/mf/` (ListarMovimentos).
Cada linha = um título financeiro (conta a pagar ou a receber) com rateio aplicado.

#### Colunas brutas (vindas do Omie):
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `nValorTitulo` | Decimal | Valor nominal do título |
| `nValPago` | Decimal | Valor pago/recebido até hoje |
| `nValAberto` | Decimal | Valor ainda em aberto |
| `cNatureza` | Texto | **"R"** = Receita / **"P"** = Despesa (pagamento) |
| `cStatus` | Texto | Status do título — valores possíveis: `A VENCER`, `ATRASADO`, `CANCELADO`, `PAGO`, `PREVISAO`, `RECEBIDO`, `VENCE HOJE` |
| `dDtVenc` | Data | Data de vencimento (regime de **competência**) |
| `DataPagamento` | Data | Data de pagamento/recebimento (regime de **caixa**) |
| `Data auxiliar` | Data | Data auxiliar para relacionamento com DRE |
| `cCodCateg` | Texto | Código da categoria (FK → Categorias) |
| `nCodCliente` | Inteiro | Código do cliente (FK → Clientes) |
| `nCodCC` | Inteiro | Código da conta corrente (FK → Conta Corrente) |
| `cCodDepartamento` | Texto | Código do departamento/centro de custo |
| `nDistrPercentual` | Decimal | Percentual de distribuição do título (0–100) |
| `categorias.nDistrPercentual` | Decimal | Percentual de rateio da categoria (0–100 ou BLANK) |
| `Ano ` | Texto | Ano como texto — **ATENÇÃO: tem espaço no final** (ex: `"2026"`) |
| `Nome mês` | Texto | Nome do mês em português com inicial maiúscula |

#### Colunas calculadas (criadas no Power BI):
| Coluna | Descrição |
|--------|-----------|
| `Valor único` | `nValPago` se PAGO/RECEBIDO, senão `nValAberto` |
| `pct rateio new` | Percentual de rateio da categoria (ou 1 se BLANK) |
| `receita` | Valor de receita após rateio (zero se despesa) |
| `despesas` | Valor de despesa após duplo rateio (zero se receita) |

### TABELAS DIMENSÃO

#### `Categorias`
Fonte: API Omie `geral/categorias/`
| Coluna | Descrição |
|--------|-----------|
| `codigo` | Código interno da categoria |
| `Categorias` | Nome da categoria |
| `Grupo` | Grupo da categoria |
| `codigo_dre` | Código que vincula à estrutura DRE |

#### `Clientes`
Fonte: API Omie `geral/clientes/`
| Coluna | Descrição |
|--------|-----------|
| `codigo_cliente_omie` | Código do cliente |
| `razao_social` | Nome/razão social |
| `cnpj_cpf` | CNPJ ou CPF |

#### `Departamentos`
Fonte: API Omie `geral/departamentos/`
| Coluna | Descrição |
|--------|-----------|
| `data.funcao.codigo` | Código do departamento |
| `Centro de Custo` | Nome do centro de custo |

#### `dre`
Fonte: API Omie `geral/dre/`
| Coluna | Descrição |
|--------|-----------|
| `codigoDRE` | Código da linha DRE |
| `Natureza` | Nome da linha no DRE |

#### `Conta Corrente`
Fonte: API Omie `geral/contacorrente/`
| Coluna | Descrição |
|--------|-----------|
| `nCodCC` | Código da conta |
| `descricao` | Nome da conta (ex: "Bradesco PJ") |

### TABELAS RESUMO

#### `tabela_mes_caixa`
Resumo mensal por regime de **caixa** (agrupado por `DataPagamento`).
Contém `Receita` e `Despesa` por mês/ano para títulos PAGO/RECEBIDO.

#### `tabela_mes_competencia`
Resumo mensal por regime de **competência** (agrupado por `dDtVenc`).
Contém `Receita` e `Despesa` por mês/ano para todos os títulos.

### RELACIONAMENTOS
| De | Para |
|----|------|
| `data[auxCateg]` → `Categorias[codigo]` | categoria |
| `data[auxCli]` → `Clientes[codigo_cliente_omie]` | cliente |
| `data[auxConta]` → `Conta Corrente[nCodCC]` | conta corrente |
| `data[aux]` → `dre[codigoDRE]` | DRE |
| `data[cCodDepartamento]` → `Departamentos[data.funcao.codigo]` | departamento |
"""

# ─────────────────────────────────────────────────────────────────────────────
# MAPEAMENTO SEMÂNTICO
# ─────────────────────────────────────────────────────────────────────────────

SEMANTIC_MAPPINGS = """
## MAPEAMENTO SEMÂNTICO — LINGUAGEM NATURAL → DAX

### MÉTRICAS PRINCIPAIS

**DISTINÇÃO CRÍTICA — Faturamento vs Receita:**
- **Faturamento** = regime de COMPETÊNCIA → todos os títulos (qualquer status, exceto CANCELADO), data via `dDtVenc`
- **Receita** = regime de CAIXA → somente títulos PAGO ou RECEBIDO, data via `DataPagamento`

**Faturamento / Faturamento total / Faturamento bruto / Total faturado:**
→ `SUM('data'[receita])` — sem filtro de cStatus (inclui todos os títulos, exceto CANCELADO quando explícito)
→ Já inclui rateio. Não use `nValorTitulo` diretamente.
→ Use `dDtVenc` como coluna de data para filtros de período.

**Receita / Receita recebida / Receita realizada / Receita em caixa / Entrada de caixa:**
→ `CALCULATE(SUM('data'[receita]), 'data'[cStatus] IN {"PAGO", "RECEBIDO"})`
→ Somente títulos efetivamente recebidos.
→ Use `DataPagamento` como coluna de data para filtros de período.

**Receita prevista / a receber / pendente (competência):**
→ `CALCULATE(SUM('data'[receita]), 'data'[cStatus] IN {"A VENCER", "ATRASADO", "VENCE HOJE", "PREVISAO"})`

**Despesa total / Custos:**
→ `SUM('data'[despesas])`
→ Já inclui duplo rateio. Não use `nValorTitulo` diretamente.

**Despesa paga (caixa):**
→ `CALCULATE(SUM('data'[despesas]), 'data'[cStatus] IN {"PAGO", "RECEBIDO"})`

**Resultado / Saldo / Lucro / Valor líquido:**
→ `SUM('data'[receita]) - SUM('data'[despesas])`
→ Positivo = lucro / superávit. Negativo = prejuízo / déficit.

**Títulos cancelados (excluir do cálculo):**
→ `CALCULATE(..., 'data'[cStatus] <> "CANCELADO")`

### FILTROS DE PERÍODO

**Ano específico (ex: "2026", "de 2025"):**
→ `'data'[Ano ] = "2026"` ← ATENÇÃO: coluna `Ano ` tem **espaço** no final do nome

**Mês específico:**
→ `'data'[Nome mês] = "Janeiro"` ← inicial maiúscula

**Nomes de meses (exatos):**
Janeiro, Fevereiro, Março, Abril, Maio, Junho,
Julho, Agosto, Setembro, Outubro, Novembro, Dezembro

**Por data de pagamento (caixa) — intervalo:**
→ `'data'[DataPagamento] >= DATE(2026,1,1) && 'data'[DataPagamento] <= DATE(2026,3,31)`

**Por data de vencimento (competência) — intervalo:**
→ `'data'[dDtVenc] >= DATE(2026,1,1) && 'data'[dDtVenc] <= DATE(2026,3,31)`

### REGIME CONTÁBIL

**Regime de CAIXA (dinheiro que de fato entrou/saiu):**
→ Filtrar `'data'[cStatus] IN {"PAGO", "RECEBIDO"}`
→ Usar `DataPagamento` como coluna de data
→ Ou usar `tabela_mes_caixa` para resumos mensais

**Regime de COMPETÊNCIA (vencimentos, DRE, provisões):**
→ Sem filtro de status (inclui todos exceto CANCELADO)
→ Usar `dDtVenc` como coluna de data
→ Ou usar `tabela_mes_competencia` para resumos mensais

**Padrão (sem especificação):** use competência.

### AGRUPAMENTOS

**Por categoria:**
→ `SUMMARIZECOLUMNS('Categorias'[Categorias], ...)`

**Por grupo de categoria:**
→ `SUMMARIZECOLUMNS('Categorias'[Grupo], ...)`

**Por departamento / centro de custo:**
→ `SUMMARIZECOLUMNS('Departamentos'[Centro de Custo], ...)`

**Por cliente:**
→ `SUMMARIZECOLUMNS('Clientes'[razao_social], ...)`

**Por conta corrente / banco:**
→ `SUMMARIZECOLUMNS('Conta Corrente'[descricao], ...)`

**Por mês/ano:**
→ `SUMMARIZECOLUMNS('data'[Ano ], 'data'[Nome mês], ...)`

**Por natureza DRE:**
→ `SUMMARIZECOLUMNS('dre'[Natureza], ...)`

**Por status:**
→ `SUMMARIZECOLUMNS('data'[cStatus], ...)`

**Por janela de data (rolling window) — caixa:**
→ `CALCULATE(SUM('data'[receita]), 'data'[DataPagamento] >= TODAY() - 7, 'data'[DataPagamento] <= TODAY(), 'data'[cStatus] IN {"PAGO", "RECEBIDO"})`
→ Substitua 7 pelo número de dias desejado (ex: 14 = últimas 2 semanas, 30 = último mês)

**Por janela de data — competência:**
→ `CALCULATE(SUM('data'[receita]), 'data'[dDtVenc] >= TODAY() - 7, 'data'[dDtVenc] <= TODAY())`

**Média por semana (últimas N semanas):**
→ `DIVIDE(CALCULATE(SUM('data'[receita]), 'data'[DataPagamento] >= TODAY() - 21, 'data'[DataPagamento] <= TODAY(), 'data'[cStatus] IN {"PAGO", "RECEBIDO"}), 3)`
→ DIVIDE(total_periodo, numero_semanas)
"""

# ─────────────────────────────────────────────────────────────────────────────
# EXEMPLOS DE DAX
# ─────────────────────────────────────────────────────────────────────────────

DAX_EXAMPLES = """
## EXEMPLOS CONCRETOS — PERGUNTA → DAX

### Q: "Qual o faturamento total de 2026?" (competência — dDtVenc, todos os status)
```dax
EVALUATE
ROW(
    "Faturamento 2026",
    CALCULATE(
        SUM('data'[receita]),
        'data'[dDtVenc] >= DATE(2026,1,1),
        'data'[dDtVenc] <= DATE(2026,12,31)
    )
)
```

### Q: "Qual a receita de 2026?" / "Quanto recebemos em 2026?" (caixa — DataPagamento, PAGO/RECEBIDO)
```dax
EVALUATE
ROW(
    "Receita Caixa 2026",
    CALCULATE(
        SUM('data'[receita]),
        'data'[DataPagamento] >= DATE(2026,1,1),
        'data'[DataPagamento] <= DATE(2026,12,31),
        'data'[cStatus] IN {"PAGO", "RECEBIDO"}
    )
)
```

### Q: "Mostre faturamento e despesa por mês em 2026" (competência — dDtVenc)
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Nome mês],
    'data'[Ano ],
    FILTER(ALL('data'), 'data'[Ano ] = "2026"),
    "Faturamento", SUM('data'[receita]),
    "Despesa", SUM('data'[despesas]),
    "Resultado", SUM('data'[receita]) - SUM('data'[despesas])
)
```

### Q: "Mostre receita e despesa por mês em 2026" (caixa — DataPagamento, PAGO/RECEBIDO)
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Nome mês],
    'data'[Ano ],
    FILTER(ALL('data'), 'data'[Ano ] = "2026" && 'data'[cStatus] IN {"PAGO", "RECEBIDO"}),
    "Receita", CALCULATE(SUM('data'[receita]), 'data'[cStatus] IN {"PAGO", "RECEBIDO"}),
    "Despesa", CALCULATE(SUM('data'[despesas]), 'data'[cStatus] IN {"PAGO", "RECEBIDO"}),
    "Resultado", CALCULATE(SUM('data'[receita]) - SUM('data'[despesas]), 'data'[cStatus] IN {"PAGO", "RECEBIDO"})
)
```

### Q: "Despesa por categoria em março de 2026"
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Categorias'[Categorias],
    FILTER(ALL('data'), 'data'[Ano ] = "2026" && 'data'[Nome mês] = "Março"),
    "Despesa", SUM('data'[despesas])
)
ORDER BY [Despesa] DESC
```

### Q: "Top 10 clientes por receita"
```dax
EVALUATE
TOPN(
    10,
    SUMMARIZECOLUMNS(
        'Clientes'[razao_social],
        "Receita", SUM('data'[receita])
    ),
    [Receita], DESC
)
```

### Q: "Resultado por departamento em 2026"
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Departamentos'[Centro de Custo],
    FILTER(ALL('data'), 'data'[Ano ] = "2026"),
    "Receita", SUM('data'[receita]),
    "Despesa", SUM('data'[despesas]),
    "Resultado", SUM('data'[receita]) - SUM('data'[despesas])
)
ORDER BY [Resultado] DESC
```

### Q: "Quanto temos a receber em aberto (atrasado ou a vencer)?"
```dax
EVALUATE
ROW(
    "A Receber",
    CALCULATE(
        SUM('data'[receita]),
        'data'[cStatus] IN {"A VENCER", "ATRASADO", "VENCE HOJE"}
    )
)
```

### Q: "Receita por conta corrente em 2026"
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Conta Corrente'[descricao],
    FILTER(ALL('data'), 'data'[Ano ] = "2026"),
    "Receita", SUM('data'[receita]),
    "Despesa", SUM('data'[despesas])
)
```

### Q: "Qual a receita da última semana?"
```dax
EVALUATE
ROW(
    "Receita Última Semana",
    CALCULATE(
        SUM('data'[receita]),
        'data'[DataPagamento] >= TODAY() - 7,
        'data'[DataPagamento] <= TODAY(),
        'data'[cStatus] IN {"PAGO", "RECEBIDO"}
    )
)
```

### Q: "Qual foi a despesa das últimas 2 semanas?"
```dax
EVALUATE
ROW(
    "Despesa Últimas 2 Semanas",
    CALCULATE(
        SUM('data'[despesas]),
        'data'[DataPagamento] >= TODAY() - 14,
        'data'[DataPagamento] <= TODAY(),
        'data'[cStatus] IN {"PAGO", "RECEBIDO"}
    )
)
```

### Q: "Qual a média semanal de receita das últimas 3 semanas?"
```dax
EVALUATE
ROW(
    "Média Semanal Receita",
    DIVIDE(
        CALCULATE(
            SUM('data'[receita]),
            'data'[DataPagamento] >= TODAY() - 21,
            'data'[DataPagamento] <= TODAY(),
            'data'[cStatus] IN {"PAGO", "RECEBIDO"}
        ),
        3
    )
)
```
"""

# ─────────────────────────────────────────────────────────────────────────────
# REGRAS CRÍTICAS
# ─────────────────────────────────────────────────────────────────────────────

CRITICAL_RULES = """
## REGRAS CRÍTICAS — ERROS COMUNS A EVITAR

1. **NUNCA use `nValorTitulo` diretamente** — use `SUM('data'[receita])` ou `SUM('data'[despesas])`
   que já incluem o rateio correto.

2. **`cNatureza`**: APENAS dois valores válidos:
   - `"R"` = Receita
   - `"P"` = Despesa/Pagamento  ← NÃO é "D"

3. **Liquidado/pago NÃO usa `cLiquidado`** — usa `cStatus IN {"PAGO", "RECEBIDO"}`

4. **`cStatus`** — valores possíveis:
   `A VENCER`, `ATRASADO`, `CANCELADO`, `PAGO`, `PREVISAO`, `RECEBIDO`, `VENCE HOJE`
   - Realizados (caixa): `{"PAGO", "RECEBIDO"}`
   - Pendentes: `{"A VENCER", "ATRASADO", "VENCE HOJE"}`
   - Excluir sempre: `CANCELADO` (quando relevante)

5. **Coluna `Ano ` tem espaço no final** — escreva sempre `'data'[Ano ]` (com espaço antes de `]`)

6. **Nomes de meses com inicial maiúscula**: "Janeiro", "Março" — nunca minúsculo

7. **Sempre use `EVALUATE`** para iniciar a query DAX

8. **Para valor único**: use `ROW("Label", expressão)` — não use SUMMARIZE de uma só linha

9. **Tabelas com espaço** usam aspas simples: `'Conta Corrente'`, `'data'`

10. **Não recriar lógica das colunas calculadas** — use as colunas `receita`, `despesas`,
    `Valor único` e `pct rateio new` diretamente, pois já existem na tabela `data`

11. **FATURAMENTO ≠ RECEITA — regimes contábeis diferentes:**
    - **Faturamento** = competência: `SUM('data'[receita])` SEM filtro de cStatus, data via `dDtVenc`
    - **Receita** = caixa: `CALCULATE(SUM('data'[receita]), 'data'[cStatus] IN {"PAGO", "RECEBIDO"})`, data via `DataPagamento`
    - NUNCA use a mesma fórmula para as duas palavras.

12. **`data[valor]` NÃO EXISTE** — a coluna correta de receita é `data[receita]` e de despesa é `data[despesas]`.
    Nunca gere `data[valor]`, `data[Valor]`, `data[total]` ou qualquer variação.
    Sempre use `SUM('data'[receita])` para receitas e `SUM('data'[despesas])` para despesas.

12. **Para perguntas com janela temporal** ("última semana", "últimas N semanas", "últimos N dias"):
    - Use `TODAY()` como referência de data atual
    - Para caixa (pago/recebido): `'data'[DataPagamento] >= TODAY() - N && 'data'[DataPagamento] <= TODAY()`
    - Para competência/vencimento: `'data'[dDtVenc] >= TODAY() - N && 'data'[dDtVenc] <= TODAY()`
    - Semana = 7 dias, 2 semanas = 14 dias, mês = 30 dias
    - NUNCA use funções como DATEADD sem confirmar que a tabela de datas está configurada
"""

# ─────────────────────────────────────────────────────────────────────────────
# EBITDA — NOTA ARQUITETURAL
# ─────────────────────────────────────────────────────────────────────────────

EBITDA_NOTE = """
## EBITDA E MEDIDAS NATIVAS DO POWER BI

**[EBITDA]** já existe como medida no dataset Power BI.
Quando o usuário pedir EBITDA, use CALCULATE com os filtros solicitados:

```dax
-- "Qual o EBITDA de março de 2026?"
EVALUATE
CALCULATE(
    ROW("EBITDA", [EBITDA]),
    'data'[Ano ] = "2026",
    'data'[Nome mês] = "Março"
)
```

```dax
-- "EBITDA por mês em 2026"
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Nome mês],
    FILTER(ALL('data'), 'data'[Ano ] = "2026"),
    "EBITDA", [EBITDA]
)
```

**Regra:** NÃO recrie a lógica do EBITDA inline — sempre referencie `[EBITDA]` diretamente.
Quando o BI for republicado com lógica atualizada, o chat reflete automaticamente.
"""

# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÃO PÚBLICA
# ─────────────────────────────────────────────────────────────────────────────

def get_omie_context() -> str:
    """
    Retorna o dicionário completo do modelo Omie para injeção nos prompts DAX.
    CRITICAL_RULES vem primeiro para maximizar atenção do LLM.
    """
    return "\n".join([
        CRITICAL_RULES,
        CALCULATED_COLUMNS,
        MEASURES,
        SCHEMA_DESCRIPTION,
        SEMANTIC_MAPPINGS,
        DAX_EXAMPLES,
        EBITDA_NOTE,
    ])


def get_omie_schema() -> dict:
    """
    Retorna o schema estruturado do modelo Omie para o orchestrator,
    substituindo o fetch dinâmico da API do Power BI.
    """
    return {
        "dataset_id": None,
        "source": "omie_erp",
        "tables": [
            {
                "name": "data",
                "description": "Tabela fato — movimentação financeira com colunas calculadas de receita e despesa",
                "isHidden": False,
                "columns": [
                    {"name": "nValorTitulo", "dataType": "decimal", "description": "Valor nominal do título"},
                    {"name": "nValPago", "dataType": "decimal", "description": "Valor pago/recebido"},
                    {"name": "nValAberto", "dataType": "decimal", "description": "Valor em aberto"},
                    {"name": "cNatureza", "dataType": "text", "description": "R=Receita, P=Despesa"},
                    {"name": "cStatus", "dataType": "text", "description": "A VENCER|ATRASADO|CANCELADO|PAGO|PREVISAO|RECEBIDO|VENCE HOJE"},
                    {"name": "dDtVenc", "dataType": "dateTime", "description": "Data de vencimento (competência)"},
                    {"name": "DataPagamento", "dataType": "dateTime", "description": "Data de pagamento (caixa)"},
                    {"name": "Data auxiliar", "dataType": "dateTime", "description": "Data auxiliar para DRE"},
                    {"name": "cCodCateg", "dataType": "text", "description": "Código da categoria"},
                    {"name": "nCodCliente", "dataType": "int64", "description": "Código do cliente"},
                    {"name": "nCodCC", "dataType": "int64", "description": "Código da conta corrente"},
                    {"name": "cCodDepartamento", "dataType": "text", "description": "Código do departamento"},
                    {"name": "nDistrPercentual", "dataType": "decimal", "description": "Percentual de distribuição do título"},
                    {"name": "categorias.nDistrPercentual", "dataType": "decimal", "description": "Percentual de rateio da categoria"},
                    {"name": "Ano ", "dataType": "text", "description": "Ano como texto (espaço no final do nome!)"},
                    {"name": "Nome mês", "dataType": "text", "description": "Nome do mês em português"},
                    {"name": "Valor único", "dataType": "decimal", "description": "CALCULADA: nValPago se PAGO/RECEBIDO, senão nValAberto"},
                    {"name": "pct rateio new", "dataType": "decimal", "description": "CALCULADA: percentual de rateio da categoria (fator 0–1)"},
                    {"name": "receita", "dataType": "decimal", "description": "CALCULADA: valor de receita após rateio — USE ESTA para somar receitas"},
                    {"name": "despesas", "dataType": "decimal", "description": "CALCULADA: valor de despesa após duplo rateio — USE ESTA para somar despesas"},
                    {"name": "auxCateg", "dataType": "text", "description": "FK → Categorias[codigo]"},
                    {"name": "auxCli", "dataType": "text", "description": "FK → Clientes[codigo_cliente_omie]"},
                    {"name": "auxConta", "dataType": "int64", "description": "FK → Conta Corrente[nCodCC]"},
                    {"name": "aux", "dataType": "text", "description": "FK → dre[codigoDRE]"},
                ],
                "measures": [
                    {"name": "Valor líquido", "expression": "SUM(data[receita]) - SUM(data[despesas])"},
                ],
            },
            {
                "name": "Categorias",
                "description": "Categorias financeiras",
                "isHidden": False,
                "columns": [
                    {"name": "codigo", "dataType": "text"},
                    {"name": "Categorias", "dataType": "text"},
                    {"name": "Grupo", "dataType": "text"},
                    {"name": "codigo_dre", "dataType": "text"},
                ],
                "measures": [],
            },
            {
                "name": "Clientes",
                "description": "Cadastro de clientes/fornecedores",
                "isHidden": False,
                "columns": [
                    {"name": "codigo_cliente_omie", "dataType": "int64"},
                    {"name": "razao_social", "dataType": "text"},
                    {"name": "cnpj_cpf", "dataType": "text"},
                ],
                "measures": [],
            },
            {
                "name": "Departamentos",
                "description": "Centros de custo e departamentos",
                "isHidden": False,
                "columns": [
                    {"name": "data.funcao.codigo", "dataType": "text"},
                    {"name": "Centro de Custo", "dataType": "text"},
                ],
                "measures": [],
            },
            {
                "name": "dre",
                "description": "Estrutura DRE",
                "isHidden": False,
                "columns": [
                    {"name": "codigoDRE", "dataType": "text"},
                    {"name": "Natureza", "dataType": "text"},
                ],
                "measures": [],
            },
            {
                "name": "Conta Corrente",
                "description": "Contas bancárias e caixas",
                "isHidden": False,
                "columns": [
                    {"name": "nCodCC", "dataType": "int64"},
                    {"name": "descricao", "dataType": "text"},
                ],
                "measures": [],
            },
            {
                "name": "tabela_mes_caixa",
                "description": "Resumo mensal — regime de caixa",
                "isHidden": False,
                "columns": [
                    {"name": "Ano ", "dataType": "text"},
                    {"name": "Nome mês", "dataType": "text"},
                    {"name": "Receita", "dataType": "decimal"},
                    {"name": "Despesa", "dataType": "decimal"},
                ],
                "measures": [],
            },
            {
                "name": "tabela_mes_competencia",
                "description": "Resumo mensal — regime de competência",
                "isHidden": False,
                "columns": [
                    {"name": "Ano ", "dataType": "text"},
                    {"name": "Nome mês", "dataType": "text"},
                    {"name": "Receita", "dataType": "decimal"},
                    {"name": "Despesa", "dataType": "decimal"},
                ],
                "measures": [],
            },
        ],
        "relationships": [
            {"fromTable": "data", "fromColumn": "auxCateg", "toTable": "Categorias", "toColumn": "codigo"},
            {"fromTable": "data", "fromColumn": "auxCli", "toTable": "Clientes", "toColumn": "codigo_cliente_omie"},
            {"fromTable": "data", "fromColumn": "auxConta", "toTable": "Conta Corrente", "toColumn": "nCodCC"},
            {"fromTable": "data", "fromColumn": "aux", "toTable": "dre", "toColumn": "codigoDRE"},
            {"fromTable": "data", "fromColumn": "cCodDepartamento", "toTable": "Departamentos", "toColumn": "data.funcao.codigo"},
        ],
    }
