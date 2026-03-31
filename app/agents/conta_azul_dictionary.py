"""
Dicionário estático do modelo de dados Conta Azul para Power BI.

Mapeamento semântico entre linguagem natural e colunas/filtros DAX
para datasets gerados pelo ERP Conta Azul — validado contra o schema real
do BI_Otero (dataset ca26e66f-6bbd-4273-9de7-9e13e720c839).
"""

# ─────────────────────────────────────────────────────────────────────────────
# DESCRIÇÃO DO MODELO DE DADOS
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_DESCRIPTION = """
## MODELO DE DADOS — CONTA AZUL ERP (Power BI — BI_Otero)

### TABELA FATO PRINCIPAL: `data`
Cada linha = um lançamento financeiro (conta a pagar ou a receber).

#### Colunas principais (nomes EXATOS como aparecem no dataset):
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `Receita/Despesa` | Texto | **"Receita"** = entrada / **"Despesa"** = saída |
| `Tipo da operação` | Texto | **"Crédito"** = receita / outros = despesa |
| `Situação` | Texto | Status do lançamento — veja valores abaixo |
| `receita` | Decimal | Coluna calculada — valor de receita (0 se for despesa) |
| `despesas` | Decimal | Coluna calculada — valor de despesa (0 se for receita) |
| `receita competencia` | Decimal | Receita por competência |
| `despesas competencia` | Decimal | Despesa por competência |
| `Valor (R$)` | Decimal | Valor bruto do lançamento |
| `Valor` | Decimal | Valor distribuído (centro de custo) |
| `liquido` | Decimal | Valor líquido |
| `Data de competência` | Data | Data de competência do lançamento |
| `Data movimento` | Data | Data de pagamento/recebimento efetivo (regime de **caixa**) |
| `Data original de vencimento` | Data | Data de vencimento original |
| `Ano_mes` | Texto | Período caixa no formato "ANOmes" — ex: "2025dezembro" |
| `Ano_mes competencia` | Texto | Período competência — ex: "2025dezembro" |
| `Categoria` | Texto | Categoria do lançamento |
| `Categoria 1` | Texto | Categoria principal |
| `Conta contabil` | Texto | Conta contábil (DRE) |
| `Nome do fornecedor/cliente` | Texto | Nome do cliente ou fornecedor |
| `Conta bancária` | Texto | Conta bancária / caixa |
| `CentroCusto` | Texto | Centro de custo |
| `Previsto/realizado` | Texto | "Pendente" = não pago / "Realizado" = pago |

#### ATENÇÃO — coluna `data caixa`:
- Armazena Unix timestamp em inteiro (ex: 1765324800) — NÃO é uma coluna de data
- NÃO use `data caixa` em comparações com DATE() — use `Data movimento` no lugar

#### Valores de `Situação`:
- **Realizados (caixa):** `"Pago"`, `"Recebido"`
- **Pendentes:** `"Em aberto"`, `"Vencido"`, `"Pendente"`
- **Cancelados:** `"Cancelado"`

#### Discriminador de tipo:
- `Tipo da operação` = `"Crédito"` → receita; qualquer outro valor → despesa
- `Receita/Despesa` = `"Receita"` | `"Despesa"` — coluna calculada equivalente
- As colunas calculadas `receita` e `despesas` já incorporam esse filtro
"""

# ─────────────────────────────────────────────────────────────────────────────
# MAPEAMENTO SEMÂNTICO
# ─────────────────────────────────────────────────────────────────────────────

SEMANTIC_MAPPINGS = """
## MAPEAMENTO SEMÂNTICO — LINGUAGEM NATURAL → DAX (CONTA AZUL)

### DISTINÇÃO CRÍTICA — Faturamento vs Receita:
- **Faturamento** = regime de COMPETÊNCIA → todos os lançamentos (qualquer status, exceto Cancelado), data via `Data de competência` ou `Ano_mes competencia`
- **Receita** = regime de CAIXA → somente lançamentos com Situação "Pago" ou "Recebido", data via `Data movimento` ou `Ano_mes`

### MÉTRICAS PRINCIPAIS

**Faturamento / Faturamento total / Total faturado:**
→ `CALCULATE(SUM('data'[receita]), 'data'[Receita/Despesa] = "Receita")`
→ Usa `Ano_mes competencia` ou `Data de competência` para filtrar por período.

**Receita / Receita recebida / Receita realizada / Entrada de caixa:**
→ `CALCULATE(SUM('data'[receita]), 'data'[Receita/Despesa] = "Receita", 'data'[Situação] IN {"Pago", "Recebido"})`
→ Usa `Ano_mes` ou `Data movimento` para filtrar por período.

**Despesa total / Custos / Gastos:**
→ `CALCULATE(SUM('data'[despesas]), 'data'[Receita/Despesa] = "Despesa")`

**Despesa paga (caixa):**
→ `CALCULATE(SUM('data'[despesas]), 'data'[Receita/Despesa] = "Despesa", 'data'[Situação] IN {"Pago", "Recebido"})`

**Resultado / Saldo / Lucro:**
→ `CALCULATE(SUM('data'[receita]) - SUM('data'[despesas]), 'data'[Situação] IN {"Pago", "Recebido"})`

**A receber (em aberto):**
→ `CALCULATE(SUM('data'[receita]), 'data'[Receita/Despesa] = "Receita", 'data'[Situação] IN {"Em aberto", "Vencido", "Pendente"})`

**A pagar (em aberto):**
→ `CALCULATE(SUM('data'[despesas]), 'data'[Receita/Despesa] = "Despesa", 'data'[Situação] IN {"Em aberto", "Vencido", "Pendente"})`

### FILTROS DE PERÍODO

**Por ano (coluna texto `Ano_mes`):**
→ `LEFT('data'[Ano_mes], 4) = "2025"` — extrai ano dos 4 primeiros caracteres
→ OU `LEFT('data'[Ano_mes competencia], 4) = "2025"` para competência

**Por mês específico:**
→ `'data'[Ano_mes] = "2025dezembro"` — formato ANO + nome do mês em minúsculo (sem espaço)
→ Ex: "2025janeiro", "2025fevereiro", ..., "2025dezembro"

**Por data de pagamento (caixa) — intervalo:**
→ `'data'[Data movimento] >= DATE(2025,1,1) && 'data'[Data movimento] <= DATE(2025,12,31)`
→ NUNCA use `data caixa` — é Unix timestamp, não coluna de data

**Por data de competência — intervalo:**
→ `'data'[Data de competência] >= DATE(2025,1,1) && 'data'[Data de competência] <= DATE(2025,12,31)`

### REGIME CONTÁBIL

**Regime de CAIXA (efetivamente pago/recebido):**
→ `'data'[Situação] IN {"Pago", "Recebido"}`
→ Usar `Data movimento` ou `Ano_mes` como coluna de data

**Regime de COMPETÊNCIA (todos os lançamentos):**
→ Sem filtro de Situação (exceto excluir Cancelado quando necessário)
→ Usar `Data de competência` ou `Ano_mes competencia` como coluna de data

**Padrão (sem especificação explícita):** use competência.

### AGRUPAMENTOS

**Por categoria:**
→ `SUMMARIZECOLUMNS('data'[Categoria], ...)`
→ OU `SUMMARIZECOLUMNS('data'[Categoria 1], ...)`

**Por conta contábil (DRE):**
→ `SUMMARIZECOLUMNS('data'[Conta contabil], ...)`

**Por cliente / fornecedor:**
→ `SUMMARIZECOLUMNS('data'[Nome do fornecedor/cliente], ...)`

**Por conta bancária:**
→ `SUMMARIZECOLUMNS('data'[Conta bancária], ...)`

**Por centro de custo:**
→ `SUMMARIZECOLUMNS('data'[CentroCusto], ...)`

**Por mês:**
→ `SUMMARIZECOLUMNS('data'[Ano_mes], ...)` para caixa
→ `SUMMARIZECOLUMNS('data'[Ano_mes competencia], ...)` para competência
"""

# ─────────────────────────────────────────────────────────────────────────────
# EXEMPLOS DE DAX
# ─────────────────────────────────────────────────────────────────────────────

DAX_EXAMPLES = """
## EXEMPLOS CONCRETOS — PERGUNTA → DAX (CONTA AZUL)

### Q: "Qual o faturamento de 2025?" (competência)
```dax
EVALUATE
ROW(
    "Faturamento 2025",
    CALCULATE(
        SUM('data'[receita]),
        'data'[Receita/Despesa] = "Receita",
        LEFT('data'[Ano_mes competencia], 4) = "2025"
    )
)
```

### Q: "Qual a receita recebida em 2025?" (caixa)
```dax
EVALUATE
ROW(
    "Receita Caixa 2025",
    CALCULATE(
        SUM('data'[receita]),
        'data'[Receita/Despesa] = "Receita",
        'data'[Situação] IN {"Pago", "Recebido"},
        LEFT('data'[Ano_mes], 4) = "2025"
    )
)
```

### Q: "Despesa por categoria em 2025"
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Categoria],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Despesa"
        && LEFT('data'[Ano_mes], 4) = "2025"
    ),
    "Despesa", SUM('data'[despesas])
)
ORDER BY [Despesa] DESC
```

### Q: "Receita por mês em 2025" (caixa)
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Ano_mes],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Situação] IN {"Pago", "Recebido"}
        && LEFT('data'[Ano_mes], 4) = "2025"
    ),
    "Receita", SUM('data'[receita])
)
```

### Q: "Faturamento por mês em 2025" (competência)
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Ano_mes competencia],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && LEFT('data'[Ano_mes competencia], 4) = "2025"
    ),
    "Faturamento", SUM('data'[receita])
)
```

### Q: "Top 10 clientes por receita em 2025"
```dax
EVALUATE
TOPN(
    10,
    SUMMARIZECOLUMNS(
        'data'[Nome do fornecedor/cliente],
        FILTER(ALL('data'),
            'data'[Receita/Despesa] = "Receita"
            && 'data'[Situação] IN {"Pago", "Recebido"}
            && LEFT('data'[Ano_mes], 4) = "2025"
        ),
        "Receita", SUM('data'[receita])
    ),
    [Receita], DESC
)
```

### Q: "Quanto temos a receber (em aberto)?"
```dax
EVALUATE
ROW(
    "A Receber",
    CALCULATE(
        SUM('data'[receita]),
        'data'[Receita/Despesa] = "Receita",
        'data'[Situação] IN {"Em aberto", "Vencido", "Pendente"}
    )
)
```

### Q: "Resultado (receita - despesa) em 2025" (caixa)
```dax
EVALUATE
ROW(
    "Resultado 2025",
    CALCULATE(
        SUM('data'[receita]) - SUM('data'[despesas]),
        'data'[Situação] IN {"Pago", "Recebido"},
        LEFT('data'[Ano_mes], 4) = "2025"
    )
)
```

### Q: "Receita por conta contábil (DRE) em 2025"
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta contabil],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && LEFT('data'[Ano_mes competencia], 4) = "2025"
    ),
    "Faturamento", SUM('data'[receita])
)
ORDER BY [Faturamento] DESC
```
"""

# ─────────────────────────────────────────────────────────────────────────────
# REGRAS CRÍTICAS
# ─────────────────────────────────────────────────────────────────────────────

CRITICAL_RULES = """
## REGRAS CRÍTICAS — CONTA AZUL / BI_OTERO (ERROS COMUNS A EVITAR)

1. **SEMPRE filtre `Receita/Despesa`** ao somar receita ou despesa:
   - Receita: `'data'[Receita/Despesa] = "Receita"`
   - Despesa: `'data'[Receita/Despesa] = "Despesa"`
   - Sem esse filtro, você soma tudo (receitas + despesas juntos).

2. **FATURAMENTO ≠ RECEITA:**
   - Faturamento = competência (sem filtro de Situação, data via `Ano_mes competencia`)
   - Receita = caixa (`Situação IN {"Pago", "Recebido"}`, data via `Ano_mes`)

3. **Filtro de ano via `LEFT()`** — a coluna `Ano_mes` é texto no formato "ANOmes":
   - `LEFT('data'[Ano_mes], 4) = "2025"` — extrai os 4 primeiros chars
   - NUNCA use `'data'[Ano_mes] = "2025"` (sem LEFT) — não funciona
   - Exemplos de valor: "2025janeiro", "2025dezembro"

4. **`Situação` — valores exatos** (com acento e capitalização correta):
   - Realizados: `"Pago"`, `"Recebido"`
   - Em aberto: `"Em aberto"`, `"Vencido"`, `"Pendente"`
   - Cancelado: `"Cancelado"` (excluir dos cálculos quando relevante)

5. **Sempre use `EVALUATE`** para iniciar a query DAX.

6. **NUNCA use `data caixa`** para filtros de data — essa coluna armazena Unix timestamps
   inteiros (ex: 1765324800), NÃO datas. Use `Data movimento` para data de caixa.

7. **Coluna de data de competência** chama-se `Data de competência` (com "de").
   Não existe `Data Competência` ou `DataCompetencia`.

8. **Coluna de data de caixa** chama-se `Data movimento`.
   Não existe `Data Pagamento` ou `DataPagamento`.

9. **Cliente/fornecedor** chama-se `Nome do fornecedor/cliente`.
   Não existe coluna `Cliente` ou `Contato`.

10. **Coluna de período caixa**: `Ano_mes` (com underscore, A maiúsculo).
    Coluna de período competência: `Ano_mes competencia` (com espaço).
    Não existe `ano_mes` sem maiúscula ou `ano_mes_competencia` com underscore.

11. **Nomes de colunas** — se o schema dinâmico mostrar nomes diferentes dos exemplos,
    use os nomes do schema dinâmico. Estes são os nomes reais do BI_Otero.

12. **Para valor único**: use `ROW("Label", expressão)` — não use SUMMARIZE de uma linha.
"""

# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÃO PÚBLICA
# ─────────────────────────────────────────────────────────────────────────────

def get_conta_azul_context() -> str:
    """
    Retorna o dicionário completo do modelo Conta Azul para injeção nos prompts DAX.
    CRITICAL_RULES vem primeiro para maximizar atenção do LLM.
    """
    return "\n".join([
        CRITICAL_RULES,
        SCHEMA_DESCRIPTION,
        SEMANTIC_MAPPINGS,
        DAX_EXAMPLES,
    ])
