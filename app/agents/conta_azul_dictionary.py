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

#### Valores de `Situação` (campo informativo — NÃO usar para filtro de caixa):
- **Concluídos:** `"Conciliado"`, `"Quitado"`
- **Pendentes:** `"Em aberto"`, `"Atrasado"`
- **Outros:** `"Perdido/Desconsiderado"`, `"Transferido"`

#### Coluna `Ano_mes` — comportamento crítico (CAIXA):
- Populada APENAS quando há movimento de caixa efetivo (seja `"Pago"` ou `"Atrasado"` mas com data de pagamento registrada)
- `Ano_mes = "2025janeiro"` já seleciona TODOS os recebimentos/pagamentos efetivos de janeiro/2025
- NÃO requer filtro adicional de `Previsto/realizado` — o próprio `Ano_mes` é o filtro de caixa
- Entradas de transferência entre contas próprias (`Categoria 1 = "Transferência de Entrada"` ou `"Transferência de Saída"`) devem ser EXCLUÍDAS explicitamente

#### Valores de `Previsto/realizado`:
- `"Pago"` — pago/recebido (mais comum)
- `"Atrasado"` — pode ter data de pagamento registrada (entrada/saída efetiva mas status não atualizado)
- `"Pendente"` — não pago/recebido
- NÃO usar `Previsto/realizado` como filtro principal de caixa — usar `Ano_mes` + excluir Transferências

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
- **Faturamento** = regime de COMPETÊNCIA → todos os lançamentos (exceto Transferências entre contas), data via `Ano_mes competencia`
- **Receita caixa** = regime de CAIXA → filtrar por `Ano_mes` (já seleciona só movimentos efetivos) + excluir "Transferência de Entrada"
- **Despesa caixa** = regime de CAIXA → filtrar por `Ano_mes` + excluir "Transferência de Saída"

### MÉTRICAS PRINCIPAIS

**Faturamento / Faturamento total / Total faturado:**
→ `CALCULATE(SUM('data'[receita]), 'data'[Receita/Despesa] = "Receita", 'data'[Categoria 1] <> "Transferência de Entrada")`
→ Usa `Ano_mes competencia` (ex: "2025janeiro") para filtrar por período.
→ A coluna `Ano_mes competencia` usa formato "ANOmes" — ex: "2025dezembro", "2025janeiro"

**Receita / Receita recebida / Receita realizada / Entrada de caixa:**
→ `CALCULATE(SUM('data'[receita]), 'data'[Receita/Despesa] = "Receita", 'data'[Categoria 1] <> "Transferência de Entrada")`
→ NÃO filtrar por `Previsto/realizado` — a coluna `Ano_mes` só é populada quando há movimento de caixa real
→ Usa `Ano_mes` (ex: "2025janeiro") para filtrar por período.

**Despesa total / Custos / Gastos (caixa):**
→ `CALCULATE(SUM('data'[despesas]), 'data'[Receita/Despesa] = "Despesa", 'data'[Categoria 1] <> "Transferência de Saída")`
→ Usa `Ano_mes` para filtrar por período.

**Resultado / Saldo / Lucro (caixa):**
→ Receita caixa - Despesa caixa para o mesmo período `Ano_mes`
→ Excluir "Transferência de Entrada" da receita e "Transferência de Saída" da despesa

**A receber (em aberto):**
→ `CALCULATE(SUM('data'[receita]), 'data'[Receita/Despesa] = "Receita", 'data'[Previsto/realizado] IN {"Pendente", "Atrasado"}, 'data'[Ano_mes] = BLANK())`

**A pagar (em aberto):**
→ `CALCULATE(SUM('data'[despesas]), 'data'[Receita/Despesa] = "Despesa", 'data'[Previsto/realizado] IN {"Pendente", "Atrasado"}, 'data'[Ano_mes] = BLANK())`

### FILTROS DE PERÍODO

**Por mês específico (caixa):**
→ `'data'[Ano_mes] = "2025janeiro"` — formato ANO + nome do mês em minúsculo (sem espaço)
→ Ex: "2025janeiro", "2025fevereiro", ..., "2025dezembro"
→ A coluna `Ano_mes` só tem valor quando houve movimento de caixa — NÃO requer filtro de status adicional

**Por mês específico (competência):**
→ `'data'[Ano_mes competencia] = "2025janeiro"` — mesmo formato ANO+mês

**Por ano (caixa):**
→ `LEFT('data'[Ano_mes], 4) = "2025"` — extrai ano dos 4 primeiros caracteres

**Por data de pagamento (intervalo):**
→ `'data'[Data movimento] >= DATE(2025,1,1) && 'data'[Data movimento] <= DATE(2025,12,31)`
→ NUNCA use `data caixa` — é Unix timestamp, não coluna de data

**Por data de competência (intervalo):**
→ `'data'[Data de competência] >= DATE(2025,1,1) && 'data'[Data de competência] <= DATE(2025,12,31)`

### REGIME CONTÁBIL

**Regime de CAIXA (dinheiro efetivamente movimentado):**
→ Usar `Ano_mes` como filtro de período (sem filtro de `Previsto/realizado`)
→ Excluir `Categoria 1 = "Transferência de Entrada"` (receita) e `"Transferência de Saída"` (despesa)
→ Motivo: `Ano_mes` só é populado quando há movimento de caixa real; transferências entre contas próprias criam entradas espelhadas que inflam os totais

**Regime de COMPETÊNCIA (DRE, faturamento):**
→ Usar `Ano_mes competencia` como filtro de período
→ Excluir `Categoria 1 = "Transferência de Entrada"` (mesma regra — não é receita operacional)
→ Usar `FonteValor = "CC"` como alternativa equivalente

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
        'data'[Categoria 1] <> "Transferência de Entrada",
        LEFT('data'[Ano_mes], 4) = "2025"
    )
)
```
-- NÃO filtrar por Previsto/realizado — Ano_mes já é o filtro de caixa

### Q: "Despesa por categoria em 2025"
```dax
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Categoria],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Despesa"
        && 'data'[Categoria 1] <> "Transferência de Saída"
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
        && 'data'[Categoria 1] <> "Transferência de Entrada"
        && LEFT('data'[Ano_mes], 4) = "2025"
    ),
    "Receita", SUM('data'[receita])
)
```
-- NÃO filtrar por Previsto/realizado — Ano_mes já é o filtro de caixa

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
            && 'data'[Categoria 1] <> "Transferência de Entrada"
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
        SUM('data'[receita]),
        'data'[Receita/Despesa] = "Receita",
        'data'[Categoria 1] <> "Transferência de Entrada",
        LEFT('data'[Ano_mes], 4) = "2025"
    )
    -
    CALCULATE(
        SUM('data'[despesas]),
        'data'[Receita/Despesa] = "Despesa",
        'data'[Categoria 1] <> "Transferência de Saída",
        LEFT('data'[Ano_mes], 4) = "2025"
    )
)
```
-- Receita e despesa calculadas separadamente para excluir transferências de cada lado

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

2. **FATURAMENTO ≠ RECEITA CAIXA:**
   - Faturamento (competência): `Receita/Despesa="Receita"` + excl `Categoria 1="Transferência de Entrada"`, data via `Ano_mes competencia`
   - Receita caixa: `Receita/Despesa="Receita"` + excl `Categoria 1="Transferência de Entrada"`, data via `Ano_mes` (sem filtro de status!)
   - Despesa caixa: `Receita/Despesa="Despesa"` + excl `Categoria 1="Transferência de Saída"`, data via `Ano_mes`

3. **`Ano_mes` É O FILTRO DE CAIXA — não use `Previsto/realizado`:**
   - `Ano_mes` só tem valor quando houve movimento de caixa real (Pago OU Atrasado-mas-pago)
   - Filtrar `Previsto/realizado = "Pago"` EXCLUI entradas tardias que foram pagas → resultado incorreto
   - Filtro correto: `'data'[Ano_mes] = "2025janeiro"` sem status adicional

4. **TRANSFERÊNCIAS ENTRE CONTAS PRÓPRIAS = excluir sempre:**
   - `Categoria 1 = "Transferência de Entrada"` → excluir da receita (não é receita operacional)
   - `Categoria 1 = "Transferência de Saída"` → excluir da despesa (não é despesa operacional)
   - Sem essa exclusão, os totais ficam inflados

5. **Filtro de ano via `LEFT()`** — a coluna `Ano_mes` é texto no formato "ANOmes":
   - `LEFT('data'[Ano_mes], 4) = "2025"` — extrai os 4 primeiros chars
   - Exemplos de valor: "2025janeiro", "2025dezembro"

6. **Sempre use `EVALUATE`** para iniciar a query DAX.

7. **NUNCA use `data caixa`** para filtros de data — armazena Unix timestamp inteiro.
   Use `Data movimento` para data de caixa efetivo.

8. **Coluna de data de competência** chama-se `Data de competência` (com "de").
9. **Coluna de data de caixa** chama-se `Data movimento`.
10. **Cliente/fornecedor** chama-se `Nome do fornecedor/cliente`.

11. **Coluna de período caixa**: `Ano_mes` (A maiúsculo, underscore).
    Coluna de período competência: `Ano_mes competencia` (com espaço).

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
