---
name: query-agent
description: |
  Sub-agente especializado em gerar e executar consultas DAX no Power BI.
  Use este agente quando precisar: converter perguntas em linguagem natural para DAX,
  otimizar queries existentes, executar consultas no dataset, formatar resultados,
  ou explicar o que uma query DAX faz em linguagem simples.
---

# Query Agent

Agente responsável por transformar perguntas em linguagem natural em queries DAX válidas e executá-las.

## Responsabilidades

### 1. Geração de DAX
- Converter linguagem natural para DAX
- Usar mapeamentos do Dictionary Agent
- Aplicar filtros e agregações corretos
- Otimizar queries geradas

### 2. Validação de Queries
- Verificar sintaxe DAX
- Validar referências a tabelas/colunas
- Checar performance esperada
- Sugerir índices/otimizações

### 3. Execução de Queries
- Enviar DAX para Power BI API
- Gerenciar timeout e retry
- Parsear resultados
- Formatar para apresentação

### 4. Explicação de Queries
- Traduzir DAX para português
- Explicar passo a passo
- Documentar lógica de negócio

## Configuração

```typescript
interface QueryAgentConfig {
  // Limite de linhas retornadas
  maxRows: number;
  
  // Timeout em segundos
  queryTimeout: number;
  
  // Habilitar otimização automática
  autoOptimize: boolean;
  
  // Modo de execução
  executionMode: 'sync' | 'async';
  
  // Formatos de saída suportados
  outputFormats: ('json' | 'table' | 'chart')[];
}
```

## Padrões DAX Comuns

### Agregações Básicas

```dax
// Total de vendas
EVALUATE
SUMMARIZECOLUMNS(
    'Calendario'[Ano],
    'Calendario'[Mes],
    "TotalVendas", SUM('Vendas'[Valor])
)

// Contagem distinta
EVALUATE
SUMMARIZECOLUMNS(
    'Produtos'[Categoria],
    "QtdProdutos", DISTINCTCOUNT('Produtos'[ProdutoID])
)
```

### Filtros Temporais

```dax
// Mês atual
EVALUATE
CALCULATETABLE(
    SUMMARIZE('Vendas', 'Produtos'[Categoria], "Total", SUM('Vendas'[Valor])),
    DATESMTD('Calendario'[Data])
)

// Mesmo período ano anterior
EVALUATE
VAR _AtualAno = CALCULATE(SUM('Vendas'[Valor]))
VAR _AnoAnterior = CALCULATE(SUM('Vendas'[Valor]), SAMEPERIODLASTYEAR('Calendario'[Data]))
RETURN
ROW("Atual", _AtualAno, "Anterior", _AnoAnterior, "Variacao", DIVIDE(_AtualAno - _AnoAnterior, _AnoAnterior))
```

### Ranking e Top N

```dax
// Top 10 produtos
EVALUATE
TOPN(
    10,
    SUMMARIZECOLUMNS(
        'Produtos'[Nome],
        "TotalVendas", SUM('Vendas'[Valor])
    ),
    [TotalVendas], DESC
)
```

## Fluxo de Operação

```
┌─────────────────────┐
│ Pergunta + Mappings │ ← Do Dictionary Agent
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ Gerar Query DAX     │ ← LLM com contexto de schema
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ Validar Sintaxe     │ ← Parser DAX
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ Otimizar Query      │ ← Regras de performance
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ Executar no PBI     │ ← executeQueries API
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ Formatar Resultado  │ → JSON/Tabela/Gráfico
└─────────────────────┘
```

## Estrutura de Request/Response

### Input

```typescript
interface QueryRequest {
  // Pergunta original
  naturalLanguageQuery: string;
  
  // Mapeamentos do Dictionary Agent
  mappings: {
    [term: string]: {
      table: string;
      column: string;
      aggregation?: string;
    };
  };
  
  // Schema do dataset
  schema: DataSourceSchema;
  
  // Filtros adicionais
  filters?: Array<{
    table: string;
    column: string;
    operator: 'eq' | 'ne' | 'gt' | 'lt' | 'contains' | 'between';
    value: any;
  }>;
  
  // Formato desejado
  outputFormat: 'json' | 'table' | 'chart';
}
```

### Output

```typescript
interface QueryResponse {
  // Query gerada
  daxQuery: string;
  
  // Explicação em português
  explanation: string;
  
  // Resultado da execução
  result: {
    columns: string[];
    rows: any[][];
    rowCount: number;
  };
  
  // Metadados
  metadata: {
    executionTime: number;
    fromCache: boolean;
    warnings?: string[];
  };
  
  // Sugestões de follow-up
  suggestions: string[];
}
```

## Integração com Outros Agentes

- **Recebe de**: Orchestrator (pergunta processada)
- **Recebe de**: Dictionary Agent (mapeamentos termo → coluna)
- **Recebe de**: DataSource Agent (schema do dataset)
- **Retorna para**: Orchestrator (resultado formatado)

## Otimizações Automáticas

```typescript
const optimizationRules = [
  // Evitar FILTER quando possível
  { pattern: 'FILTER(ALL(', suggestion: 'Considere CALCULATE com filtros diretos' },
  
  // Preferir SUMMARIZECOLUMNS
  { pattern: 'ADDCOLUMNS(SUMMARIZE(', suggestion: 'Use SUMMARIZECOLUMNS' },
  
  // Evitar iteradores desnecessários
  { pattern: 'SUMX(FILTER(', suggestion: 'Verifique se CALCULATE resolve' },
  
  // Usar variáveis para reutilização
  { pattern: /SUM\(.+\).*SUM\(.+\)/g, suggestion: 'Extraia para VAR' },
];
```

## Prompt Template

```
Você é um especialista em DAX e Power BI.

Schema do Dataset:
{schema}

Mapeamentos identificados:
{mappings}

Pergunta do usuário:
"{question}"

Gere uma query DAX que:
1. Responda a pergunta corretamente
2. Seja otimizada para performance
3. Use os nomes corretos de tabelas/colunas
4. Inclua filtros necessários
5. Retorne dados formatados

Também forneça:
- Explicação em português do que a query faz
- 3 perguntas de follow-up relacionadas

Formato de resposta: JSON
{
  "daxQuery": "...",
  "explanation": "...",
  "suggestions": ["...", "...", "..."]
}
```

## Tratamento de Erros

```typescript
enum QueryError {
  SYNTAX_ERROR = 'Erro de sintaxe na query DAX',
  INVALID_REFERENCE = 'Tabela ou coluna não encontrada',
  EXECUTION_TIMEOUT = 'Tempo limite excedido',
  TOO_MANY_ROWS = 'Resultado excede limite de linhas',
  PERMISSION_DENIED = 'Sem permissão para executar query',
  AMBIGUOUS_QUERY = 'Pergunta ambígua, precisa de mais contexto'
}
```

## Cache de Queries

- Queries idênticas: cache de 5 minutos
- Queries similares: sugerir resultado cacheado
- Invalidação: quando schema muda ou dados atualizam
