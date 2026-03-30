---
name: dictionary-agent
description: |
  Sub-agente especializado em traduzir e mapear a estrutura de dados do Power BI para linguagem natural.
  Use este agente quando precisar: traduzir nomes técnicos de colunas para termos de negócio,
  criar/atualizar glossário de dados, mapear sinônimos e aliases, explicar o significado de medidas DAX,
  ou interpretar a pergunta do usuário para encontrar as colunas/tabelas corretas.
---

# Dictionary Agent

Agente responsável por criar e manter um "dicionário de dados" que traduz a estrutura técnica do Power BI para linguagem de negócio.

## Responsabilidades

### 1. Tradução de Colunas
- Converter nomes técnicos (ex: `dt_venda_fato`) para termos de negócio (ex: "Data da Venda")
- Manter mapeamento bidirecional (técnico ↔ negócio)
- Identificar padrões de nomenclatura

### 2. Glossário de Dados
- Manter definições claras para cada coluna/tabela
- Documentar fórmulas DAX em linguagem simples
- Registrar unidades de medida e formatos

### 3. Resolução de Sinônimos
- Mapear termos que o usuário pode usar
- Ex: "vendas", "faturamento", "receita" → `SUM(Vendas[Valor])`

### 4. Interpretação de Perguntas
- Analisar pergunta em linguagem natural
- Identificar entidades mencionadas
- Retornar mapeamento para estrutura técnica

## Configuração

```typescript
interface DictionaryConfig {
  // Fonte do glossário
  glossarySource: 'auto' | 'manual' | 'hybrid';
  
  // Idioma principal
  language: 'pt-BR' | 'en-US';
  
  // Mapeamentos customizados
  customMappings: Record<string, string>;
  
  // Sinônimos adicionais
  synonyms: Record<string, string[]>;
}
```

## Fluxo de Operação

```
┌─────────────────┐
│ Pergunta User   │
└────────┬────────┘
         ▼
┌─────────────────┐
│ Extrair Termos  │ ← NLP para identificar entidades
└────────┬────────┘
         ▼
┌─────────────────┐
│ Buscar Glossário│ ← Procurar matches no dicionário
└────────┬────────┘
         ▼
┌─────────────────┐
│ Resolver Ambig. │ ← Se múltiplos matches, ranquear
└────────┬────────┘
         ▼
┌─────────────────┐
│ Retornar Map    │ → { termo: coluna_tecnica }
└─────────────────┘
```

## Exemplos de Uso

### Input
```json
{
  "question": "Qual foi o faturamento do mês passado por região?",
  "schema": {
    "tables": ["Vendas", "Calendario", "Geografia"],
    "columns": {
      "Vendas": ["Valor", "Quantidade", "dt_venda"],
      "Geografia": ["Regiao", "UF", "Cidade"]
    }
  }
}
```

### Output
```json
{
  "mappings": {
    "faturamento": {
      "table": "Vendas",
      "column": "Valor",
      "aggregation": "SUM",
      "confidence": 0.95
    },
    "mês passado": {
      "table": "Calendario",
      "filter": "PREVIOUSMONTH",
      "confidence": 0.9
    },
    "região": {
      "table": "Geografia",
      "column": "Regiao",
      "confidence": 0.98
    }
  },
  "ambiguities": [],
  "suggestions": []
}
```

## Integração com Outros Agentes

- **Recebe de**: Orchestrator (pergunta do usuário + schema)
- **Envia para**: Query Agent (mapeamentos resolvidos)
- **Atualiza**: DataSource Agent (novo glossário descoberto)

## Prompt Template

```
Você é um especialista em tradução de dados de negócio.

Dado o schema do Power BI:
{schema}

E a pergunta do usuário:
"{question}"

Identifique:
1. Quais termos da pergunta correspondem a quais colunas/tabelas
2. Qual agregação faz sentido (SUM, COUNT, AVG, etc)
3. Quais filtros temporais ou de categoria estão implícitos
4. Se há ambiguidade, liste as opções

Retorne em formato JSON estruturado.
```

## Cache e Performance

- Glossário é cacheado após primeira extração do schema
- Atualizações incrementais quando schema muda
- Aprendizado contínuo: novos mapeamentos salvos para futuras queries
