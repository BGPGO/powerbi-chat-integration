# 🔌 Power BI Chat Integration

Sistema de chat conversacional integrado ao Power BI com sub-agentes especializados para tradução de dados, compreensão de estruturas e consultas inteligentes.

## 📐 Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                      FRONTEND (Chat UI)                         │
│                   React + Tailwind + shadcn/ui                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATOR AGENT                          │
│              (Coordena todos os sub-agentes)                    │
└─────────────────────────────────────────────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  DICTIONARY     │  │  DATA SOURCE    │  │   QUERY         │
│  AGENT          │  │  AGENT          │  │   AGENT         │
│                 │  │                 │  │                 │
│ • Traduz colunas│  │ • Mapeia fontes │  │ • Gera DAX      │
│ • Mapeia nomes  │  │ • Entende schema│  │ • Interpreta    │
│ • Glossário     │  │ • Conexões BI   │  │   perguntas     │
└─────────────────┘  └─────────────────┘  └─────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    POWER BI SERVICE API                         │
│                    (REST API + Embedded)                        │
└─────────────────────────────────────────────────────────────────┘
```

## 🔗 Opções de Conexão com Power BI

### Opção 1: Power BI REST API (⭐ Recomendado)
**Melhor para**: Acesso a workspaces, datasets, relatórios
**Auth**: Azure AD / Service Principal
**Endpoints principais**:
- `GET /groups` - Listar workspaces
- `GET /datasets` - Acessar datasets
- `POST /datasets/{id}/executeQueries` - Executar DAX

### Opção 2: Power BI Embedded
**Melhor para**: Embedar relatórios na aplicação
**Auth**: App-owns-data ou User-owns-data
**Vantagens**: Visualizações nativas do Power BI

### Opção 3: XMLA Endpoint (Premium/PPU)
**Melhor para**: Acesso direto ao modelo semântico
**Auth**: Azure AD
**Vantagens**: Controle total, acesso completo ao schema

## 📁 Estrutura do Projeto

```
powerbi-chat-integration/
├── README.md
├── package.json
├── .env.example
├── src/
│   ├── app/                    # Next.js App Router
│   ├── components/             # React Components
│   ├── lib/                    # Core Logic
│   ├── skills/                 # Sub-agents Skills (dinâmico)
│   └── types/                  # TypeScript Types
├── docs/                       # Documentação
└── tests/                      # Testes
```

## 🚀 Quick Start

```bash
npm install
cp .env.example .env.local
npm run dev
```

## 📚 Sub-Agentes

| Agente | Função | Skill Config |
|--------|--------|--------------|
| Dictionary | Traduz colunas e estruturas | `skills/dictionary/` |
| DataSource | Entende fontes de dados | `skills/datasource/` |
| Query | Gera consultas DAX | `skills/query/` |
