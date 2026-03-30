/**
 * Skills Configuration
 * 
 * Sistema de configuração dinâmica para sub-agentes.
 * Cada skill pode ser habilitada/desabilitada e configurada independentemente.
 */

import { z } from 'zod';

// ===========================================
// SCHEMAS DE CONFIGURAÇÃO
// ===========================================

const BaseSkillConfigSchema = z.object({
  enabled: z.boolean().default(true),
  model: z.string().default('claude-sonnet-4-20250514'),
  temperature: z.number().min(0).max(1).default(0.3),
  maxTokens: z.number().default(4096),
  timeout: z.number().default(30000), // ms
  retries: z.number().default(3),
});

const DictionarySkillConfigSchema = BaseSkillConfigSchema.extend({
  glossarySource: z.enum(['auto', 'manual', 'hybrid']).default('hybrid'),
  language: z.enum(['pt-BR', 'en-US']).default('pt-BR'),
  customMappings: z.record(z.string()).default({}),
  synonyms: z.record(z.array(z.string())).default({}),
  cacheGlossary: z.boolean().default(true),
  glossaryCacheTTL: z.number().default(3600), // segundos
});

const DataSourceSkillConfigSchema = BaseSkillConfigSchema.extend({
  connectionType: z.enum(['rest-api', 'xmla', 'embedded']).default('rest-api'),
  schemaRefreshInterval: z.number().default(30), // minutos
  allowedDatasets: z.array(z.string()).default([]),
  analysisDepth: z.enum(['basic', 'full', 'deep']).default('full'),
  cacheSchema: z.boolean().default(true),
  schemaCacheTTL: z.number().default(1800), // segundos
});

const QuerySkillConfigSchema = BaseSkillConfigSchema.extend({
  maxRows: z.number().default(10000),
  queryTimeout: z.number().default(60), // segundos
  autoOptimize: z.boolean().default(true),
  executionMode: z.enum(['sync', 'async']).default('sync'),
  outputFormats: z.array(z.enum(['json', 'table', 'chart'])).default(['json', 'table']),
  cacheQueries: z.boolean().default(true),
  queryCacheTTL: z.number().default(300), // segundos
  explainQueries: z.boolean().default(true),
});

// ===========================================
// TIPOS
// ===========================================

export type DictionarySkillConfig = z.infer<typeof DictionarySkillConfigSchema>;
export type DataSourceSkillConfig = z.infer<typeof DataSourceSkillConfigSchema>;
export type QuerySkillConfig = z.infer<typeof QuerySkillConfigSchema>;

export interface SkillsConfig {
  dictionary: DictionarySkillConfig;
  datasource: DataSourceSkillConfig;
  query: QuerySkillConfig;
}

// ===========================================
// CONFIGURAÇÃO PADRÃO
// ===========================================

export const defaultSkillsConfig: SkillsConfig = {
  dictionary: {
    enabled: true,
    model: 'claude-sonnet-4-20250514',
    temperature: 0.2,
    maxTokens: 2048,
    timeout: 15000,
    retries: 3,
    glossarySource: 'hybrid',
    language: 'pt-BR',
    customMappings: {
      // Exemplos de mapeamentos customizados
      'faturamento': 'Vendas[Valor]',
      'receita': 'Vendas[Valor]',
      'quantidade vendida': 'Vendas[Quantidade]',
    },
    synonyms: {
      'vendas': ['faturamento', 'receita', 'venda', 'vendido'],
      'cliente': ['consumidor', 'comprador', 'conta'],
      'produto': ['item', 'mercadoria', 'sku'],
    },
    cacheGlossary: true,
    glossaryCacheTTL: 3600,
  },
  
  datasource: {
    enabled: true,
    model: 'claude-sonnet-4-20250514',
    temperature: 0.1,
    maxTokens: 4096,
    timeout: 60000,
    retries: 3,
    connectionType: 'rest-api',
    schemaRefreshInterval: 30,
    allowedDatasets: [], // vazio = todos permitidos
    analysisDepth: 'full',
    cacheSchema: true,
    schemaCacheTTL: 1800,
  },
  
  query: {
    enabled: true,
    model: 'claude-sonnet-4-20250514',
    temperature: 0.3,
    maxTokens: 4096,
    timeout: 30000,
    retries: 3,
    maxRows: 10000,
    queryTimeout: 60,
    autoOptimize: true,
    executionMode: 'sync',
    outputFormats: ['json', 'table', 'chart'],
    cacheQueries: true,
    queryCacheTTL: 300,
    explainQueries: true,
  },
};

// ===========================================
// FUNÇÕES DE CONFIGURAÇÃO
// ===========================================

/**
 * Carrega configuração de skills do ambiente ou usa defaults
 */
export function loadSkillsConfig(overrides?: Partial<SkillsConfig>): SkillsConfig {
  const config = { ...defaultSkillsConfig };
  
  // Aplicar overrides se fornecidos
  if (overrides) {
    if (overrides.dictionary) {
      config.dictionary = { ...config.dictionary, ...overrides.dictionary };
    }
    if (overrides.datasource) {
      config.datasource = { ...config.datasource, ...overrides.datasource };
    }
    if (overrides.query) {
      config.query = { ...config.query, ...overrides.query };
    }
  }
  
  // Validar configurações
  DictionarySkillConfigSchema.parse(config.dictionary);
  DataSourceSkillConfigSchema.parse(config.datasource);
  QuerySkillConfigSchema.parse(config.query);
  
  return config;
}

/**
 * Habilita ou desabilita uma skill específica
 */
export function toggleSkill(
  config: SkillsConfig, 
  skill: keyof SkillsConfig, 
  enabled: boolean
): SkillsConfig {
  return {
    ...config,
    [skill]: {
      ...config[skill],
      enabled,
    },
  };
}

/**
 * Atualiza configuração de uma skill específica
 */
export function updateSkillConfig<K extends keyof SkillsConfig>(
  config: SkillsConfig,
  skill: K,
  updates: Partial<SkillsConfig[K]>
): SkillsConfig {
  return {
    ...config,
    [skill]: {
      ...config[skill],
      ...updates,
    },
  };
}

/**
 * Retorna apenas skills habilitadas
 */
export function getEnabledSkills(config: SkillsConfig): (keyof SkillsConfig)[] {
  return (Object.keys(config) as (keyof SkillsConfig)[])
    .filter(skill => config[skill].enabled);
}

// ===========================================
// SKILL REGISTRY
// ===========================================

export interface SkillDefinition {
  name: string;
  description: string;
  skillPath: string;
  configSchema: z.ZodType<any>;
}

export const skillRegistry: Record<keyof SkillsConfig, SkillDefinition> = {
  dictionary: {
    name: 'Dictionary Agent',
    description: 'Traduz nomes técnicos para linguagem de negócio',
    skillPath: '/src/skills/dictionary/SKILL.md',
    configSchema: DictionarySkillConfigSchema,
  },
  datasource: {
    name: 'DataSource Agent',
    description: 'Conecta e mapeia fontes de dados do Power BI',
    skillPath: '/src/skills/datasource/SKILL.md',
    configSchema: DataSourceSkillConfigSchema,
  },
  query: {
    name: 'Query Agent',
    description: 'Gera e executa consultas DAX',
    skillPath: '/src/skills/query/SKILL.md',
    configSchema: QuerySkillConfigSchema,
  },
};
