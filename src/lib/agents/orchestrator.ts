/**
 * Orchestrator Agent
 * 
 * Agente principal que coordena todos os sub-agentes para processar
 * perguntas do usuário e retornar respostas estruturadas.
 */

import Anthropic from '@anthropic-ai/sdk';
import { SkillsConfig, loadSkillsConfig } from '@/skills/config';
import { DictionaryAgent } from './dictionary';
import { DataSourceAgent } from './datasource';
import { QueryAgent } from './query';

// ===========================================
// TIPOS
// ===========================================

export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  metadata?: {
    query?: string;
    result?: any;
    sources?: string[];
    suggestions?: string[];
  };
}

export interface ProcessingContext {
  sessionId: string;
  history: ChatMessage[];
  schema?: any;
  glossary?: Map<string, string>;
}

export interface OrchestratorConfig {
  anthropicApiKey: string;
  model: string;
  skills: SkillsConfig;
}

export interface ProcessResult {
  response: string;
  daxQuery?: string;
  data?: any;
  visualization?: 'table' | 'chart' | 'number' | 'text';
  suggestions: string[];
  processingSteps: Array<{
    agent: string;
    action: string;
    duration: number;
  }>;
}

// ===========================================
// ORCHESTRATOR AGENT
// ===========================================

export class OrchestratorAgent {
  private client: Anthropic;
  private config: OrchestratorConfig;
  private dictionaryAgent: DictionaryAgent;
  private dataSourceAgent: DataSourceAgent;
  private queryAgent: QueryAgent;
  
  constructor(config: OrchestratorConfig) {
    this.config = config;
    this.client = new Anthropic({ apiKey: config.anthropicApiKey });
    
    // Inicializar sub-agentes
    this.dictionaryAgent = new DictionaryAgent(config.skills.dictionary);
    this.dataSourceAgent = new DataSourceAgent(config.skills.datasource);
    this.queryAgent = new QueryAgent(config.skills.query);
  }
  
  /**
   * Processa uma mensagem do usuário
   */
  async process(
    userMessage: string, 
    context: ProcessingContext
  ): Promise<ProcessResult> {
    const steps: ProcessResult['processingSteps'] = [];
    const startTime = Date.now();
    
    try {
      // 1. Classificar a intenção
      const intent = await this.classifyIntent(userMessage, context);
      steps.push({
        agent: 'orchestrator',
        action: `Intent classified: ${intent.type}`,
        duration: Date.now() - startTime,
      });
      
      // 2. Roteamento baseado na intenção
      switch (intent.type) {
        case 'data_query':
          return this.handleDataQuery(userMessage, context, steps);
          
        case 'schema_exploration':
          return this.handleSchemaExploration(userMessage, context, steps);
          
        case 'definition_request':
          return this.handleDefinitionRequest(userMessage, context, steps);
          
        case 'conversation':
          return this.handleConversation(userMessage, context, steps);
          
        default:
          return this.handleUnknown(userMessage, context, steps);
      }
      
    } catch (error) {
      console.error('Orchestrator error:', error);
      return {
        response: 'Desculpe, ocorreu um erro ao processar sua pergunta. Pode tentar reformular?',
        suggestions: [
          'Quais dados estão disponíveis?',
          'Mostre o schema do dataset',
          'Ajuda com consultas',
        ],
        processingSteps: steps,
      };
    }
  }
  
  // ===========================================
  // CLASSIFICAÇÃO DE INTENÇÃO
  // ===========================================
  
  private async classifyIntent(
    message: string, 
    context: ProcessingContext
  ): Promise<{ type: string; confidence: number; entities: string[] }> {
    const systemPrompt = `
Você é um classificador de intenções para um assistente de BI.

Classifique a mensagem do usuário em uma das seguintes categorias:
- data_query: Perguntas que requerem consulta a dados (vendas, métricas, comparações)
- schema_exploration: Perguntas sobre estrutura (tabelas, colunas, relacionamentos)
- definition_request: Perguntas sobre definições (o que é X, como é calculado Y)
- conversation: Conversação geral, cumprimentos, ajuda

Retorne JSON:
{
  "type": "categoria",
  "confidence": 0.0-1.0,
  "entities": ["entidades", "identificadas"]
}
`;

    const response = await this.client.messages.create({
      model: this.config.model,
      max_tokens: 256,
      system: systemPrompt,
      messages: [{ role: 'user', content: message }],
    });
    
    const text = response.content[0].type === 'text' ? response.content[0].text : '';
    
    try {
      return JSON.parse(text);
    } catch {
      return { type: 'conversation', confidence: 0.5, entities: [] };
    }
  }
  
  // ===========================================
  // HANDLERS POR TIPO DE INTENÇÃO
  // ===========================================
  
  /**
   * Processa consulta a dados
   */
  private async handleDataQuery(
    message: string,
    context: ProcessingContext,
    steps: ProcessResult['processingSteps']
  ): Promise<ProcessResult> {
    let stepStart = Date.now();
    
    // 1. Garantir que temos schema
    if (!context.schema) {
      context.schema = await this.dataSourceAgent.getSchema();
      steps.push({
        agent: 'datasource',
        action: 'Schema loaded',
        duration: Date.now() - stepStart,
      });
      stepStart = Date.now();
    }
    
    // 2. Traduzir termos para colunas
    const mappings = await this.dictionaryAgent.translate(message, context.schema);
    steps.push({
      agent: 'dictionary',
      action: `Mapped ${Object.keys(mappings).length} terms`,
      duration: Date.now() - stepStart,
    });
    stepStart = Date.now();
    
    // 3. Gerar e executar query
    const queryResult = await this.queryAgent.generateAndExecute({
      question: message,
      mappings,
      schema: context.schema,
    });
    steps.push({
      agent: 'query',
      action: 'Query executed',
      duration: Date.now() - stepStart,
    });
    stepStart = Date.now();
    
    // 4. Formatar resposta
    const response = await this.formatDataResponse(
      message, 
      queryResult.data, 
      queryResult.explanation
    );
    steps.push({
      agent: 'orchestrator',
      action: 'Response formatted',
      duration: Date.now() - stepStart,
    });
    
    return {
      response,
      daxQuery: queryResult.daxQuery,
      data: queryResult.data,
      visualization: this.inferVisualization(queryResult.data),
      suggestions: queryResult.suggestions || [],
      processingSteps: steps,
    };
  }
  
  /**
   * Processa exploração de schema
   */
  private async handleSchemaExploration(
    message: string,
    context: ProcessingContext,
    steps: ProcessResult['processingSteps']
  ): Promise<ProcessResult> {
    const stepStart = Date.now();
    
    const schema = await this.dataSourceAgent.getSchema();
    steps.push({
      agent: 'datasource',
      action: 'Schema retrieved',
      duration: Date.now() - stepStart,
    });
    
    // Formatar descrição do schema
    const response = await this.formatSchemaResponse(message, schema);
    
    return {
      response,
      data: schema,
      visualization: 'text',
      suggestions: [
        'Mostre as colunas da tabela Vendas',
        'Quais são os relacionamentos?',
        'Quais medidas estão disponíveis?',
      ],
      processingSteps: steps,
    };
  }
  
  /**
   * Processa pedido de definição
   */
  private async handleDefinitionRequest(
    message: string,
    context: ProcessingContext,
    steps: ProcessResult['processingSteps']
  ): Promise<ProcessResult> {
    const stepStart = Date.now();
    
    const definition = await this.dictionaryAgent.getDefinition(
      message, 
      context.schema
    );
    steps.push({
      agent: 'dictionary',
      action: 'Definition retrieved',
      duration: Date.now() - stepStart,
    });
    
    return {
      response: definition.explanation,
      suggestions: definition.relatedTerms?.map(t => `O que é ${t}?`) || [],
      processingSteps: steps,
    };
  }
  
  /**
   * Processa conversação geral
   */
  private async handleConversation(
    message: string,
    context: ProcessingContext,
    steps: ProcessResult['processingSteps']
  ): Promise<ProcessResult> {
    const systemPrompt = `
Você é um assistente de Business Intelligence amigável.
Ajude o usuário a entender os dados disponíveis e fazer análises.
Seja conciso e direto.

Dados disponíveis no Power BI:
${context.schema ? JSON.stringify(context.schema.tables?.map((t: any) => t.name)) : 'Não carregado ainda'}
`;

    const response = await this.client.messages.create({
      model: this.config.model,
      max_tokens: 1024,
      system: systemPrompt,
      messages: [
        ...context.history.map(h => ({ 
          role: h.role as 'user' | 'assistant', 
          content: h.content 
        })),
        { role: 'user', content: message },
      ],
    });
    
    const text = response.content[0].type === 'text' ? response.content[0].text : '';
    
    return {
      response: text,
      suggestions: [
        'Mostre total de vendas do mês',
        'Quais tabelas estão disponíveis?',
        'Top 10 produtos mais vendidos',
      ],
      processingSteps: steps,
    };
  }
  
  /**
   * Processa intenção desconhecida
   */
  private async handleUnknown(
    message: string,
    context: ProcessingContext,
    steps: ProcessResult['processingSteps']
  ): Promise<ProcessResult> {
    return {
      response: `Não entendi completamente sua pergunta. Você pode:
        
• Perguntar sobre dados: "Qual foi o faturamento do mês passado?"
• Explorar estrutura: "Quais tabelas existem?"
• Pedir definições: "O que é margem de contribuição?"

Como posso ajudar?`,
      suggestions: [
        'Mostre os dados disponíveis',
        'Ajuda',
        'Exemplos de perguntas',
      ],
      processingSteps: steps,
    };
  }
  
  // ===========================================
  // HELPERS
  // ===========================================
  
  private async formatDataResponse(
    question: string, 
    data: any, 
    explanation: string
  ): Promise<string> {
    // Usar LLM para formatar resposta natural
    const systemPrompt = `
Você é um analista de dados. Dado os resultados de uma query, 
forneça uma resposta clara e concisa em português.
Inclua os números principais e insights relevantes.
`;

    const response = await this.client.messages.create({
      model: this.config.model,
      max_tokens: 1024,
      system: systemPrompt,
      messages: [{
        role: 'user',
        content: `
Pergunta: ${question}

Explicação da Query: ${explanation}

Dados retornados:
${JSON.stringify(data, null, 2)}

Forneça uma resposta natural para o usuário.
`,
      }],
    });
    
    return response.content[0].type === 'text' ? response.content[0].text : '';
  }
  
  private async formatSchemaResponse(message: string, schema: any): Promise<string> {
    const tables = schema.tables || [];
    
    let response = `📊 **Estrutura do Dataset**\n\n`;
    
    for (const table of tables.slice(0, 5)) {
      response += `**${table.name}**\n`;
      response += `  Colunas: ${table.columns?.length || 0}\n`;
      response += `  Medidas: ${table.measures?.length || 0}\n\n`;
    }
    
    if (tables.length > 5) {
      response += `... e mais ${tables.length - 5} tabelas.\n`;
    }
    
    return response;
  }
  
  private inferVisualization(data: any): ProcessResult['visualization'] {
    if (!data) return 'text';
    
    // Se retornou apenas um número
    if (typeof data === 'number' || (Array.isArray(data) && data.length === 1)) {
      return 'number';
    }
    
    // Se é uma lista de itens
    if (Array.isArray(data) && data.length > 1) {
      // Se tem muitas colunas numéricas, sugere gráfico
      const firstRow = data[0];
      const numericColumns = Object.values(firstRow || {}).filter(
        v => typeof v === 'number'
      ).length;
      
      if (numericColumns > 1) return 'chart';
      return 'table';
    }
    
    return 'table';
  }
}

// ===========================================
// FACTORY
// ===========================================

let orchestratorInstance: OrchestratorAgent | null = null;

export function getOrchestrator(config?: OrchestratorConfig): OrchestratorAgent {
  if (!orchestratorInstance && config) {
    orchestratorInstance = new OrchestratorAgent(config);
  }
  
  if (!orchestratorInstance) {
    throw new Error('Orchestrator not initialized');
  }
  
  return orchestratorInstance;
}
