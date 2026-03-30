/**
 * Power BI API Client
 * 
 * Cliente para comunicação com a API REST do Power BI.
 * Suporta autenticação via Service Principal ou User Token.
 */

import { ConfidentialClientApplication, PublicClientApplication } from '@azure/msal-browser';

// ===========================================
// TIPOS
// ===========================================

export interface PowerBIConfig {
  tenantId: string;
  clientId: string;
  clientSecret?: string;
  workspaceId: string;
  authMode: 'service-principal' | 'user-token';
}

export interface Dataset {
  id: string;
  name: string;
  configuredBy: string;
  createdDate: string;
  isRefreshable: boolean;
  isOnPremGatewayRequired: boolean;
}

export interface Table {
  name: string;
  columns: Column[];
  measures: Measure[];
  isHidden: boolean;
}

export interface Column {
  name: string;
  dataType: string;
  isHidden: boolean;
  description?: string;
  formatString?: string;
}

export interface Measure {
  name: string;
  expression: string;
  description?: string;
  formatString?: string;
}

export interface Relationship {
  name: string;
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
  crossFilteringBehavior: 'oneDirection' | 'bothDirections';
  cardinality: 'oneToOne' | 'oneToMany' | 'manyToOne' | 'manyToMany';
}

export interface QueryResult {
  tables: Array<{
    name: string;
    columns: string[];
    rows: any[][];
  }>;
}

export interface DatasetSchema {
  tables: Table[];
  relationships: Relationship[];
}

// ===========================================
// POWER BI CLIENT
// ===========================================

export class PowerBIClient {
  private config: PowerBIConfig;
  private accessToken: string | null = null;
  private tokenExpiry: Date | null = null;
  
  private readonly baseUrl = 'https://api.powerbi.com/v1.0/myorg';
  private readonly scope = 'https://analysis.windows.net/powerbi/api/.default';
  
  constructor(config: PowerBIConfig) {
    this.config = config;
  }
  
  // ===========================================
  // AUTENTICAÇÃO
  // ===========================================
  
  /**
   * Obtém token de acesso via Service Principal
   */
  private async getServicePrincipalToken(): Promise<string> {
    const tokenUrl = `https://login.microsoftonline.com/${this.config.tenantId}/oauth2/v2.0/token`;
    
    const params = new URLSearchParams({
      client_id: this.config.clientId,
      client_secret: this.config.clientSecret!,
      scope: this.scope,
      grant_type: 'client_credentials',
    });
    
    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params,
    });
    
    if (!response.ok) {
      throw new Error(`Auth failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    this.accessToken = data.access_token;
    this.tokenExpiry = new Date(Date.now() + data.expires_in * 1000);
    
    return this.accessToken;
  }
  
  /**
   * Garante que temos um token válido
   */
  private async ensureToken(): Promise<string> {
    if (this.accessToken && this.tokenExpiry && this.tokenExpiry > new Date()) {
      return this.accessToken;
    }
    
    if (this.config.authMode === 'service-principal') {
      return this.getServicePrincipalToken();
    }
    
    throw new Error('User token auth requires MSAL setup');
  }
  
  /**
   * Faz requisição autenticada para a API
   */
  private async request<T>(
    endpoint: string, 
    options: RequestInit = {}
  ): Promise<T> {
    const token = await this.ensureToken();
    
    const url = endpoint.startsWith('http') ? endpoint : `${this.baseUrl}${endpoint}`;
    
    const response = await fetch(url, {
      ...options,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });
    
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`API Error ${response.status}: ${error}`);
    }
    
    return response.json();
  }
  
  // ===========================================
  // WORKSPACES
  // ===========================================
  
  /**
   * Lista todos os workspaces acessíveis
   */
  async listWorkspaces(): Promise<Array<{ id: string; name: string; type: string }>> {
    const data = await this.request<{ value: any[] }>('/groups');
    return data.value.map(w => ({
      id: w.id,
      name: w.name,
      type: w.type,
    }));
  }
  
  /**
   * Obtém detalhes de um workspace específico
   */
  async getWorkspace(workspaceId?: string): Promise<{ id: string; name: string }> {
    const id = workspaceId || this.config.workspaceId;
    const data = await this.request<any>(`/groups/${id}`);
    return { id: data.id, name: data.name };
  }
  
  // ===========================================
  // DATASETS
  // ===========================================
  
  /**
   * Lista datasets de um workspace
   */
  async listDatasets(workspaceId?: string): Promise<Dataset[]> {
    const id = workspaceId || this.config.workspaceId;
    const data = await this.request<{ value: Dataset[] }>(`/groups/${id}/datasets`);
    return data.value;
  }
  
  /**
   * Obtém schema de um dataset via DMV queries
   */
  async getDatasetSchema(datasetId: string, workspaceId?: string): Promise<DatasetSchema> {
    const wsId = workspaceId || this.config.workspaceId;
    
    // Query para obter tabelas e colunas
    const tablesQuery = `
      EVALUATE
      SELECTCOLUMNS(
        INFO.TABLES(),
        "TableName", [Name],
        "IsHidden", [IsHidden]
      )
    `;
    
    const columnsQuery = `
      EVALUATE
      SELECTCOLUMNS(
        INFO.COLUMNS(),
        "TableName", [TableName],
        "ColumnName", [Name],
        "DataType", [DataType],
        "IsHidden", [IsHidden],
        "Description", [Description],
        "FormatString", [FormatString]
      )
    `;
    
    const measuresQuery = `
      EVALUATE
      SELECTCOLUMNS(
        INFO.MEASURES(),
        "TableName", [TableName],
        "MeasureName", [Name],
        "Expression", [Expression],
        "Description", [Description],
        "FormatString", [FormatString]
      )
    `;
    
    const relationshipsQuery = `
      EVALUATE
      SELECTCOLUMNS(
        INFO.RELATIONSHIPS(),
        "RelationshipName", [Name],
        "FromTable", [FromTableName],
        "FromColumn", [FromColumnName],
        "ToTable", [ToTableName],
        "ToColumn", [ToColumnName],
        "CrossFilterDirection", [CrossFilteringBehavior],
        "Cardinality", [Cardinality]
      )
    `;
    
    // Executar queries
    const [tablesResult, columnsResult, measuresResult, relationshipsResult] = await Promise.all([
      this.executeQuery(datasetId, tablesQuery, wsId),
      this.executeQuery(datasetId, columnsQuery, wsId),
      this.executeQuery(datasetId, measuresQuery, wsId),
      this.executeQuery(datasetId, relationshipsQuery, wsId),
    ]);
    
    // Montar schema
    const tables: Table[] = this.parseTablesResult(
      tablesResult, 
      columnsResult, 
      measuresResult
    );
    
    const relationships: Relationship[] = this.parseRelationshipsResult(relationshipsResult);
    
    return { tables, relationships };
  }
  
  /**
   * Parseia resultado de tabelas
   */
  private parseTablesResult(
    tablesResult: QueryResult,
    columnsResult: QueryResult,
    measuresResult: QueryResult
  ): Table[] {
    const tables: Map<string, Table> = new Map();
    
    // Criar tabelas
    if (tablesResult.tables[0]) {
      for (const row of tablesResult.tables[0].rows) {
        tables.set(row[0], {
          name: row[0],
          isHidden: row[1],
          columns: [],
          measures: [],
        });
      }
    }
    
    // Adicionar colunas
    if (columnsResult.tables[0]) {
      for (const row of columnsResult.tables[0].rows) {
        const table = tables.get(row[0]);
        if (table) {
          table.columns.push({
            name: row[1],
            dataType: row[2],
            isHidden: row[3],
            description: row[4],
            formatString: row[5],
          });
        }
      }
    }
    
    // Adicionar medidas
    if (measuresResult.tables[0]) {
      for (const row of measuresResult.tables[0].rows) {
        const table = tables.get(row[0]);
        if (table) {
          table.measures.push({
            name: row[1],
            expression: row[2],
            description: row[3],
            formatString: row[4],
          });
        }
      }
    }
    
    return Array.from(tables.values());
  }
  
  /**
   * Parseia resultado de relacionamentos
   */
  private parseRelationshipsResult(result: QueryResult): Relationship[] {
    if (!result.tables[0]) return [];
    
    return result.tables[0].rows.map(row => ({
      name: row[0],
      fromTable: row[1],
      fromColumn: row[2],
      toTable: row[3],
      toColumn: row[4],
      crossFilteringBehavior: row[5] === 1 ? 'bothDirections' : 'oneDirection',
      cardinality: this.mapCardinality(row[6]),
    }));
  }
  
  private mapCardinality(value: number): Relationship['cardinality'] {
    const map: Record<number, Relationship['cardinality']> = {
      1: 'oneToMany',
      2: 'manyToOne',
      3: 'manyToMany',
      4: 'oneToOne',
    };
    return map[value] || 'oneToMany';
  }
  
  // ===========================================
  // QUERIES
  // ===========================================
  
  /**
   * Executa query DAX no dataset
   */
  async executeQuery(
    datasetId: string, 
    daxQuery: string, 
    workspaceId?: string
  ): Promise<QueryResult> {
    const wsId = workspaceId || this.config.workspaceId;
    
    const data = await this.request<any>(
      `/groups/${wsId}/datasets/${datasetId}/executeQueries`,
      {
        method: 'POST',
        body: JSON.stringify({
          queries: [{ query: daxQuery }],
          serializerSettings: { includeNulls: true },
        }),
      }
    );
    
    // Parsear resultado
    const result = data.results[0];
    if (result.error) {
      throw new Error(`DAX Error: ${result.error.message}`);
    }
    
    return {
      tables: result.tables.map((t: any) => ({
        name: t.name || 'Result',
        columns: t.columns?.map((c: any) => c.name) || [],
        rows: t.rows || [],
      })),
    };
  }
  
  // ===========================================
  // REFRESH
  // ===========================================
  
  /**
   * Dispara refresh de um dataset
   */
  async refreshDataset(datasetId: string, workspaceId?: string): Promise<void> {
    const wsId = workspaceId || this.config.workspaceId;
    
    await this.request(`/groups/${wsId}/datasets/${datasetId}/refreshes`, {
      method: 'POST',
      body: JSON.stringify({ notifyOption: 'NoNotification' }),
    });
  }
  
  /**
   * Verifica status do último refresh
   */
  async getRefreshHistory(
    datasetId: string, 
    workspaceId?: string
  ): Promise<Array<{ status: string; startTime: string; endTime?: string }>> {
    const wsId = workspaceId || this.config.workspaceId;
    
    const data = await this.request<{ value: any[] }>(
      `/groups/${wsId}/datasets/${datasetId}/refreshes`
    );
    
    return data.value.map(r => ({
      status: r.status,
      startTime: r.startTime,
      endTime: r.endTime,
    }));
  }
}

// ===========================================
// FACTORY
// ===========================================

let clientInstance: PowerBIClient | null = null;

export function getPowerBIClient(config?: PowerBIConfig): PowerBIClient {
  if (!clientInstance && config) {
    clientInstance = new PowerBIClient(config);
  }
  
  if (!clientInstance) {
    throw new Error('PowerBI client not initialized. Provide config.');
  }
  
  return clientInstance;
}

export function createPowerBIClient(config: PowerBIConfig): PowerBIClient {
  clientInstance = new PowerBIClient(config);
  return clientInstance;
}
