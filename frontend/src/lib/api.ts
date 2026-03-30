import axios, { AxiosInstance, AxiosError } from 'axios';
import type {
  ChatRequest,
  ChatResponse,
  ConversationDetail,
  ConversationSummary,
  DatasetInfo,
  DatasetsResponse,
  QueryResult,
  SchemaResponse,
  WorkspaceInfo,
  WorkspacesResponse,
} from '../types';

export interface ReportInfo {
  id: string;
  name: string;
  embed_url: string;
  web_url: string;
  dataset_id?: string;
  workspace_id?: string;
  powerbi_report_id?: string;
  embed_token?: string | null;
  embed_token_expiry?: string | null;
}

export interface EmbedTokenResponse {
  token: string;
  token_id: string;
  expiration: string;
  embed_url: string;
  report_id: string;
}

// API base URL from environment
const API_BASE_URL = import.meta.env.VITE_API_URL || '/api/v1';

// Create axios instance
const api: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: 60000, // 60s for long queries
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log(`[API] ${config.method?.toUpperCase()} ${config.url}`);
    return config;
  },
  (error) => {
    console.error('[API] Request error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    const message = (error.response?.data as { error?: string })?.error 
      || error.message 
      || 'An unexpected error occurred';
    
    console.error('[API] Response error:', message);
    return Promise.reject(new Error(message));
  }
);

// ============================================================================
// Chat API
// ============================================================================

export const chatApi = {
  /**
   * Send a chat message
   */
  sendMessage: async (request: ChatRequest): Promise<ChatResponse> => {
    const response = await api.post<ChatResponse>('/chat', request);
    return response.data;
  },

  /**
   * List conversations
   */
  listConversations: async (
    page = 1,
    pageSize = 20,
    datasetId?: string
  ): Promise<{ conversations: ConversationSummary[]; total: number }> => {
    const params = new URLSearchParams({
      page: page.toString(),
      page_size: pageSize.toString(),
    });
    if (datasetId) params.append('dataset_id', datasetId);

    const response = await api.get(`/chat/conversations?${params}`);
    return response.data;
  },

  /**
   * Get a specific conversation
   */
  getConversation: async (id: string): Promise<ConversationDetail> => {
    const response = await api.get<ConversationDetail>(`/chat/conversations/${id}`);
    return response.data;
  },

  /**
   * Delete a conversation
   */
  deleteConversation: async (id: string): Promise<void> => {
    await api.delete(`/chat/conversations/${id}`);
  },
};

// ============================================================================
// Workspaces API
// ============================================================================

export const workspacesApi = {
  /**
   * List all workspaces
   */
  list: async (): Promise<WorkspaceInfo[]> => {
    const response = await api.get<WorkspacesResponse>('/workspaces');
    return response.data.workspaces;
  },

  /**
   * Get a specific workspace
   */
  get: async (id: string): Promise<WorkspaceInfo> => {
    const response = await api.get<WorkspaceInfo>(`/workspaces/${id}`);
    return response.data;
  },

  /**
   * List datasets in a workspace
   */
  listDatasets: async (workspaceId: string): Promise<DatasetInfo[]> => {
    const response = await api.get<DatasetsResponse>(`/workspaces/${workspaceId}/datasets`);
    return response.data.datasets;
  },
};

// ============================================================================
// Datasets API
// ============================================================================

export const datasetsApi = {
  /**
   * Get a specific dataset
   */
  get: async (id: string, workspaceId?: string): Promise<DatasetInfo> => {
    const params = workspaceId ? `?workspace_id=${workspaceId}` : '';
    const response = await api.get<DatasetInfo>(`/datasets/${id}${params}`);
    return response.data;
  },

  /**
   * Get dataset schema
   */
  getSchema: async (
    id: string,
    workspaceId?: string,
    includeGlossary = true
  ): Promise<SchemaResponse> => {
    const params = new URLSearchParams();
    if (workspaceId) params.append('workspace_id', workspaceId);
    params.append('include_glossary', includeGlossary.toString());

    const response = await api.get<SchemaResponse>(`/datasets/${id}/schema?${params}`);
    return response.data;
  },

  /**
   * Execute a DAX query
   */
  executeQuery: async (
    id: string,
    query: string,
    workspaceId?: string,
    maxRows = 10000
  ): Promise<QueryResult> => {
    const params = new URLSearchParams();
    if (workspaceId) params.append('workspace_id', workspaceId);
    params.append('max_rows', maxRows.toString());

    const response = await api.post<QueryResult>(
      `/datasets/${id}/query?${params}`,
      query,
      { headers: { 'Content-Type': 'text/plain' } }
    );
    return response.data;
  },

  /**
   * Get refresh history
   */
  getRefreshHistory: async (
    id: string,
    workspaceId?: string,
    top = 10
  ): Promise<{ refreshes: unknown[]; total: number }> => {
    const params = new URLSearchParams();
    if (workspaceId) params.append('workspace_id', workspaceId);
    params.append('top', top.toString());

    const response = await api.get(`/datasets/${id}/refresh?${params}`);
    return response.data;
  },
};

// ============================================================================
// Health API
// ============================================================================

export const healthApi = {
  /**
   * Full health check
   */
  check: async (): Promise<{
    status: 'healthy' | 'degraded' | 'unhealthy';
    services: Array<{ name: string; status: string; latency_ms?: number }>;
  }> => {
    const response = await api.get('/health');
    return response.data;
  },

  /**
   * Simple liveness check
   */
  live: async (): Promise<boolean> => {
    try {
      await api.get('/health/live');
      return true;
    } catch {
      return false;
    }
  },
};

// ============================================================================
// Reports API
// ============================================================================

export const reportsApi = {
  list: async (): Promise<ReportInfo[]> => {
    const response = await api.get<ReportInfo[]>('/reports');
    return response.data;
  },
};

// ============================================================================
// Export API
// ============================================================================

export interface ReportPage {
  name: string;
  display_name: string;
  order: number;
}

export interface StorytellingJobStatus {
  id: string;
  status: 'queued' | 'processing' | 'completed' | 'failed';
  progress: number;
  total_steps: number;
  current_step: string;
  error: string | null;
  created_at: string;
  updated_at: string;
}

export const exportApi = {
  listPages: async (reportId?: string): Promise<ReportPage[]> => {
    const params = reportId ? `?report_id=${reportId}` : '';
    const response = await api.get<ReportPage[]>(`/export/pages${params}`);
    return response.data;
  },

  exportPdf: async (
    embedUrl: string,
    filters?: Record<string, string | string[]>,
    reportName?: string,
  ): Promise<Blob> => {
    const response = await api.post(
      '/export/pdf',
      { embed_url: embedUrl, filters: filters ?? null, report_name: reportName ?? 'Relatório' },
      { responseType: 'blob', timeout: 180000 },
    );
    return response.data as Blob;
  },

  startStorytelling: async (
    embedUrl: string,
    filters?: Record<string, string | string[]>,
    reportName?: string,
  ): Promise<{ job_id: string; status: string }> => {
    const response = await api.post('/export/storytelling', {
      embed_url: embedUrl,
      filters: filters ?? null,
      report_name: reportName ?? 'Relatório',
    });
    return response.data;
  },

  getStorytellingStatus: async (jobId: string): Promise<StorytellingJobStatus> => {
    const response = await api.get<StorytellingJobStatus>(`/export/storytelling/${jobId}`);
    return response.data;
  },

  downloadStorytelling: async (jobId: string): Promise<Blob> => {
    const response = await api.get(`/export/storytelling/${jobId}/download`, {
      responseType: 'blob',
      timeout: 30000,
    });
    return response.data as Blob;
  },
};

export default api;
