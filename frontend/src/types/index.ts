// ============================================================================
// Enums
// ============================================================================

export type MessageRole = 'user' | 'assistant' | 'system';

export type IntentType = 
  | 'schema_query' 
  | 'translation' 
  | 'data_query' 
  | 'exploration' 
  | 'hybrid';

export type VisualizationType = 
  | 'line_chart' 
  | 'bar_chart' 
  | 'pie_chart' 
  | 'card' 
  | 'table' 
  | 'area_chart' 
  | 'scatter_plot';

export type AgentType = 'dictionary' | 'datasource' | 'query_builder';

// ============================================================================
// Chat Types
// ============================================================================

export interface ChatMessage {
  id: string;
  role: MessageRole;
  content: string;
  timestamp: Date;
  metadata?: Record<string, unknown>;
  isLoading?: boolean;
}

export interface ChatRequest {
  message: string;
  conversation_id?: string;
  dataset_id?: string;
  workspace_id?: string;
  report_id?: string;
  current_page?: string;
}

export interface VisualizationSuggestion {
  type: VisualizationType;
  title: string;
  description: string;
  config: Record<string, unknown>;
  confidence: number;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  execution_time_ms: number;
  dax_query?: string;
  truncated: boolean;
}

export interface AgentResponse {
  agent: AgentType;
  content: string;
  data: Record<string, unknown>;
  execution_time_ms: number;
}

export interface PowerBIFilter {
  year?: string | null;
  month?: string | null;
  months_in_range?: string[] | null;
  quarter?: number | null;
  rolling_window_days?: number | null;
  description: string;
  has_filter: boolean;
}

export interface ChatResponse {
  conversation_id: string;
  message: string;
  intent: IntentType;
  agents_used: AgentType[];
  query_result?: QueryResult;
  visualizations: VisualizationSuggestion[];
  suggestions: string[];
  agent_responses: AgentResponse[];
  total_time_ms: number;
  tokens_used: number;
  powerbi_filters?: PowerBIFilter | null;
}

// ============================================================================
// Schema Types
// ============================================================================

export interface ColumnInfo {
  name: string;
  data_type: string;
  description?: string;
  business_name?: string;
  sample_values: unknown[];
  is_key: boolean;
  is_nullable: boolean;
}

export interface TableInfo {
  name: string;
  description?: string;
  business_name?: string;
  columns: ColumnInfo[];
  row_count?: number;
  relationships: string[];
}

export interface DatasetInfo {
  id: string;
  name: string;
  description?: string;
  workspace_id: string;
  tables: TableInfo[];
  last_refresh?: Date;
  configured_by?: string;
}

export interface SchemaResponse {
  dataset: DatasetInfo;
  glossary: Record<string, string>;
}

// ============================================================================
// Workspace Types
// ============================================================================

export interface WorkspaceInfo {
  id: string;
  name: string;
  description?: string;
  type: string;
  is_read_only: boolean;
}

export interface WorkspacesResponse {
  workspaces: WorkspaceInfo[];
  total: number;
}

export interface DatasetsResponse {
  workspace_id: string;
  datasets: DatasetInfo[];
  total: number;
}

// ============================================================================
// Conversation Types
// ============================================================================

export interface ConversationSummary {
  id: string;
  title: string;
  created_at: Date;
  updated_at: Date;
  message_count: number;
  dataset_id?: string;
}

export interface ConversationDetail {
  id: string;
  title: string;
  messages: ChatMessage[];
  created_at: Date;
  updated_at: Date;
  dataset_id?: string;
  metadata: Record<string, unknown>;
}

// ============================================================================
// UI State Types
// ============================================================================

export interface AppState {
  // Current conversation
  conversationId: string | null;
  messages: ChatMessage[];
  isLoading: boolean;
  error: string | null;
  
  // Selected resources
  selectedWorkspace: WorkspaceInfo | null;
  selectedDataset: DatasetInfo | null;
  
  // UI state
  sidebarOpen: boolean;
  darkMode: boolean;
  showSchema: boolean;
}

export interface ChatStore extends AppState {
  // Actions
  sendMessage: (content: string) => Promise<void>;
  clearConversation: () => void;
  setWorkspace: (workspace: WorkspaceInfo | null) => void;
  setDataset: (dataset: DatasetInfo | null) => void;
  toggleSidebar: () => void;
  toggleDarkMode: () => void;
  toggleSchema: () => void;
  setError: (error: string | null) => void;
}
