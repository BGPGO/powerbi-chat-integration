"""
Pydantic models for API requests and responses.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ============================================================================
# Enums
# ============================================================================

class MessageRole(str, Enum):
    """Chat message roles."""
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class IntentType(str, Enum):
    """Query intent classification."""
    SCHEMA_QUERY = "schema_query"
    TRANSLATION = "translation"
    DATA_QUERY = "data_query"
    EXPLORATION = "exploration"
    HYBRID = "hybrid"


class VisualizationType(str, Enum):
    """Supported visualization types."""
    LINE_CHART = "line_chart"
    BAR_CHART = "bar_chart"
    PIE_CHART = "pie_chart"
    CARD = "card"
    TABLE = "table"
    AREA_CHART = "area_chart"
    SCATTER_PLOT = "scatter_plot"


class AgentType(str, Enum):
    """Available agent types."""
    DICTIONARY = "dictionary"
    DATASOURCE = "datasource"
    QUERY_BUILDER = "query_builder"


# ============================================================================
# Chat Models
# ============================================================================

class ChatMessage(BaseModel):
    """Single chat message."""
    role: MessageRole
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChatRequest(BaseModel):
    """Incoming chat request."""
    message: str = Field(..., min_length=1, max_length=4000)
    conversation_id: str | None = Field(default=None, description="Existing conversation ID")
    dataset_id: str | None = Field(default=None, description="Specific dataset to query")
    workspace_id: str | None = Field(default=None, description="Power BI workspace ID")
    report_id: str | None = Field(default=None, description="Power BI report ID (for hard filter extraction)")
    current_page: str | None = Field(default=None, description="Display name of the active Power BI page")

    class Config:
        json_schema_extra = {
            "example": {
                "message": "Qual foi o faturamento total do último trimestre?",
                "conversation_id": "conv_abc123",
                "dataset_id": "dataset_xyz"
            }
        }


class VisualizationSuggestion(BaseModel):
    """Suggested visualization for query results."""
    type: VisualizationType
    title: str
    description: str
    config: dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(ge=0, le=1)


class QueryResult(BaseModel):
    """Results from a DAX query execution."""
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    execution_time_ms: float
    dax_query: str | None = None
    truncated: bool = False


class AgentResponse(BaseModel):
    """Response from a single agent."""
    agent: AgentType
    content: str
    data: dict[str, Any] = Field(default_factory=dict)
    execution_time_ms: float


class PowerBIFilter(BaseModel):
    """Filters to apply to the embedded Power BI report via URL parameters."""
    year: str | None = None
    month: str | None = None
    months_in_range: list[str] | None = None
    quarter: int | None = None
    rolling_window_days: int | None = None
    description: str = ""
    has_filter: bool = False


class ChatResponse(BaseModel):
    """Complete chat response."""
    conversation_id: str
    message: str
    intent: IntentType
    agents_used: list[AgentType]
    query_result: QueryResult | None = None
    visualizations: list[VisualizationSuggestion] = Field(default_factory=list)
    suggestions: list[str] = Field(default_factory=list, description="Follow-up questions")
    agent_responses: list[AgentResponse] = Field(default_factory=list)
    total_time_ms: float
    tokens_used: int = 0
    powerbi_filters: PowerBIFilter | None = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "conversation_id": "conv_abc123",
                "message": "O faturamento total do último trimestre foi R$ 1.234.567,89.",
                "intent": "data_query",
                "agents_used": ["dictionary", "query_builder"],
                "query_result": {
                    "columns": ["Trimestre", "Faturamento"],
                    "rows": [{"Trimestre": "Q4 2023", "Faturamento": 1234567.89}],
                    "row_count": 1,
                    "execution_time_ms": 234.5,
                    "dax_query": "EVALUATE SUMMARIZE(...)"
                },
                "visualizations": [
                    {
                        "type": "card",
                        "title": "Faturamento Q4",
                        "description": "Total do trimestre",
                        "confidence": 0.95
                    }
                ],
                "suggestions": [
                    "Compare com o trimestre anterior",
                    "Quebre por região"
                ],
                "total_time_ms": 1234.5,
                "tokens_used": 450
            }
        }


# ============================================================================
# Schema Models
# ============================================================================

class ColumnInfo(BaseModel):
    """Information about a table column."""
    name: str
    data_type: str
    description: str | None = None
    business_name: str | None = None
    sample_values: list[Any] = Field(default_factory=list)
    is_key: bool = False
    is_nullable: bool = True


class TableInfo(BaseModel):
    """Information about a dataset table."""
    name: str
    description: str | None = None
    business_name: str | None = None
    columns: list[ColumnInfo] = Field(default_factory=list)
    row_count: int | None = None
    relationships: list[str] = Field(default_factory=list)


class DatasetInfo(BaseModel):
    """Information about a Power BI dataset."""
    id: str
    name: str
    description: str | None = None
    workspace_id: str
    tables: list[TableInfo] = Field(default_factory=list)
    last_refresh: datetime | None = None
    configured_by: str | None = None


class SchemaResponse(BaseModel):
    """Response containing dataset schema."""
    dataset: DatasetInfo
    glossary: dict[str, str] = Field(default_factory=dict, description="Term translations")


# ============================================================================
# Report Models
# ============================================================================

class ReportInfo(BaseModel):
    """Power BI report information."""
    id: str
    name: str
    embed_url: str
    web_url: str
    dataset_id: str | None = None
    workspace_id: str | None = None
    powerbi_report_id: str | None = None  # ID real do Power BI para usar na API REST
    embed_token: str | None = None
    embed_token_expiry: str | None = None


class EmbedTokenResponse(BaseModel):
    """Embed token for Power BI report."""
    token: str
    token_id: str
    expiration: str
    embed_url: str
    report_id: str


# ============================================================================
# Workspace Models
# ============================================================================

class WorkspaceInfo(BaseModel):
    """Power BI workspace information."""
    id: str
    name: str
    description: str | None = None
    type: str = "Workspace"
    is_read_only: bool = False


class WorkspacesResponse(BaseModel):
    """List of available workspaces."""
    workspaces: list[WorkspaceInfo]
    total: int


class DatasetsResponse(BaseModel):
    """List of datasets in a workspace."""
    workspace_id: str
    datasets: list[DatasetInfo]
    total: int


# ============================================================================
# History Models
# ============================================================================

class ConversationSummary(BaseModel):
    """Summary of a conversation."""
    id: str
    title: str
    created_at: datetime
    updated_at: datetime
    message_count: int
    dataset_id: str | None = None


class ConversationDetail(BaseModel):
    """Full conversation with messages."""
    id: str
    title: str
    messages: list[ChatMessage]
    created_at: datetime
    updated_at: datetime
    dataset_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ConversationsResponse(BaseModel):
    """List of conversations."""
    conversations: list[ConversationSummary]
    total: int
    page: int = 1
    page_size: int = 20


# ============================================================================
# Error Models
# ============================================================================

class ErrorDetail(BaseModel):
    """Detailed error information."""
    code: str
    message: str
    field: str | None = None


class ErrorResponse(BaseModel):
    """API error response."""
    error: str
    details: list[ErrorDetail] = Field(default_factory=list)
    request_id: str | None = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "Query execution failed",
                "details": [
                    {
                        "code": "DAX_ERROR",
                        "message": "Invalid column reference: [Sales Amount]"
                    }
                ],
                "request_id": "req_abc123"
            }
        }


# ============================================================================
# Health Models
# ============================================================================

class ServiceHealth(BaseModel):
    """Health status of a service."""
    name: str
    status: Literal["healthy", "degraded", "unhealthy"]
    latency_ms: float | None = None
    message: str | None = None


class HealthResponse(BaseModel):
    """Application health check response."""
    status: Literal["healthy", "degraded", "unhealthy"]
    version: str
    environment: str
    services: list[ServiceHealth] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
