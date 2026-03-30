"""
Chat API routes - main conversation endpoint.
"""

import time
import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from app.agents.orchestrator import create_orchestrator, OrchestratorState
from app.models.schemas import (
    AgentResponse,
    AgentType,
    ChatRequest,
    ChatResponse,
    ConversationDetail,
    ConversationsResponse,
    ConversationSummary,
    ErrorResponse,
    IntentType,
    PowerBIFilter,
    QueryResult,
    VisualizationSuggestion,
    VisualizationType,
)

logger = structlog.get_logger()

router = APIRouter()

# In-memory conversation store (replace with database in production)
conversations: dict[str, ConversationDetail] = {}


async def get_orchestrator():
    """Dependency to get orchestrator instance."""
    return create_orchestrator()


@router.post(
    "",
    response_model=ChatResponse,
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    },
    summary="Send a chat message",
    description="Send a natural language message to query Power BI data"
)
async def send_message(
    request: ChatRequest,
    orchestrator = Depends(get_orchestrator)
):
    """
    Process a chat message and return AI-generated response with data.
    
    The orchestrator will:
    1. Classify the intent of the message
    2. Route to appropriate agents (dictionary, datasource, query_builder)
    3. Execute any necessary DAX queries
    4. Return formatted response with visualizations
    """
    start_time = time.perf_counter()
    
    # Get or create conversation
    conversation_id = request.conversation_id or f"conv_{uuid.uuid4().hex[:12]}"
    
    logger.info(
        "Processing chat message",
        conversation_id=conversation_id,
        message_length=len(request.message),
        dataset_id=request.dataset_id
    )
    
    try:
        # Build initial state for orchestrator
        initial_state: OrchestratorState = {
            "messages": [{"role": "user", "content": request.message}],
            "current_query": request.message,
            "intent": None,
            "dataset_id": request.dataset_id,
            "workspace_id": request.workspace_id,
            "report_id": request.report_id,
            "current_page": request.current_page,
            "schema_context": {},
            "translation_context": {},
            "query_result": None,
            "agents_called": [],
            "agent_outputs": {},
            "final_response": None,
            "suggestions": [],
            "error": None,
            "template_dax": None,
            "resolved_measure": None,
            "powerbi_filters": None,
        }
        
        # Run orchestrator
        result = await orchestrator.ainvoke(initial_state)
        
        # Extract results
        total_time = (time.perf_counter() - start_time) * 1000
        
        # Build query result if present
        query_result = None
        if result.get("query_result"):
            qr = result["query_result"]
            query_result = QueryResult(
                columns=qr.get("columns", []),
                rows=qr.get("rows", []),
                row_count=len(qr.get("rows", [])),
                execution_time_ms=qr.get("execution_time_ms", 0),
                dax_query=qr.get("dax_query"),
                truncated=qr.get("truncated", False)
            )
        
        # Build visualization suggestions
        visualizations = []
        if result.get("agent_outputs", {}).get("query_builder", {}).get("visualizations"):
            for viz in result["agent_outputs"]["query_builder"]["visualizations"]:
                visualizations.append(VisualizationSuggestion(
                    type=VisualizationType(viz.get("type", "table")),
                    title=viz.get("title", ""),
                    description=viz.get("description", ""),
                    config=viz.get("config", {}),
                    confidence=viz.get("confidence", 0.5)
                ))
        
        # Build agent responses
        agent_responses = []
        for agent_name, output in result.get("agent_outputs", {}).items():
            if output:
                agent_responses.append(AgentResponse(
                    agent=AgentType(agent_name),
                    content=output.get("content", ""),
                    data=output.get("data", {}),
                    execution_time_ms=output.get("execution_time_ms", 0)
                ))
        
        # Map intent
        intent_mapping = {
            "schema_query": IntentType.SCHEMA_QUERY,
            "translation": IntentType.TRANSLATION,
            "data_query": IntentType.DATA_QUERY,
            "exploration": IntentType.EXPLORATION,
            "hybrid": IntentType.HYBRID
        }
        intent = intent_mapping.get(result.get("intent", ""), IntentType.DATA_QUERY)
        
        # Build PowerBI filters if present
        pbi_filters_data = result.get("powerbi_filters")
        powerbi_filters = None
        if pbi_filters_data:
            powerbi_filters = PowerBIFilter(
                year=pbi_filters_data.get("year"),
                month=pbi_filters_data.get("month"),
                months_in_range=pbi_filters_data.get("months_in_range"),
                quarter=pbi_filters_data.get("quarter"),
                rolling_window_days=pbi_filters_data.get("rolling_window_days"),
                description=pbi_filters_data.get("description", ""),
                has_filter=pbi_filters_data.get("has_filter", False),
            )

        response = ChatResponse(
            conversation_id=conversation_id,
            message=result.get("final_response", "Não foi possível processar sua solicitação."),
            intent=intent,
            agents_used=[AgentType(a) for a in result.get("agents_called", [])],
            query_result=query_result,
            visualizations=visualizations,
            suggestions=result.get("suggestions", []),
            agent_responses=agent_responses,
            total_time_ms=total_time,
            tokens_used=result.get("tokens_used", 0),
            powerbi_filters=powerbi_filters,
        )
        
        logger.info(
            "Chat message processed",
            conversation_id=conversation_id,
            intent=intent.value,
            agents_used=[a.value for a in response.agents_used],
            total_time_ms=total_time
        )
        
        return response
        
    except Exception as e:
        logger.exception("Failed to process chat message", error=str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to process message",
                "details": [{"code": "PROCESSING_ERROR", "message": str(e)}]
            }
        )


@router.get(
    "/conversations",
    response_model=ConversationsResponse,
    summary="List conversations",
    description="Get paginated list of conversation history"
)
async def list_conversations(
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    dataset_id: str | None = None
):
    """List all conversations, optionally filtered by dataset."""
    # Filter conversations
    filtered = list(conversations.values())
    if dataset_id:
        filtered = [c for c in filtered if c.dataset_id == dataset_id]
    
    # Sort by updated_at descending
    filtered.sort(key=lambda c: c.updated_at, reverse=True)
    
    # Paginate
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]
    
    summaries = [
        ConversationSummary(
            id=c.id,
            title=c.title,
            created_at=c.created_at,
            updated_at=c.updated_at,
            message_count=len(c.messages),
            dataset_id=c.dataset_id
        )
        for c in page_items
    ]
    
    return ConversationsResponse(
        conversations=summaries,
        total=len(filtered),
        page=page,
        page_size=page_size
    )


@router.get(
    "/conversations/{conversation_id}",
    response_model=ConversationDetail,
    responses={404: {"model": ErrorResponse}},
    summary="Get conversation",
    description="Get full conversation history by ID"
)
async def get_conversation(conversation_id: str):
    """Get a specific conversation with all messages."""
    if conversation_id not in conversations:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Conversation not found",
                "details": [{"code": "NOT_FOUND", "message": f"No conversation with ID: {conversation_id}"}]
            }
        )
    
    return conversations[conversation_id]


@router.delete(
    "/conversations/{conversation_id}",
    status_code=204,
    responses={404: {"model": ErrorResponse}},
    summary="Delete conversation",
    description="Delete a conversation and all its messages"
)
async def delete_conversation(conversation_id: str):
    """Delete a conversation."""
    if conversation_id not in conversations:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Conversation not found",
                "details": [{"code": "NOT_FOUND", "message": f"No conversation with ID: {conversation_id}"}]
            }
        )
    
    del conversations[conversation_id]
    logger.info("Conversation deleted", conversation_id=conversation_id)
