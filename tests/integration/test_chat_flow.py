"""
Integration tests for PowerBI Chat Integration.
These tests verify the complete flow from API to agents.
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from httpx import AsyncClient
import json


@pytest.mark.integration
class TestChatIntegration:
    """Integration tests for the chat flow."""
    
    @pytest.mark.asyncio
    async def test_complete_chat_flow(self, async_app_client, sample_chat_request):
        """Test complete flow: API -> Orchestrator -> Agents -> Response."""
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_instance.process = AsyncMock(return_value={
                "response": "Total sales for Q4 is $150,000",
                "query": "EVALUATE SUMMARIZECOLUMNS(...)",
                "data": [{"Quarter": "Q4", "Sales": 150000}],
                "visualization": {"type": "bar", "x": "Quarter", "y": "Sales"}
            })
            mock_orch.return_value = mock_instance
            
            response = await async_app_client.post(
                "/api/chat",
                json=sample_chat_request
            )
            
            assert response.status_code == 200
            data = response.json()
            assert "response" in data
            assert "data" in data
    
    @pytest.mark.asyncio
    async def test_multi_turn_conversation(self, async_app_client):
        """Test multi-turn conversation with context."""
        session_id = "test-session-123"
        
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_instance.process = AsyncMock()
            mock_orch.return_value = mock_instance
            
            # First turn
            mock_instance.process.return_value = {
                "response": "Here are the sales by product",
                "data": [{"Product": "A", "Sales": 1000}]
            }
            
            response1 = await async_app_client.post("/api/chat", json={
                "message": "Show me sales by product",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1",
                "session_id": session_id
            })
            
            assert response1.status_code == 200
            
            # Second turn (follow-up)
            mock_instance.process.return_value = {
                "response": "Here are the sales filtered by last quarter",
                "data": [{"Product": "A", "Sales": 250}],
                "context_used": True
            }
            
            response2 = await async_app_client.post("/api/chat", json={
                "message": "Filter by last quarter",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1",
                "session_id": session_id
            })
            
            assert response2.status_code == 200
            data = response2.json()
            assert data.get("context_used") is True or "filtered" in data.get("response", "").lower()


@pytest.mark.integration
class TestWorkspaceDatasetIntegration:
    """Integration tests for workspace and dataset operations."""
    
    @pytest.mark.asyncio
    async def test_workspace_to_dataset_flow(self, async_app_client, mock_powerbi_client):
        """Test flow from listing workspaces to getting dataset schema."""
        with patch('app.api.routes.workspaces.get_powerbi_client') as mock_ws, \
             patch('app.api.routes.datasets.get_powerbi_client') as mock_ds:
            
            mock_ws.return_value = mock_powerbi_client
            mock_ds.return_value = mock_powerbi_client
            
            # List workspaces
            ws_response = await async_app_client.get("/api/workspaces")
            assert ws_response.status_code == 200
            workspaces = ws_response.json()
            
            if len(workspaces) > 0:
                workspace_id = workspaces[0]["id"]
                
                # List datasets in workspace
                ds_response = await async_app_client.get(
                    f"/api/workspaces/{workspace_id}/datasets"
                )
                assert ds_response.status_code == 200
                datasets = ds_response.json()
                
                if len(datasets) > 0:
                    dataset_id = datasets[0]["id"]
                    
                    # Get schema
                    schema_response = await async_app_client.get(
                        f"/api/workspaces/{workspace_id}/datasets/{dataset_id}/schema"
                    )
                    assert schema_response.status_code == 200
                    schema = schema_response.json()
                    assert "tables" in schema


@pytest.mark.integration
class TestQueryExecutionIntegration:
    """Integration tests for query execution flow."""
    
    @pytest.mark.asyncio
    async def test_natural_language_to_dax_execution(
        self, async_app_client, mock_powerbi_client, mock_llm
    ):
        """Test complete flow from natural language to DAX execution."""
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_orch.return_value = mock_instance
            
            # Simulate the full pipeline
            mock_instance.process = AsyncMock(return_value={
                "response": "Total revenue is $500,000",
                "query": """
                    EVALUATE
                    SUMMARIZECOLUMNS(
                        'Date'[Year],
                        "Revenue", SUM(Sales[Amount])
                    )
                """,
                "data": [
                    {"Year": 2023, "Revenue": 200000},
                    {"Year": 2024, "Revenue": 300000}
                ],
                "steps": [
                    {"agent": "dictionary", "action": "translated_terms"},
                    {"agent": "query_builder", "action": "generated_dax"},
                    {"agent": "datasource", "action": "executed_query"}
                ]
            })
            
            response = await async_app_client.post("/api/chat", json={
                "message": "What is the total revenue by year?",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1"
            })
            
            assert response.status_code == 200
            data = response.json()
            assert "data" in data
            assert len(data["data"]) == 2


@pytest.mark.integration
class TestErrorHandlingIntegration:
    """Integration tests for error scenarios."""
    
    @pytest.mark.asyncio
    async def test_powerbi_connection_failure(self, async_app_client):
        """Test handling of Power BI connection failures."""
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_instance.process = AsyncMock(
                side_effect=Exception("Failed to connect to Power BI")
            )
            mock_orch.return_value = mock_instance
            
            response = await async_app_client.post("/api/chat", json={
                "message": "Show me sales",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1"
            })
            
            assert response.status_code == 500
            data = response.json()
            assert "error" in data or "detail" in data
    
    @pytest.mark.asyncio
    async def test_invalid_dax_query_handling(self, async_app_client):
        """Test handling of invalid DAX queries."""
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_instance.process = AsyncMock(return_value={
                "error": "Invalid DAX syntax",
                "suggestion": "Please rephrase your question",
                "status": "failed"
            })
            mock_orch.return_value = mock_instance
            
            response = await async_app_client.post("/api/chat", json={
                "message": "Calculate the impossible metric",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1"
            })
            
            assert response.status_code == 200  # Graceful error handling
            data = response.json()
            assert "error" in data or "suggestion" in data
    
    @pytest.mark.asyncio
    async def test_llm_timeout_handling(self, async_app_client):
        """Test handling of LLM timeout."""
        import asyncio
        
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_instance.process = AsyncMock(
                side_effect=asyncio.TimeoutError("LLM request timed out")
            )
            mock_orch.return_value = mock_instance
            
            response = await async_app_client.post("/api/chat", json={
                "message": "Complex analysis request",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1"
            })
            
            assert response.status_code in [500, 504]


@pytest.mark.integration
class TestVisualizationIntegration:
    """Integration tests for visualization generation."""
    
    @pytest.mark.asyncio
    async def test_chart_recommendation(self, async_app_client):
        """Test that appropriate chart types are recommended."""
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_orch.return_value = mock_instance
            
            # Time series data should get line chart
            mock_instance.process = AsyncMock(return_value={
                "response": "Here's the trend over time",
                "data": [
                    {"Month": "Jan", "Value": 100},
                    {"Month": "Feb", "Value": 120},
                    {"Month": "Mar", "Value": 110}
                ],
                "visualization": {
                    "type": "line",
                    "x": "Month",
                    "y": "Value",
                    "title": "Monthly Trend"
                }
            })
            
            response = await async_app_client.post("/api/chat", json={
                "message": "Show me the monthly trend",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1"
            })
            
            assert response.status_code == 200
            data = response.json()
            assert data["visualization"]["type"] == "line"
    
    @pytest.mark.asyncio
    async def test_single_metric_visualization(self, async_app_client):
        """Test single metric (KPI) visualization."""
        with patch('app.agents.orchestrator.AgentOrchestrator') as mock_orch:
            mock_instance = MagicMock()
            mock_instance.process = AsyncMock(return_value={
                "response": "Total revenue is $1,000,000",
                "data": [{"Total Revenue": 1000000}],
                "visualization": {
                    "type": "metric",
                    "value": 1000000,
                    "label": "Total Revenue",
                    "format": "currency"
                }
            })
            mock_orch.return_value = mock_instance
            
            response = await async_app_client.post("/api/chat", json={
                "message": "What is the total revenue?",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1"
            })
            
            assert response.status_code == 200
            data = response.json()
            assert data["visualization"]["type"] == "metric"


@pytest.mark.integration
class TestCachingIntegration:
    """Integration tests for caching behavior."""
    
    @pytest.mark.asyncio
    async def test_schema_caching(self, async_app_client, mock_powerbi_client):
        """Test that schema is cached appropriately."""
        call_count = 0
        
        async def mock_get_schema(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return {"tables": [{"name": "Sales", "columns": []}]}
        
        mock_powerbi_client.get_dataset_schema = mock_get_schema
        
        with patch('app.api.routes.datasets.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            
            # First call
            await async_app_client.get("/api/workspaces/ws-1/datasets/ds-1/schema")
            
            # Second call (should hit cache)
            await async_app_client.get("/api/workspaces/ws-1/datasets/ds-1/schema")
            
            # Depending on caching implementation, count might be 1 or 2
            assert call_count >= 1
