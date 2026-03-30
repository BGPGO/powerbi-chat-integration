"""
Unit tests for PowerBI Chat Integration API routes.
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
import json


class TestHealthRoutes:
    """Tests for health check endpoints."""
    
    def test_health_check(self, app_client):
        """Test basic health check endpoint."""
        response = app_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    def test_readiness_check(self, app_client):
        """Test readiness probe endpoint."""
        response = app_client.get("/health/ready")
        
        assert response.status_code == 200
        data = response.json()
        assert "ready" in data
    
    def test_liveness_check(self, app_client):
        """Test liveness probe endpoint."""
        response = app_client.get("/health/live")
        
        assert response.status_code == 200


class TestChatRoutes:
    """Tests for chat endpoints."""
    
    def test_send_message_success(self, app_client, sample_chat_request):
        """Test successful message sending."""
        with patch('app.api.routes.chat.process_message') as mock_process:
            mock_process.return_value = {
                "response": "Here are your sales results",
                "query": "EVALUATE...",
                "data": [{"product": "A", "sales": 100}],
                "visualization": {"type": "bar"}
            }
            
            response = app_client.post("/api/chat", json=sample_chat_request)
            
            assert response.status_code == 200
            data = response.json()
            assert "response" in data
    
    def test_send_message_missing_fields(self, app_client):
        """Test message sending with missing required fields."""
        response = app_client.post("/api/chat", json={"message": "test"})
        
        assert response.status_code == 422  # Validation error
    
    def test_send_message_empty_message(self, app_client):
        """Test sending empty message."""
        response = app_client.post("/api/chat", json={
            "message": "",
            "workspace_id": "ws-1",
            "dataset_id": "ds-1"
        })
        
        assert response.status_code == 422
    
    def test_send_message_with_context(self, app_client, sample_chat_request):
        """Test message sending with conversation context."""
        sample_chat_request["context"] = {
            "previous_query": "SELECT...",
            "filters": {"date_range": "last_month"}
        }
        
        with patch('app.api.routes.chat.process_message') as mock_process:
            mock_process.return_value = {"response": "Filtered results"}
            
            response = app_client.post("/api/chat", json=sample_chat_request)
            
            assert response.status_code == 200
    
    def test_get_chat_history(self, app_client):
        """Test retrieving chat history."""
        with patch('app.api.routes.chat.get_history') as mock_history:
            mock_history.return_value = [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi!"}
            ]
            
            response = app_client.get("/api/chat/history/session-123")
            
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 2
    
    def test_clear_chat_history(self, app_client):
        """Test clearing chat history."""
        with patch('app.api.routes.chat.clear_history') as mock_clear:
            mock_clear.return_value = True
            
            response = app_client.delete("/api/chat/history/session-123")
            
            assert response.status_code == 200


class TestWorkspaceRoutes:
    """Tests for workspace management endpoints."""
    
    def test_list_workspaces(self, app_client, mock_powerbi_client):
        """Test listing available workspaces."""
        with patch('app.api.routes.workspaces.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.get("/api/workspaces")
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
    
    def test_get_workspace_details(self, app_client, mock_powerbi_client):
        """Test getting workspace details."""
        mock_powerbi_client.get_workspace = AsyncMock(return_value={
            "id": "ws-1",
            "name": "Test Workspace",
            "datasets": [],
            "reports": []
        })
        
        with patch('app.api.routes.workspaces.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.get("/api/workspaces/ws-1")
            
            assert response.status_code == 200
            data = response.json()
            assert data["id"] == "ws-1"
    
    def test_get_nonexistent_workspace(self, app_client, mock_powerbi_client):
        """Test getting non-existent workspace."""
        mock_powerbi_client.get_workspace = AsyncMock(side_effect=Exception("Not found"))
        
        with patch('app.api.routes.workspaces.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.get("/api/workspaces/nonexistent")
            
            assert response.status_code == 404


class TestDatasetRoutes:
    """Tests for dataset management endpoints."""
    
    def test_list_datasets(self, app_client, mock_powerbi_client):
        """Test listing datasets in a workspace."""
        with patch('app.api.routes.datasets.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.get("/api/workspaces/ws-1/datasets")
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
    
    def test_get_dataset_schema(self, app_client, mock_powerbi_client, sample_schema):
        """Test getting dataset schema."""
        mock_powerbi_client.get_dataset_schema = AsyncMock(return_value=sample_schema)
        
        with patch('app.api.routes.datasets.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.get("/api/workspaces/ws-1/datasets/ds-1/schema")
            
            assert response.status_code == 200
            data = response.json()
            assert "tables" in data
    
    def test_refresh_dataset(self, app_client, mock_powerbi_client):
        """Test triggering dataset refresh."""
        mock_powerbi_client.refresh_dataset = AsyncMock(return_value={"status": "started"})
        
        with patch('app.api.routes.datasets.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.post("/api/workspaces/ws-1/datasets/ds-1/refresh")
            
            assert response.status_code == 200
    
    def test_execute_query(self, app_client, mock_powerbi_client, sample_query_result):
        """Test executing DAX query."""
        mock_powerbi_client.execute_query = AsyncMock(return_value=sample_query_result)
        
        with patch('app.api.routes.datasets.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.post(
                "/api/workspaces/ws-1/datasets/ds-1/query",
                json={"query": "EVALUATE Sales"}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert "results" in data
    
    def test_execute_invalid_query(self, app_client, mock_powerbi_client):
        """Test executing invalid DAX query."""
        mock_powerbi_client.execute_query = AsyncMock(
            side_effect=Exception("Invalid DAX syntax")
        )
        
        with patch('app.api.routes.datasets.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.post(
                "/api/workspaces/ws-1/datasets/ds-1/query",
                json={"query": "INVALID QUERY"}
            )
            
            assert response.status_code == 400


class TestErrorHandling:
    """Tests for API error handling."""
    
    def test_internal_server_error(self, app_client):
        """Test handling of internal server errors."""
        with patch('app.api.routes.chat.process_message', side_effect=Exception("Internal error")):
            response = app_client.post("/api/chat", json={
                "message": "test",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1"
            })
            
            assert response.status_code == 500
            data = response.json()
            assert "error" in data
    
    def test_authentication_error(self, app_client, mock_powerbi_client):
        """Test handling of authentication errors."""
        mock_powerbi_client.get_access_token = AsyncMock(
            side_effect=Exception("Authentication failed")
        )
        
        with patch('app.api.routes.workspaces.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.get("/api/workspaces")
            
            assert response.status_code in [401, 500]
    
    def test_rate_limit_error(self, app_client):
        """Test handling of rate limit errors."""
        # Simulate rate limit by making many requests
        # This is a placeholder - actual rate limiting would be tested differently
        pass
    
    def test_timeout_error(self, app_client, mock_powerbi_client):
        """Test handling of timeout errors."""
        import asyncio
        mock_powerbi_client.execute_query = AsyncMock(
            side_effect=asyncio.TimeoutError("Query timeout")
        )
        
        with patch('app.api.routes.datasets.get_powerbi_client', return_value=mock_powerbi_client):
            response = app_client.post(
                "/api/workspaces/ws-1/datasets/ds-1/query",
                json={"query": "EVALUATE LargeTable"}
            )
            
            assert response.status_code == 504
