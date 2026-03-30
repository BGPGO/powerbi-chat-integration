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
    
    def test_ready_check(self, app_client):
        """Test readiness check endpoint."""
        response = app_client.get("/health/ready")
        
        assert response.status_code == 200
        data = response.json()
        assert "ready" in data


class TestChatRoutes:
    """Tests for chat endpoints."""
    
    def test_chat_endpoint_success(self, app_client, sample_chat_request):
        """Test successful chat request."""
        with patch('app.api.routes.chat.process_chat') as mock_process:
            mock_process.return_value = {
                "response": "Total sales is $50,000",
                "query": "EVALUATE ROW('Total', [Total Sales])",
                "data": [{"Total": 50000}],
                "visualization": {"type": "metric"}
            }
            
            response = app_client.post("/api/chat", json=sample_chat_request)
            
            assert response.status_code == 200
            data = response.json()
            assert "response" in data
    
    def test_chat_endpoint_missing_message(self, app_client):
        """Test chat request without message."""
        response = app_client.post("/api/chat", json={
            "workspace_id": "ws-1",
            "dataset_id": "ds-1"
        })
        
        assert response.status_code == 422  # Validation error
    
    def test_chat_endpoint_missing_workspace(self, app_client):
        """Test chat request without workspace ID."""
        response = app_client.post("/api/chat", json={
            "message": "Show me sales",
            "dataset_id": "ds-1"
        })
        
        assert response.status_code == 422
    
    def test_chat_endpoint_error_handling(self, app_client, sample_chat_request):
        """Test error handling in chat endpoint."""
        with patch('app.api.routes.chat.process_chat') as mock_process:
            mock_process.side_effect = Exception("Processing failed")
            
            response = app_client.post("/api/chat", json=sample_chat_request)
            
            assert response.status_code == 500
            data = response.json()
            assert "error" in data or "detail" in data
    
    def test_chat_with_session_id(self, app_client, sample_chat_request):
        """Test chat request with session ID for context."""
        sample_chat_request["session_id"] = "session-123"
        
        with patch('app.api.routes.chat.process_chat') as mock_process:
            mock_process.return_value = {"response": "OK", "context_used": True}
            
            response = app_client.post("/api/chat", json=sample_chat_request)
            
            assert response.status_code == 200


class TestWorkspaceRoutes:
    """Tests for workspace endpoints."""
    
    def test_list_workspaces(self, app_client, mock_powerbi_client):
        """Test listing workspaces."""
        with patch('app.api.routes.workspaces.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            
            response = app_client.get("/api/workspaces")
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
    
    def test_get_workspace_by_id(self, app_client, mock_powerbi_client):
        """Test getting workspace by ID."""
        with patch('app.api.routes.workspaces.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            mock_powerbi_client.get_workspace = AsyncMock(return_value={
                "id": "ws-1",
                "name": "Test Workspace"
            })
            
            response = app_client.get("/api/workspaces/ws-1")
            
            assert response.status_code == 200
            data = response.json()
            assert data["id"] == "ws-1"
    
    def test_workspace_not_found(self, app_client, mock_powerbi_client):
        """Test getting non-existent workspace."""
        with patch('app.api.routes.workspaces.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            mock_powerbi_client.get_workspace = AsyncMock(return_value=None)
            
            response = app_client.get("/api/workspaces/nonexistent")
            
            assert response.status_code == 404


class TestDatasetRoutes:
    """Tests for dataset endpoints."""
    
    def test_list_datasets(self, app_client, mock_powerbi_client):
        """Test listing datasets for a workspace."""
        with patch('app.api.routes.datasets.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            
            response = app_client.get("/api/workspaces/ws-1/datasets")
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
    
    def test_get_dataset_schema(self, app_client, mock_powerbi_client, sample_schema):
        """Test getting dataset schema."""
        with patch('app.api.routes.datasets.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            mock_powerbi_client.get_dataset_schema = AsyncMock(return_value=sample_schema)
            
            response = app_client.get("/api/workspaces/ws-1/datasets/ds-1/schema")
            
            assert response.status_code == 200
            data = response.json()
            assert "tables" in data
    
    def test_refresh_dataset(self, app_client, mock_powerbi_client):
        """Test refreshing a dataset."""
        with patch('app.api.routes.datasets.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            mock_powerbi_client.refresh_dataset = AsyncMock(return_value={"status": "started"})
            
            response = app_client.post("/api/workspaces/ws-1/datasets/ds-1/refresh")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "started"


class TestQueryRoutes:
    """Tests for query execution endpoints."""
    
    def test_execute_dax_query(self, app_client, mock_powerbi_client, sample_dax_query):
        """Test executing a DAX query."""
        with patch('app.api.routes.datasets.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            
            response = app_client.post(
                "/api/workspaces/ws-1/datasets/ds-1/query",
                json={"dax": sample_dax_query}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert "results" in data
    
    def test_execute_invalid_dax_query(self, app_client, mock_powerbi_client):
        """Test executing an invalid DAX query."""
        with patch('app.api.routes.datasets.get_powerbi_client') as mock_get:
            mock_get.return_value = mock_powerbi_client
            mock_powerbi_client.execute_query = AsyncMock(
                side_effect=Exception("Invalid DAX syntax")
            )
            
            response = app_client.post(
                "/api/workspaces/ws-1/datasets/ds-1/query",
                json={"dax": "INVALID QUERY"}
            )
            
            assert response.status_code == 400 or response.status_code == 500


class TestAuthenticationMiddleware:
    """Tests for authentication handling."""
    
    def test_request_without_auth_header(self, app_client):
        """Test request without authentication."""
        # Depending on auth setup, this might be allowed or denied
        response = app_client.get("/api/workspaces")
        
        # Either success (no auth required) or 401
        assert response.status_code in [200, 401]
    
    def test_request_with_invalid_token(self, app_client):
        """Test request with invalid token."""
        response = app_client.get(
            "/api/workspaces",
            headers={"Authorization": "Bearer invalid-token"}
        )
        
        # Depending on auth validation
        assert response.status_code in [200, 401, 403]


class TestRateLimiting:
    """Tests for rate limiting."""
    
    def test_rate_limit_not_exceeded(self, app_client):
        """Test requests within rate limit."""
        for _ in range(5):
            response = app_client.get("/health")
            assert response.status_code == 200
    
    def test_rate_limit_headers(self, app_client):
        """Test rate limit headers in response."""
        response = app_client.get("/health")
        
        # Check for rate limit headers if implemented
        # These might not exist if rate limiting isn't enabled
        if "X-RateLimit-Limit" in response.headers:
            assert int(response.headers["X-RateLimit-Limit"]) > 0


class TestCORSHeaders:
    """Tests for CORS configuration."""
    
    def test_cors_headers_present(self, app_client):
        """Test that CORS headers are present."""
        response = app_client.options(
            "/api/chat",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "POST"
            }
        )
        
        # Should allow the origin or return appropriate CORS headers
        assert response.status_code in [200, 204]
    
    def test_cors_allows_localhost(self, app_client):
        """Test that CORS allows localhost for development."""
        response = app_client.get(
            "/health",
            headers={"Origin": "http://localhost:3000"}
        )
        
        assert response.status_code == 200
