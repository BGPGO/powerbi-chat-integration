"""
Pytest configuration and fixtures for PowerBI Chat Integration tests.
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch
from typing import Generator, Any
import asyncio

# Mock environment variables before importing app modules
import os
os.environ.setdefault("AZURE_TENANT_ID", "test-tenant-id")
os.environ.setdefault("AZURE_CLIENT_ID", "test-client-id")
os.environ.setdefault("AZURE_CLIENT_SECRET", "test-client-secret")
os.environ.setdefault("POWERBI_WORKSPACE_ID", "test-workspace-id")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("DATABASE_URL", "sqlite:///./test.db")

from fastapi.testclient import TestClient
from httpx import AsyncClient


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_powerbi_client():
    """Mock PowerBI client for testing."""
    client = Mock()
    client.get_access_token = AsyncMock(return_value="mock-access-token")
    client.list_workspaces = AsyncMock(return_value=[
        {"id": "ws-1", "name": "Workspace 1", "type": "Workspace"},
        {"id": "ws-2", "name": "Workspace 2", "type": "Workspace"},
    ])
    client.list_datasets = AsyncMock(return_value=[
        {"id": "ds-1", "name": "Sales Dataset", "configuredBy": "user@test.com"},
        {"id": "ds-2", "name": "HR Dataset", "configuredBy": "user@test.com"},
    ])
    client.get_dataset_schema = AsyncMock(return_value={
        "tables": [
            {
                "name": "Sales",
                "columns": [
                    {"name": "Date", "dataType": "DateTime"},
                    {"name": "Amount", "dataType": "Decimal"},
                    {"name": "Product", "dataType": "String"},
                ]
            }
        ]
    })
    client.execute_query = AsyncMock(return_value={
        "results": [
            {"tables": [{"rows": [{"Amount": 1000}, {"Amount": 2000}]}]}
        ]
    })
    return client


@pytest.fixture
def mock_openai_response():
    """Mock OpenAI API response."""
    response = Mock()
    response.choices = [
        Mock(message=Mock(content="This is a test response from the AI."))
    ]
    return response


@pytest.fixture
def mock_llm():
    """Mock LangChain LLM for testing agents."""
    llm = Mock()
    llm.invoke = Mock(return_value=Mock(content="Mock LLM response"))
    llm.ainvoke = AsyncMock(return_value=Mock(content="Mock async LLM response"))
    return llm


@pytest.fixture
def sample_chat_request():
    """Sample chat request payload."""
    return {
        "message": "Show me total sales by product",
        "workspace_id": "test-workspace-id",
        "dataset_id": "test-dataset-id",
        "session_id": "test-session-123"
    }


@pytest.fixture
def sample_schema():
    """Sample Power BI schema for testing."""
    return {
        "tables": [
            {
                "name": "Sales",
                "columns": [
                    {"name": "SalesId", "dataType": "Int64", "isHidden": False},
                    {"name": "Date", "dataType": "DateTime", "isHidden": False},
                    {"name": "Amount", "dataType": "Decimal", "isHidden": False},
                    {"name": "ProductId", "dataType": "Int64", "isHidden": False},
                ],
                "measures": [
                    {"name": "Total Sales", "expression": "SUM(Sales[Amount])"},
                    {"name": "Avg Sales", "expression": "AVERAGE(Sales[Amount])"},
                ]
            },
            {
                "name": "Products",
                "columns": [
                    {"name": "ProductId", "dataType": "Int64", "isHidden": False},
                    {"name": "ProductName", "dataType": "String", "isHidden": False},
                    {"name": "Category", "dataType": "String", "isHidden": False},
                ]
            }
        ],
        "relationships": [
            {
                "fromTable": "Sales",
                "fromColumn": "ProductId",
                "toTable": "Products",
                "toColumn": "ProductId"
            }
        ]
    }


@pytest.fixture
def sample_dax_query():
    """Sample DAX query for testing."""
    return """
    EVALUATE
    SUMMARIZECOLUMNS(
        Products[ProductName],
        "Total Sales", [Total Sales]
    )
    ORDER BY [Total Sales] DESC
    """


@pytest.fixture
def sample_query_result():
    """Sample query result from Power BI."""
    return {
        "results": [
            {
                "tables": [
                    {
                        "rows": [
                            {"Products[ProductName]": "Widget A", "[Total Sales]": 15000},
                            {"Products[ProductName]": "Widget B", "[Total Sales]": 12000},
                            {"Products[ProductName]": "Widget C", "[Total Sales]": 8000},
                        ]
                    }
                ]
            }
        ]
    }


@pytest.fixture
def app_client():
    """Create test client for FastAPI app."""
    from app.main import app
    
    with patch('app.connectors.powerbi.client.PowerBIClient') as mock_pbi:
        mock_pbi.return_value = Mock()
        client = TestClient(app)
        yield client


@pytest.fixture
async def async_app_client():
    """Create async test client for FastAPI app."""
    from app.main import app
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client
