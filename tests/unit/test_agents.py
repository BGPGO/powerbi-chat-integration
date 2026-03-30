"""
Unit tests for PowerBI Chat Integration agents.
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import json


class TestDictionaryAgent:
    """Tests for the Dictionary Agent."""
    
    @pytest.fixture
    def agent(self, mock_llm):
        """Create dictionary agent instance."""
        with patch('app.agents.dictionary_agent.ChatOpenAI', return_value=mock_llm):
            from app.agents.dictionary_agent import DictionaryAgent
            return DictionaryAgent()
    
    def test_translate_schema_to_business_terms(self, agent, sample_schema):
        """Test schema translation to business-friendly terms."""
        # Mock the LLM response
        agent.llm.invoke = Mock(return_value=Mock(content=json.dumps({
            "tables": {
                "Sales": {
                    "business_name": "Transações de Vendas",
                    "description": "Registro de todas as vendas realizadas"
                }
            },
            "columns": {
                "Sales.Amount": {
                    "business_name": "Valor da Venda",
                    "description": "Valor monetário da transação"
                }
            }
        })))
        
        result = agent.translate_schema(sample_schema)
        
        assert result is not None
        assert "tables" in result or isinstance(result, dict)
    
    def test_extract_key_metrics(self, agent, sample_schema):
        """Test extraction of key metrics from schema."""
        agent.llm.invoke = Mock(return_value=Mock(content=json.dumps({
            "metrics": [
                {"name": "Total Sales", "description": "Soma total de vendas"},
                {"name": "Avg Sales", "description": "Média de vendas"}
            ]
        })))
        
        result = agent.extract_metrics(sample_schema)
        
        assert "metrics" in result
        assert len(result["metrics"]) == 2
    
    def test_handle_empty_schema(self, agent):
        """Test handling of empty schema."""
        empty_schema = {"tables": [], "relationships": []}
        
        result = agent.translate_schema(empty_schema)
        
        assert result is not None


class TestDataSourceAgent:
    """Tests for the DataSource Agent."""
    
    @pytest.fixture
    def agent(self, mock_powerbi_client):
        """Create datasource agent instance."""
        with patch('app.agents.datasource_agent.PowerBIClient', return_value=mock_powerbi_client):
            from app.agents.datasource_agent import DataSourceAgent
            return DataSourceAgent(client=mock_powerbi_client)
    
    @pytest.mark.asyncio
    async def test_list_available_datasets(self, agent, mock_powerbi_client):
        """Test listing available datasets."""
        result = await agent.list_datasets("test-workspace-id")
        
        assert len(result) == 2
        assert result[0]["name"] == "Sales Dataset"
    
    @pytest.mark.asyncio
    async def test_get_schema(self, agent, mock_powerbi_client):
        """Test retrieving dataset schema."""
        result = await agent.get_schema("test-workspace-id", "ds-1")
        
        assert "tables" in result
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "Sales"
    
    @pytest.mark.asyncio
    async def test_validate_connection(self, agent, mock_powerbi_client):
        """Test connection validation."""
        result = await agent.validate_connection("test-workspace-id")
        
        assert result["status"] == "connected"
    
    @pytest.mark.asyncio
    async def test_handle_connection_error(self, agent, mock_powerbi_client):
        """Test handling of connection errors."""
        mock_powerbi_client.list_datasets.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            await agent.list_datasets("test-workspace-id")
        
        assert "Connection failed" in str(exc_info.value)


class TestQueryBuilderAgent:
    """Tests for the Query Builder Agent."""
    
    @pytest.fixture
    def agent(self, mock_llm):
        """Create query builder agent instance."""
        with patch('app.agents.query_builder_agent.ChatOpenAI', return_value=mock_llm):
            from app.agents.query_builder_agent import QueryBuilderAgent
            return QueryBuilderAgent()
    
    def test_generate_simple_dax_query(self, agent, sample_schema):
        """Test generation of simple DAX query."""
        agent.llm.invoke = Mock(return_value=Mock(content="""
        EVALUATE
        SUMMARIZECOLUMNS(
            Products[ProductName],
            "Total Sales", [Total Sales]
        )
        """))
        
        result = agent.generate_query(
            question="Show total sales by product",
            schema=sample_schema
        )
        
        assert "EVALUATE" in result
        assert "SUMMARIZECOLUMNS" in result
    
    def test_generate_filtered_query(self, agent, sample_schema):
        """Test generation of filtered DAX query."""
        agent.llm.invoke = Mock(return_value=Mock(content="""
        EVALUATE
        FILTER(
            SUMMARIZECOLUMNS(
                Products[ProductName],
                "Total Sales", [Total Sales]
            ),
            [Total Sales] > 10000
        )
        """))
        
        result = agent.generate_query(
            question="Show products with sales over 10000",
            schema=sample_schema
        )
        
        assert "FILTER" in result
        assert "10000" in result
    
    def test_generate_time_based_query(self, agent, sample_schema):
        """Test generation of time-based DAX query."""
        agent.llm.invoke = Mock(return_value=Mock(content="""
        EVALUATE
        SUMMARIZECOLUMNS(
            'Date'[Month],
            "Monthly Sales", [Total Sales]
        )
        ORDER BY 'Date'[Month]
        """))
        
        result = agent.generate_query(
            question="Show monthly sales trend",
            schema=sample_schema
        )
        
        assert "Month" in result or "Date" in result
    
    def test_validate_dax_syntax(self, agent):
        """Test DAX syntax validation."""
        valid_query = "EVALUATE SUMMARIZECOLUMNS('Table'[Column])"
        invalid_query = "SELECT * FROM Table"
        
        assert agent.validate_syntax(valid_query) == True
        assert agent.validate_syntax(invalid_query) == False
    
    def test_suggest_visualizations(self, agent, sample_query_result):
        """Test visualization suggestions based on query results."""
        result = agent.suggest_visualization(sample_query_result)
        
        assert "type" in result
        assert result["type"] in ["bar", "line", "pie", "table"]


class TestOrchestrator:
    """Tests for the Agent Orchestrator."""
    
    @pytest.fixture
    def orchestrator(self, mock_llm, mock_powerbi_client):
        """Create orchestrator instance with mocked dependencies."""
        with patch('app.agents.orchestrator.ChatOpenAI', return_value=mock_llm), \
             patch('app.agents.orchestrator.PowerBIClient', return_value=mock_powerbi_client):
            from app.agents.orchestrator import AgentOrchestrator
            return AgentOrchestrator()
    
    @pytest.mark.asyncio
    async def test_process_simple_question(self, orchestrator):
        """Test processing a simple question."""
        result = await orchestrator.process(
            message="What is the total sales?",
            workspace_id="test-workspace",
            dataset_id="test-dataset"
        )
        
        assert "response" in result
        assert result["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_route_to_dictionary_agent(self, orchestrator):
        """Test routing schema-related questions to dictionary agent."""
        result = await orchestrator.route(
            message="What tables are available?",
            context={}
        )
        
        assert result["agent"] == "dictionary"
    
    @pytest.mark.asyncio
    async def test_route_to_query_builder(self, orchestrator):
        """Test routing query questions to query builder."""
        result = await orchestrator.route(
            message="Show me sales by region",
            context={"schema": {"tables": []}}
        )
        
        assert result["agent"] == "query_builder"
    
    @pytest.mark.asyncio
    async def test_handle_ambiguous_request(self, orchestrator):
        """Test handling of ambiguous requests."""
        result = await orchestrator.process(
            message="Help me",
            workspace_id="test-workspace",
            dataset_id="test-dataset"
        )
        
        assert "clarification" in result or "suggestions" in result
    
    @pytest.mark.asyncio
    async def test_maintain_conversation_context(self, orchestrator):
        """Test that conversation context is maintained."""
        # First message
        await orchestrator.process(
            message="Show sales by product",
            workspace_id="test-workspace",
            dataset_id="test-dataset",
            session_id="session-123"
        )
        
        # Follow-up message
        result = await orchestrator.process(
            message="Now filter by last month",
            workspace_id="test-workspace",
            dataset_id="test-dataset",
            session_id="session-123"
        )
        
        # Should understand context from previous message
        assert result["status"] == "success"


class TestAgentState:
    """Tests for agent state management."""
    
    def test_state_initialization(self):
        """Test initial state structure."""
        from app.agents.orchestrator import create_initial_state
        
        state = create_initial_state(
            message="Test message",
            workspace_id="ws-1",
            dataset_id="ds-1"
        )
        
        assert state["message"] == "Test message"
        assert state["workspace_id"] == "ws-1"
        assert state["dataset_id"] == "ds-1"
        assert state["history"] == []
    
    def test_state_update_after_agent_execution(self):
        """Test state updates after agent execution."""
        from app.agents.orchestrator import update_state
        
        initial_state = {
            "message": "Test",
            "history": [],
            "context": {}
        }
        
        updated_state = update_state(
            initial_state,
            agent="dictionary",
            result={"schema": {"tables": []}}
        )
        
        assert len(updated_state["history"]) == 1
        assert updated_state["history"][0]["agent"] == "dictionary"
