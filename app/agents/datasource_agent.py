"""
DataSource Agent - Conexão e Extração do Power BI
Responsável por gerenciar conexão e extrair metadados do Power BI

Capacidades:
- Autenticar no Power BI
- Listar workspaces e datasets
- Extrair schema das tabelas
- Monitorar status de refresh
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig
from app.core.cache import get_cache
from app.core.config import get_settings


logger = logging.getLogger(__name__)


@dataclass
class DatasetInfo:
    """Informações de um dataset"""
    id: str
    name: str
    description: str
    configured_by: str
    is_refreshable: bool
    last_refresh: Optional[str]


@dataclass
class TableInfo:
    """Informações de uma tabela"""
    name: str
    columns: List[Dict[str, Any]]
    row_count: Optional[int]
    is_hidden: bool


class DataSourceAgent:
    """
    Agente especializado em conexão com Power BI
    
    Exemplo de uso:
        agent = DataSourceAgent()
        schema = await agent.extract_schema()
        datasets = await agent.list_available_datasets()
    """
    
    SYSTEM_PROMPT = """
    Você é um especialista em conexão e extração de dados do Power BI.
    
    Seu papel é:
    1. Gerenciar conexões com o Power BI de forma segura
    2. Extrair metadados de datasets e tabelas
    3. Monitorar status de refresh
    4. Fornecer informações sobre a estrutura dos dados
    
    Sempre forneça informações precisas sobre o estado atual dos dados.
    """
    
    def __init__(self):
        settings = get_settings()
        self.cache = get_cache()
        
        # Inicializa cliente Power BI
        self.config = PowerBIConfig.from_env()
        self.client = PowerBIClient(self.config)
        
        # LLM para análise de schema
        self.llm = ChatAnthropic(
            model="claude-sonnet-4-6",
            temperature=0.0,
            api_key=settings.anthropic_api_key.get_secret_value(),
        )
        
        self._current_dataset_id: Optional[str] = None
    
    async def list_available_datasets(
        self, 
        workspace_id: Optional[str] = None
    ) -> List[DatasetInfo]:
        """Lista datasets disponíveis no workspace"""
        
        cache_key = f"datasets_{workspace_id or self.config.workspace_id}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
        
        try:
            datasets = await self.client.list_datasets(workspace_id)
            
            result = [
                DatasetInfo(
                    id=ds["id"],
                    name=ds["name"],
                    description=ds.get("description", ""),
                    configured_by=ds.get("configuredBy", ""),
                    is_refreshable=ds.get("isRefreshable", False),
                    last_refresh=None  # Será preenchido se necessário
                )
                for ds in datasets
            ]
            
            await self.cache.set(cache_key, result, ttl=300)
            return result
            
        except Exception as e:
            logger.error(f"Failed to list datasets: {e}")
            raise DataSourceError(f"Erro ao listar datasets: {str(e)}")
    
    async def extract_schema(
        self, 
        dataset_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extrai schema completo de um dataset
        
        Args:
            dataset_id: ID do dataset (usa o primeiro disponível se não especificado)
            
        Returns:
            Schema com tabelas, colunas e relacionamentos
        """
        
        # Se não especificou dataset, usa o primeiro disponível
        if not dataset_id:
            datasets = await self.list_available_datasets()
            if not datasets:
                raise DataSourceError("Nenhum dataset disponível no workspace")
            dataset_id = datasets[0].id
            logger.info(f"Using first available dataset: {dataset_id}")
        
        self._current_dataset_id = dataset_id
        
        # Verifica cache
        cache_key = f"schema_{dataset_id}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
        
        try:
            # Obtém tabelas
            tables_raw = await self.client.get_tables(dataset_id)
            
            tables = []
            for table in tables_raw:
                table_info = {
                    "name": table["name"],
                    "columns": [
                        {
                            "name": col["name"],
                            "dataType": col.get("dataType", "unknown"),
                            "isHidden": col.get("isHidden", False),
                        }
                        for col in table.get("columns", [])
                    ],
                    "measures": [
                        {
                            "name": m["name"],
                            "expression": m.get("expression", ""),
                        }
                        for m in table.get("measures", [])
                    ],
                    "isHidden": table.get("isHidden", False),
                }
                tables.append(table_info)
            
            # Monta schema completo
            schema = {
                "dataset_id": dataset_id,
                "tables": tables,
                "relationships": await self._extract_relationships(dataset_id),
                "extracted_at": self._get_timestamp(),
            }
            
            # Cache por 5 minutos
            await self.cache.set(cache_key, schema, ttl=300)
            
            logger.info(f"Extracted schema with {len(tables)} tables")
            return schema
            
        except Exception as e:
            logger.error(f"Failed to extract schema: {e}")
            raise DataSourceError(f"Erro ao extrair schema: {str(e)}")
    
    async def _extract_relationships(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Extrai relacionamentos entre tabelas"""
        
        # A API do Power BI não expõe relacionamentos diretamente via REST
        # Precisamos usar o endpoint de metadados do modelo
        try:
            # Tenta extrair via query DMV
            dmv_query = """
            SELECT 
                [ID],
                [Name],
                [FromTableID],
                [ToTableID],
                [FromColumnID],
                [ToColumnID],
                [CrossFilteringBehavior]
            FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS
            """
            
            result = await self.client.execute_query(dataset_id, dmv_query)
            
            relationships = []
            for row in result.get("rows", []):
                relationships.append({
                    "id": row.get("[ID]"),
                    "name": row.get("[Name]"),
                    "fromTableId": row.get("[FromTableID]"),
                    "toTableId": row.get("[ToTableID]"),
                    "crossFilteringBehavior": row.get("[CrossFilteringBehavior]"),
                })
            
            return relationships
            
        except Exception as e:
            logger.warning(f"Could not extract relationships via DMV: {e}")
            return []
    
    async def get_refresh_status(
        self, 
        dataset_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Obtém status de refresh do dataset"""
        
        ds_id = dataset_id or self._current_dataset_id
        if not ds_id:
            raise DataSourceError("Nenhum dataset selecionado")
        
        try:
            history = await self.client.get_refresh_history(ds_id)
            
            if not history:
                return {
                    "status": "unknown",
                    "last_refresh": None,
                    "message": "Nenhum histórico de refresh disponível"
                }
            
            latest = history[0]
            return {
                "status": latest.get("status", "Unknown"),
                "last_refresh": latest.get("startTime"),
                "end_time": latest.get("endTime"),
                "refresh_type": latest.get("refreshType"),
                "message": latest.get("serviceExceptionJson") if latest.get("status") == "Failed" else None
            }
            
        except Exception as e:
            logger.error(f"Failed to get refresh status: {e}")
            raise DataSourceError(f"Erro ao obter status de refresh: {str(e)}")
    
    async def get_table_preview(
        self, 
        table_name: str, 
        top_n: int = 5
    ) -> Dict[str, Any]:
        """Obtém preview de dados de uma tabela"""
        
        if not self._current_dataset_id:
            raise DataSourceError("Nenhum dataset selecionado")
        
        try:
            # Query DAX para obter preview
            dax_query = f"EVALUATE TOPN({top_n}, '{table_name}')"
            
            result = await self.client.execute_query(
                self._current_dataset_id, 
                dax_query
            )
            
            return {
                "table": table_name,
                "columns": result.get("columns", []),
                "rows": result.get("rows", []),
                "row_count": result.get("row_count", 0),
            }
            
        except Exception as e:
            logger.error(f"Failed to get table preview: {e}")
            raise DataSourceError(f"Erro ao obter preview da tabela: {str(e)}")
    
    async def analyze_data_quality(
        self, 
        table_name: str
    ) -> Dict[str, Any]:
        """Analisa qualidade dos dados de uma tabela"""
        
        if not self._current_dataset_id:
            raise DataSourceError("Nenhum dataset selecionado")
        
        schema = await self.extract_schema()
        
        # Encontra a tabela
        table = None
        for t in schema.get("tables", []):
            if t["name"] == table_name:
                table = t
                break
        
        if not table:
            raise DataSourceError(f"Tabela '{table_name}' não encontrada")
        
        # Analisa cada coluna
        quality_report = {
            "table": table_name,
            "columns": [],
        }
        
        for col in table.get("columns", []):
            col_name = col["name"]
            
            # Query para análise básica
            analysis_query = f"""
            EVALUATE
            SUMMARIZE(
                '{table_name}',
                "total_rows", COUNTROWS('{table_name}'),
                "distinct_{col_name}", DISTINCTCOUNT('{table_name}'[{col_name}]),
                "blank_{col_name}", COUNTBLANK('{table_name}'[{col_name}])
            )
            """
            
            try:
                result = await self.client.execute_query(
                    self._current_dataset_id, 
                    analysis_query
                )
                
                if result.get("rows"):
                    row = result["rows"][0]
                    quality_report["columns"].append({
                        "name": col_name,
                        "data_type": col["dataType"],
                        "distinct_count": row.get(f"distinct_{col_name}"),
                        "blank_count": row.get(f"blank_{col_name}"),
                        "total_rows": row.get("total_rows"),
                    })
            except Exception as e:
                logger.warning(f"Could not analyze column {col_name}: {e}")
                quality_report["columns"].append({
                    "name": col_name,
                    "data_type": col["dataType"],
                    "error": str(e),
                })
        
        return quality_report
    
    async def suggest_dataset(self, question: str) -> Optional[str]:
        """Sugere o melhor dataset para responder uma pergunta"""
        
        datasets = await self.list_available_datasets()
        
        if not datasets:
            return None
        
        if len(datasets) == 1:
            return datasets[0].id
        
        # Usa LLM para escolher o melhor dataset
        datasets_info = "\n".join([
            f"- {ds.name} (ID: {ds.id}): {ds.description or 'Sem descrição'}"
            for ds in datasets
        ])
        
        prompt = f"""
        Com base na pergunta do usuário, qual dataset seria mais apropriado?
        
        PERGUNTA: {question}
        
        DATASETS DISPONÍVEIS:
        {datasets_info}
        
        Responda apenas com o ID do dataset mais apropriado.
        """
        
        response = await self.llm.ainvoke([
            SystemMessage(content=self.SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ])
        
        suggested_id = response.content.strip()
        
        # Valida se o ID existe
        for ds in datasets:
            if ds.id == suggested_id:
                return suggested_id
        
        # Se não encontrou, retorna o primeiro
        return datasets[0].id
    
    def _get_timestamp(self) -> str:
        """Retorna timestamp atual"""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    async def close(self):
        """Fecha conexões"""
        await self.client.close()


class DataSourceError(Exception):
    """Erro no agente de fonte de dados"""
    pass
