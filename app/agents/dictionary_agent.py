"""
Dictionary Agent - Tradução e Documentação de Schema
Responsável por converter nomenclatura técnica para linguagem de negócio

Capacidades:
- Traduzir nomes de colunas e tabelas
- Mapear relacionamentos
- Gerar glossário de termos
- Documentar schema automaticamente
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from app.core.cache import get_cache
from app.core.config import get_settings


logger = logging.getLogger(__name__)


@dataclass
class ColumnTranslation:
    """Tradução de uma coluna"""
    original: str
    translated: str
    description: str
    data_type: str
    business_domain: str
    examples: List[str]


@dataclass
class TableTranslation:
    """Tradução de uma tabela"""
    original: str
    translated: str
    description: str
    purpose: str
    columns: List[ColumnTranslation]


class DictionaryAgent:
    """
    Agente especializado em tradução de schemas
    
    Exemplo de uso:
        agent = DictionaryAgent()
        translated = await agent.translate_schema(schema, question)
    """
    
    SYSTEM_PROMPT = """
    Você é um especialista em tradução de schemas de dados para linguagem de negócio.
    
    Seu papel é:
    1. Converter nomes técnicos de colunas e tabelas para termos compreensíveis
    2. Identificar o domínio de negócio de cada campo
    3. Gerar descrições claras e concisas
    4. Manter consistência nas traduções
    
    Regras:
    - Use termos comuns no contexto brasileiro de negócios
    - Prefira nomes em português
    - Mantenha siglas conhecidas (CPF, CNPJ, SKU, etc)
    - Identifique padrões de nomenclatura (dt_ = data, vlr_ = valor, qtd_ = quantidade)
    
    Padrões comuns:
    - dt_, data_, dat_ → Data de...
    - vlr_, val_, valor_ → Valor de...
    - qtd_, qt_, quantidade_ → Quantidade de...
    - cd_, cod_, codigo_ → Código de...
    - nm_, nome_ → Nome de...
    - id_, _id → Identificador de...
    - fl_, flag_ → Indicador de...
    - ds_, desc_ → Descrição de...
    - nr_, num_ → Número de...
    """
    
    def __init__(self):
        settings = get_settings()
        self.cache = get_cache()
        
        self.llm = ChatAnthropic(
            model="claude-sonnet-4-6",
            temperature=0.1,
            api_key=settings.anthropic_api_key.get_secret_value(),
        )
        
        self._translation_cache: Dict[str, str] = {}
    
    async def translate_schema(
        self, 
        schema: Dict[str, Any],
        question: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Traduz um schema completo para linguagem de negócio
        
        Args:
            schema: Schema técnico com tabelas e colunas
            question: Pergunta do usuário (para contexto)
            
        Returns:
            Schema traduzido com descrições
        """
        
        # Verifica cache
        cache_key = self._generate_cache_key(schema)
        cached = await self.cache.get(cache_key)
        if cached:
            logger.info("Using cached schema translation")
            return cached
        
        translated_schema = {
            "tables": [],
            "glossary": {},
            "relationships": [],
        }
        
        # Traduz cada tabela
        for table in schema.get("tables", []):
            translated_table = await self._translate_table(table, question)
            translated_schema["tables"].append(translated_table)
            
            # Adiciona ao glossário
            for col in translated_table.get("columns", []):
                self._add_to_glossary(
                    translated_schema["glossary"],
                    col["original"],
                    col["translated"]
                )
        
        # Traduz relacionamentos
        for rel in schema.get("relationships", []):
            translated_rel = await self._translate_relationship(rel)
            translated_schema["relationships"].append(translated_rel)
        
        # Salva no cache
        await self.cache.set(cache_key, translated_schema, ttl=3600)
        
        return translated_schema
    
    async def _translate_table(
        self, 
        table: Dict[str, Any],
        question: Optional[str] = None
    ) -> Dict[str, Any]:
        """Traduz uma tabela individual"""
        
        table_name = table.get("name", "")
        columns = table.get("columns", [])
        
        # Prepara lista de colunas para tradução
        column_list = "\n".join([
            f"- {col['name']} ({col.get('dataType', 'unknown')})"
            for col in columns
        ])
        
        prompt = f"""
        Traduza a seguinte tabela e suas colunas para linguagem de negócio:
        
        TABELA: {table_name}
        
        COLUNAS:
        {column_list}
        
        {"CONTEXTO DA PERGUNTA: " + question if question else ""}
        
        Responda no formato JSON:
        {{
            "table_name": "Nome traduzido da tabela",
            "table_description": "Descrição do propósito da tabela",
            "columns": [
                {{
                    "original": "nome_original",
                    "translated": "Nome Traduzido",
                    "description": "Descrição clara",
                    "business_domain": "vendas|financeiro|cliente|produto|etc"
                }}
            ]
        }}
        """
        
        response = await self.llm.ainvoke([
            SystemMessage(content=self.SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ])
        
        # Parse da resposta
        try:
            import json
            # Extrai JSON da resposta
            content = response.content
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                translation = json.loads(content[json_start:json_end])
            else:
                raise ValueError("No JSON found in response")
                
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse translation response: {e}")
            translation = self._fallback_translation(table_name, columns)
        
        return {
            "original": table_name,
            "translated": translation.get("table_name", table_name),
            "description": translation.get("table_description", ""),
            "columns": translation.get("columns", []),
        }
    
    def _fallback_translation(
        self, 
        table_name: str, 
        columns: List[Dict]
    ) -> Dict[str, Any]:
        """Tradução de fallback usando regras simples"""
        
        # Regras de tradução comuns
        prefix_map = {
            "dt_": "Data de ",
            "vlr_": "Valor de ",
            "qtd_": "Quantidade de ",
            "cd_": "Código de ",
            "nm_": "Nome de ",
            "fl_": "Indicador de ",
            "ds_": "Descrição de ",
            "nr_": "Número de ",
        }
        
        def translate_name(name: str) -> str:
            lower_name = name.lower()
            for prefix, replacement in prefix_map.items():
                if lower_name.startswith(prefix):
                    rest = name[len(prefix):].replace("_", " ").title()
                    return replacement + rest
            return name.replace("_", " ").title()
        
        return {
            "table_name": translate_name(table_name),
            "table_description": f"Tabela {translate_name(table_name)}",
            "columns": [
                {
                    "original": col["name"],
                    "translated": translate_name(col["name"]),
                    "description": f"Campo {translate_name(col['name'])}",
                    "business_domain": "geral"
                }
                for col in columns
            ]
        }
    
    async def _translate_relationship(self, rel: Dict[str, Any]) -> Dict[str, Any]:
        """Traduz um relacionamento entre tabelas"""
        
        return {
            "from_table": rel.get("fromTable", ""),
            "to_table": rel.get("toTable", ""),
            "from_column": rel.get("fromColumn", ""),
            "to_column": rel.get("toColumn", ""),
            "cardinality": rel.get("crossFilteringBehavior", ""),
            "description": f"Relacionamento de {rel.get('fromTable')} para {rel.get('toTable')}",
        }
    
    def _add_to_glossary(
        self, 
        glossary: Dict[str, str], 
        original: str, 
        translated: str
    ):
        """Adiciona termo ao glossário"""
        if original and translated:
            glossary[original.lower()] = translated
    
    def _generate_cache_key(self, schema: Dict) -> str:
        """Gera chave de cache para um schema"""
        import hashlib
        import json
        
        schema_str = json.dumps(schema, sort_keys=True)
        return f"dict_schema_{hashlib.md5(schema_str.encode()).hexdigest()}"
    
    # ─────────────────────────────────────────────────────────────
    # PUBLIC API METHODS
    # ─────────────────────────────────────────────────────────────
    
    async def translate_column(self, column_name: str, context: str = "") -> ColumnTranslation:
        """Traduz uma única coluna"""
        
        if column_name in self._translation_cache:
            return self._translation_cache[column_name]
        
        prompt = f"""
        Traduza o nome da coluna "{column_name}" para linguagem de negócio.
        {"Contexto: " + context if context else ""}
        
        Responda em JSON:
        {{
            "translated": "Nome traduzido",
            "description": "Descrição do campo",
            "data_type": "tipo inferido",
            "business_domain": "domínio"
        }}
        """
        
        response = await self.llm.ainvoke([
            SystemMessage(content=self.SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ])
        
        # Parse e retorna
        import json
        try:
            content = response.content
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            data = json.loads(content[json_start:json_end])
            
            translation = ColumnTranslation(
                original=column_name,
                translated=data.get("translated", column_name),
                description=data.get("description", ""),
                data_type=data.get("data_type", "unknown"),
                business_domain=data.get("business_domain", "geral"),
                examples=data.get("examples", [])
            )
            
            self._translation_cache[column_name] = translation
            return translation
            
        except (json.JSONDecodeError, ValueError):
            return ColumnTranslation(
                original=column_name,
                translated=column_name.replace("_", " ").title(),
                description="",
                data_type="unknown",
                business_domain="geral",
                examples=[]
            )
    
    async def generate_glossary(self, schema: Dict[str, Any]) -> Dict[str, str]:
        """Gera glossário completo do schema"""
        
        translated = await self.translate_schema(schema)
        return translated.get("glossary", {})
    
    async def explain_term(self, term: str, schema: Dict[str, Any]) -> str:
        """Explica um termo técnico no contexto do schema"""
        
        prompt = f"""
        Explique o termo "{term}" no contexto de análise de dados.
        
        Schema disponível: {schema}
        
        Forneça:
        1. O que o termo significa
        2. Em qual tabela ele aparece
        3. Como ele se relaciona com outros campos
        4. Exemplos de valores esperados
        """
        
        response = await self.llm.ainvoke([
            SystemMessage(content=self.SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ])
        
        return response.content
