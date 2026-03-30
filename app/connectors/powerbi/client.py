"""
Power BI REST API Client
Autenticação via Service Principal (Azure AD) + acesso à API REST do Power BI.
Compatível com licença Power BI Pro.
"""

import asyncio
import logging
import os
import time as _time
from typing import Any, Dict, List, Optional

import httpx
import msal

logger = logging.getLogger(__name__)


class PowerBIConfig:
    """Configuração do cliente Power BI"""

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        workspace_id: str,
        dataset_id: Optional[str] = None,
        report_id: Optional[str] = None,
        api_url: str = "https://api.powerbi.com/v1.0/myorg",
        timeout_seconds: int = 30,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.workspace_id = workspace_id
        self.dataset_id = dataset_id
        self.report_id = report_id
        self.api_url = api_url
        self.timeout_seconds = timeout_seconds
        self.scope = ["https://analysis.windows.net/powerbi/api/.default"]

    @classmethod
    def from_env(cls) -> "PowerBIConfig":
        """Cria configuração a partir do settings (lê o .env via pydantic-settings)"""
        from app.core.config import get_settings
        s = get_settings()
        return cls(
            tenant_id=s.azure_tenant_id,
            client_id=s.azure_client_id,
            client_secret=s.azure_client_secret.get_secret_value(),
            workspace_id=s.powerbi_workspace_id,
            dataset_id=s.powerbi_dataset_id,
            report_id=s.powerbi_report_id,
        )


class PowerBIClient:
    """
    Cliente assíncrono para a API REST do Power BI.

    Exemplo de uso:
        config = PowerBIConfig.from_env()
        client = PowerBIClient(config)
        datasets = await client.list_datasets()
        result = await client.execute_query(dataset_id, "EVALUATE 'MinhaTabela'")
        await client.close()
    """

    def __init__(self, config: PowerBIConfig):
        self.config = config
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._http = httpx.AsyncClient(timeout=config.timeout_seconds)

    # ─────────────────────────────────────────────────────────────
    # Autenticação
    # ─────────────────────────────────────────────────────────────

    async def _get_token(self) -> str:
        """Obtém token de acesso via MSAL (cache por 50 minutos)"""
        now = _time.time()
        if self._token and now < self._token_expiry:
            return self._token

        app = msal.ConfidentialClientApplication(
            client_id=self.config.client_id,
            client_credential=self.config.client_secret,
            authority=f"https://login.microsoftonline.com/{self.config.tenant_id}",
        )

        result = app.acquire_token_for_client(scopes=self.config.scope)

        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "Unknown error"))
            raise PowerBIAuthError(f"Falha ao obter token Azure AD: {error}")

        self._token = result["access_token"]
        self._token_expiry = now + 3000  # 50 minutos
        logger.debug("Token Azure AD obtido com sucesso")
        return self._token

    async def _headers(self) -> Dict[str, str]:
        token = await self._get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def _get(self, path: str) -> Dict[str, Any]:
        headers = await self._headers()
        url = f"{self.config.api_url}{path}"
        resp = await self._http.get(url, headers=headers)
        self._raise_for_status(resp)
        return resp.json()

    async def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        headers = await self._headers()
        url = f"{self.config.api_url}{path}"
        resp = await self._http.post(url, headers=headers, json=payload)
        self._raise_for_status(resp)
        return resp.json()

    def _raise_for_status(self, resp: httpx.Response) -> None:
        if resp.status_code == 401:
            raise PowerBIAuthError("Token inválido ou sem permissão (401)")
        if resp.status_code == 403:
            raise PowerBIAuthError(
                "Acesso negado (403). Verifique se o Service Principal tem acesso ao workspace."
            )
        if resp.status_code == 404:
            url = str(resp.url)
            if "executeQueries" in url:
                raise PowerBIAuthError(
                    "executeQueries retornou 404. O Service Principal precisa de permissao "
                    "'Build' no dataset ou papel Contributor/Member/Admin no workspace. "
                    "Acesse o portal Power BI, abra o workspace, va em Configuracoes > Acesso "
                    "e eleve o papel do Service Principal para Contributor."
                )
            raise PowerBINotFoundError(f"Recurso não encontrado (404): {resp.url}")
        if not resp.is_success:
            raise PowerBIError(
                f"Erro na API Power BI ({resp.status_code}): {resp.text[:300]}"
            )

    # ─────────────────────────────────────────────────────────────
    # Datasets e Tabelas
    # ─────────────────────────────────────────────────────────────

    async def list_datasets(self, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista todos os datasets no workspace"""
        wid = workspace_id or self.config.workspace_id
        data = await self._get(f"/groups/{wid}/datasets")
        return data.get("value", [])

    async def get_tables(
        self, dataset_id: str, workspace_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Retorna tabelas de um dataset via API REST"""
        wid = workspace_id or self.config.workspace_id
        data = await self._get(f"/groups/{wid}/datasets/{dataset_id}/tables")
        return data.get("value", [])

    async def get_refresh_history(
        self, dataset_id: str, workspace_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Retorna histórico de refresh do dataset"""
        wid = workspace_id or self.config.workspace_id
        data = await self._get(f"/groups/{wid}/datasets/{dataset_id}/refreshes")
        return data.get("value", [])

    # ─────────────────────────────────────────────────────────────
    # Execução de Queries DAX
    # ─────────────────────────────────────────────────────────────

    async def execute_query(
        self,
        dataset_id: str,
        dax_query: str,
        workspace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executa uma query DAX via REST API (disponível no Power BI Pro).

        Args:
            dataset_id: ID do dataset
            dax_query: Query DAX (deve começar com EVALUATE)
            workspace_id: ID do workspace (usa o padrão se omitido)

        Returns:
            Dict com 'columns', 'rows', 'row_count'
        """
        wid = workspace_id or self.config.workspace_id
        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True},
        }

        logger.debug(f"Executando DAX query no dataset {dataset_id}")
        data = await self._post(
            f"/groups/{wid}/datasets/{dataset_id}/executeQueries", payload
        )

        # A resposta vem em: results[0].tables[0].rows
        results = data.get("results", [])
        if not results:
            return {"columns": [], "rows": [], "row_count": 0}

        tables = results[0].get("tables", [])
        if not tables:
            return {"columns": [], "rows": [], "row_count": 0}

        rows = tables[0].get("rows", [])
        columns = list(rows[0].keys()) if rows else []

        # Limpa prefixo "Tabela[Coluna]" → "Coluna"
        cleaned_rows = []
        for row in rows:
            cleaned = {}
            for k, v in row.items():
                # Remove prefixo "NomeTabela[" e "]"
                clean_key = k.split("[")[-1].rstrip("]") if "[" in k else k
                cleaned[clean_key] = v
            cleaned_rows.append(cleaned)

        clean_columns = [c.split("[")[-1].rstrip("]") if "[" in c else c for c in columns]

        return {
            "columns": clean_columns,
            "rows": cleaned_rows,
            "row_count": len(cleaned_rows),
        }

    # ─────────────────────────────────────────────────────────────
    # Export para PDF
    # ─────────────────────────────────────────────────────────────

    async def export_report_to_pdf(
        self,
        report_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
        pages: Optional[List[str]] = None,
    ) -> bytes:
        """
        Exporta um relatório Power BI para PDF (disponível no Pro).
        Usa a Export to File API com polling assíncrono.

        Args:
            report_id: ID do relatório (usa o padrão se omitido)
            workspace_id: ID do workspace
            pages: Lista de nomes de páginas (exporta todas se omitido)

        Returns:
            Bytes do arquivo PDF
        """
        rid = report_id or self.config.report_id
        if not rid:
            raise PowerBIError("report_id não configurado")

        wid = workspace_id or self.config.workspace_id
        headers = await self._headers()

        payload: Dict[str, Any] = {"format": "PDF"}
        if pages:
            payload["powerBIReportConfiguration"] = {
                "pages": [{"pageName": p} for p in pages]
            }

        # Inicia o job de exportação
        start_url = f"{self.config.api_url}/groups/{wid}/reports/{rid}/ExportTo"
        resp = await self._http.post(start_url, headers=headers, json=payload)
        self._raise_for_status(resp)
        export_id = resp.json()["id"]
        logger.info(f"Export job iniciado: {export_id}")

        # Polling até completar (máx 60 segundos)
        status_url = f"{self.config.api_url}/groups/{wid}/reports/{rid}/exports/{export_id}"
        for attempt in range(30):
            await asyncio.sleep(2)
            headers = await self._headers()
            status_resp = await self._http.get(status_url, headers=headers)
            self._raise_for_status(status_resp)
            status_data = status_resp.json()
            status = status_data.get("status", "")

            logger.debug(f"Export status ({attempt + 1}/30): {status}")

            if status == "Succeeded":
                # Baixa o arquivo
                file_url = f"{status_url}/file"
                headers = await self._headers()
                file_resp = await self._http.get(file_url, headers=headers)
                self._raise_for_status(file_resp)
                logger.info(f"PDF exportado com sucesso ({len(file_resp.content)} bytes)")
                return file_resp.content

            if status == "Failed":
                err_msg = status_data.get("error", {}).get("message", "Erro desconhecido")
                raise PowerBIError(f"Export falhou: {err_msg}")

        raise PowerBIError("Export para PDF excedeu o tempo limite (60s)")

    # ─────────────────────────────────────────────────────────────
    # Export para PNG (página individual)
    # ─────────────────────────────────────────────────────────────

    async def export_page_as_image(
        self,
        report_id: str,
        page_name: str,
        workspace_id: Optional[str] = None,
    ) -> bytes:
        """
        Exporta uma página do relatório como PNG (disponível no Pro).
        Usa a Export to File API com polling assíncrono.

        Args:
            report_id: ID do relatório
            page_name: Nome interno da página (ex: "ReportSection1")
            workspace_id: ID do workspace

        Returns:
            Bytes da imagem PNG
        """
        rid = report_id or self.config.report_id
        if not rid:
            raise PowerBIError("report_id não configurado")

        wid = workspace_id or self.config.workspace_id
        headers = await self._headers()

        payload: Dict[str, Any] = {
            "format": "PNG",
            "powerBIReportConfiguration": {
                "pages": [{"pageName": page_name}]
            },
        }

        start_url = f"{self.config.api_url}/groups/{wid}/reports/{rid}/ExportTo"
        resp = await self._http.post(start_url, headers=headers, json=payload)
        self._raise_for_status(resp)
        export_id = resp.json()["id"]
        logger.info(f"Export PNG iniciado para página '{page_name}': {export_id}")

        status_url = f"{self.config.api_url}/groups/{wid}/reports/{rid}/exports/{export_id}"
        for attempt in range(30):
            await asyncio.sleep(2)
            headers = await self._headers()
            status_resp = await self._http.get(status_url, headers=headers)
            self._raise_for_status(status_resp)
            status_data = status_resp.json()
            status = status_data.get("status", "")

            logger.debug(f"Export PNG status ({attempt + 1}/30): {status}")

            if status == "Succeeded":
                file_url = f"{status_url}/file"
                headers = await self._headers()
                file_resp = await self._http.get(file_url, headers=headers)
                self._raise_for_status(file_resp)

                content = file_resp.content
                # Power BI retorna ZIP quando exporta múltiplas páginas; para 1 página geralmente PNG direto.
                # Verifica pela assinatura PK (ZIP magic bytes) por segurança.
                if content[:4] == b"PK\x03\x04":
                    import io as _io
                    import zipfile
                    with zipfile.ZipFile(_io.BytesIO(content)) as zf:
                        png_names = [n for n in zf.namelist() if n.lower().endswith(".png")]
                        if png_names:
                            content = zf.read(png_names[0])

                logger.info(f"PNG exportado com sucesso ({len(content)} bytes)")
                return content

            if status == "Failed":
                err_msg = status_data.get("error", {}).get("message", "Erro desconhecido")
                raise PowerBIError(f"Export PNG falhou: {err_msg}")

        raise PowerBIError("Export PNG excedeu o tempo limite (60s)")

    # ─────────────────────────────────────────────────────────────
    # Utilitários
    # ─────────────────────────────────────────────────────────────

    async def list_report_pages(
        self, report_id: str, workspace_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Lista as páginas de um relatório"""
        wid = workspace_id or self.config.workspace_id
        data = await self._get(f"/groups/{wid}/reports/{report_id}/pages")
        return data.get("value", [])

    async def list_workspaces(self) -> List[Dict[str, Any]]:
        """Lista todos os workspaces acessíveis ao service principal"""
        data = await self._get("/groups")
        return data.get("value", [])

    async def list_reports(self, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista relatórios no workspace"""
        wid = workspace_id or self.config.workspace_id
        data = await self._get(f"/groups/{wid}/reports")
        return data.get("value", [])

    async def generate_embed_token(
        self, report_id: str, workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Gera embed token para visualização de relatório"""
        wid = workspace_id or self.config.workspace_id
        payload = {"accessLevel": "View"}
        return await self._post(
            f"/groups/{wid}/reports/{report_id}/GenerateToken", payload
        )

    async def get_dataset_info(
        self, dataset_id: str, workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retorna metadados de um dataset"""
        wid = workspace_id or self.config.workspace_id
        return await self._get(f"/groups/{wid}/datasets/{dataset_id}")

    async def download_report_pbix(
        self,
        report_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
    ) -> bytes:
        """
        Baixa o arquivo PBIX do relatório para extração de filtros.
        GET /groups/{workspace_id}/reports/{report_id}/Export

        Requer que o Service Principal tenha papel de Admin ou Contributor no workspace
        e que o relatório não esteja protegido por RLS/sensitividade.
        """
        rid = report_id or self.config.report_id
        if not rid:
            raise PowerBIError("report_id não configurado — defina POWERBI_REPORT_ID no .env")
        wid = workspace_id or self.config.workspace_id
        headers = await self._headers()
        url = f"{self.config.api_url}/groups/{wid}/reports/{rid}/Export"
        logger.info("Baixando PBIX do relatório %s para extração de filtros...", rid)
        resp = await self._http.get(url, headers=headers)
        if resp.status_code == 403:
            raise PowerBIAuthError(
                "Acesso negado ao baixar PBIX (403). O Service Principal precisa de papel "
                "Admin ou Contributor no workspace para exportar o arquivo .pbix."
            )
        self._raise_for_status(resp)
        logger.info("PBIX baixado: %d bytes", len(resp.content))
        return resp.content

    async def close(self) -> None:
        """Fecha o cliente HTTP"""
        await self._http.aclose()


# ─────────────────────────────────────────────────────────────
# Exceções
# ─────────────────────────────────────────────────────────────


class PowerBIError(Exception):
    """Erro genérico do Power BI"""


class PowerBIAuthError(PowerBIError):
    """Erro de autenticação/autorização"""


class PowerBINotFoundError(PowerBIError):
    """Recurso não encontrado"""


# ─────────────────────────────────────────────────────────────
# Dependency injection para FastAPI
# ─────────────────────────────────────────────────────────────

_client: Optional[PowerBIClient] = None


def get_powerbi_client() -> PowerBIClient:
    """FastAPI dependency — retorna cliente singleton"""
    global _client
    if _client is None:
        config = PowerBIConfig.from_env()
        _client = PowerBIClient(config)
    return _client
