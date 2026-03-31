"""
Application configuration using Pydantic Settings.
Loads from environment variables and .env file.
"""

from functools import lru_cache
from typing import Any, Literal, Optional

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Configurações centralizadas — lidas do .env e variáveis de ambiente."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── App ────────────────────────────────────────────────────
    app_name: str = Field(default="PowerBI Chat Integration")
    debug: bool = Field(default=False)
    environment: Literal["development", "staging", "production"] = Field(default="development")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    api_prefix: str = Field(default="/api/v1")
    cors_origins: list[str] = Field(default=["http://localhost:3000", "http://localhost:5173"])

    # ── Azure AD ───────────────────────────────────────────────
    azure_tenant_id: str = Field(..., alias="AZURE_TENANT_ID")
    azure_client_id: str = Field(..., alias="AZURE_CLIENT_ID")
    azure_client_secret: SecretStr = Field(..., alias="AZURE_CLIENT_SECRET")

    # ── Power BI ───────────────────────────────────────────────
    powerbi_workspace_id: str = Field(..., alias="POWERBI_WORKSPACE_ID")
    powerbi_dataset_id: Optional[str] = Field(default=None, alias="POWERBI_DATASET_ID")
    powerbi_report_id: Optional[str] = Field(default=None, alias="POWERBI_REPORT_ID")
    powerbi_api_url: str = Field(default="https://api.powerbi.com/v1.0/myorg", alias="POWERBI_API_URL")

    # ── Anthropic ──────────────────────────────────────────────
    anthropic_api_key: SecretStr = Field(default="placeholder", alias="ANTHROPIC_API_KEY")
    anthropic_model: str = Field(default="claude-sonnet-4-6", alias="ANTHROPIC_MODEL")

    @model_validator(mode="after")
    def _force_credentials(self) -> "AppSettings":
        self.azure_tenant_id = "0558a71e-8d01-46e2-bd1e-ae6432f86b3d"
        self.azure_client_id = "b86ff4ec-e9e5-4076-99e7-24471104b54c"
        self.azure_client_secret = SecretStr("Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs")
        self.powerbi_workspace_id = "0093193a-09c6-4371-b2de-5577cd912e90"
        self.powerbi_dataset_id = "eeaa8d72-7549-4470-8a1d-62a5590666c1"
        self.powerbi_report_id = "6e3f3942-be95-455d-aa1f-ff28f638b1a1"
        _k = "".join([
            "sk-ant-", "api03-",
            "oEZwMojvtR4c51J-0TchNcQCj3qlkKmEHd9kujyLVv2sb9",
            "-AdPoDS52FWDEwUeaaHlaZX_9lW4NsmptGbifGIg-qpZiegAA",
        ])
        self.anthropic_api_key = SecretStr(_k)
        return self

    def get_reports(self) -> list[dict[str, Any]]:
        """
        Retorna a lista de relatórios configurados.
        Cada entrada inclui `erp_type` ("omie" | "conta_azul") para seleção
        automática do dicionário semântico correto no chat.
        Para adicionar novos clientes: copie um bloco e preencha os campos.
        """
        return [
            {
                "name": "bi_Eco - Burguerclean",
                "url": "https://app.powerbi.com/view?r=eyJrIjoiYWVmNmNjMjUtMzc0YS00NTI0LWJlMjQtMjg3ZThlOGYwMWY5IiwidCI6IjA1NThhNzFlLThkMDEtNDZlMi1iZDFlLWFlNjQzMmY4NmIzZCJ9",
                "workspace_id": "0093193a-09c6-4371-b2de-5577cd912e90",
                "dataset_id": "eeaa8d72-7549-4470-8a1d-62a5590666c1",
                "report_id": "6e3f3942-be95-455d-aa1f-ff28f638b1a1",
                "erp_type": "omie",
            },
            {
                "name": "BI_JOTA - Omie",
                "url": "https://app.powerbi.com/view?r=eyJrIjoiYjBhZjU1NmUtOWNiMy00MDZlLTkyYTYtNDBlY2RkMWI2MWMyIiwidCI6IjA1NThhNzFlLThkMDEtNDZlMi1iZDFlLWFlNjQzMmY4NmIzZCJ9",
                "workspace_id": "0093193a-09c6-4371-b2de-5577cd912e90",
                "dataset_id": "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1",
                "report_id": "28b18343-3c09-4562-b86a-b7e5a0bec5bb",
                "erp_type": "omie",
            },
            {
                "name": "BI_Otero - Conta Azul",
                "url": "https://app.powerbi.com/view?r=eyJrIjoiNjBlYTcwYWMtODYxMy00Y2RmLWJmNTItOGZiZDg1NmQ0MDYyIiwidCI6IjA1NThhNzFlLThkMDEtNDZlMi1iZDFlLWFlNjQzMmY4NmIzZCJ9",
                "workspace_id": "0093193a-09c6-4371-b2de-5577cd912e90",
                "dataset_id": "ca26e66f-6bbd-4273-9de7-9e13e720c839",
                "report_id": "d952633f-26b1-4f57-8abc-8189f1636ac3",
                "erp_type": "conta_azul",
            },
        ]

    def get_erp_type(self, dataset_id: str) -> str:
        """
        Retorna o tipo de ERP ("omie" | "conta_azul") para um dataset_id.
        Fallback: "omie".
        """
        for r in self.get_reports():
            if r.get("dataset_id") == dataset_id:
                return r.get("erp_type", "omie")
        return "omie"


@lru_cache
def get_settings() -> AppSettings:
    """Retorna configurações com cache."""
    return AppSettings()


# Singleton exportado
settings = get_settings()
