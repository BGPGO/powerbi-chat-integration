"""
clone_reports.py — Clona relatórios do workspace pessoal para o workspace compartilhado.

Usa autenticação de USUÁRIO (device code flow) — não o Service Principal.
Você vai precisar aprovar o login no navegador uma vez.

Uso:
    python clone_reports.py

O script vai:
1. Pedir que você faça login no navegador (device code)
2. Listar todos os relatórios do seu workspace pessoal
3. Clonar BI_OTERO e bi_JOTA para o workspace BI export
4. Mostrar os IDs dos relatórios clonados (para adicionar ao bi_connections.json)
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import msal

# ─────────────────────────────────────────────────────────────
# Carrega .env
# ─────────────────────────────────────────────────────────────

for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())

TENANT_ID         = os.environ["AZURE_TENANT_ID"]
CLIENT_ID         = os.environ["AZURE_CLIENT_ID"]   # seu app Azure AD
API_URL           = "https://api.powerbi.com/v1.0/myorg"
SCOPES            = [
    "https://analysis.windows.net/powerbi/api/Report.ReadWrite.All",
    "https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All",
    "https://analysis.windows.net/powerbi/api/Workspace.ReadWrite.All",
]

# Nome do workspace destino (onde o SP já tem acesso)
TARGET_WORKSPACE_NAME = "BI - Export"  # ajuste se o nome for diferente

# Relatórios a clonar (nomes exatos como aparecem no Power BI)
REPORTS_TO_CLONE = ["BI_OTERO", "bi_JOTA"]


# ─────────────────────────────────────────────────────────────
# Autenticação via Device Code Flow (credenciais do USUÁRIO)
# ─────────────────────────────────────────────────────────────

def get_user_token() -> str:
    """
    Autentica via device code flow.
    Você vai ver uma URL + código no terminal — acesse no navegador e faça login.
    """
    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    )

    # Tenta cache primeiro
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            print("✓ Token obtido do cache")
            return result["access_token"]

    # Device code flow
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Falha ao iniciar device flow: {flow}")

    print("\n" + "="*60)
    print("AÇÃO NECESSÁRIA:")
    print(flow["message"])
    print("="*60 + "\n")

    result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        error = result.get("error_description", result.get("error", "Erro desconhecido"))
        raise RuntimeError(f"Falha na autenticação: {error}")

    print("✓ Login realizado com sucesso!\n")
    return result["access_token"]


# ─────────────────────────────────────────────────────────────
# Chamadas à API Power BI
# ─────────────────────────────────────────────────────────────

async def api_get(client: httpx.AsyncClient, token: str, path: str) -> Dict:
    headers = {"Authorization": f"Bearer {token}"}
    resp = await client.get(f"{API_URL}{path}", headers=headers)
    resp.raise_for_status()
    return resp.json()


async def api_post(client: httpx.AsyncClient, token: str, path: str, body: Dict) -> Dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    resp = await client.post(f"{API_URL}{path}", headers=headers, json=body)
    resp.raise_for_status()
    return resp.json()


async def list_my_reports(client: httpx.AsyncClient, token: str) -> List[Dict]:
    """Lista relatórios do workspace pessoal (My Workspace = sem grupo)."""
    data = await api_get(client, token, "/reports")
    return data.get("value", [])


async def list_my_datasets(client: httpx.AsyncClient, token: str) -> List[Dict]:
    """Lista datasets do workspace pessoal."""
    data = await api_get(client, token, "/datasets")
    return data.get("value", [])


async def list_workspaces(client: httpx.AsyncClient, token: str) -> List[Dict]:
    """Lista todos os workspaces que o usuário tem acesso."""
    data = await api_get(client, token, "/groups")
    return data.get("value", [])


async def list_workspace_reports(client: httpx.AsyncClient, token: str, workspace_id: str) -> List[Dict]:
    """Lista relatórios de um workspace específico."""
    data = await api_get(client, token, f"/groups/{workspace_id}/reports")
    return data.get("value", [])


async def list_workspace_datasets(client: httpx.AsyncClient, token: str, workspace_id: str) -> List[Dict]:
    """Lista datasets de um workspace específico."""
    data = await api_get(client, token, f"/groups/{workspace_id}/datasets")
    return data.get("value", [])


async def clone_report(
    client: httpx.AsyncClient,
    token: str,
    report_id: str,
    new_name: str,
    target_workspace_id: str,
    target_dataset_id: Optional[str] = None,
) -> Dict:
    """
    Clona um relatório do workspace pessoal para o workspace destino.
    A API de clone funciona tanto para /me/reports quanto para /groups/{id}/reports.
    """
    body: Dict[str, Any] = {
        "name": new_name,
        "targetWorkspaceId": target_workspace_id,
    }
    if target_dataset_id:
        body["targetDatasetId"] = target_dataset_id

    return await api_post(client, token, f"/reports/{report_id}/Clone", body)


async def clone_dataset(
    client: httpx.AsyncClient,
    token: str,
    dataset_id: str,
    target_workspace_id: str,
) -> Optional[str]:
    """
    Tenta exportar/mover dataset para o workspace destino.
    Nota: Para datasets do workspace pessoal, o clone do relatório geralmente
    copia o dataset automaticamente se não existir no destino.
    Retorna o ID do dataset no workspace destino, se disponível.
    """
    # Não há endpoint direto de clone de dataset — o clone do relatório cuida disso
    # Esta função é apenas informativa
    return None


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("CLONE DE RELATÓRIOS POWER BI")
    print("Personal Workspace -> Workspace Compartilhado")
    print("=" * 60)

    # 1. Autentica com credenciais do usuário
    token = get_user_token()

    async with httpx.AsyncClient(timeout=60.0) as client:

        # 2. Encontra o workspace destino
        print(f"Buscando workspace '{TARGET_WORKSPACE_NAME}'...")
        workspaces = await list_workspaces(client, token)
        target_ws = next(
            (w for w in workspaces if w.get("name", "").lower() == TARGET_WORKSPACE_NAME.lower()),
            None,
        )
        if not target_ws:
            print(f"\n✗ Workspace '{TARGET_WORKSPACE_NAME}' não encontrado!")
            print("Workspaces disponíveis:")
            for w in workspaces:
                print(f"  - {w['name']} (id: {w['id']})")
            print("\nAjuste TARGET_WORKSPACE_NAME no script e rode novamente.")
            return

        target_workspace_id = target_ws["id"]
        print(f"✓ Workspace destino encontrado: {target_ws['name']} (id: {target_workspace_id})\n")

        # 3. Lista relatórios do workspace pessoal
        print("Listando relatórios do seu workspace pessoal...")
        my_reports = await list_my_reports(client, token)

        if not my_reports:
            print("✗ Nenhum relatório encontrado no workspace pessoal.")
            print("  Verifique se o app Azure AD tem permissão 'Report.ReadWrite.All' ou 'Report.Read.All'")
            return

        print(f"✓ {len(my_reports)} relatório(s) encontrado(s) no workspace pessoal:")
        for r in my_reports:
            print(f"  - '{r['name']}' (id: {r['id']}, datasetId: {r.get('datasetId', 'N/A')})")

        # 4. Encontra e clona os relatórios alvo
        print(f"\nProcurando relatórios: {REPORTS_TO_CLONE}")
        cloned = []

        for target_name in REPORTS_TO_CLONE:
            # Busca case-insensitive
            report = next(
                (r for r in my_reports if r["name"].lower() == target_name.lower()),
                None,
            )

            if not report:
                print(f"\n  ✗ '{target_name}' não encontrado no workspace pessoal.")
                print(f"    Nomes disponíveis: {[r['name'] for r in my_reports]}")
                continue

            print(f"\n  Clonando '{report['name']}' → workspace '{TARGET_WORKSPACE_NAME}'...")
            try:
                cloned_report = await clone_report(
                    client=client,
                    token=token,
                    report_id=report["id"],
                    new_name=report["name"],  # mantém o mesmo nome
                    target_workspace_id=target_workspace_id,
                )

                new_report_id = cloned_report.get("id", "")
                new_dataset_id = cloned_report.get("datasetId", "")

                print(f"  ✓ Clonado com sucesso!")
                print(f"    Report ID (destino):  {new_report_id}")
                print(f"    Dataset ID (destino): {new_dataset_id}")

                cloned.append({
                    "name": report["name"],
                    "original_report_id": report["id"],
                    "original_dataset_id": report.get("datasetId", ""),
                    "cloned_report_id": new_report_id,
                    "cloned_dataset_id": new_dataset_id,
                    "workspace_id": target_workspace_id,
                })

            except httpx.HTTPStatusError as e:
                print(f"  ✗ Erro ao clonar '{report['name']}': {e.response.status_code}")
                body = e.response.text
                print(f"    Detalhe: {body[:300]}")

        # 5. Verifica o que foi clonado no workspace destino
        if cloned:
            print(f"\n{'='*60}")
            print("RESULTADO — Adicione ao bi_connections.json:")
            print("="*60)

            connections = []
            for c in cloned:
                connections.append({
                    "id": c["name"].lower().replace(" ", "_"),
                    "name": c["name"],
                    "system": "detectar_automaticamente",
                    "workspace_id": c["workspace_id"],
                    "dataset_id": c["cloned_dataset_id"],
                    "report_id": c["cloned_report_id"],
                })

            print(json.dumps(connections, indent=2, ensure_ascii=False))

            # Salva em arquivo
            out_path = Path("bi_connections_new.json")
            out_path.write_text(
                json.dumps(connections, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"\n✓ Salvo em: {out_path}")
            print("\nPróximo passo: rode 'python setup_bi_registry.py' para registrar os BIs.")

        else:
            print("\n✗ Nenhum relatório foi clonado.")

        # 6. Lista o que está no workspace destino agora
        print(f"\n{'='*60}")
        print(f"Relatórios atualmente no workspace '{TARGET_WORKSPACE_NAME}':")
        dest_reports = await list_workspace_reports(client, token, target_workspace_id)
        for r in dest_reports:
            print(f"  - '{r['name']}' (reportId: {r['id']}, datasetId: {r.get('datasetId', 'N/A')})")

        dest_datasets = await list_workspace_datasets(client, token, target_workspace_id)
        print(f"\nDatasets no workspace '{TARGET_WORKSPACE_NAME}':")
        for d in dest_datasets:
            print(f"  - '{d['name']}' (id: {d['id']}, isRefreshable: {d.get('isRefreshable', '?')})")


if __name__ == "__main__":
    asyncio.run(main())
