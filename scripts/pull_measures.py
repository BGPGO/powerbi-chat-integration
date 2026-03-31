"""
Script para puxar colunas, amostras e medidas dos 3 datasets Power BI.
Estratégia compatível com Power BI Pro (sem DMV/XMLA):
  1. EVALUATE TOPN(1, 'tabela') → colunas + valores de amostra
  2. REST API /tables → lista de tabelas
  3. Probe em tabelas de medidas comuns
  4. REST API /relationships

Executa direto com: python scripts/pull_measures.py
"""

import asyncio
import sys
import os
import io

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Force UTF-8 output (Windows terminal fix)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

DATASETS = {
    # "Burguerclean (Omie)": "eeaa8d72-7549-4470-8a1d-62a5590666c1",  # já extraído
    "Jota (Omie)": "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1",
    "Otero (Conta Azul)": "ca26e66f-6bbd-4273-9de7-9e13e720c839",
}

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"

# Tabelas de medidas comuns em BIs brasileiros
MEASURE_TABLE_CANDIDATES = [
    "_Medidas", "_medidas", "Medidas", "medidas",
    "_Métricas", "Métricas", "metricas",
    "KPIs", "kpis", "_KPIs",
    "Calculations", "calculations",
    "Cálculos", "calculos",
    "__Medidas", "Medidas_",
    "fMedidas",
]

# Tabelas de dados comuns
DATA_TABLE_CANDIDATES = [
    "data", "Data", "DATA",
    "Lançamentos", "lancamentos",
    "Movimentações", "movimentacoes",
    "Financeiro", "financeiro",
    "Fluxo", "fluxo",
    "fFluxo", "fFinanceiro",
]


async def try_topn(client, dataset_id, table_name, workspace_id):
    """Tenta TOPN(1, tabela). Retorna (colunas, amostra) ou None."""
    try:
        result = await client.execute_query(
            dataset_id=dataset_id,
            dax_query=f"EVALUATE TOPN(1, '{table_name}')",
            workspace_id=workspace_id,
        )
        cols = result.get("columns", [])
        rows = result.get("rows", [])
        if cols:
            return cols, rows[0] if rows else {}
    except Exception:
        pass
    return None


async def pull_for_dataset(client: PowerBIClient, name: str, dataset_id: str):
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"  dataset_id: {dataset_id}")
    print(f"{'='*70}")

    # ── 1. REST API: listar tabelas ────────────────────────────
    print("\n--- TABELAS (REST /tables) ---")
    try:
        resp = await client._get(f"/groups/{WORKSPACE_ID}/datasets/{dataset_id}/tables")
        rest_tables = resp.get("value", [])
        if rest_tables:
            for t in rest_tables:
                print(f"  • {t.get('name', '?')}")
        else:
            print("  (endpoint retornou lista vazia — dataset importado, não push)")
    except Exception as e:
        print(f"  Erro: {e}")

    # ── 2. REST API: relacionamentos ───────────────────────────
    print("\n--- RELACIONAMENTOS (REST /relationships) ---")
    try:
        resp = await client._get(f"/groups/{WORKSPACE_ID}/datasets/{dataset_id}/relationships")
        rels = resp.get("value", [])
        if rels:
            for r in rels:
                print(f"  {r.get('fromTable')}[{r.get('fromColumn')}] → {r.get('toTable')}[{r.get('toColumn')}]  ({r.get('crossFilteringBehavior','')})")
        else:
            print("  (nenhum relacionamento encontrado)")
    except Exception as e:
        print(f"  Erro: {e}")

    # ── 3. Probe tabelas de dados ──────────────────────────────
    print("\n--- TABELAS DE DADOS (TOPN probe) ---")
    found_data_tables = []
    for tname in DATA_TABLE_CANDIDATES:
        await asyncio.sleep(2)  # respeita rate limit Pro
        r = await try_topn(client, dataset_id, tname, WORKSPACE_ID)
        if r:
            cols, sample = r
            found_data_tables.append(tname)
            print(f"\n  TABELA: '{tname}' — {len(cols)} colunas")
            for col in cols:
                sv = sample.get(col, "")
                sv_str = f"  → amostra: {repr(sv)}" if sv is not None and sv != "" else ""
                print(f"    [{col}]{sv_str}")

    if not found_data_tables:
        print("  (nenhuma tabela de dados encontrada com os nomes candidatos)")

    # ── 4. Probe tabelas de medidas ────────────────────────────
    print("\n--- TABELAS DE MEDIDAS (TOPN probe) ---")
    found_measure_tables = []
    for tname in MEASURE_TABLE_CANDIDATES:
        r = await try_topn(client, dataset_id, tname, WORKSPACE_ID)
        if r:
            cols, sample = r
            found_measure_tables.append(tname)
            print(f"\n  TABELA DE MEDIDAS: '{tname}' — {len(cols)} colunas")
            for col in cols:
                sv = sample.get(col, "")
                sv_str = f"  → amostra: {repr(sv)}" if sv is not None and sv != "" else ""
                print(f"    [{col}]{sv_str}")

    if not found_measure_tables:
        print("  (nenhuma tabela de medidas encontrada)")

    # ── 5. Tentar queries simples para confirmar acesso ────────
    print("\n--- TESTE DE ACESSO ---")
    try:
        result = await client.execute_query(
            dataset_id=dataset_id,
            dax_query="EVALUATE ROW(\"OK\", 1)",
            workspace_id=WORKSPACE_ID,
        )
        print(f"  Acesso DAX: OK — resultado: {result.get('rows', [])}")
    except Exception as e:
        print(f"  Acesso DAX: FALHOU — {e}")


async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID,
        timeout_seconds=60,
    )
    client = PowerBIClient(config)

    try:
        for name, dataset_id in DATASETS.items():
            await pull_for_dataset(client, name, dataset_id)
    finally:
        await client.close()

    print(f"\n{'='*70}")
    print("Concluído.")


if __name__ == "__main__":
    asyncio.run(main())
