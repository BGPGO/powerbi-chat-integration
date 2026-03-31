# -*- coding: utf-8 -*-
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"
DID = "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1"

async def q(client, dax):
    try:
        r = await client.execute_query(DID, dax, WORKSPACE_ID)
        return r.get("rows", [])
    except Exception as e:
        return [{"_erro": str(e)[:100]}]

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)
    try:
        # 1. Quantas empresas (Conta) existem no dataset?
        print("=== Contas no dataset ===")
        rows = await q(client, """
EVALUATE SUMMARIZECOLUMNS('data'[Conta], "Qtd", COUNTROWS('data'))
ORDER BY [Qtd] DESC""")
        for r in rows: print(f"  Conta={r.get('Conta')!r}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 2. cGrupo + cStatus para jan/2025 pago
        print("\n=== cGrupo vs cStatus (cNatureza=R, jan/2025 pago) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[cGrupo], 'data'[cStatus],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[DataPagamento] >= DATE(2025,1,1)
        && 'data'[DataPagamento] <= DATE(2025,1,31)
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  cGrupo={r.get('cGrupo')!r}  cStatus={r.get('cStatus')!r}  "
                  f"Receita=R${float(r.get('Receita') or 0):>12,.2f}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 3. Receita por Conta + jan/2025
        print("\n=== Receita por Conta (caixa, jan/2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta],
    FILTER(ALL('data'),
        'data'[cStatus] IN {"PAGO","RECEBIDO"}
        && 'data'[DataPagamento] >= DATE(2025,1,1)
        && 'data'[DataPagamento] <= DATE(2025,1,31)
    ),
    "Receita", SUM('data'[receita])
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  Conta={r.get('Conta')!r}  Receita=R${float(r.get('Receita') or 0):>12,.2f}")

        await asyncio.sleep(3)

        # 4. liquido column vs receita
        print("\n=== liquido vs receita (caixa, jan/2025) ===")
        rows = await q(client, """
EVALUATE ROW(
    "SUM_receita", CALCULATE(SUM('data'[receita]),
        'data'[cStatus] IN {"PAGO","RECEBIDO"},
        'data'[DataPagamento] >= DATE(2025,1,1),
        'data'[DataPagamento] <= DATE(2025,1,31)),
    "SUM_liquido", CALCULATE(SUM('data'[liquido]),
        'data'[cNatureza] = "R",
        'data'[cStatus] IN {"PAGO","RECEBIDO"},
        'data'[DataPagamento] >= DATE(2025,1,1),
        'data'[DataPagamento] <= DATE(2025,1,31)),
    "SUM_nValPago_cNat_R", CALCULATE(SUM('data'[nValPago]),
        'data'[cNatureza] = "R",
        'data'[cStatus] IN {"PAGO","RECEBIDO"},
        'data'[DataPagamento] >= DATE(2025,1,1),
        'data'[DataPagamento] <= DATE(2025,1,31))
)""")
        for k, v in (rows[0] if rows else {}).items():
            print(f"  {k}: R$ {float(v or 0):>14,.2f}")

        await asyncio.sleep(3)

        # 5. Aux_caixa_data sample — ver o que o aux usa
        print("\n=== aux_caixa_data samples (jan/2025 pago) ===")
        rows = await q(client, """
EVALUATE
TOPN(5,
    FILTER('data',
        'data'[cStatus] IN {"PAGO","RECEBIDO"}
        && 'data'[DataPagamento] >= DATE(2025,1,1)
        && 'data'[DataPagamento] <= DATE(2025,1,31)
    ),
    'data'[DataPagamento], ASC
)""")
        for r in rows:
            print(f"  Conta={r.get('Conta')!r}  cGrupo={r.get('cGrupo')!r}  "
                  f"cStatus={r.get('cStatus')!r}  receita={r.get('receita')}  "
                  f"nValPago={r.get('nValPago')}  aux={r.get('aux_caixa_data')!r}")

        await asyncio.sleep(3)

        # 6. Competência — tentar com cStatus != CANCELADO
        print("\n=== Competência excluindo CANCELADO (jan 2025) ===")
        rows = await q(client, """
EVALUATE ROW(
    "comp_excl_cancel", CALCULATE(SUM('data'[receita competencia]),
        'data'[cStatus] <> "CANCELADO",
        'data'[cNatureza] = "R",
        'data'[ano_mes_competencia] = "janeiro2025"),
    "comp_cgrupo_receber", CALCULATE(SUM('data'[receita competencia]),
        'data'[cGrupo] = "CONTA_A_RECEBER",
        'data'[ano_mes_competencia] = "janeiro2025"),
    "comp_cgrupo_prev_contrato", CALCULATE(SUM('data'[receita competencia]),
        'data'[cGrupo] IN {"CONTA_A_RECEBER","PREVISAO_CONTRATO"},
        'data'[ano_mes_competencia] = "janeiro2025"),
    "comp_todos", CALCULATE(SUM('data'[receita competencia]),
        'data'[ano_mes_competencia] = "janeiro2025")
)""")
        for k, v in (rows[0] if rows else {}).items():
            alvo = ""
            if abs(float(v or 0) - 477907.05) < 100: alvo = "  <-- MATCH JAN!"
            print(f"  {k}: R$ {float(v or 0):>14,.2f}{alvo}")

    finally:
        await client.close()

asyncio.run(main())
