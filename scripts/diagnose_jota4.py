# -*- coding: utf-8 -*-
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"
DID = "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1"

TARGET_JAN = 501728.00
TARGET_MAI = 496455.38
TARGET_OUT = 552866.44

async def q(client, dax):
    try:
        r = await client.execute_query(DID, dax, WORKSPACE_ID)
        return r.get("rows", [])
    except Exception as e:
        return [{"_erro": str(e)[:120]}]

def fmt(v):
    try: return f"R$ {float(v):>14,.2f}"
    except: return str(v)

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)
    try:
        # ── 1. Conta × cGrupo breakdown (ano_mes_caixa=janeiro2025) ──
        print("=== Conta x cGrupo (ano_mes_caixa=janeiro2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta], 'data'[cGrupo],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY 'data'[Conta], [Receita] DESC""")
        for r in rows:
            print(f"  {r.get('Conta')!r:25}  {r.get('cGrupo')!r:30}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 2. Same but for maio and outubro ──
        print("\n=== Conta x cGrupo (ano_mes_caixa=maio2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta], 'data'[cGrupo],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "maio2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY 'data'[Conta], [Receita] DESC""")
        for r in rows:
            print(f"  {r.get('Conta')!r:25}  {r.get('cGrupo')!r:30}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        print("\n=== Conta x cGrupo (ano_mes_caixa=outubro2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta], 'data'[cGrupo],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "outubro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY 'data'[Conta], [Receita] DESC""")
        for r in rows:
            print(f"  {r.get('Conta')!r:25}  {r.get('cGrupo')!r:30}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 3. cOrigem breakdown (pistas sobre o que é duplicado) ──
        print("\n=== cOrigem breakdown (ano_mes_caixa=janeiro2025, cNatureza=R) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[cGrupo], 'data'[cOrigem],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  cGrupo={r.get('cGrupo')!r:30}  cOrigem={r.get('cOrigem')!r:10}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 4. Check if CONTA_CORRENTE_REC has nCodTitulo = "0" (extract transactions) ──
        print("\n=== CONTA_CORRENTE_REC sample rows (jan 2025) ===")
        rows = await q(client, """
EVALUATE
TOPN(10,
    FILTER('data',
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "janeiro2025"
        && 'data'[cGrupo] = "CONTA_CORRENTE_REC"
    ),
    'data'[receita], DESC
)""")
        for r in rows:
            print(f"  Conta={r.get('Conta')!r:20}  cOrigem={r.get('cOrigem')!r:6}  "
                  f"nCodTitulo={r.get('nCodTitulo')!r:20}  nCodCC={r.get('nCodCC')!r:15}  "
                  f"receita={fmt(r.get('receita'))}  nValPago={fmt(r.get('nValPago'))}")

        await asyncio.sleep(3)

        # ── 5. CONTA_CORRENTE_REC — is it all from EXTR (extratos)? ──
        print("\n=== CONTA_CORRENTE_REC + cOrigem=EXTR (jan 2025) — compare total ===")
        rows = await q(client, """
EVALUATE ROW(
    "CCAREC_total", CALCULATE(SUM('data'[receita]),
        'data'[cNatureza] = "R",
        'data'[ano_mes_caixa] = "janeiro2025",
        'data'[cGrupo] = "CONTA_CORRENTE_REC"),
    "CCAREC_EXTR", CALCULATE(SUM('data'[receita]),
        'data'[cNatureza] = "R",
        'data'[ano_mes_caixa] = "janeiro2025",
        'data'[cGrupo] = "CONTA_CORRENTE_REC",
        'data'[cOrigem] = "EXTR"),
    "CCAREC_VENR", CALCULATE(SUM('data'[receita]),
        'data'[cNatureza] = "R",
        'data'[ano_mes_caixa] = "janeiro2025",
        'data'[cGrupo] = "CONTA_CORRENTE_REC",
        'data'[cOrigem] = "VENR"),
    "CCAREC_outros", CALCULATE(SUM('data'[receita]),
        'data'[cNatureza] = "R",
        'data'[ano_mes_caixa] = "janeiro2025",
        'data'[cGrupo] = "CONTA_CORRENTE_REC",
        NOT 'data'[cOrigem] IN {"EXTR","VENR"})
)""")
        for k, v in (rows[0] if rows else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # ── 6. CONTA_A_RECEBER excluindo duplicatas de CCAREC ──
        # Test: CONTA_A_RECEBER only excluding rows that have a CCAREC counterpart
        print("\n=== CONTA_A_RECEBER + cOrigem breakdown (jan 2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[cOrigem],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "janeiro2025"
        && 'data'[cGrupo] = "CONTA_A_RECEBER"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  cOrigem={r.get('cOrigem')!r}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

    finally:
        await client.close()

asyncio.run(main())
