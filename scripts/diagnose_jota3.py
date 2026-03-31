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

def chk(v, target):
    try:
        diff = float(v or 0) - target
        if abs(diff) < 1.0: return "  ★★★ MATCH!"
        return f"  diff={diff:+,.2f}"
    except: return ""

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)
    try:
        # ── 1. Distinct cStatus for Jan 2025 (DataPagamento) + cNatureza=R ──
        print("=== cStatus breakdown (DataPagamento jan/2025, cNatureza=R) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[cStatus],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[DataPagamento] >= DATE(2025,1,1)
        && 'data'[DataPagamento] <= DATE(2025,1,31)
    ),
    "Receita", SUM('data'[receita]),
    "ValUnico", SUM('data'[valor_unico_2]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  cStatus={r.get('cStatus')!r}  receita={fmt(r.get('Receita'))}  valor_unico_2={fmt(r.get('ValUnico'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 2. ano_mes_caixa distinct values for 2025 ──
        print("\n=== ano_mes_caixa distinct (2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[ano_mes_caixa],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] <> BLANK()
        && RIGHT('data'[ano_mes_caixa], 4) = "2025"
    ),
    "Receita", SUM('data'[receita]),
    "ValUnico", SUM('data'[valor_unico_2]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  ano_mes_caixa={r.get('ano_mes_caixa')!r}  receita={fmt(r.get('Receita'))}  valor_unico_2={fmt(r.get('ValUnico'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 3. SUM(receita) with ano_mes_caixa filter for 3 months ──
        print("\n=== receita + ano_mes_caixa (jan/mai/out) ===")
        for mes, label, target in [("janeiro2025","Jan",TARGET_JAN), ("maio2025","Mai",TARGET_MAI), ("outubro2025","Out",TARGET_OUT)]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cNatureza] = "R",
    'data'[ano_mes_caixa] = "{mes}"))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

        await asyncio.sleep(3)

        # ── 4. SUM(valor_unico_2) with ano_mes_caixa filter ──
        print("\n=== valor_unico_2 + ano_mes_caixa (jan/mai/out) ===")
        for mes, label, target in [("janeiro2025","Jan",TARGET_JAN), ("maio2025","Mai",TARGET_MAI), ("outubro2025","Out",TARGET_OUT)]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[valor_unico_2]),
    'data'[cNatureza] = "R",
    'data'[ano_mes_caixa] = "{mes}"))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

        await asyncio.sleep(3)

        # ── 5. Previsto/realizado values breakdown ──
        print("\n=== Previsto/realizado + ano_mes_caixa=janeiro2025 ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Previsto/realizado],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  Previsto/realizado={r.get('Previsto/realizado')!r}  receita={fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 6. previsto column values breakdown ──
        print("\n=== previsto column + ano_mes_caixa=janeiro2025 ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[previsto],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  previsto={r.get('previsto')!r}  receita={fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 7. receita per conta with ano_mes_caixa ──
        print("\n=== receita por Conta + ano_mes_caixa=janeiro2025 ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "ValUnico", SUM('data'[valor_unico_2]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  Conta={r.get('Conta')!r}  receita={fmt(r.get('Receita'))}  valor_unico_2={fmt(r.get('ValUnico'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── 8. liquido + ano_mes_caixa ──
        print("\n=== liquido + ano_mes_caixa (jan/mai/out) ===")
        for mes, label, target in [("janeiro2025","Jan",TARGET_JAN), ("maio2025","Mai",TARGET_MAI), ("outubro2025","Out",TARGET_OUT)]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[liquido]),
    'data'[cNatureza] = "R",
    'data'[ano_mes_caixa] = "{mes}"))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

        await asyncio.sleep(3)

        # ── 9. nValPago + ano_mes_caixa ──
        print("\n=== nValPago + ano_mes_caixa (jan/mai/out) ===")
        for mes, label, target in [("janeiro2025","Jan",TARGET_JAN), ("maio2025","Mai",TARGET_MAI), ("outubro2025","Out",TARGET_OUT)]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[nValPago]),
    'data'[cNatureza] = "R",
    'data'[ano_mes_caixa] = "{mes}"))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

        await asyncio.sleep(3)

        # ── 10. Valor único + ano_mes_caixa ──
        print("\n=== Valor único + ano_mes_caixa (jan/mai/out) ===")
        for mes, label, target in [("janeiro2025","Jan",TARGET_JAN), ("maio2025","Mai",TARGET_MAI), ("outubro2025","Out",TARGET_OUT)]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[Valor único]),
    'data'[cNatureza] = "R",
    'data'[ano_mes_caixa] = "{mes}"))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

    finally:
        await client.close()

asyncio.run(main())
