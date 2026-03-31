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
        # ── Hypothesis: CCAREC + cOrigem IN {"BAXR","EXTR"} + ano_mes_caixa ──
        # Jan: BAXR=431,686.06 + EXTR=70,041.94 = 501,728.00 ← MATCH!
        # Verify for all 3 months

        print("=== cOrigem breakdown por mês (CCAREC, cNatureza=R) ===")
        for mes in ["janeiro2025", "maio2025", "outubro2025"]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE
SUMMARIZECOLUMNS(
    'data'[cOrigem],
    FILTER(ALL('data'),
        'data'[cNatureza] = "R"
        && 'data'[ano_mes_caixa] = "{mes}"
        && 'data'[cGrupo] = "CONTA_CORRENTE_REC"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
            print(f"\n  {mes}:")
            total = 0
            baxr_extr = 0
            for r in rows:
                v = float(r.get('Receita') or 0)
                total += v
                origem = r.get('cOrigem', '')
                if origem in ('BAXR', 'EXTR'):
                    baxr_extr += v
                print(f"    cOrigem={origem!r:8}  {fmt(v)}  Qtd={r.get('Qtd')}")
            print(f"    TOTAL CCAREC: {fmt(total)}")
            print(f"    BAXR+EXTR:   {fmt(baxr_extr)}")

        await asyncio.sleep(3)

        # ── Test formula: receita + CCAREC + BAXR/EXTR + ano_mes_caixa ──
        print("\n=== FORMULA: receita + CCAREC + cOrigem IN {BAXR,EXTR} + ano_mes_caixa ===")
        for mes, label, target in [("janeiro2025","Jan",TARGET_JAN), ("maio2025","Mai",TARGET_MAI), ("outubro2025","Out",TARGET_OUT)]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cNatureza] = "R",
    'data'[cGrupo] = "CONTA_CORRENTE_REC",
    'data'[cOrigem] IN {{"BAXR","EXTR"}},
    'data'[ano_mes_caixa] = "{mes}"))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

        await asyncio.sleep(3)

        # ── Also test without cNatureza=R filter (to check if it matters) ──
        print("\n=== FORMULA sem cNatureza: receita + CCAREC + BAXR/EXTR + ano_mes_caixa ===")
        for mes, label, target in [("janeiro2025","Jan",TARGET_JAN), ("maio2025","Mai",TARGET_MAI), ("outubro2025","Out",TARGET_OUT)]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cGrupo] = "CONTA_CORRENTE_REC",
    'data'[cOrigem] IN {{"BAXR","EXTR"}},
    'data'[ano_mes_caixa] = "{mes}"))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

        await asyncio.sleep(3)

        # ── Test with DataPagamento instead of ano_mes_caixa ──
        print("\n=== FORMULA com DataPagamento: receita + CCAREC + BAXR/EXTR ===")
        for m, label, target in [(1,"Jan",TARGET_JAN), (5,"Mai",TARGET_MAI), (10,"Out",TARGET_OUT)]:
            import calendar
            last_day = calendar.monthrange(2025, m)[1]
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cGrupo] = "CONTA_CORRENTE_REC",
    'data'[cOrigem] IN {{"BAXR","EXTR"}},
    'data'[DataPagamento] >= DATE(2025,{m},1),
    'data'[DataPagamento] <= DATE(2025,{m},{last_day})))""")
            v = rows[0].get("v") if rows else None
            print(f"  {label}: {fmt(v)}{chk(v, target)}")

    finally:
        await client.close()

asyncio.run(main())
