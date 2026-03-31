# -*- coding: utf-8 -*-
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"
DID = "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1"

T_JAN, T_MAI, T_OUT = 438339.25, 447217.82, 438612.24

async def q(client, dax):
    try:
        r = await client.execute_query(DID, dax, WORKSPACE_ID)
        return r.get("rows", [])
    except Exception as e:
        return [{"_erro": str(e)[:100]}]

def fmt(v):
    try: return f"R$ {float(v):>14,.2f}"
    except: return str(v)

def chk(v, t):
    try:
        d = float(v or 0) - t
        return "  ★★★ MATCH!" if abs(d) < 1 else f"  diff={d:+,.2f}"
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
        # 1. cGrupo breakdown para despesas (cNatureza=P) janeiro2025
        print("=== cGrupo + cOrigem breakdown DESPESAS (jan2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[cGrupo], 'data'[cOrigem],
    FILTER(ALL('data'),
        'data'[cNatureza] = "P"
        && 'data'[ano_mes_caixa] = "janeiro2025"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Despesa] DESC""")
        total = 0
        for r in rows:
            v = float(r.get('Despesa') or 0)
            total += v
            print(f"  cGrupo={r.get('cGrupo')!r:30}  cOrigem={r.get('cOrigem')!r:8}  {fmt(v)}  Qtd={r.get('Qtd')}")
        print(f"  TOTAL: {fmt(total)}")

        await asyncio.sleep(3)

        # 2. Testar fórmulas de despesa para os 3 meses
        formulas = [
            ("CCPAG + BAXP/EXTR",
             lambda m: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTR\"}},'data'[ano_mes_caixa]=\"{m}\")"),
            ("CCPAG + BAXP/EXTR/TRAR",
             lambda m: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTR\",\"TRAR\"}},'data'[ano_mes_caixa]=\"{m}\")"),
            ("cNatureza=P + ano_mes_caixa (tudo)",
             lambda m: f"CALCULATE(SUM('data'[despesas]),'data'[cNatureza]=\"P\",'data'[ano_mes_caixa]=\"{m}\")"),
            ("CCPAG all cOrigem",
             lambda m: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[ano_mes_caixa]=\"{m}\")"),
            ("cNatureza=P + BAXP/EXTR (qualquer cGrupo)",
             lambda m: f"CALCULATE(SUM('data'[despesas]),'data'[cNatureza]=\"P\",'data'[cOrigem] IN {{\"BAXP\",\"EXTR\"}},'data'[ano_mes_caixa]=\"{m}\")"),
            ("cNatureza=P + cOrigem IN BAXP/EXTR/TRAR",
             lambda m: f"CALCULATE(SUM('data'[despesas]),'data'[cNatureza]=\"P\",'data'[cOrigem] IN {{\"BAXP\",\"EXTR\",\"TRAR\"}},'data'[ano_mes_caixa]=\"{m}\")"),
        ]

        for label, fn in formulas:
            await asyncio.sleep(2)
            vals = []
            for mes, tgt in [("janeiro2025", T_JAN), ("maio2025", T_MAI), ("outubro2025", T_OUT)]:
                rows = await q(client, f"EVALUATE ROW(\"v\", {fn(mes)})")
                v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
                vals.append((v, tgt))
                await asyncio.sleep(1)
            hits = sum(1 for v,t in vals if isinstance(v,(int,float)) and abs(float(v)-t)<1)
            star = "★★★" if hits==3 else ("★★" if hits==2 else ("★" if hits==1 else ""))
            print(f"\n{star} [{label}]")
            for (v, t), m in zip(vals, ["Jan","Mai","Out"]):
                print(f"  {m}: {fmt(v)}{chk(v,t)}")

    finally:
        await client.close()

asyncio.run(main())
