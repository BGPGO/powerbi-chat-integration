# -*- coding: utf-8 -*-
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"
DID = "eeaa8d72-7549-4470-8a1d-62a5590666c1"

T_CX  = {1: 378761.84, 5: 341099.80, 10: 363740.16}
T_DSP = {1: 470739.36, 5: 303566.21, 10: 291440.52}

async def q(client, dax):
    try:
        r = await client.execute_query(DID, dax, WORKSPACE_ID)
        return r.get("rows", [])
    except Exception as e:
        return [{"_erro": str(e)[:200]}]

def fmt(v):
    try: return f"R$ {float(v):>14,.2f}"
    except: return str(v)

def chk(v, t):
    try:
        d = float(v or 0) - t
        return "  ★★★ MATCH!" if abs(d) < 1 else f"  diff={d:+,.2f}"
    except: return ""

async def try3(client, label, fn, targets):
    vals = []
    for m, mn in [(1,"janeiro"),(5,"maio"),(10,"outubro")]:
        rows = await q(client, f"EVALUATE ROW(\"v\", {fn(m,mn)})")
        v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else rows[0].get("_erro","ERR")[:80]
        vals.append((m, v))
        await asyncio.sleep(1)
    hits = sum(1 for m,v in vals if isinstance(v,(int,float)) and abs(float(v)-targets[m])<1)
    star = "★★★" if hits==3 else ("★★" if hits==2 else ("★" if hits==1 else "✗"))
    print(f"\n{star} [{label}]")
    for m,v in vals:
        mname = {1:"Jan",5:"Mai",10:"Out"}[m]
        print(f"  {mname}: {fmt(v)}{chk(v,targets[m])}")
    await asyncio.sleep(2)

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)
    try:
        # 1. nCodTitulo distinct vs total — CONTA_A_RECEBER jan
        print("=== nCodTitulo DISTINCT vs TOTAL — CONTA_A_RECEBER jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "TotalRows", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025"),
    "nCodTitulo_Distinct", CALCULATE(DISTINCTCOUNT('data'[nCodTitulo]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025"),
    "nCodTitRepet_Distinct", CALCULATE(DISTINCTCOUNT('data'[nCodTitRepet]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumReceita_Total", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumReceita_nCodTitEqRepet", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodTitulo] = 'data'[nCodTitRepet])
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 2. CONTA_A_RECEBER + nCodTitulo=nCodTitRepet (jan/mai/out)
        print("\n=== RECEITA CONTA_A_RECEBER + nCodTitulo=nCodTitRepet (jan/mai/out) ===")
        await try3(client, "CONTA_A_RECEBER + nCodTitulo=nCodTitRepet",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[nCodTitulo]='data'[nCodTitRepet],'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 3. CONTA_A_RECEBER + nCodTitulo<>nCodTitRepet  (repeats only)
        print("\n=== CONTA_A_RECEBER: nCodTitulo<>nCodTitRepet (repeats) jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "SumReceita_repeats", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodTitulo] <> 'data'[nCodTitRepet]),
    "Qtd_repeats", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodTitulo] <> 'data'[nCodTitRepet]),
    "SumReceita_orig", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodTitulo] = 'data'[nCodTitRepet]),
    "Qtd_orig", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodTitulo] = 'data'[nCodTitRepet])
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 4. nCodMovCCRepet para BAXR — ver se há distinção similar
        print("\n=== BAXR: nCodMovCC vs nCodMovCCRepet (jan) ===")
        rows = await q(client, """
EVALUATE
ROW(
    "nCodMovCC_eq_Repet", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodMovCC] = 'data'[nCodMovCCRepet]),
    "nCodMovCC_ne_Repet", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodMovCC] <> 'data'[nCodMovCCRepet]),
    "Qtd_eq", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodMovCC] = 'data'[nCodMovCCRepet]),
    "Qtd_ne", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nCodMovCC] <> 'data'[nCodMovCCRepet])
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 5. BAXR onde nCodMovCC <> nCodMovCCRepet (jan/mai/out)
        print("\n=== RECEITA BAXR onde nCodMovCC<>nCodMovCCRepet (jan/mai/out) ===")
        await try3(client, "BAXR + nCodMovCC<>nCodMovCCRepet",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem]=\"BAXR\",'data'[nCodMovCC]<>'data'[nCodMovCCRepet],'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 6. BAXR onde nCodMovCC = nCodMovCCRepet (jan/mai/out)
        print("\n=== RECEITA BAXR onde nCodMovCC=nCodMovCCRepet (jan/mai/out) ===")
        await try3(client, "BAXR + nCodMovCC=nCodMovCCRepet",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem]=\"BAXR\",'data'[nCodMovCC]='data'[nCodMovCCRepet],'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 7. DESPESA: nCodMovCC vs nCodMovCCRepet para BAXP
        print("\n=== DESPESA BAXP onde nCodMovCC<>nCodMovCCRepet (jan/mai/out) ===")
        await try3(client, "BAXP + nCodMovCC<>nCodMovCCRepet",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem]=\"BAXP\",'data'[nCodMovCC]<>'data'[nCodMovCCRepet],'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

        # 8. DESPESA BAXP + EXTP onde nCodMovCC<>nCodMovCCRepet (jan/mai/out)
        print("\n=== DESPESA BAXP+EXTP onde nCodMovCC<>nCodMovCCRepet (jan/mai/out) ===")
        await try3(client, "BAXP+EXTP + nCodMovCC<>nCodMovCCRepet",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[nCodMovCC]<>'data'[nCodMovCCRepet],'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
