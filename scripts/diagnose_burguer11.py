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
        # 1. CONTA_A_RECEBER por nCodCC jan
        print("=== CONTA_A_RECEBER nCodCC breakdown jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[nCodCC], 'data'[Conta],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            v = r.get('Receita') or 0
            print(f"  nCodCC={r.get('nCodCC')!r:15}  Conta={str(r.get('Conta')):25}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")

        await asyncio.sleep(3)

        # 2. CONTA_A_RECEBER nCodCC=5037024503 (jan/mai/out)
        print("\n=== CONTA_A_RECEBER nCodCC=5037024503 (jan/mai/out) ===")
        await try3(client, "CONTA_A_RECEBER + nCodCC=5037024503",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[nCodCC]=\"5037024503\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 3. CONTA_CORRENTE_REC por nCodCC (jan/mai/out) para cada conta pequena
        print("\n=== BAXR: soma contas menores (excl top 2) jan ===")
        rows = await q(client, """
EVALUATE
ROW("v",
    CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025",
        NOT 'data'[nCodCC] IN {"5774708301","5037024503"}
    )
)""")
        v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
        print(f"  Contas menores (excl top 2): {fmt(v)}{chk(v, T_CX[1])}")

        await asyncio.sleep(2)

        # 4. Ver se Filial tem entradas em CONTA_A_RECEBER onde nCodCC ≠ 5037024503
        print("\n=== Filial CONTA_A_RECEBER por nCodCC jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[nCodCC],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[app_key]="2470956529041"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            v = r.get('Receita') or 0
            print(f"  nCodCC={r.get('nCodCC')!r:15}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")

        await asyncio.sleep(3)

        # 5. Tentar fórmula com Filial + CONTA_A_RECEBER + nCodCC=5037024503 (jan/mai/out)
        print("\n=== Filial + CONTA_A_RECEBER + nCodCC=5037024503 (jan/mai/out) ===")
        await try3(client, "Filial + CONTA_A_RECEBER + nCodCC=5037024503",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[app_key]=\"2470956529041\",'data'[nCodCC]=\"5037024503\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 6. BAXR Filial (5037024503) cTipo breakdown
        print("\n=== Filial BAXR nCodCC=5037024503 cTipo breakdown jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cTipo],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem]="BAXR"
        && 'data'[nCodCC]="5037024503"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            v = r.get('Receita') or 0
            print(f"  cTipo={r.get('cTipo')!r:10}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")

        await asyncio.sleep(3)

        # 7. Filial + BAXR + nCodCC=5037024503 excl BOL (jan/mai/out)
        print("\n=== Filial BAXR nCodCC=5037024503 excl BOL (jan/mai/out) ===")
        await try3(client, "Filial BAXR 5037024503 excl BOL",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem]=\"BAXR\",'data'[nCodCC]=\"5037024503\",'data'[cTipo]<>\"BOL\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 8. DESPESA por nCodCC (CONTA_CORRENTE_PAG BAXP+EXTP) jan
        print("\n=== DESPESA BAXP+EXTP por nCodCC jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[nCodCC], 'data'[Conta],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_PAG"
        && 'data'[cOrigem] IN {"BAXP","EXTP"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Despesa] DESC""")
        for r in rows:
            v = r.get('Despesa') or 0
            print(f"  nCodCC={r.get('nCodCC')!r:15}  Conta={str(r.get('Conta')):25}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_DSP[1])}")

    finally:
        await client.close()

asyncio.run(main())
