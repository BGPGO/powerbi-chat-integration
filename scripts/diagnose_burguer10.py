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
        # 1. cOperacao breakdown BAXR jan
        print("=== cOperacao breakdown BAXR+EXTR jan (CONTA_CORRENTE_REC) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cOperacao],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            v = r.get('Receita') or 0
            print(f"  cOperacao={r.get('cOperacao')!r:10}  {fmt(v)}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 2. cOperacao CONTA_A_RECEBER jan
        print("\n=== cOperacao breakdown CONTA_A_RECEBER jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cOperacao],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows[:10]:
            v = r.get('Receita') or 0
            print(f"  cOperacao={r.get('cOperacao')!r:10}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")

        await asyncio.sleep(3)

        # 3. cTipo breakdown BAXR jan
        print("\n=== cTipo breakdown BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cTipo],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows[:10]:
            v = r.get('Receita') or 0
            print(f"  cTipo={r.get('cTipo')!r:10}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")

        await asyncio.sleep(3)

        # 4. cTipo CONTA_A_RECEBER jan
        print("\n=== cTipo breakdown CONTA_A_RECEBER jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cTipo],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows[:10]:
            v = r.get('Receita') or 0
            print(f"  cTipo={r.get('cTipo')!r:10}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")

        await asyncio.sleep(3)

        # 5. Filial CONTA_A_RECEBER nCodTitulo=nCodTitRepet (originals)
        print("\n=== Filial CONTA_A_RECEBER nCodTitulo=nCodTitRepet (jan/mai/out) ===")
        await try3(client, "Filial + CONTA_A_RECEBER + nCodTitulo=nCodTitRepet",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[app_key]=\"2470956529041\",'data'[nCodTitulo]='data'[nCodTitRepet],'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 6. TODA a receita de ANO_MES_CAIXA (todos cGrupo) jan
        print("\n=== TODA receita cNatureza=R + Previsto=Realizado + jan (jan/mai/out) ===")
        await try3(client, "cNatureza=R + Realizado + ano_mes_caixa (todos grupos)",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cNatureza]=\"R\",'data'[Previsto/realizado]=\"Realizado\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 7. Tentar Filial BAXR + cTipo≠99999 ou alguma cTipo específica
        print("\n=== BAXR Filial cTipo=BOL (jan/mai/out) ===")
        await try3(client, "BAXR + Filial + cTipo=BOL",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem]=\"BAXR\",'data'[app_key]=\"2470956529041\",'data'[cTipo]=\"BOL\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 8. DESPESA: nCodTitulo=nCodTitRepet CONTA_A_PAGAR
        print("\n=== DESPESA CONTA_A_PAGAR nCodTitulo=nCodTitRepet (jan/mai/out) ===")
        await try3(client, "CONTA_A_PAGAR + nCodTitulo=nCodTitRepet",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_A_PAGAR\",'data'[nCodTitulo]='data'[nCodTitRepet],'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

        # 9. Total de receita em todo o dataset com Previsto=Realizado jan
        print("\n=== SUM total receita (ano_mes_caixa, Realizado, todos grupos) jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cGrupo],
    FILTER(ALL('data'),
        'data'[Previsto/realizado]="Realizado"
        && 'data'[cNatureza]="R"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        total = 0
        for r in rows:
            v = r.get('Receita') or 0
            total += float(v)
            print(f"  cGrupo={r.get('cGrupo')!r:35}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")
        print(f"  TOTAL: {fmt(total)}")

    finally:
        await client.close()

asyncio.run(main())
