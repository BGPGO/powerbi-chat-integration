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
        # 1. app_key breakdown BAXR jan — múltiplas empresas?
        print("=== app_key breakdown BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key], 'data'[Conta],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        print(f"  {'app_key':20}  {'Conta':25}  {'Receita':20}  Qtd")
        for r in rows:
            print(f"  {str(r.get('app_key')):20}  {str(r.get('Conta')):25}  {fmt(r.get('Receita'))}  {r.get('Qtd')}")

        await asyncio.sleep(3)

        # 2. app_key distinct total no dataset
        print("\n=== Todos os app_key no dataset ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key], 'data'[Conta],
    "TotalReceita", SUM('data'[receita]),
    "TotalDespesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [TotalReceita] DESC""")
        for r in rows:
            print(f"  app_key={str(r.get('app_key')):20}  Conta={str(r.get('Conta')):25}  rec={fmt(r.get('TotalReceita'))}  dsp={fmt(r.get('TotalDespesa'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 3. Testar por app_key específico (jan/mai/out) — app_key da amostra = '2470856862474'
        print("\n=== RECEITA BAXR+EXTR filtrada por cada app_key (jan) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key],
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
            print(f"  app_key={str(r.get('app_key')):20}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_CX[1])}")

        await asyncio.sleep(3)

        # 4. DESPESA BAXP+EXTP por app_key (jan)
        print("\n=== DESPESA BAXP+EXTP por app_key (jan) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key],
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
            print(f"  app_key={str(r.get('app_key')):20}  {fmt(v)}  Qtd={r.get('Qtd')}  {chk(v, T_DSP[1])}")

        await asyncio.sleep(3)

        # 5. Se app_key único identificar empresa, testar jan/mai/out com cada um
        # Pega os app_keys distintos primeiro
        print("\n=== RECEITA BAXR+EXTR por app_key (jan/mai/out) ===")
        rows_ak = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key],
    FILTER(ALL('data'), 'data'[cGrupo]="CONTA_CORRENTE_REC"),
    "Qtd", COUNTROWS('data')
)""")
        app_keys = [r.get('app_key') for r in rows_ak if r.get('app_key') and "_erro" not in r]

        for ak in app_keys[:5]:  # test up to 5 app_keys
            print(f"\n  --- app_key={ak} ---")
            await try3(client, f"BAXR+EXTR app_key={ak}",
                lambda m,mn,ak=ak: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[app_key]=\"{ak}\",'data'[ano_mes_caixa]=\"{mn}2025\")",
                T_CX)

        print("\n=== DESPESA BAXP+EXTP por app_key (jan/mai/out) ===")
        rows_ak = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key],
    FILTER(ALL('data'), 'data'[cGrupo]="CONTA_CORRENTE_PAG"),
    "Qtd", COUNTROWS('data')
)""")
        app_keys_pag = [r.get('app_key') for r in rows_ak if r.get('app_key') and "_erro" not in r]

        for ak in app_keys_pag[:5]:
            print(f"\n  --- app_key={ak} ---")
            await try3(client, f"BAXP+EXTP app_key={ak}",
                lambda m,mn,ak=ak: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[app_key]=\"{ak}\",'data'[ano_mes_caixa]=\"{mn}2025\")",
                T_DSP)

    finally:
        await client.close()

asyncio.run(main())
