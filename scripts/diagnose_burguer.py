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
        return [{"_erro": str(e)[:120]}]

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
        v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else rows[0].get("_erro","ERR")
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
        # 1. cOrigem breakdown jan receita
        print("=== cOrigem breakdown RECEITA jan 2025 ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cOrigem],
    FILTER(ALL('data'),
        'data'[ano_mes_caixa] = "janeiro2025"
        && 'data'[cGrupo] = "CONTA_CORRENTE_REC"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  cOrigem={r.get('cOrigem')!r:10}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 2. Verificar se cat_idx e cc_idx existem
        print("\n=== Verificar cat_idx e cc_idx (jan, BAXR+EXTR) ===")
        rows = await q(client, """
EVALUATE
ROW(
    "TotalLinhas", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumReceita", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "nCodLanc_distintos", CALCULATE(DISTINCTCOUNT('data'[nCodLanc]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 3. cat_idx=1 filter
        print("\n=== RECEITA cat_idx=1 (jan/mai/out) ===")
        await try3(client, "receita caixa + cat_idx=1",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[cat_idx]=1,'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 4. cc_idx=1 filter
        print("\n=== RECEITA cc_idx=1 (jan/mai/out) ===")
        await try3(client, "receita caixa + cc_idx=1",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[cc_idx]=1,'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 5. Ratio: sum(receita)/nCodLanc — para entender multiplicador
        print("\n=== cat_idx breakdown jan BAXR+EXTR ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cat_idx],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [cat_idx] ASC""")
        for r in rows:
            print(f"  cat_idx={r.get('cat_idx')}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 6. cc_idx breakdown
        print("\n=== cc_idx breakdown jan BAXR+EXTR ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cc_idx],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [cc_idx] ASC""")
        for r in rows:
            print(f"  cc_idx={r.get('cc_idx')}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── DESPESA ──────────────────────────────────────────────────
        print("\n" + "="*60)
        print("DESPESA CAIXA")
        print("="*60)

        # 7. cOrigem breakdown despesa
        print("\n=== cOrigem breakdown DESPESA jan 2025 ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cOrigem],
    FILTER(ALL('data'),
        'data'[ano_mes_caixa] = "janeiro2025"
        && 'data'[cGrupo] = "CONTA_CORRENTE_PAG"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Despesa] DESC""")
        for r in rows:
            print(f"  cOrigem={r.get('cOrigem')!r:10}  {fmt(r.get('Despesa'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 8. cat_idx=1 despesa
        print("\n=== DESPESA cat_idx=1 (jan/mai/out) ===")
        await try3(client, "despesa caixa + cat_idx=1",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[cat_idx]=1,'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

        # 9. cc_idx=1 despesa
        print("\n=== DESPESA cc_idx=1 (jan/mai/out) ===")
        await try3(client, "despesa caixa + cc_idx=1",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[cc_idx]=1,'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
