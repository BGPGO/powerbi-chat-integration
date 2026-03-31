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
        # 1. caregoria breakdown BAXR jan
        print("=== caregoria breakdown BAXR+EXTR jan (receita) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[caregoria],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
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
            print(f"  {r.get('caregoria')!r:55}  {fmt(v)}  Qtd={r.get('Qtd')}")
        print(f"  TOTAL: {fmt(total)}")

        await asyncio.sleep(3)

        # 2. dre_nat breakdown BAXR jan
        print("\n=== dre_nat breakdown BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[dre_nat],
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
            print(f"  {r.get('dre_nat')!r:50}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 3. cCodCateg breakdown BAXR jan
        print("\n=== cCodCateg breakdown BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cCodCateg],
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
            print(f"  cCodCateg={r.get('cCodCateg')!r:20}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 4. Previsto/realizado breakdown BAXR jan
        print("\n=== Previsto/realizado breakdown BAXR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Previsto/realizado],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  Previsto/realizado={r.get('Previsto/realizado')!r}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 5. Tentar CONTA_A_RECEBER com categoria "Clientes" apenas
        print("\n=== nCodMovCC distinct count BAXR jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "TotalRows", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "nCodMovCC_distinct", CALCULATE(DISTINCTCOUNT('data'[nCodMovCC]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "nCodMovCCRepet_distinct", CALCULATE(DISTINCTCOUNT('data'[nCodMovCCRepet]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {v}")

        await asyncio.sleep(3)

        # 6. Receita Bruta de Vendas only (dre_nat)
        print("\n=== RECEITA: dre_nat=Receita Bruta de Vendas (jan/mai/out) ===")
        await try3(client, "BAXR+EXTR + dre_nat=Receita Bruta de Vendas",
            lambda m,mn: (
                f"CALCULATE(SUM('data'[receita]),"
                f"'data'[cGrupo]=\"CONTA_CORRENTE_REC\","
                f"'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},"
                f"'data'[dre_nat]=\"Receita Bruta de Vendas\","
                f"'data'[ano_mes_caixa]=\"{mn}2025\")"
            ),
            T_CX)

        # 7. Receita filtrando caregoria que começa com "Clientes"
        print("\n=== RECEITA: CONTAINSSTRING(caregoria,'Clientes') (jan/mai/out) ===")
        await try3(client, "BAXR+EXTR + caregoria contém Clientes",
            lambda m,mn: (
                f"CALCULATE(SUM('data'[receita]),"
                f"'data'[cGrupo]=\"CONTA_CORRENTE_REC\","
                f"'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},"
                f"CONTAINSSTRING('data'[caregoria],\"Clientes\"),"
                f"'data'[ano_mes_caixa]=\"{mn}2025\")"
            ),
            T_CX)

        # ── DESPESA ──────────────────────────────────────────────────
        print("\n" + "="*60)

        # 8. caregoria breakdown DESPESA jan
        print("\n=== caregoria breakdown BAXP+EXTP jan (despesas) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[caregoria],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_PAG"
        && 'data'[cOrigem] IN {"BAXP","EXTP"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Despesa] DESC""")
        for r in rows[:15]:  # top 15
            print(f"  {r.get('caregoria')!r:55}  {fmt(r.get('Despesa'))}  Qtd={r.get('Qtd')}")
        if len(rows) > 15:
            print(f"  ... ({len(rows)-15} categorias adicionais)")

        await asyncio.sleep(3)

        # 9. dre_nat breakdown DESPESA jan
        print("\n=== dre_nat breakdown BAXP+EXTP jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[dre_nat],
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
            print(f"  {r.get('dre_nat')!r:55}  {fmt(r.get('Despesa'))}  Qtd={r.get('Qtd')}")

    finally:
        await client.close()

asyncio.run(main())
