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
        # 1. CONTA_A_RECEBER por app_key jan
        print("=== CONTA_A_RECEBER por app_key (jan) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key], 'data'[Conta],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "nValPago", SUM('data'[nValPago]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  app_key={str(r.get('app_key')):20}  Conta={str(r.get('Conta')):25}  rec={fmt(r.get('Receita'))}  nValPago={fmt(r.get('nValPago'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 2. nValAberto para CONTA_A_RECEBER
        print("\n=== nValAberto para CONTA_A_RECEBER jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "SumReceita", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumNValAberto", CALCULATE(SUM('data'[nValAberto]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumNValTitulo", CALCULATE(SUM('data'[nValorTitulo]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 3. TOPN 3 de CONTA_A_RECEBER para ver estrutura de uma linha
        print("\n=== CONTA_A_RECEBER: sample row (TOPN 1) ===")
        rows = await q(client, """
EVALUATE
TOPN(1,
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[ano_mes_caixa]="janeiro2025"
    )
)""")
        if rows and "_erro" not in rows[0]:
            for k, v in rows[0].items():
                if v is not None:
                    print(f"  [{k}] = {v!r}")
        else:
            print(f"  {rows}")

        await asyncio.sleep(3)

        # 4. Tentar SUM(nValLiquido) BAXR jan
        print("\n=== nValLiquido BAXR+EXTR (jan/mai/out) ===")
        await try3(client, "nValLiquido CONTA_CORRENTE_REC BAXR",
            lambda m,mn: f"CALCULATE(SUM('data'[nValLiquido]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 5. Faturamento por app_key — ver qual app_key produz 1,665,985
        print("\n=== FATURAMENTO por app_key (jan) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[app_key], 'data'[Conta],
    FILTER(ALL('data'),
        'data'[cNatureza]="R"
        && 'data'[cStatus]<>"CANCELADO"
        && 'data'[ano_mes_competencia]="janeiro2025"
    ),
    "Fat", SUM('data'[receita competencia]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Fat] DESC""")
        total_fat = 0
        for r in rows:
            v = r.get('Fat') or 0
            total_fat += float(v)
            print(f"  app_key={str(r.get('app_key')):20}  Conta={str(r.get('Conta')):25}  {fmt(v)}  Qtd={r.get('Qtd')}")
        print(f"  TOTAL: {fmt(total_fat)}")

        await asyncio.sleep(3)

        # 6. cOrigem breakdown CONTA_A_RECEBER jan
        print("\n=== cOrigem breakdown CONTA_A_RECEBER jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cOrigem], 'data'[cStatus],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  cOrigem={r.get('cOrigem')!r:10}  cStatus={r.get('cStatus')!r:12}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 7. Testar: BAXR+EXTR + cNatureza=R + Filial (app_key 2470956529041) (jan/mai/out)
        print("\n=== RECEITA Filial only BAXR+EXTR (jan/mai/out) ===")
        await try3(client, "BAXR+EXTR + Filial app_key",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[app_key]=\"2470956529041\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 8. Testar CONTA_A_RECEBER + Filial (jan/mai/out)
        print("\n=== RECEITA Filial CONTA_A_RECEBER (jan/mai/out) ===")
        await try3(client, "CONTA_A_RECEBER + Filial app_key",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[app_key]=\"2470956529041\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 9. Testar CONTA_A_RECEBER + Filial nValPago (jan/mai/out)
        print("\n=== nValPago Filial CONTA_A_RECEBER (jan/mai/out) ===")
        await try3(client, "nValPago + CONTA_A_RECEBER + Filial",
            lambda m,mn: f"CALCULATE(SUM('data'[nValPago]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[app_key]=\"2470956529041\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 10. Despesa Filial BAXP+EXTP
        print("\n=== DESPESA Filial BAXP+EXTP (jan/mai/out) ===")
        await try3(client, "BAXP+EXTP + Filial app_key",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[app_key]=\"2470956529041\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

        # 11. Despesa Matriz BAXP+EXTP
        print("\n=== DESPESA Matriz BAXP+EXTP (jan/mai/out) ===")
        await try3(client, "BAXP+EXTP + Matriz app_key",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[app_key]=\"2470856862474\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
