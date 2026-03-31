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
        # 1. nValPago vs receita para BAXR jan
        print("=== nValPago vs receita BAXR jan (CONTA_CORRENTE_REC) ===")
        rows = await q(client, """
EVALUATE
ROW(
    "SumReceita",   CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumNValPago",  CALCULATE(SUM('data'[nValPago]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumNValMovCC", CALCULATE(SUM('data'[nValorMovCC]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 2. nValPago de CONTA_A_RECEBER BAXR jan
        print("\n=== nValPago de CONTA_A_RECEBER BAXR jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "SumReceita_CAR",   CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumNValPago_CAR",  CALCULATE(SUM('data'[nValPago]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumNValTitulo_CAR", CALCULATE(SUM('data'[nValorTitulo]),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025"),
    "Qtd_CAR", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_A_RECEBER",
        'data'[cOrigem]="BAXR",
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 3. Checar tabelas disponíveis no dataset (além de 'data')
        print("\n=== Outras tabelas (testar nomes comuns) ===")
        for tname in ["vendas", "Vendas", "faturamento", "Faturamento", "caixa", "Caixa",
                      "financeiro", "Financeiro", "pdv", "PDV", "resumo", "Resumo"]:
            rows = await q(client, f"EVALUATE TOPN(1, '{tname}')")
            if rows and "_erro" not in rows[0]:
                print(f"  TABELA EXISTE: [{tname}] — colunas: {list(rows[0].keys())[:5]}")
            else:
                print(f"  [{tname}]: não existe")
            await asyncio.sleep(0.3)

        await asyncio.sleep(2)

        # 4. Conta + cOrigem + cCodCateg para BAXR jan da FILIAL
        print("\n=== CONTA_CORRENTE_REC BAXR: nCodCC breakdown ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[nCodCC],
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
            print(f"  nCodCC={r.get('nCodCC')!r:15}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 5. Tentar cOrigem=BAXR APENAS de CONTA_A_RECEBER com nValPago (jan/mai/out)
        print("\n=== RECEITA nValPago CONTA_A_RECEBER BAXR (jan/mai/out) ===")
        await try3(client, "nValPago + CONTA_A_RECEBER + BAXR",
            lambda m,mn: f"CALCULATE(SUM('data'[nValPago]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[cOrigem]=\"BAXR\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 6. Tentar nValPago CONTA_CORRENTE_REC BAXR (jan/mai/out)
        print("\n=== RECEITA nValPago CONTA_CORRENTE_REC BAXR (jan/mai/out) ===")
        await try3(client, "nValPago + CONTA_CORRENTE_REC + BAXR",
            lambda m,mn: f"CALCULATE(SUM('data'[nValPago]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem]=\"BAXR\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 7. cStatus breakdown CONTA_A_RECEBER jan
        print("\n=== cStatus breakdown CONTA_A_RECEBER jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cStatus],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_A_RECEBER"
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "nValPago_sum", SUM('data'[nValPago]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  cStatus={r.get('cStatus')!r:15}  receita={fmt(r.get('Receita'))}  nValPago={fmt(r.get('nValPago_sum'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 8. DESPESA: nValPago CONTA_A_PAGAR BAXP (jan/mai/out)
        print("\n=== DESPESA nValPago CONTA_A_PAGAR BAXP (jan/mai/out) ===")
        await try3(client, "nValPago + CONTA_A_PAGAR + BAXP",
            lambda m,mn: f"CALCULATE(SUM('data'[nValPago]),'data'[cGrupo]=\"CONTA_A_PAGAR\",'data'[cOrigem]=\"BAXP\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

        # 9. DESPESA nValPago CONTA_CORRENTE_PAG BAXP
        print("\n=== DESPESA nValPago CONTA_CORRENTE_PAG BAXP (jan/mai/out) ===")
        await try3(client, "nValPago + CONTA_CORRENTE_PAG + BAXP",
            lambda m,mn: f"CALCULATE(SUM('data'[nValPago]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem]=\"BAXP\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
