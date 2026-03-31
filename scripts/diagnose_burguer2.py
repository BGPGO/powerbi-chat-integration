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
        # 1. nCodLanc distinct vs total rows (ver rateio)
        print("=== nCodLanc duplicatas (BAXR, jan) ===")
        rows = await q(client, """
EVALUATE
ROW(
    "TotalLinhas", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "nCodLanc_distintos", CALCULATE(DISTINCTCOUNT('data'[nCodLanc]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumReceita", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumValorRS", CALCULATE(SUM('data'[Valor (R$)]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 2. Tentar SUM(Valor (R$)) ao invés de receita
        print("\n=== SUM(Valor (R$)) BAXR+EXTR (jan/mai/out) ===")
        await try3(client, "SUM(Valor(R$)) BAXR+EXTR",
            lambda m,mn: f"CALCULATE(SUM('data'[Valor (R$)]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 3. Tentar CONTA_A_RECEBER + cStatus=PAGO
        print("\n=== CONTA_A_RECEBER + cStatus PAGO/RECEBIDO (jan/mai/out) ===")
        await try3(client, "CONTA_A_RECEBER + PAGO/RECEBIDO",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[cStatus] IN {{\"PAGO\",\"RECEBIDO\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 4. cNatureza=R + cGrupo=CONTA_CORRENTE_REC + BAXR
        print("\n=== cNatureza=R + BAXR+EXTR (jan/mai/out) ===")
        await try3(client, "cNatureza=R + BAXR+EXTR",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cNatureza]=\"R\",'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 5. cGrupo breakdown (quais grupos existem?)
        print("\n=== cGrupo breakdown RECEITA jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cGrupo],
    FILTER(ALL('data'), 'data'[ano_mes_caixa]="janeiro2025"),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  cGrupo={r.get('cGrupo')!r:35}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 6. cNatureza breakdown dentro de BAXR (ver se tem "P" no CONTA_CORRENTE_REC)
        print("\n=== cNatureza breakdown BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cNatureza],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  cNatureza={r.get('cNatureza')!r}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 7. SUMX por nCodLanc (deduplicar manualmente)
        print("\n=== SUMX DISTINCT nCodLanc receita (jan/mai/out) ===")
        await try3(client, "SUMX distinct nCodLanc receita",
            lambda m,mn: (
                f"SUMX(SUMMARIZE(FILTER(ALL('data'),"
                f"'data'[cGrupo]=\"CONTA_CORRENTE_REC\""
                f"&&'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}}"
                f"&&'data'[ano_mes_caixa]=\"{mn}2025\"),"
                f"'data'[nCodLanc],'data'[receita]),'data'[receita])"
            ),
            T_CX)

        # ── DESPESA ──────────────────────────────────────────────────
        print("\n" + "="*60)

        # 8. nCodLanc distinct despesa
        print("\n=== nCodLanc duplicatas DESPESA (BAXP+EXTP, jan) ===")
        rows = await q(client, """
EVALUATE
ROW(
    "TotalLinhas", CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_CORRENTE_PAG",
        'data'[cOrigem] IN {"BAXP","EXTP"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "nCodLanc_distintos", CALCULATE(DISTINCTCOUNT('data'[nCodLanc]),
        'data'[cGrupo]="CONTA_CORRENTE_PAG",
        'data'[cOrigem] IN {"BAXP","EXTP"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumDespesa", CALCULATE(SUM('data'[despesas]),
        'data'[cGrupo]="CONTA_CORRENTE_PAG",
        'data'[cOrigem] IN {"BAXP","EXTP"},
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 9. cNatureza breakdown despesa BAXP+EXTP
        print("\n=== cNatureza breakdown BAXP+EXTP jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[cNatureza],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_PAG"
        && 'data'[cOrigem] IN {"BAXP","EXTP"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  cNatureza={r.get('cNatureza')!r}  {fmt(r.get('Despesa'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 10. cNatureza=P + BAXP+EXTP
        print("\n=== DESPESA cNatureza=P + BAXP+EXTP (jan/mai/out) ===")
        await try3(client, "cNatureza=P + BAXP+EXTP",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cNatureza]=\"P\",'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
