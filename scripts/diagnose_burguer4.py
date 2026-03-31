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
        # 1. Valor único vs receita breakdown
        print("=== [Valor único] vs [receita] BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "SumReceita",   CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumValorUnico", CALCULATE(SUM('data'[Valor único]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumValorUnico2", CALCULATE(SUM('data'[valor_unico_2]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumLiquido",   CALCULATE(SUM('data'[liquido]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "TotalLinhas",  CALCULATE(COUNTROWS('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 2. Teste [Valor único] como fórmula de caixa
        print("\n=== RECEITA SUM(Valor único) BAXR+EXTR (jan/mai/out) ===")
        await try3(client, "SUM(Valor único) + BAXR+EXTR",
            lambda m,mn: f"CALCULATE(SUM('data'[Valor único]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 3. Teste [valor_unico_2]
        print("\n=== RECEITA SUM(valor_unico_2) BAXR+EXTR (jan/mai/out) ===")
        await try3(client, "SUM(valor_unico_2) + BAXR+EXTR",
            lambda m,mn: f"CALCULATE(SUM('data'[valor_unico_2]),'data'[cGrupo]=\"CONTA_CORRENTE_REC\",'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 4. Conta breakdown BAXR jan (quantas filiais?)
        print("\n=== Conta breakdown BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Conta],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "ValorUnico", SUM('data'[Valor único]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  Conta={r.get('Conta')!r:25}  receita={fmt(r.get('Receita'))}  ValorUnico={fmt(r.get('ValorUnico'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 5. É Transferência breakdown
        print("\n=== É Transferência breakdown BAXR jan ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[É Transferência],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {"BAXR","EXTR"}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "ValorUnico", SUM('data'[Valor único]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  É Transf={r.get('É Transferência')}  receita={fmt(r.get('Receita'))}  ValorUnico={fmt(r.get('ValorUnico'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 6. nDistrPercentual breakdown (rateio: NULL vs non-null)
        print("\n=== nDistrPercentual IS NULL vs não-null BAXR jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "ComRateio_receita", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nDistrPercentual] <> BLANK()),
    "SemRateio_receita", CALCULATE(SUM('data'[receita]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nDistrPercentual] = BLANK()),
    "ComRateio_ValUniq", CALCULATE(SUM('data'[Valor único]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nDistrPercentual] <> BLANK()),
    "SemRateio_ValUniq", CALCULATE(SUM('data'[Valor único]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025",
        'data'[nDistrPercentual] = BLANK())
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # ── DESPESA ──────────────────────────────────────────────────
        print("\n" + "="*60)

        # 7. Valor único despesa
        print("\n=== [Valor único] vs [despesas] BAXP+EXTP jan ===")
        rows = await q(client, """
EVALUATE
ROW(
    "SumDespesas",   CALCULATE(SUM('data'[despesas]),
        'data'[cGrupo]="CONTA_CORRENTE_PAG",
        'data'[cOrigem] IN {"BAXP","EXTP"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumValorUnico", CALCULATE(SUM('data'[Valor único]),
        'data'[cGrupo]="CONTA_CORRENTE_PAG",
        'data'[cOrigem] IN {"BAXP","EXTP"},
        'data'[ano_mes_caixa]="janeiro2025"),
    "SumValorUnico2", CALCULATE(SUM('data'[valor_unico_2]),
        'data'[cGrupo]="CONTA_CORRENTE_PAG",
        'data'[cOrigem] IN {"BAXP","EXTP"},
        'data'[ano_mes_caixa]="janeiro2025")
)""")
        for k,v in (rows[0] if rows and "_erro" not in rows[0] else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 8. SUM(Valor único) despesa
        print("\n=== DESPESA SUM(Valor único) BAXP+EXTP (jan/mai/out) ===")
        await try3(client, "SUM(Valor único) + BAXP+EXTP",
            lambda m,mn: f"CALCULATE(SUM('data'[Valor único]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

        # 9. SUM(valor_unico_2) despesa
        print("\n=== DESPESA SUM(valor_unico_2) BAXP+EXTP (jan/mai/out) ===")
        await try3(client, "SUM(valor_unico_2) + BAXP+EXTP",
            lambda m,mn: f"CALCULATE(SUM('data'[valor_unico_2]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
