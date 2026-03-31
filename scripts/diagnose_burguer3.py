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
        # 1. Ver colunas disponíveis (1 linha da tabela)
        print("=== Schema (TOPN 1) ===")
        rows = await q(client, "EVALUATE TOPN(1, 'data')")
        if rows and "_erro" not in rows[0]:
            for k, v in rows[0].items():
                print(f"  [{k}] = {v!r}")
        else:
            print(f"  {rows}")

        await asyncio.sleep(3)

        # 2. Empresa/filial breakdown BAXR jan
        print("\n=== Empresa/cEmpresa breakdown BAXR jan ===")
        for col in ["cEmpresa", "nCodEmp", "Empresa", "empresa"]:
            rows = await q(client, f"""
EVALUATE
SUMMARIZECOLUMNS('data'[{col}],
    FILTER(ALL('data'),
        'data'[cGrupo]="CONTA_CORRENTE_REC"
        && 'data'[cOrigem] IN {{"BAXR","EXTR"}}
        && 'data'[ano_mes_caixa]="janeiro2025"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
            if rows and "_erro" not in rows[0]:
                print(f"  Coluna [{col}] EXISTE:")
                for r in rows:
                    print(f"    {r.get(col)!r}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")
                break
            else:
                print(f"  [{col}]: não existe")
            await asyncio.sleep(1)

        await asyncio.sleep(2)

        # 3. DataPagamento filter (sem ano_mes_caixa)
        print("\n=== RECEITA: DataPagamento filter (sem ano_mes_caixa) ===")
        await try3(client, "CONTA_A_RECEBER + PAGO + DataPagamento",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[cStatus] IN {{\"PAGO\",\"RECEBIDO\"}},'data'[DataPagamento]>=DATE(2025,{m},1),'data'[DataPagamento]<=DATE(2025,{m},31))",
            T_CX)

        # 4. liquido column test
        print("\n=== SUM(liquido) BAXR+EXTR jan ===")
        rows = await q(client, """
EVALUATE ROW("v",
    CALCULATE(SUM('data'[liquido]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {"BAXR","EXTR"},
        'data'[ano_mes_caixa]="janeiro2025"))""")
        v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else rows[0].get("_erro","ERR")
        print(f"  liquido BAXR: {fmt(v)}{chk(v, T_CX[1])}")

        await asyncio.sleep(2)

        # 5. Verificar se existe coluna ID única alternativa
        print("\n=== Colunas ID possíveis (DISTINCTCOUNT) jan BAXR ===")
        for col in ["nCodLanc", "RowID", "nCodTit", "nCodCC", "ID", "id", "nCodPag", "nCodRec"]:
            rows = await q(client, f"""
EVALUATE ROW("v",
    CALCULATE(DISTINCTCOUNT('data'[{col}]),
        'data'[cGrupo]="CONTA_CORRENTE_REC",
        'data'[cOrigem] IN {{"BAXR","EXTR"}},
        'data'[ano_mes_caixa]="janeiro2025"))""")
            if rows and "_erro" not in rows[0]:
                v = list(rows[0].values())[0]
                print(f"  [{col}]: EXISTE → distinct={v}")
            else:
                print(f"  [{col}]: não existe")
            await asyncio.sleep(0.5)

        await asyncio.sleep(2)

        # 6. Tentar cOrigem=BAXR apenas (sem EXTR) com dedup via AVG
        print("\n=== CONTA_A_RECEBER sem filtro de status + ano_mes_caixa ===")
        await try3(client, "CONTA_A_RECEBER + ano_mes_caixa (todos status)",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[cGrupo]=\"CONTA_A_RECEBER\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_CX)

        # 7. DESPESA: DataPagamento filter
        print("\n=== DESPESA: DataPagamento filter ===")
        await try3(client, "CONTA_A_PAGAR + PAGO + DataPagamento",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_A_PAGAR\",'data'[cStatus] IN {{\"PAGO\",\"LIQUIDADO\"}},'data'[DataPagamento]>=DATE(2025,{m},1),'data'[DataPagamento]<=DATE(2025,{m},31))",
            T_DSP)

        # 8. CONTA_CORRENTE_PAG + DataPagamento (sem cOrigem)
        await try3(client, "CONTA_CORRENTE_PAG + ano_mes_caixa (todos cOrigem excl TRAP)",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[cGrupo]=\"CONTA_CORRENTE_PAG\",'data'[cOrigem]<>\"TRAP\",'data'[ano_mes_caixa]=\"{mn}2025\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
