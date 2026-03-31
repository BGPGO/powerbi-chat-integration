# -*- coding: utf-8 -*-
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"
DID = "ca26e66f-6bbd-4273-9de7-9e13e720c839"

T_CX  = {1: 648260.10, 5: 577325.24, 10: 624249.13}
T_FAT = {1: 508526.06, 5: 615318.79, 10: 653345.74}
T_DSP = {1: 600031.69, 5: 519138.32, 10: 605520.65}

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

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)
    try:
        # 1. Checar se RowIDs se repetem (duplicatas por rateio)
        print("=== Duplicatas de RowID (jan 2025, receita, pago) ===")
        rows = await q(client, """
EVALUATE
ROW(
    "TotalLinhas", CALCULATE(COUNTROWS('data'),
        'data'[Receita/Despesa] = "Receita",
        'data'[Previsto/realizado] = "Pago",
        'data'[Ano_mes] = "2025janeiro"),
    "RowIDsDistintos", CALCULATE(DISTINCTCOUNT('data'[RowID]),
        'data'[Receita/Despesa] = "Receita",
        'data'[Previsto/realizado] = "Pago",
        'data'[Ano_mes] = "2025janeiro"),
    "SumReceita", CALCULATE(SUM('data'[receita]),
        'data'[Receita/Despesa] = "Receita",
        'data'[Previsto/realizado] = "Pago",
        'data'[Ano_mes] = "2025janeiro")
)""")
        for k,v in (rows[0] if rows else {}).items():
            print(f"  {k}: {fmt(v)}")

        await asyncio.sleep(3)

        # 2. FonteValor breakdown (CC vs outros)
        print("\n=== FonteValor breakdown (jan 2025, receita, pago) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[FonteValor],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Previsto/realizado] = "Pago"
        && 'data'[Ano_mes] = "2025janeiro"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data'),
    "RowIDs", DISTINCTCOUNT('data'[RowID])
)""")
        for r in rows:
            print(f"  FonteValor={r.get('FonteValor')!r}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}  RowIDs={r.get('RowIDs')}")

        await asyncio.sleep(3)

        # 3. Se FonteValor=CC é rateio — testar filtro FonteValor != CC
        print("\n=== RECEITA excluindo FonteValor=CC (jan/mai/out) ===")
        for m, mn, t in [(1,"janeiro",T_CX[1]),(5,"maio",T_CX[5]),(10,"outubro",T_CX[10])]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Receita/Despesa] = "Receita",
    'data'[Previsto/realizado] = "Pago",
    'data'[Ano_mes] = "2025{mn}",
    'data'[FonteValor] <> "CC"))""")
            v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
            mname = {1:"Jan",5:"Mai",10:"Out"}[m]
            print(f"  {mname}: {fmt(v)}{chk(v,t)}")

        await asyncio.sleep(3)

        # 4. cat_idx=1 filter (pegar só a linha principal)
        print("\n=== RECEITA com cat_idx=1 (jan/mai/out) ===")
        for m, mn, t in [(1,"janeiro",T_CX[1]),(5,"maio",T_CX[5]),(10,"outubro",T_CX[10])]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Receita/Despesa] = "Receita",
    'data'[Previsto/realizado] = "Pago",
    'data'[Ano_mes] = "2025{mn}",
    'data'[cat_idx] = 1))""")
            v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
            mname = {1:"Jan",5:"Mai",10:"Out"}[m]
            print(f"  {mname}: {fmt(v)}{chk(v,t)}")

        await asyncio.sleep(3)

        # 5. cc_idx=1 filter
        print("\n=== RECEITA com cc_idx=1 (jan/mai/out) ===")
        for m, mn, t in [(1,"janeiro",T_CX[1]),(5,"maio",T_CX[5]),(10,"outubro",T_CX[10])]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Receita/Despesa] = "Receita",
    'data'[Previsto/realizado] = "Pago",
    'data'[Ano_mes] = "2025{mn}",
    'data'[cc_idx] = 1))""")
            v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
            mname = {1:"Jan",5:"Mai",10:"Out"}[m]
            print(f"  {mname}: {fmt(v)}{chk(v,t)}")

        await asyncio.sleep(3)

        # 6. Valor (col distribuída) ao invés de receita
        print("\n=== SUM(Valor) receita + pago (jan/mai/out) ===")
        for m, mn, t in [(1,"janeiro",T_CX[1]),(5,"maio",T_CX[5]),(10,"outubro",T_CX[10])]:
            await asyncio.sleep(2)
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[Valor]),
    'data'[Receita/Despesa] = "Receita",
    'data'[Previsto/realizado] = "Pago",
    'data'[Ano_mes] = "2025{mn}"))""")
            v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
            mname = {1:"Jan",5:"Mai",10:"Out"}[m]
            print(f"  {mname}: {fmt(v)}{chk(v,t)}")

        await asyncio.sleep(3)

        # 7. Conta bancária breakdown (ver se alguma conta está duplicada ou é transferência)
        print("\n=== Conta bancária (jan 2025, receita, pago) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta bancária],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Previsto/realizado] = "Pago"
        && 'data'[Ano_mes] = "2025janeiro"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  {r.get('Conta bancária')!r:40}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 8. Faturamento — checar categoria que soma 22k em jan e 25k em out
        print("\n=== FATURAMENTO excesso por categoria (jan 2025 — excesso = 22.000,00) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Categoria 1],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Ano_mes competencia] = "2025janeiro"
    ),
    "Fat", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Fat] DESC""")
        for r in rows:
            print(f"  {r.get('Categoria 1')!r:45}  {fmt(r.get('Fat'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # 9. Faturamento sem Transferências (conta bancária ou categoria suspeita)
        print("\n=== FATURAMENTO excluindo Conta contabil suspeita (jan/mai/out) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS(
    'data'[Conta contabil],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Ano_mes competencia] = "2025janeiro"
    ),
    "Fat", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Fat] DESC""")
        for r in rows:
            print(f"  {r.get('Conta contabil')!r:50}  {fmt(r.get('Fat'))}  Qtd={r.get('Qtd')}")

    finally:
        await client.close()

asyncio.run(main())
