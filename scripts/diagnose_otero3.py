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

async def try3(client, label, fn, targets):
    vals = []
    for m, mn in [(1,"janeiro"),(5,"maio"),(10,"outubro")]:
        rows = await q(client, f"EVALUATE ROW(\"v\", {fn(m,mn)})")
        v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
        vals.append((m, v))
        await asyncio.sleep(1)
    hits = sum(1 for m,v in vals if isinstance(v,(int,float)) and abs(float(v)-targets[m])<1)
    star = "★★★" if hits==3 else ("★★" if hits==2 else ("★" if hits==1 else ""))
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
        # ── RECEITA CAIXA ──────────────────────────────────────────────
        print("=" * 60)
        print("RECEITA CAIXA — investigar excesso")
        print("=" * 60)

        # Categoria por caixa jan — qual categoria tem os 12.702,98 extras?
        print("\n=== Categoria 1 breakdown caixa (jan 2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Categoria 1],
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
            print(f"  {r.get('Categoria 1')!r:48}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # Testar excluir "Transferência de Entrada" e "Receitas Pessoais dos Sócios" etc
        print("\n=== RECEITA excluindo Transferência de Entrada (jan/mai/out) ===")
        await try3(client, "receita + pago + Ano_mes — excl Transf Entrada",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[Categoria 1]<>\"Transferência de Entrada\")",
            T_CX)

        print("\n=== RECEITA excluindo FonteValor=Categoria (fallback) (jan/mai/out) ===")
        await try3(client, "receita + pago + Ano_mes — excl fallback",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[FonteValor]=\"CC\")",
            T_CX)

        # Verificar Tipo da operação para entradas Receita+Pago
        print("\n=== Tipo da operação breakdown caixa (jan 2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Tipo da operação],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Previsto/realizado] = "Pago"
        && 'data'[Ano_mes] = "2025janeiro"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  Tipo={r.get('Tipo da operação')!r}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # Testar somente Tipo da operação = Crédito
        print("\n=== RECEITA Tipo=Crédito + pago + Ano_mes (jan/mai/out) ===")
        await try3(client, "receita + Tipo=Crédito + pago + Ano_mes",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Tipo da operação]=\"Crédito\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")",
            T_CX)

        # Testar excluindo categoria que começa com "Transferência"
        print("\n=== RECEITA excluindo categorias de Transferência (jan/mai/out) ===")
        await try3(client, "receita + pago — excl. categorias Transferência*",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",NOT CONTAINSSTRING('data'[Categoria 1],\"Transferência\"))",
            T_CX)

        # ── FATURAMENTO ──────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("FATURAMENTO COMP — excluir transferências")
        print("=" * 60)

        await try3(client, "fat — excl Transferência de Entrada",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Ano_mes competencia]=\"2025{mn}\",'data'[Categoria 1]<>\"Transferência de Entrada\")",
            T_FAT)

        await try3(client, "fat — excl todas categorias Transferência*",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Ano_mes competencia]=\"2025{mn}\",NOT CONTAINSSTRING('data'[Categoria 1],\"Transferência\"))",
            T_FAT)

        await try3(client, "fat — FonteValor=CC apenas",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Ano_mes competencia]=\"2025{mn}\",'data'[FonteValor]=\"CC\")",
            T_FAT)

        await try3(client, "fat — Tipo=Crédito apenas",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Tipo da operação]=\"Crédito\",'data'[Ano_mes competencia]=\"2025{mn}\")",
            T_FAT)

        # ── DESPESA CAIXA ──────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("DESPESA CAIXA — investigar diferença")
        print("=" * 60)

        print("\n=== Categoria 1 breakdown DESPESA caixa (jan 2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Categoria 1],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Despesa"
        && 'data'[Previsto/realizado] = "Pago"
        && 'data'[Ano_mes] = "2025janeiro"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Despesa] DESC""")
        for r in rows:
            print(f"  {r.get('Categoria 1')!r:48}  {fmt(r.get('Despesa'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        print("\n=== Tipo da operação breakdown DESPESA caixa (jan 2025) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Tipo da operação],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Despesa"
        && 'data'[Previsto/realizado] = "Pago"
        && 'data'[Ano_mes] = "2025janeiro"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  Tipo={r.get('Tipo da operação')!r}  {fmt(r.get('Despesa'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        await try3(client, "despesa — excl Transferência*",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",NOT CONTAINSSTRING('data'[Categoria 1],\"Transferência\"))",
            T_DSP)

        await try3(client, "despesa — Tipo=Débito apenas",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Tipo da operação]=\"Débito\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")",
            T_DSP)

        await try3(client, "despesa — FonteValor=CC apenas",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[FonteValor]=\"CC\")",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
