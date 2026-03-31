# -*- coding: utf-8 -*-
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"
DID = "ca26e66f-6bbd-4273-9de7-9e13e720c839"

T_CX  = {1: 648260.10, 5: 577325.24, 10: 624249.13}
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
        # ── Todos os valores de Previsto/realizado para Receita jan ──
        print("=== Previsto/realizado (RECEITA, jan 2025, todos os status) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Previsto/realizado], 'data'[Situação],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Ano_mes] = "2025janeiro"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  Previsto/realizado={r.get('Previsto/realizado')!r:15}  Situação={r.get('Situação')!r:20}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── Receita CAIXA — combinar Pago + excl Transf Entrada + incluir outras categorias ──
        print("\n=== RECEITA: FonteValor=CC (jan/mai/out) ===")
        await try3(client, "receita + Pago + Ano_mes + FonteValor=CC",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[FonteValor]=\"CC\")",
            T_CX)

        # ── Receita CAIXA — testar excluindo "Transferência de Entrada" + incluir mais status ──
        print("\n=== RECEITA: excl Transf Entrada + status adicional (jan/mai/out) ===")
        # Que outros status de Previsto/realizado existem para Receita jan?
        # Tentar: Pago + Pendente filtrado por data?
        await try3(client, "receita + excl Transf Entrada + Pago + Ano_mes",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[Categoria 1]<>\"Transferência de Entrada\")",
            T_CX)

        # ── Tentar usar Conta bancária como filtro (excluir ou incluir apenas certas contas) ──
        print("\n=== RECEITA por Conta bancária excl Transf (jan) ===")
        for conta in ["OteroPay - Contabilidade", "OteroPay - Banco"]:
            rows = await q(client, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Receita/Despesa]="Receita",
    'data'[Previsto/realizado]="Pago",
    'data'[Ano_mes]="2025janeiro",
    'data'[Conta bancária]="{conta}",
    'data'[Categoria 1]<>"Transferência de Entrada"))""")
            v = list(rows[0].values())[0] if rows and "_erro" not in rows[0] else None
            print(f"  {conta}: {fmt(v)}")
            await asyncio.sleep(1)

        await asyncio.sleep(2)

        # ── Tentar incluir entradas de Competência não-pagas mas com data de mov = jan ──
        # Verificar se existem entradas com Previsto/realizado != Pago mas Data movimento = jan
        print("\n=== Entradas Receita em jan com Previsto/realizado != Pago ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Previsto/realizado],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Data movimento] >= DATE(2025,1,1)
        && 'data'[Data movimento] <= DATE(2025,1,31)
        && 'data'[Previsto/realizado] <> "Pago"
    ),
    "Receita", SUM('data'[receita]),
    "Qtd", COUNTROWS('data')
)""")
        for r in rows:
            print(f"  {r.get('Previsto/realizado')!r}  {fmt(r.get('Receita'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # ── Tentar cc_idx=1 excluindo Transferência de Entrada ──
        print("\n=== RECEITA cc_idx=1 excl Transf Entrada (jan/mai/out) ===")
        await try3(client, "receita + Pago + cc_idx=1 + excl Transf Entrada",
            lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[cc_idx]=1,'data'[Categoria 1]<>\"Transferência de Entrada\")",
            T_CX)

        # ── DESPESA CAIXA ──────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("DESPESA CAIXA — investigar gap")
        print("=" * 60)

        # Todos os status para despesa jan
        print("\n=== Previsto/realizado (DESPESA, jan 2025, todos os status) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Previsto/realizado], 'data'[Situação],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Despesa"
        && 'data'[Ano_mes] = "2025janeiro"
    ),
    "Despesa", SUM('data'[despesas]),
    "Qtd", COUNTROWS('data')
)
ORDER BY [Despesa] DESC""")
        for r in rows:
            print(f"  {r.get('Previsto/realizado')!r:15}  {r.get('Situação')!r:20}  {fmt(r.get('Despesa'))}  Qtd={r.get('Qtd')}")

        await asyncio.sleep(3)

        # Despesa incluindo todas as entradas pagas com Ano_mes (sem filtro de Transf)
        print("\n=== DESPESA: FonteValor=CC excl Transf Saida (jan/mai/out) ===")
        await try3(client, "despesa + Pago + Ano_mes + FonteValor=CC + excl Transf Saida",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[FonteValor]=\"CC\",'data'[Categoria 1]<>\"Transferência de Saída\")",
            T_DSP)

        # Despesa excluindo apenas Transf Saida
        await try3(client, "despesa + Pago + Ano_mes + excl Transf Saida",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[Categoria 1]<>\"Transferência de Saída\")",
            T_DSP)

        # Despesa com cc_idx=1
        await try3(client, "despesa + Pago + Ano_mes + cc_idx=1",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[cc_idx]=1)",
            T_DSP)

        # Despesa com cc_idx=1 excl Transf Saida
        await try3(client, "despesa + Pago + Ano_mes + cc_idx=1 + excl Transf Saida",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[cc_idx]=1,'data'[Categoria 1]<>\"Transferência de Saída\")",
            T_DSP)

        # cc_idx=1 excl todas Transferência*
        await try3(client, "despesa + Pago + Ano_mes + cc_idx=1 + excl Transf*",
            lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\",'data'[cc_idx]=1,NOT CONTAINSSTRING('data'[Categoria 1],\"Transferência\"))",
            T_DSP)

    finally:
        await client.close()

asyncio.run(main())
