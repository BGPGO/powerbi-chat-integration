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
        return [{"_erro": str(e)[:100]}]

def fmt(v):
    try: return f"R$ {float(v):>14,.2f}"
    except: return str(v)

def chk(v, t):
    try:
        d = float(v or 0) - t
        return "  ★★★ MATCH!" if abs(d) < 1 else f"  diff={d:+,.2f}"
    except: return ""

async def try3(client, label, fn, targets):
    await asyncio.sleep(2)
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

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)
    MESES_PT = {1:"janeiro",5:"maio",10:"outubro"}
    try:
        # ── Receita caixa ──────────────────────────────────────────
        print("=" * 55)
        print("RECEITA CAIXA (alvo: 648.260 / 577.325 / 624.249)")
        print("=" * 55)

        # Verificar se o Ano_mes format é "2025janeiro" ou algo diferente
        print("\n=== Ano_mes valores existentes (receita, pago) ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Ano_mes],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && 'data'[Previsto/realizado] = "Pago"
        && RIGHT('data'[Ano_mes], 4) = "2025"
    ),
    "Receita", SUM('data'[receita])
)
ORDER BY [Receita] DESC""")
        for r in rows:
            print(f"  Ano_mes={r.get('Ano_mes')!r}  {fmt(r.get('Receita'))}")

        await asyncio.sleep(3)

        formulas_cx = [
            ("receita + Pago + Ano_mes=2025X",
             lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")"),
            ("receita + Situação IN Conciliado/Quitado + Ano_mes",
             lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Situação] IN {{\"Conciliado\",\"Quitado\"}},'data'[Ano_mes]=\"2025{mn}\")"),
            ("receita + Pago + Data movimento",
             lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Data movimento]>=DATE(2025,{m},1),'data'[Data movimento]<=DATE(2025,{m},31))"),
            ("liquido + Pago + Ano_mes",
             lambda m,mn: f"CALCULATE(SUM('data'[liquido]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")"),
            ("Valor (R$) + Pago + Ano_mes",
             lambda m,mn: f"CALCULATE(SUM('data'[Valor (R$)]),'data'[Receita/Despesa]=\"Receita\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")"),
            ("receita + Tipo da operação=Crédito + Pago + Ano_mes",
             lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Tipo da operação]=\"Crédito\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")"),
        ]

        for label, fn in formulas_cx:
            await try3(client, label, fn, T_CX)

        # ── Faturamento competência ──────────────────────────────────
        print("\n" + "=" * 55)
        print("FATURAMENTO COMP (alvo: 508.526 / 615.318 / 653.345)")
        print("=" * 55)

        # Verificar formato Ano_mes competencia
        print("\n=== Ano_mes competencia valores ===")
        rows = await q(client, """
EVALUATE
SUMMARIZECOLUMNS('data'[Ano_mes competencia],
    FILTER(ALL('data'),
        'data'[Receita/Despesa] = "Receita"
        && RIGHT('data'[Ano_mes competencia], 4) = "2025"
    ),
    "Fat", SUM('data'[receita])
)
ORDER BY [Fat] DESC""")
        for r in rows:
            print(f"  Ano_mes comp={r.get('Ano_mes competencia')!r}  {fmt(r.get('Fat'))}")

        await asyncio.sleep(3)

        formulas_fat = [
            ("receita + Receita/Despesa=Receita + Ano_mes comp",
             lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Ano_mes competencia]=\"2025{mn}\")"),
            ("receita + Tipo=Crédito + Ano_mes comp",
             lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Tipo da operação]=\"Crédito\",'data'[Ano_mes competencia]=\"2025{mn}\")"),
            ("receita + Receita/Despesa=Receita + excl Cancelado + Ano_mes comp",
             lambda m,mn: f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Situação]<>\"Cancelado\",'data'[Ano_mes competencia]=\"2025{mn}\")"),
            ("receita comp + Receita/Despesa=Receita + Ano_mes comp",
             lambda m,mn: f"CALCULATE(SUM('data'[receita competencia]),'data'[Receita/Despesa]=\"Receita\",'data'[Ano_mes competencia]=\"2025{mn}\")" if False else
                          f"CALCULATE(SUM('data'[receita]),'data'[Receita/Despesa]=\"Receita\",'data'[Ano_mes competencia]=\"2025{mn}\")"),
        ]

        for label, fn in formulas_fat:
            await try3(client, label, fn, T_FAT)

        # ── Despesa caixa ──────────────────────────────────────────
        print("\n" + "=" * 55)
        print("DESPESA CAIXA (alvo: 600.031 / 519.138 / 605.520)")
        print("=" * 55)

        formulas_dsp = [
            ("despesas + Pago + Ano_mes",
             lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")"),
            ("despesas + Situação Conciliado/Quitado + Ano_mes",
             lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Situação] IN {{\"Conciliado\",\"Quitado\"}},'data'[Ano_mes]=\"2025{mn}\")"),
            ("despesas + Tipo != Crédito + Pago + Ano_mes",
             lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Tipo da operação]<>\"Crédito\",'data'[Previsto/realizado]=\"Pago\",'data'[Ano_mes]=\"2025{mn}\")"),
            ("despesas + Pago + Data movimento",
             lambda m,mn: f"CALCULATE(SUM('data'[despesas]),'data'[Receita/Despesa]=\"Despesa\",'data'[Previsto/realizado]=\"Pago\",'data'[Data movimento]>=DATE(2025,{m},1),'data'[Data movimento]<=DATE(2025,{m},31))"),
        ]

        for label, fn in formulas_dsp:
            await try3(client, label, fn, T_DSP)

    finally:
        await client.close()

asyncio.run(main())
