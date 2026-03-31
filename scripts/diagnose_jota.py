# -*- coding: utf-8 -*-
"""
Diagnóstico Jota — testa variações de query para bater com os valores corretos:
  Receita caixa: 501.728,00 / 496.455,38 / 552.866,44
  Competência:   477.907,05 / 440.622,45 / 464.955,09
"""
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"
DID = "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1"  # Jota

# Alvos para validar
TARGETS_CAIXA = {"jan": 501728.00, "mai": 496455.38, "out": 552866.44}
TARGETS_COMP  = {"jan": 477907.05, "mai": 440622.45, "out": 464955.09}

async def q(client, dax, label=""):
    try:
        r = await client.execute_query(DID, dax, WORKSPACE_ID)
        rows = r.get("rows", [])
        return rows[0] if rows else {}
    except Exception as e:
        return {"_erro": str(e)[:80]}

def fmt(v):
    try: return f"R$ {float(v):>14,.2f}"
    except: return str(v)

def match(v, target):
    try: return "✓ MATCH" if abs(float(v or 0) - target) < 1.0 else f"  (alvo {target:,.2f})"
    except: return ""

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)

    QUERIES = [
        # (label, dax_jan, dax_mai, dax_out, tipo)
        ("receita + cStatus + DataPagamento", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cStatus] IN {{"PAGO","RECEBIDO"}},
    'data'[DataPagamento] >= DATE(2025,{m},{a}),
    'data'[DataPagamento] <= DATE(2025,{m},{b})))""", "caixa"),

        ("receita + cStatus + Data auxiliar", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cStatus] IN {{"PAGO","RECEBIDO"}},
    'data'[Data auxiliar] >= DATE(2025,{m},{a}),
    'data'[Data auxiliar] <= DATE(2025,{m},{b})))""", "caixa"),

        ("receita + Previsto/realizado=Realizado + DataPagamento", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Previsto/realizado] = "Realizado",
    'data'[DataPagamento] >= DATE(2025,{m},{a}),
    'data'[DataPagamento] <= DATE(2025,{m},{b})))""", "caixa"),

        ("receita + Previsto/realizado=Realizado + ano_mes_caixa", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Previsto/realizado] = "Realizado",
    'data'[ano_mes_caixa] = "{['janeiro','fevereiro','março','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro'][m-1]}2025"))""", "caixa"),

        ("receita + cGrupo=CONTA_CORRENTE_REC + DataPagamento", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cGrupo] = "CONTA_CORRENTE_REC",
    'data'[DataPagamento] >= DATE(2025,{m},{a}),
    'data'[DataPagamento] <= DATE(2025,{m},{b})))""", "caixa"),

        ("valor_unico_2 + cNatureza=R + DataPagamento", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[valor_unico_2]),
    'data'[cNatureza] = "R",
    'data'[DataPagamento] >= DATE(2025,{m},{a}),
    'data'[DataPagamento] <= DATE(2025,{m},{b})))""", "caixa"),

        ("valor_unico_2 + cNatureza=R + Data auxiliar", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[valor_unico_2]),
    'data'[cNatureza] = "R",
    'data'[Data auxiliar] >= DATE(2025,{m},{a}),
    'data'[Data auxiliar] <= DATE(2025,{m},{b})))""", "caixa"),

        ("nValPago + cNatureza=R + DataPagamento", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[nValPago]),
    'data'[cNatureza] = "R",
    'data'[DataPagamento] >= DATE(2025,{m},{a}),
    'data'[DataPagamento] <= DATE(2025,{m},{b})))""", "caixa"),

        # --- Competência ---
        ("receita competencia + dDtVenc", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita competencia]),
    'data'[dDtVenc] >= DATE(2025,{m},{a}),
    'data'[dDtVenc] <= DATE(2025,{m},{b})))""", "comp"),

        ("receita competencia + Data", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita competencia]),
    'data'[Data] >= DATE(2025,{m},{a}),
    'data'[Data] <= DATE(2025,{m},{b})))""", "comp"),

        ("receita competencia + ano_mes_competencia", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita competencia]),
    'data'[ano_mes_competencia] = "{['janeiro','fevereiro','março','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro'][m-1]}2025"))""", "comp"),

        ("valor_unico_comp + cNatureza=R + dDtVenc", lambda m,a,b: f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[valor_unico_comp]),
    'data'[cNatureza] = "R",
    'data'[dDtVenc] >= DATE(2025,{m},{a}),
    'data'[dDtVenc] <= DATE(2025,{m},{b})))""", "comp"),
    ]

    MESES = [(1,1,31,"jan"), (5,1,31,"mai"), (10,1,31,"out")]

    for label, dax_fn, tipo in QUERIES:
        await asyncio.sleep(2)
        targets = TARGETS_CAIXA if tipo == "caixa" else TARGETS_COMP
        results = []
        for (m, a, b, mkey) in MESES:
            row = await q(client, dax_fn(m, a, b))
            v = list(row.values())[0] if row and "_erro" not in row else None
            results.append((mkey, v))

        jan_v = results[0][1]; mai_v = results[1][1]; out_v = results[2][1]
        jan_m = match(jan_v, targets["jan"])
        mai_m = match(mai_v, targets["mai"])
        out_m = match(out_v, targets["out"])

        hit = sum(1 for m in [jan_m, mai_m, out_m] if "MATCH" in str(m))
        flag = "★★★" if hit == 3 else ("★★" if hit == 2 else ("★" if hit == 1 else ""))
        print(f"\n[{tipo.upper()}] {flag} {label}")
        print(f"  Jan: {fmt(jan_v)} {jan_m}")
        print(f"  Mai: {fmt(mai_v)} {mai_m}")
        print(f"  Out: {fmt(out_v)} {out_m}")

    await client.close()

asyncio.run(main())
