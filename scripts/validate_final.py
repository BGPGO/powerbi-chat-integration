# -*- coding: utf-8 -*-
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

WORKSPACE_ID = "0093193a-09c6-4371-b2de-5577cd912e90"

BIS = {
    "Jota":        "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1",
    "Burguerclean":"eeaa8d72-7549-4470-8a1d-62a5590666c1",
    "Otero":       "ca26e66f-6bbd-4273-9de7-9e13e720c839",
}

MESES = [
    (1,  "janeiro",  "Janeiro",  31),
    (5,  "maio",     "Maio",     31),
    (10, "outubro",  "Outubro",  31),
]

async def q(client, did, dax):
    try:
        r = await client.execute_query(did, dax, WORKSPACE_ID)
        rows = r.get("rows", [])
        if rows:
            v = list(rows[0].values())[0]
            return float(v) if v is not None else 0.0
        return 0.0
    except Exception as e:
        return f"ERRO: {str(e)[:80]}"

def fmt(v):
    if isinstance(v, str): return v
    return f"R$ {v:>14,.2f}"

async def validate_bi(client, name, did):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    for m_num, m_lower, m_cap, m_last in MESES:
        await asyncio.sleep(2)

        if name in ("Jota", "Burguerclean"):
            # Omie: caixa = CONTA_CORRENTE_REC + cOrigem IN {BAXR, EXTR}
            # Período via ano_mes_caixa = "mesANO"
            cx = await q(client, did, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[cGrupo] = "CONTA_CORRENTE_REC",
    'data'[cOrigem] IN {{"BAXR","EXTR"}},
    'data'[ano_mes_caixa] = "{m_lower}2025"))""")

            await asyncio.sleep(2)

            # Omie: competência = receita competencia + cNatureza=R + cStatus<>CANCELADO
            comp = await q(client, did, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita competencia]),
    'data'[cNatureza] = "R",
    'data'[cStatus] <> "CANCELADO",
    'data'[ano_mes_competencia] = "{m_lower}2025"))""")

            await asyncio.sleep(2)

            # Omie: despesa caixa = CONTA_CORRENTE_PAG ou cNatureza=P + BAXP/EXTR
            # Tentativa 1: cNatureza=P + CCAREC equivalente
            desp = await q(client, did, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[despesas]),
    'data'[cGrupo] = "CONTA_CORRENTE_PAG",
    'data'[cOrigem] IN {{"BAXP","EXTR"}},
    'data'[ano_mes_caixa] = "{m_lower}2025"))""")

        else:
            # Otero (Conta Azul): caixa = Previsto/realizado = "Pago"
            cx = await q(client, did, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Receita/Despesa] = "Receita",
    'data'[Previsto/realizado] = "Pago",
    'data'[Ano_mes] = "2025{m_lower}"))""")

            await asyncio.sleep(2)

            # Otero: faturamento competência
            comp = await q(client, did, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[receita]),
    'data'[Receita/Despesa] = "Receita",
    'data'[Ano_mes competencia] = "2025{m_lower}"))""")

            await asyncio.sleep(2)

            # Otero: despesa caixa
            desp = await q(client, did, f"""
EVALUATE ROW("v", CALCULATE(SUM('data'[despesas]),
    'data'[Receita/Despesa] = "Despesa",
    'data'[Previsto/realizado] = "Pago",
    'data'[Ano_mes] = "2025{m_lower}"))""")

        await asyncio.sleep(2)

        print(f"\n  {m_cap}/2025:")
        print(f"    Receita caixa:   {fmt(cx)}")
        print(f"    Faturamento comp:{fmt(comp)}")
        print(f"    Despesa caixa:   {fmt(desp)}")

async def main():
    config = PowerBIConfig(
        tenant_id="0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
        client_id="b86ff4ec-e9e5-4076-99e7-24471104b54c",
        client_secret="Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        workspace_id=WORKSPACE_ID, timeout_seconds=60,
    )
    client = PowerBIClient(config)
    try:
        for name, did in BIS.items():
            await validate_bi(client, name, did)
            await asyncio.sleep(5)
    finally:
        await client.close()

asyncio.run(main())
