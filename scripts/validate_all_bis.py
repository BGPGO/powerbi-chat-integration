# -*- coding: utf-8 -*-
"""
Validação final de fórmulas DAX para todos os 3 BIs.
Compara receita caixa, faturamento comp e despesa caixa contra valores confirmados.

Jota/Burguerclean (Omie):   cGrupo=CONTA_CORRENTE_REC + cOrigem IN {BAXR, EXTR}
Otero (Conta Azul):         Ano_mes (sem status) + excl Transferências
"""
import asyncio, sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig

# ── Configurações dos BIs ─────────────────────────────────────────────────────
BIS = {
    "Jota": {
        "workspace_id": "0093193a-09c6-4371-b2de-5577cd912e90",
        "dataset_id":   "d19fb6a1-d17c-4b59-a7d3-4ac36c5e9af1",
        "config": {
            "tenant_id":     "0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
            "client_id":     "b86ff4ec-e9e5-4076-99e7-24471104b54c",
            "client_secret": "Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        },
        "type": "omie",
        "targets": {
            "receita_cx":  {1: 501728.00,  5: 496455.38,  10: 552866.44},
            "fat_comp":    {1: 477907.05,  5: None,        10: None},
            "despesa_cx":  {1: 438339.25,  5: 447217.82,  10: 438612.24},
        },
        "formulas": {
            "receita_cx": lambda m, mn: (
                f"CALCULATE(SUM('data'[receita]),"
                f"'data'[cGrupo]=\"CONTA_CORRENTE_REC\","
                f"'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},"
                f"'data'[ano_mes_caixa]=\"{mn}2025\")"
            ),
            "fat_comp": lambda m, mn: (
                f"CALCULATE(SUM('data'[receita competencia]),"
                f"'data'[cNatureza]=\"R\","
                f"'data'[cStatus]<>\"CANCELADO\","
                f"'data'[ano_mes_competencia]=\"{mn}2025\")"
            ),
            "despesa_cx": lambda m, mn: (
                f"CALCULATE(SUM('data'[despesas]),"
                f"'data'[cGrupo]=\"CONTA_CORRENTE_PAG\","
                f"'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},"
                f"'data'[ano_mes_caixa]=\"{mn}2025\")"
            ),
        },
    },
    "Burguerclean": {
        "workspace_id": "0093193a-09c6-4371-b2de-5577cd912e90",
        "dataset_id":   "eeaa8d72-7549-4470-8a1d-62a5590666c1",
        "config": {
            "tenant_id":     "0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
            "client_id":     "b86ff4ec-e9e5-4076-99e7-24471104b54c",
            "client_secret": "Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        },
        "type": "omie",
        "targets": {
            "receita_cx":  {1: 1674475.14, 5: 1452200.40, 10: 1573199.41},
            "fat_comp":    {1: None,        5: None,        10: None},
            "despesa_cx":  {1: None,        5: None,        10: None},
        },
        "formulas": {
            "receita_cx": lambda m, mn: (
                f"CALCULATE(SUM('data'[receita]),"
                f"'data'[cGrupo]=\"CONTA_CORRENTE_REC\","
                f"'data'[cOrigem] IN {{\"BAXR\",\"EXTR\"}},"
                f"'data'[ano_mes_caixa]=\"{mn}2025\")"
            ),
            "fat_comp": lambda m, mn: (
                f"CALCULATE(SUM('data'[receita competencia]),"
                f"'data'[cNatureza]=\"R\","
                f"'data'[cStatus]<>\"CANCELADO\","
                f"'data'[ano_mes_competencia]=\"{mn}2025\")"
            ),
            "despesa_cx": lambda m, mn: (
                f"CALCULATE(SUM('data'[despesas]),"
                f"'data'[cGrupo]=\"CONTA_CORRENTE_PAG\","
                f"'data'[cOrigem] IN {{\"BAXP\",\"EXTP\"}},"
                f"'data'[ano_mes_caixa]=\"{mn}2025\")"
            ),
        },
    },
    "Otero": {
        "workspace_id": "0093193a-09c6-4371-b2de-5577cd912e90",
        "dataset_id":   "ca26e66f-6bbd-4273-9de7-9e13e720c839",
        "config": {
            "tenant_id":     "0558a71e-8d01-46e2-bd1e-ae6432f86b3d",
            "client_id":     "b86ff4ec-e9e5-4076-99e7-24471104b54c",
            "client_secret": "Pqp8Q~zy_rTGVGjjlMPSOtwTOgzDCWk1S7G3Laqs",
        },
        "type": "conta_azul",
        "targets": {
            "receita_cx": {1: 648260.10, 5: 577325.24, 10: 624249.13},
            "fat_comp":   {1: 508526.06, 5: 615318.79, 10: 653345.74},
            "despesa_cx": {1: 600031.69, 5: 519138.32, 10: 605520.65},
        },
        "formulas": {
            "receita_cx": lambda m, mn: (
                f"CALCULATE(SUM('data'[receita]),"
                f"'data'[Receita/Despesa]=\"Receita\","
                f"'data'[Categoria 1]<>\"Transferência de Entrada\","
                f"'data'[Ano_mes]=\"2025{mn}\")"
            ),
            "fat_comp": lambda m, mn: (
                f"CALCULATE(SUM('data'[receita]),"
                f"'data'[Receita/Despesa]=\"Receita\","
                f"'data'[Categoria 1]<>\"Transferência de Entrada\","
                f"'data'[Ano_mes competencia]=\"2025{mn}\")"
            ),
            "despesa_cx": lambda m, mn: (
                f"CALCULATE(SUM('data'[despesas]),"
                f"'data'[Receita/Despesa]=\"Despesa\","
                f"'data'[Categoria 1]<>\"Transferência de Saída\","
                f"'data'[Ano_mes]=\"2025{mn}\")"
            ),
        },
    },
}

MESES = [(1, "janeiro", "jan"), (5, "maio", "mai"), (10, "outubro", "out")]

# ─────────────────────────────────────────────────────────────────────────────

def fmt(v):
    try: return f"R$ {float(v):>14,.2f}"
    except: return str(v)

def chk(v, t):
    if t is None: return "  (sem alvo)"
    try:
        d = float(v or 0) - t
        return "  ★★★ MATCH!" if abs(d) < 1 else f"  diff={d:+,.2f}"
    except: return ""

async def run_bi(bi_name, bi_cfg):
    cfg = bi_cfg["config"]
    config = PowerBIConfig(
        tenant_id=cfg["tenant_id"],
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        workspace_id=bi_cfg["workspace_id"],
        timeout_seconds=60,
    )
    client = PowerBIClient(config)
    try:
        print(f"\n{'='*60}")
        print(f"BI: {bi_name}  ({bi_cfg['type'].upper()})")
        print(f"{'='*60}")

        for metric, label in [
            ("receita_cx", "RECEITA CAIXA"),
            ("fat_comp",   "FATURAMENTO COMP"),
            ("despesa_cx", "DESPESA CAIXA"),
        ]:
            targets = bi_cfg["targets"][metric]
            fn = bi_cfg["formulas"][metric]

            # choose month name format based on BI type
            meses_iter = MESES  # (num, pt_lower, short)
            hits = 0
            vals = []
            for m, mn, mshort in meses_iter:
                dax = f"EVALUATE ROW(\"v\", {fn(m, mn)})"
                try:
                    r = await client.execute_query(bi_cfg["dataset_id"], dax, bi_cfg["workspace_id"])
                    rows = r.get("rows", [])
                    v = list(rows[0].values())[0] if rows else None
                except Exception as e:
                    v = f"ERR:{str(e)[:60]}"
                vals.append((mshort, m, v))
                await asyncio.sleep(1)

            # score
            for mshort, m, v in vals:
                t = targets.get(m)
                if t and isinstance(v, (int, float)) and abs(float(v) - t) < 1:
                    hits += 1
            total = sum(1 for _, m, _ in vals if targets.get(m) is not None)
            stars = "★★★" if hits == total and total > 0 else ("★★" if hits == 2 else ("★" if hits == 1 else "✗"))
            print(f"\n  {stars} {label}")
            for mshort, m, v in vals:
                t = targets.get(m)
                print(f"    {mshort}: {fmt(v)}{chk(v, t)}")
    finally:
        await client.close()


async def main():
    for bi_name, bi_cfg in BIS.items():
        await run_bi(bi_name, bi_cfg)
        await asyncio.sleep(3)
    print("\n\n" + "="*60)
    print("VALIDAÇÃO CONCLUÍDA")
    print("="*60)

asyncio.run(main())
