"""
Export API — exportação de relatórios Power BI para PDF.
"""

import asyncio
import datetime
import logging
from io import BytesIO
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from app.agents.storytelling_agent import generate_page_storytelling
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig, PowerBIError
from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Request / response models ─────────────────────────────────────────────────

class ExportRequest(BaseModel):
    report_id: Optional[str] = None   # usa POWERBI_REPORT_ID do .env se omitido
    pages: Optional[list[str]] = None  # nomes internos das páginas; None = todas


class PageInfo(BaseModel):
    name: str          # nome interno (usado na API)
    display_name: str  # nome visível ao usuário
    order: int


class PageForExport(BaseModel):
    name: str          # nome interno da página
    display_name: str  # nome visível


class StorytellingExportRequest(BaseModel):
    report_id: Optional[str] = None
    report_name: Optional[str] = None
    pages: Optional[list[PageForExport]] = None  # None = todas as páginas


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sanitize(text: str) -> str:
    """Substitui caracteres fora do Latin-1 para compatibilidade com fontes core do fpdf2."""
    replacements = {
        "\u2013": "-", "\u2014": "--",
        "\u2018": "'", "\u2019": "'",
        "\u201c": '"', "\u201d": '"',
        "\u2026": "...", "\u2022": "-",
        "\u00b7": "-",
    }
    for char, rep in replacements.items():
        text = text.replace(char, rep)
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _build_storytelling_pdf(
    report_name: str,
    pages_data: list[dict],
) -> bytes:
    """
    Monta o PDF com capa + seções de storytelling + imagem BI por página.

    pages_data: lista de dicts com chaves:
        display_name (str), storytelling (str), image_bytes (bytes | None)
    """
    from fpdf import FPDF

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)

    # ── Capa ─────────────────────────────────────────────────────
    pdf.add_page()

    # Barra superior (teal escuro)
    pdf.set_fill_color(36, 76, 90)
    pdf.rect(0, 0, 210, 72, "F")

    # Faixa amarela à esquerda
    pdf.set_fill_color(242, 200, 17)
    pdf.rect(0, 0, 8, 72, "F")

    # Nome do relatório
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_xy(18, 18)
    pdf.multi_cell(182, 9, _sanitize(report_name), align="L")

    # Subtítulo
    pdf.set_font("Helvetica", "", 12)
    pdf.set_xy(18, pdf.get_y() + 2)
    pdf.cell(0, 8, "Analise com Storytelling de IA")

    # Data e resumo (abaixo da barra)
    pdf.set_text_color(60, 60, 60)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_xy(18, 82)
    date_str = datetime.date.today().strftime("%d/%m/%Y")
    pdf.cell(0, 6, f"Gerado em: {date_str}", new_x="LMARGIN", new_y="NEXT")

    pdf.set_xy(18, 94)
    pdf.set_font("Helvetica", "", 11)
    n = len(pages_data)
    pdf.cell(0, 6, f"Este documento apresenta {n} tela{'s' if n != 1 else ''} com narrativa de IA.")

    # ── Seções por página ─────────────────────────────────────────
    for page_data in pages_data:
        pdf.add_page()

        # Barra de cabeçalho da tela
        pdf.set_fill_color(36, 76, 90)
        pdf.rect(0, 0, 210, 22, "F")
        pdf.set_fill_color(242, 200, 17)
        pdf.rect(0, 0, 6, 22, "F")

        # Título da tela
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_xy(12, 6)
        pdf.cell(0, 10, _sanitize(page_data["display_name"]))

        # Seção de narrativa
        pdf.set_text_color(36, 76, 90)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(15, 28)
        pdf.cell(0, 5, "NARRATIVA", new_x="LMARGIN", new_y="NEXT")

        pdf.set_draw_color(242, 200, 17)
        pdf.set_line_width(0.4)
        pdf.line(15, pdf.get_y(), 195, pdf.get_y())
        pdf.ln(3)

        pdf.set_text_color(50, 50, 50)
        pdf.set_font("Helvetica", "", 10)
        pdf.set_x(15)
        storytelling = _sanitize(page_data.get("storytelling", ""))
        pdf.multi_cell(180, 5, storytelling, align="J")

        # Seção de visualização (imagem BI)
        if page_data.get("image_bytes"):
            pdf.ln(5)
            pdf.set_text_color(36, 76, 90)
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_x(15)
            pdf.cell(0, 5, "VISUALIZACAO", new_x="LMARGIN", new_y="NEXT")

            pdf.set_draw_color(242, 200, 17)
            pdf.line(15, pdf.get_y(), 195, pdf.get_y())
            pdf.ln(3)

            y_img = pdf.get_y()
            available_h = 282 - y_img  # A4 297mm − margem inferior 15mm
            img_w = 180
            img_h = min(available_h, round(img_w * 9 / 16))

            if img_h > 15:
                img_io = BytesIO(page_data["image_bytes"])
                try:
                    pdf.image(img_io, x=15, y=y_img, w=img_w, h=img_h)
                    pdf.set_y(y_img + img_h + 5)
                except Exception as img_err:
                    logger.warning("Não foi possível inserir imagem da página: %s", img_err)

    return bytes(pdf.output())


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/pages", response_model=list[PageInfo], summary="Lista páginas do relatório")
async def list_pages(report_id: Optional[str] = None):
    """
    Retorna as páginas disponíveis do relatório para seleção no modal de export.
    """
    rid = report_id or settings.powerbi_report_id
    if not rid:
        raise HTTPException(status_code=400, detail="report_id não configurado")

    try:
        client = PowerBIClient(PowerBIConfig.from_env())
        pages_raw = await client.list_report_pages(rid)
        await client.close()

        return [
            PageInfo(
                name=p["name"],
                display_name=p.get("displayName", p["name"]),
                order=p.get("order", i),
            )
            for i, p in enumerate(pages_raw)
        ]
    except PowerBIError as e:
        logger.error("Erro ao listar páginas: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/pdf", summary="Exporta relatório para PDF")
async def export_pdf(body: ExportRequest):
    """
    Exporta o relatório (ou páginas selecionadas) para PDF via Power BI API.
    Retorna o arquivo PDF como download.
    """
    rid = body.report_id or settings.powerbi_report_id
    if not rid:
        raise HTTPException(status_code=400, detail="report_id não configurado")

    try:
        client = PowerBIClient(PowerBIConfig.from_env())
        pdf_bytes = await client.export_report_to_pdf(
            report_id=rid,
            pages=body.pages or None,
        )
        await client.close()

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=relatorio.pdf"},
        )
    except PowerBIError as e:
        logger.error("Erro ao exportar PDF: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/storytelling", summary="Exporta relatório com storytelling de IA em PDF")
async def export_storytelling(body: StorytellingExportRequest):
    """
    Para cada página selecionada:
    1. Exporta a tela como imagem PNG via Power BI API
    2. Gera narrativa de storytelling via Claude
    3. Monta PDF customizado: capa + (narrativa + imagem) por tela
    """
    rid = body.report_id or settings.powerbi_report_id
    if not rid:
        raise HTTPException(status_code=400, detail="report_id não configurado")

    report_name = body.report_name or "Relatório Power BI"

    # Resolve lista de páginas
    pages = body.pages
    if not pages:
        try:
            client = PowerBIClient(PowerBIConfig.from_env())
            pages_raw = await client.list_report_pages(rid)
            await client.close()
            pages = [
                PageForExport(
                    name=p["name"],
                    display_name=p.get("displayName", p["name"]),
                )
                for p in sorted(pages_raw, key=lambda x: x.get("order", 0))
            ]
        except PowerBIError as e:
            logger.error("Erro ao listar páginas para storytelling: %s", e)
            raise HTTPException(status_code=502, detail=str(e))

    if not pages:
        raise HTTPException(status_code=400, detail="Nenhuma página disponível para exportar")

    # Processa todas as páginas em paralelo (máx 3 exports simultâneos no Power BI)
    # Fluxo por página: 1) exporta PNG  2) passa imagem para Claude vision → storytelling
    _semaphore = asyncio.Semaphore(3)
    client = PowerBIClient(PowerBIConfig.from_env())

    async def _process_page(page: PageForExport) -> dict:
        async with _semaphore:
            # Etapa 1 — exporta PNG
            try:
                image_bytes = await client.export_page_as_image(rid, page.name)
            except Exception as exc:
                logger.error("Erro ao exportar imagem de '%s': %s", page.name, exc)
                image_bytes = None

            # Etapa 2 — storytelling com visão (usa a imagem obtida)
            try:
                storytelling_text = await generate_page_storytelling(
                    page.display_name, report_name, image_bytes=image_bytes
                )
            except Exception as exc:
                logger.error("Erro ao gerar storytelling de '%s': %s", page.display_name, exc)
                storytelling_text = f"Análise da tela {page.display_name}."

            return {
                "display_name": page.display_name,
                "storytelling": storytelling_text,
                "image_bytes": image_bytes,
            }

    try:
        pages_data: list[dict] = list(
            await asyncio.gather(*[_process_page(p) for p in pages])
        )
    finally:
        await client.close()

    # Monta o PDF
    try:
        pdf_bytes = _build_storytelling_pdf(report_name, pages_data)
    except Exception as e:
        logger.error("Erro ao montar PDF de storytelling: %s", e)
        raise HTTPException(status_code=500, detail=f"Erro ao gerar PDF: {e}")

    safe_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in report_name)
    filename = f"{safe_name}_storytelling.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
