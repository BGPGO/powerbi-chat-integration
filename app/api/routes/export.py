"""
Export API — PDF export and AI Storytelling.

Endpoints:
  GET  /pages                      — List report pages (Power BI API)
  POST /pdf                        — Simple PDF via Playwright screenshots
  POST /storytelling               — Start async storytelling job
  GET  /storytelling/{job_id}      — Poll job status/progress
  GET  /storytelling/{job_id}/download — Download completed PDF
"""

import asyncio
import datetime
import io
import logging
from io import BytesIO
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from app.agents.storytelling_agent import generate_page_storytelling
from app.connectors.powerbi.client import PowerBIClient, PowerBIConfig, PowerBIError
from app.core.config import settings
from app.services.job_manager import JobStatus, job_manager
from app.services.screenshot_service import capture_report_screenshots
from app.services.storytelling_service import generate_storytelling_pdf

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Request / Response models ─────────────────────────────────────

class ExportRequest(BaseModel):
    report_id: Optional[str] = None
    pages: Optional[list[str]] = None


class ScreenshotPdfRequest(BaseModel):
    embed_url: str
    filters: Optional[dict] = None
    report_name: str = "Relatório"


class StorytellingRequest(BaseModel):
    embed_url: str
    filters: Optional[dict] = None
    report_name: str = "Relatório"


class PageInfo(BaseModel):
    name: str
    display_name: str
    order: int


class PageForExport(BaseModel):
    name: str          # nome interno da página
    display_name: str  # nome visível


class StorytellingExportRequest(BaseModel):
    report_id: Optional[str] = None
    report_name: Optional[str] = None
    pages: Optional[list[PageForExport]] = None  # None = todas as páginas


class JobStatusResponse(BaseModel):
    id: str
    status: str
    progress: int
    total_steps: int
    current_step: str
    error: Optional[str] = None
    created_at: str
    updated_at: str


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


# ── Simple PDF (Playwright screenshots) ──────────────────────────


@router.post("/pdf", summary="Gera PDF simples via screenshot do relatório")
async def export_pdf_screenshot(body: ScreenshotPdfRequest):
    """
    Captura screenshots do relatório Power BI embeddado e retorna como PDF.
    Aceita a URL do iframe (já com filtros) ou filtros OData separados.
    """
    try:
        screenshots = await capture_report_screenshots(
            embed_url=body.embed_url,
            filters=body.filters,
        )

        if not screenshots:
            raise HTTPException(status_code=502, detail="Nenhuma página capturada")

        from fpdf import FPDF

        pdf = FPDF(orientation="L", unit="mm", format="A4")
        for shot in screenshots:
            pdf.add_page()
            img_stream = io.BytesIO(shot.image_bytes)
            pdf.image(img_stream, x=5, y=5, w=287)

        pdf_bytes = pdf.output()

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{body.report_name}.pdf"'
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao gerar PDF: %s", e)
        raise HTTPException(status_code=500, detail=f"Erro ao gerar PDF: {e}")


# ── Storytelling (async job) ──────────────────────────────────────


async def _run_storytelling_job(
    job_id: str,
    embed_url: str,
    filters: Optional[dict],
    report_name: str,
):
    """Background task: screenshots → Claude Vision → PDF."""
    try:
        job_manager.update_progress(job_id, 0, "Capturando screenshots do relatório...")

        def on_screenshot_progress(page_idx, total, page_name):
            job_manager.update_progress(
                job_id,
                page_idx,
                f"Capturando {page_name} ({page_idx + 1}/{total})",
            )

        screenshots = await capture_report_screenshots(
            embed_url=embed_url,
            filters=filters,
            on_progress=on_screenshot_progress,
        )

        if not screenshots:
            job_manager.fail_job(job_id, "Nenhuma página capturada do relatório")
            return

        # Update total steps now that we know page count
        job = job_manager.get_job(job_id)
        if job:
            job.total_steps = len(screenshots)

        def on_analysis_progress(step_idx, total, step_desc):
            job_manager.update_progress(job_id, step_idx, step_desc)

        pdf_bytes = await generate_storytelling_pdf(
            screenshots=screenshots,
            report_name=report_name,
            on_progress=on_analysis_progress,
        )

        job_manager.complete_job(job_id, pdf_bytes)
        logger.info("Storytelling job %s completed: %d bytes", job_id, len(pdf_bytes))

    except Exception as e:
        logger.exception("Storytelling job %s failed: %s", job_id, e)
        job_manager.fail_job(job_id, str(e))


@router.post(
    "/storytelling",
    summary="Inicia geração de PDF com storytelling (assíncrono)",
)
async def start_storytelling(body: StorytellingRequest):
    """
    Inicia job assíncrono de storytelling.

    Pipeline:
    1. Playwright captura screenshots do BI (com filtros)
    2. Claude Vision analisa cada página e gera narrativa executiva
    3. fpdf2 monta PDF branded com narrativa + screenshots

    Retorna job_id para polling via GET /storytelling/{job_id}.
    """
    job_manager.cleanup_old_jobs()
    job = job_manager.create_job(total_steps=1)

    asyncio.create_task(
        _run_storytelling_job(
            job_id=job.id,
            embed_url=body.embed_url,
            filters=body.filters,
            report_name=body.report_name,
        )
    )

    return {"job_id": job.id, "status": "queued"}


@router.get(
    "/storytelling/{job_id}",
    response_model=JobStatusResponse,
    summary="Status do job de storytelling",
)
async def get_storytelling_status(job_id: str):
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")
    return job.to_dict()


@router.get(
    "/storytelling/{job_id}/download",
    summary="Download do PDF de storytelling concluído",
)
async def download_storytelling(job_id: str):
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=409,
            detail=f"Job não concluído (status: {job.status.value})",
        )

    pdf_bytes = job_manager.get_file(job_id)
    if not pdf_bytes:
        raise HTTPException(status_code=500, detail="Arquivo não encontrado")

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="storytelling_{job_id}.pdf"'
        },
    )
