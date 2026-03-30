"""
Storytelling service: generates executive narrative PDF from Power BI screenshots.
Uses Claude Vision to analyze each page and fpdf2 to assemble the final PDF.
"""

import base64
import io
import logging
from datetime import datetime
from typing import Optional

import anthropic
from fpdf import FPDF

from app.core.config import settings
from app.services.screenshot_service import PageScreenshot

logger = logging.getLogger(__name__)

# Brand colors
COLOR_PRIMARY = (0, 105, 120)     # Teal
COLOR_ACCENT = (230, 185, 50)     # Gold
COLOR_TEXT = (50, 50, 50)         # Dark gray
COLOR_LIGHT_BG = (245, 245, 245)  # Light gray


ANALYSIS_PROMPT = """Você é um analista financeiro sênior da BGP GO - Expertise Financeira.
Analise esta página de dashboard Power BI e gere uma narrativa executiva em português brasileiro.

Diretrizes:
- Identifique os KPIs principais e seus valores
- Destaque tendências (crescimento, queda, estabilidade)
- Aponte pontos de atenção ou oportunidades
- Use linguagem profissional mas acessível para C-Level
- Seja objetivo: 3-5 parágrafos no máximo
- Use dados concretos (números, percentuais) quando visíveis
- NÃO invente dados que não estão visíveis na imagem

Página: {page_name}
"""


async def analyze_screenshot(
    image_bytes: bytes,
    page_name: str,
) -> str:
    """Send a screenshot to Claude Vision and get executive narrative."""
    client = anthropic.AsyncAnthropic(
        api_key=settings.anthropic_api_key.get_secret_value()
    )

    b64_image = base64.b64encode(image_bytes).decode("utf-8")

    response = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": b64_image,
                        },
                    },
                    {
                        "type": "text",
                        "text": ANALYSIS_PROMPT.format(page_name=page_name),
                    },
                ],
            }
        ],
    )

    return response.content[0].text


class StorytellingPDF(FPDF):
    """Custom PDF with BGP GO branding."""

    def __init__(self):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.set_auto_page_break(auto=True, margin=20)

    def _header_bar(self):
        self.set_fill_color(*COLOR_PRIMARY)
        self.rect(0, 0, 210, 12, "F")
        self.set_fill_color(*COLOR_ACCENT)
        self.rect(0, 12, 210, 2, "F")

    def _footer_bar(self):
        self.set_y(-15)
        self.set_fill_color(*COLOR_PRIMARY)
        self.rect(0, self.get_y() + 5, 210, 10, "F")
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "", 7)
        self.cell(0, 10, "BGP GO - Expertise Financeira | Relatório gerado por IA", align="C")

    def cover_page(self, report_name: str):
        self.add_page()
        self._header_bar()

        # Title area
        self.set_y(80)
        self.set_font("Helvetica", "B", 28)
        self.set_text_color(*COLOR_PRIMARY)
        self.cell(0, 15, "Relatório Executivo", ln=True, align="C")

        self.set_font("Helvetica", "", 14)
        self.set_text_color(*COLOR_TEXT)
        self.cell(0, 10, "Análise com Storytelling por IA", ln=True, align="C")

        self.ln(10)
        self.set_fill_color(*COLOR_ACCENT)
        self.rect(70, self.get_y(), 70, 1, "F")

        self.ln(15)
        self.set_font("Helvetica", "", 12)
        self.cell(0, 8, report_name, ln=True, align="C")

        now = datetime.now().strftime("%d/%m/%Y às %H:%M")
        self.cell(0, 8, f"Gerado em {now}", ln=True, align="C")

        self._footer_bar()

    def add_analysis_page(
        self,
        page_name: str,
        narrative: str,
        screenshot: Optional[bytes] = None,
    ):
        self.add_page()
        self._header_bar()
        self.ln(18)

        # Page title
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*COLOR_PRIMARY)
        self.cell(0, 10, page_name, ln=True)

        self.set_fill_color(*COLOR_ACCENT)
        self.rect(10, self.get_y(), 40, 1, "F")
        self.ln(5)

        # Screenshot (if provided, scaled to fit width)
        if screenshot:
            try:
                img_stream = io.BytesIO(screenshot)
                # Save temp and embed — fpdf2 supports BytesIO
                self.image(img_stream, x=10, w=190)
                self.ln(5)
            except Exception as e:
                logger.warning("Failed to embed screenshot: %s", e)

        # Narrative
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*COLOR_TEXT)
        # Handle encoding — fpdf2 with built-in fonts uses latin-1
        safe_text = narrative.encode("latin-1", errors="replace").decode("latin-1")
        self.multi_cell(0, 6, safe_text)

        self._footer_bar()


async def generate_storytelling_pdf(
    screenshots: list[PageScreenshot],
    report_name: str,
    on_progress: Optional[callable] = None,
) -> bytes:
    """
    Generate a storytelling PDF from Power BI screenshots.

    Args:
        screenshots: List of page screenshots
        report_name: Name of the report for the cover page
        on_progress: Optional callback(step_index, total_steps, step_description)

    Returns:
        PDF file as bytes
    """
    total = len(screenshots)
    pdf = StorytellingPDF()
    pdf.cover_page(report_name)

    for i, shot in enumerate(screenshots):
        step_desc = f"Analisando {shot.page_name} ({i+1}/{total})"
        logger.info(step_desc)
        if on_progress:
            on_progress(i, total, step_desc)

        narrative = await analyze_screenshot(shot.image_bytes, shot.page_name)

        pdf.add_analysis_page(
            page_name=shot.page_name,
            narrative=narrative,
            screenshot=shot.image_bytes,
        )

    pdf_bytes = pdf.output()
    logger.info("Storytelling PDF generated: %d bytes, %d pages", len(pdf_bytes), total)
    return pdf_bytes
