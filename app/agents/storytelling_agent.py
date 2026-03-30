"""
Storytelling agent — gera narrativa textual para páginas de dashboards Power BI usando Claude.
Quando a imagem da tela está disponível, usa visão para extrair números e insights reais.
"""

import base64
import logging
from typing import Optional

from anthropic import AsyncAnthropic

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_FALLBACK_TEMPLATE = (
    "Esta tela apresenta indicadores e análises relacionados a {page}. "
    "Os dados exibidos permitem acompanhar o desempenho e identificar tendências "
    "relevantes para a tomada de decisão estratégica."
)


async def generate_page_storytelling(
    page_display_name: str,
    report_name: str,
    image_bytes: Optional[bytes] = None,
) -> str:
    """
    Gera storytelling executivo para uma página de dashboard BI.

    Quando image_bytes é fornecido, usa a API de visão do Claude para ler os
    gráficos e KPIs reais da tela e produzir insights com números específicos.
    Caso contrário, gera narrativa textual genérica baseada no nome da tela.

    Args:
        page_display_name: Nome legível da página (ex: "Visão Geral de Vendas")
        report_name: Nome do relatório Power BI
        image_bytes: PNG da tela exportada (opcional)

    Returns:
        Texto de storytelling em português (2–4 parágrafos)
    """
    s = get_settings()
    api_key = s.anthropic_api_key.get_secret_value()
    model = s.anthropic_model or "claude-sonnet-4-6"

    client = AsyncAnthropic(api_key=api_key)
    try:
        if image_bytes:
            content = _build_vision_content(image_bytes, page_display_name, report_name)
            system = (
                "Você é um analista de negócios sênior especializado em interpretar "
                "dashboards de Business Intelligence e comunicar insights de forma executiva. "
                "Analise a imagem com atenção aos números, gráficos e tendências visíveis."
            )
        else:
            content = _build_text_content(page_display_name, report_name)
            system = (
                "Você é um analista de negócios sênior especializado em criar narrativas "
                "executivas para dashboards de Business Intelligence. "
                "Seu texto deve ser claro, objetivo e em português brasileiro."
            )

        response = await client.messages.create(
            model=model,
            max_tokens=800,
            system=system,
            messages=[{"role": "user", "content": content}],
        )

        text = response.content[0].text.strip()
        logger.info(
            "Storytelling gerado para '%s' (vision=%s)",
            page_display_name,
            image_bytes is not None,
        )
        return text

    except Exception as exc:
        logger.error("Erro ao gerar storytelling para '%s': %s", page_display_name, exc)
        return _FALLBACK_TEMPLATE.format(page=page_display_name)

    finally:
        await client.close()


def _build_vision_content(
    image_bytes: bytes,
    page_display_name: str,
    report_name: str,
) -> list:
    """Monta o conteúdo multimodal (imagem + texto) para a API de visão do Claude."""
    image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
    return [
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": image_b64,
            },
        },
        {
            "type": "text",
            "text": (
                f'Analise esta tela do dashboard "{report_name}" (página: "{page_display_name}") '
                "e gere um storytelling executivo em português brasileiro.\n\n"
                "O texto deve:\n"
                "- Descrever os principais indicadores visíveis com seus valores reais\n"
                "- Identificar tendências e variações mostradas nos gráficos\n"
                "- Destacar pontos de atenção ou resultados positivos relevantes\n"
                "- Ter tom executivo e objetivo\n"
                "- Ter 2 a 4 parágrafos\n"
                "- Basear-se APENAS nos dados visíveis na imagem\n\n"
                "Retorne apenas o texto do storytelling, sem títulos ou marcadores."
            ),
        },
    ]


def _build_text_content(page_display_name: str, report_name: str) -> str:
    """Monta o prompt textual para quando não há imagem disponível."""
    return (
        f'Crie um texto de storytelling executivo para a tela "{page_display_name}" '
        f'do relatório de BI "{report_name}".\n\n'
        "O texto deve:\n"
        "- Ter 2 a 3 parágrafos curtos\n"
        "- Explicar o objetivo desta tela e quais análises ela permite\n"
        "- Indicar como o usuário deve interpretar os indicadores apresentados\n"
        "- Usar tom executivo e objetivo\n"
        "- Não inventar valores ou números específicos\n"
        "- Ser escrito em português brasileiro\n\n"
        "Retorne apenas o texto do storytelling, sem títulos ou marcadores."
    )
