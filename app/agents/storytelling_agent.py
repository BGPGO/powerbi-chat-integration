"""
Storytelling agent — gera narrativa textual para páginas de dashboards Power BI usando Claude.
"""

import logging

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
) -> str:
    """
    Usa Claude para gerar um texto executivo de storytelling para uma página de dashboard BI.

    Args:
        page_display_name: Nome legível da página (ex: "Visão Geral de Vendas")
        report_name: Nome do relatório Power BI

    Returns:
        Texto de storytelling em português (2–3 parágrafos)
    """
    s = get_settings()
    api_key = s.anthropic_api_key.get_secret_value()
    model = s.anthropic_model or "claude-sonnet-4-6"

    client = AsyncAnthropic(api_key=api_key)
    try:
        response = await client.messages.create(
            model=model,
            max_tokens=700,
            system=(
                "Você é um analista de negócios sênior especializado em criar narrativas "
                "executivas para dashboards de Business Intelligence. "
                "Seu texto deve ser claro, objetivo e em português brasileiro. "
                "Foque no propósito da tela e no que o usuário pode analisar nela."
            ),
            messages=[{
                "role": "user",
                "content": (
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
                ),
            }],
        )
        text = response.content[0].text.strip()
        logger.info("Storytelling gerado para página '%s'", page_display_name)
        return text

    except Exception as exc:
        logger.error("Erro ao gerar storytelling para '%s': %s", page_display_name, exc)
        return _FALLBACK_TEMPLATE.format(page=page_display_name)

    finally:
        await client.close()
