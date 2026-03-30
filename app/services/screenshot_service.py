"""
Screenshot service using Playwright.
Takes Power BI public embed URLs and captures screenshots of each page.
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Optional

from playwright.async_api import async_playwright, Browser, Page

logger = logging.getLogger(__name__)

RENDER_WAIT_MS = 8000  # Wait for Power BI visuals to fully render
NAV_TIMEOUT_MS = 60000  # Max wait for page navigation
VIEWPORT = {"width": 1920, "height": 1080}


@dataclass
class PageScreenshot:
    page_name: str
    page_index: int
    image_bytes: bytes


async def _wait_for_powerbi_render(page: Page):
    """Wait for Power BI report to fully render."""
    # Wait for the main visual container to appear
    try:
        await page.wait_for_selector(
            "visual-container, .visualContainer, .visual, explore-canvas",
            timeout=30000,
        )
    except Exception:
        logger.warning("Visual container selector not found, falling back to timer")

    # Extra wait for animations and data loading
    await asyncio.sleep(RENDER_WAIT_MS / 1000)


def _build_filtered_url(base_url: str, filters: Optional[dict] = None) -> str:
    """Build Power BI embed URL with OData filter parameters."""
    if not filters:
        return base_url

    filter_parts = []
    for table_col, value in filters.items():
        # Format: Table/Column eq 'value'
        if isinstance(value, list):
            conditions = " or ".join(
                f"{table_col} eq '{v}'" for v in value
            )
            filter_parts.append(f"({conditions})")
        else:
            filter_parts.append(f"{table_col} eq '{value}'")

    filter_str = " and ".join(filter_parts)

    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}$filter={filter_str}"


async def capture_report_screenshots(
    embed_url: str,
    filters: Optional[dict] = None,
    on_progress: Optional[callable] = None,
) -> list[PageScreenshot]:
    """
    Capture screenshots of a Power BI report.

    If the URL already contains filters (captured from iframe src), uses it directly.
    Otherwise, applies filters dict as OData query params.

    Args:
        embed_url: Power BI public embed URL
        filters: Optional dict of OData filters to apply
        on_progress: Optional callback(page_index, total_pages, page_name)

    Returns:
        List of PageScreenshot with PNG bytes for each page
    """
    url = _build_filtered_url(embed_url, filters)
    screenshots: list[PageScreenshot] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport=VIEWPORT,
            device_scale_factor=2,  # Retina quality
        )
        page = await context.new_page()

        logger.info("Navigating to Power BI report: %s", url[:100])
        await page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)
        await _wait_for_powerbi_render(page)

        # Try to find page navigation tabs
        page_tabs = await page.query_selector_all(
            ".navigation .pages-container button, "
            "[role='tab'], "
            ".pageTabs button, "
            "mat-tab-header .mat-tab-label"
        )

        if not page_tabs:
            # Single page report — screenshot the whole thing
            logger.info("Single page report detected")
            if on_progress:
                on_progress(0, 1, "Página 1")

            # Hide any toolbars/headers for clean screenshot
            await page.evaluate("""
                () => {
                    const header = document.querySelector('.logoBarWrapper, .reportHeader');
                    if (header) header.style.display = 'none';
                }
            """)

            img = await page.screenshot(type="png", full_page=False)
            screenshots.append(PageScreenshot(
                page_name="Página 1",
                page_index=0,
                image_bytes=img,
            ))
        else:
            total = len(page_tabs)
            logger.info("Found %d pages in report", total)

            for i, tab in enumerate(page_tabs):
                tab_text = await tab.inner_text()
                page_name = tab_text.strip() or f"Página {i + 1}"

                if on_progress:
                    on_progress(i, total, page_name)

                logger.info("Capturing page %d/%d: %s", i + 1, total, page_name)

                await tab.click()
                await asyncio.sleep(3)  # Wait for page transition + data load
                await _wait_for_powerbi_render(page)

                # Hide toolbar
                await page.evaluate("""
                    () => {
                        const header = document.querySelector('.logoBarWrapper, .reportHeader');
                        if (header) header.style.display = 'none';
                    }
                """)

                img = await page.screenshot(type="png", full_page=False)
                screenshots.append(PageScreenshot(
                    page_name=page_name,
                    page_index=i,
                    image_bytes=img,
                ))

        await browser.close()

    logger.info("Captured %d screenshots", len(screenshots))
    return screenshots
