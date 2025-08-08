from dataclasses import asdict, dataclass
from typing import TypeVar, cast
from playwright.async_api import Page, async_playwright
from playwright_stealth import Stealth 
from scrapy import Request, Spider
from scrapy.statscollectors import StatsCollector
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy_playwright.handler import Config as ScrapyPlaywrightConfig
from scrapy_playwright.handler import (
    ScrapyPlaywrightDownloadHandler,
)
import asyncio
import importlib.metadata
from packaging.version import Version
import logging

logger = logging.getLogger("scrapy-playwright-stealth")

__all__ = ["ScrapyPlaywrightStealthDownloadHandler"]

PlaywrightHandler = TypeVar("PlaywrightHandler", bound="ScrapyPlaywrightStealthDownloadHandler")
playwright_stealth_version = Version(importlib.metadata.version("playwright_stealth"))

@dataclass
class Config(ScrapyPlaywrightConfig):
    use_stealth: bool = True

    @classmethod
    def from_settings(cls, settings: Settings) -> "Config":
        base = super().from_settings(settings)
        base_dict = asdict(base)
        base_dict["use_stealth"] = settings.getbool("PLAYWRIGHT_USE_STEALTH", True)
        return cls(**base_dict)


class ScrapyPlaywrightStealthDownloadHandler(ScrapyPlaywrightDownloadHandler):

    def __init__(self, crawler: Crawler) -> None:
        super().__init__(crawler=crawler)
        self.config = Config.from_settings(crawler.settings)
        logger.debug("Using playwright_stealth version {playwright_stealth_version}")

    async def _launch(self) -> None:
        """Launch Playwright manager and configured startup context(s) with stealth mode option.
        This method was added to handle breaking changes in `playwright_stealth` version 2.0.0.
        :versionadded: 0.1.2
        """
        self.stats = cast(StatsCollector, self.stats) # Handle pyright errors
        if self.config.use_stealth:
            logger.info("Starting download handler")
            self.playwright_context_manager = Stealth().use_async(async_playwright()).manager
            self.playwright = await self.playwright_context_manager.start()
            self.browser_type = getattr(self.playwright, self.config.browser_type_name)
            if self.config.startup_context_kwargs:
                logger.info("Launching %i startup context(s)", len(self.config.startup_context_kwargs))
                await asyncio.gather(
                    *[
                        self._create_browser_context(name=name, context_kwargs=kwargs)
                        for name, kwargs in self.config.startup_context_kwargs.items()
                    ]
                )
                self._set_max_concurrent_context_count()
                logger.info("Startup context(s) launched")
                self.stats.set_value("playwright/page_count", self._get_total_page_count())
        else:
            await super()._launch()

    async def _create_page(self, request: Request, spider: Spider) -> Page:
        page = await super()._create_page(request, spider)
        if playwright_stealth_version < Version("2.0.0") and self.config.use_stealth:
            from playwright_stealth import stealth_async # pyright: ignore[reportAttributeAccessIssue]
            await stealth_async(page)
        return page
