from dataclasses import asdict, dataclass
from typing import TypeVar, Any, Optional, Callable, cast
from playwright.async_api import Browser, Page, async_playwright
from playwright_stealth import Stealth 
from scrapy import Request, Spider
from scrapy.statscollectors import StatsCollector
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy_playwright.handler import Config as ScrapyPlaywrightConfig
from scrapy_playwright.handler import (
    ScrapyPlaywrightDownloadHandler,
    BrowserContextWrapper,
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
    
    async def _maybe_launch_browser_stealth(self) -> None:
        async with self.browser_launch_lock:
            if not hasattr(self, "browser"):
                logger.info("Launching stealth browser %s", self.browser_type.name)
                self.browser = await self.browser_type.launch(**self.config.launch_options)
                logger.info("Stealth browser %s launched", self.browser_type.name)
                self.stats.inc_value("playwright/browser_count")
                self.browser.on("disconnected", cast(Callable[[Browser], Any | None], self._browser_disconnected_callback))

    async def _create_browser_context(
        self, 
        name: str,
        context_kwargs: Optional[dict],
        spider: Optional[Spider] = None
    ) -> BrowserContextWrapper:
        if playwright_stealth_version >= Version("2.0.0") and self.config.use_stealth:
            context_kwargs = context_kwargs or {}
            persistent = remote = False
            if hasattr(self, "context_semaphore"):
                await self.context_semaphore.acquire()
            await self._maybe_launch_browser_stealth()
            context = await self.browser.new_context(**context_kwargs)
            context.on(
                "close", self._make_close_browser_context_callback(name, persistent, remote, spider)
            )
            self.stats.inc_value("playwright/context_count")
            self.stats.inc_value(f"playwright/context_count/persistent/{persistent}")
            self.stats.inc_value(f"playwright/context_count/remote/{remote}")
            logger.debug(
                "Stealth browser context started: '%s' (persistent=%s, remote=%s)",
                name,
                persistent,
                remote,
                extra={
                    "spider": spider,
                    "context_name": name,
                    "persistent": persistent,
                    "remote": remote,
                },
            )
            if self.config.navigation_timeout is not None:
                context.set_default_navigation_timeout(self.config.navigation_timeout)
            self.context_wrappers[name] = BrowserContextWrapper(
                context=context,
                semaphore=asyncio.Semaphore(value=self.config.max_pages_per_context),
                persistent=persistent,
            )
            self._set_max_concurrent_context_count()
            return self.context_wrappers[name]
        else:
            if hasattr(self, "context_semaphore") and self.context_semaphore.locked():
                self.context_semaphore.release()
            res: BrowserContextWrapper = await super()._create_browser_context(name, context_kwargs, spider)
            return res

    async def _close(self) -> None:
        if playwright_stealth_version >= Version("2.0.0") and self.playwright_context_manager_stealth:
            # NOTE: The 3 arguments are not used for some reason, so ¯\_(ツ)_/¯
            await self.playwright_context_manager_stealth.__aexit__(None, None, None)
        await super()._close()

    async def _launch(self) -> None:
        """Launch Playwright manager and configured startup context(s) with stealth mode option.
        This method was added to handle breaking changes in `playwright_stealth` version 2.0.0.
        :versionadded: 0.1.2
        """
        self.stats = cast(StatsCollector, self.stats) # Handle pyright errors
        if playwright_stealth_version >= Version("2.0.0") and self.config.use_stealth:
            logger.info("Starting download handler")
            self.playwright_context_manager_stealth = Stealth().use_async(async_playwright())
            self.playwright = await self.playwright_context_manager_stealth.__aenter__()
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
        logger.debug(f"{'*' * 100}\nwebdriver detected: {await page.evaluate("navigator.webdriver")}\n{'*' * 100}")
        if playwright_stealth_version < Version("2.0.0") and self.config.use_stealth:
            from playwright_stealth import stealth_async # pyright: ignore[reportAttributeAccessIssue]
            await stealth_async(page)
        return page
