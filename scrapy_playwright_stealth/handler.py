from dataclasses import asdict, dataclass
from typing import TypeVar

from playwright.async_api import Page
from playwright_stealth import stealth_async
from scrapy import Request, Spider
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy_playwright.handler import Config as ScrapyPlaywrightConfig
from scrapy_playwright.handler import (
    ScrapyPlaywrightDownloadHandler,
)

__all__ = ["ScrapyPlaywrightStealthDownloadHandler"]

PlaywrightHandler = TypeVar("PlaywrightHandler", bound="ScrapyPlaywrightStealthDownloadHandler")


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

    async def _create_page(self, request: Request, spider: Spider) -> Page:
        page = await super()._create_page(request, spider)
        if self.config.use_stealth:
            await stealth_async(page)
        return page
