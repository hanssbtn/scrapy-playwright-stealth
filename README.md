# scrapy-playwright-stealth: Playwright integration for Scrapy with stealth features 🥷🏻

`scrapy-playwright-stealth` is a minimal extension of [`scrapy-playwright`](https://github.com/scrapy-plugins/scrapy-playwright)
that integrates stealth features via [`tf-playwright-stealth`](https://github.com/tinyfish-io/tf-playwright-stealth).

This package simplifies the use of browser automation stealth techniques with Scrapy. It
automatically applies the spoofing methods provided by `tf-playwright-stealth` to every page
managed by `scrapy-playwright`.

## Installation

```bash
pip install scrapy-playwright-stealth
```

In case you haven't installed the browsers for playwright before, you will also need to run:

```bash
playwright install
```

## Activation

Replace the default `http` and/or `https` Download Handlers through `DOWNLOAD_HANDLERS` in
`settings.py`:

```python
# settings.py
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright_stealth.handler.ScrapyPlaywrightStealthDownloadHandler",
    "https": "scrapy_playwright_stealth.handler.ScrapyPlaywrightStealthDownloadHandler",
}
```

The download handler enables the stealth features by default. You can pass
`PLAYWRIGHT_USE_STEALTH=False` in `settings.py` to deactivate it. In this case the package will
behave exactly as `scrapy-playwright` behaves.

## Usage

Refer to the `scrapy-playwright` [documentation](https://github.com/scrapy-plugins/scrapy-playwright).
