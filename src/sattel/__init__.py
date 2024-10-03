import asyncio
import configparser
import os
import json
import sys

from contextlib import contextmanager
from typing import List, Optional, Iterator

from PFERD.crawl.crawler import DownloadToken, CrawlToken
from PFERD.transformer import RuleParseError
from PFERD.logging import log as pferd_log
from PFERD.crawl import CrawlError
from PFERD.pferd import Pferd, PferdLoadError
from PFERD.auth import AuthLoadError, AuthError
from PFERD.cli import ParserLoadError
from PFERD.config import Config, ConfigLoadError, ConfigOptionError


def load_config_parser() -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    Config.load_parser(parser)
    return parser


def die(e: Exception):
    log({"kind": "error", "exception": type(e).__name__, "error": str(e)})
    sys.exit(1)


def log(obj: dict):
    print(f"{json.dumps(obj)}")


def load_config() -> Config:
    try:
        return Config(load_config_parser())
    except (ConfigLoadError, ParserLoadError, Exception) as e:
        die(e)


def get_pferd(config: Config, json_args: str) -> Pferd:
    args = json.loads(json_args)

    crawlers: Optional[List[str]] = args.get("crawlers", None)
    skips: Optional[List[str]] = args.get("skips", None)

    try:
        return Pferd(config, crawlers, skips)
    except PferdLoadError as e:
        die(e)


async def run(pferd: Pferd) -> None:
    # These two functions must run inside the same event loop as the
    # crawlers, so that any new objects (like Conditions or Futures) can
    # obtain the correct event loop.
    pferd._load_authenticators()
    pferd._load_crawlers()

    for name in pferd._crawlers_to_run:
        crawler = pferd._crawlers[name]

        log({"kind": "crawl", "crawler": name})

        try:
            await crawler.run()
        except (CrawlError, AuthError, Exception) as e:
            die(e)


class DownloadBar:
    id = 0

    def __init__(self, path: str):
        self.id = DownloadBar.id
        DownloadBar.id += 1
        self.total = 0
        self.progress = 0
        log({"kind": "download_bar", "event": "begin",
             "path": path, "id": self.id})

    def advance(self, amount: float = 1):
        self.progress += amount
        log({"kind": "download_bar", "event": "advance",
            "progress": self.progress, "id": self.id})

    def set_total(self, total: float):
        self.total = total
        log({"kind": "download_bar", "event": "set_total",
            "total": total, "id": self.id})


class CrawlBar:
    id = 0

    def __init__(self, path: str):
        self.id = CrawlBar.id
        CrawlBar.id += 1
        log({"kind": "crawl_bar", "event": "begin",
             "path": path, "id": self.id})

    def advance(self, amount: float = 1):
        die(RuntimeError("advance should never be called on a crawl bar"))

    def set_total(self, total: float):
        die(RuntimeError("set_total should never be called on a crawl bar"))


def patch_log():
    def noop(*args, **kwargs):
        pass

    @contextmanager
    def crawl_bar(path: str) -> Iterator[CrawlBar]:
        bar = CrawlBar(path)
        try:
            yield bar
        finally:
            log({"kind": "crawl_bar", "event": "done", "id": bar.id})

    @contextmanager
    def download_bar(path: str) -> Iterator[DownloadBar]:
        bar = DownloadBar(path)
        try:
            yield bar
        finally:
            log({"kind": "download_bar", "event": "done", "id": bar.id})

    async def crawl_on_aenter(self) -> CrawlBar:
        await self._stack.enter_async_context(self._limiter.limit_crawl())
        bar = self._stack.enter_context(crawl_bar(str(self._path)))
        return bar

    async def download_on_aenter(self) -> DownloadBar:
        await self._stack.enter_async_context(self._limiter.limit_download())
        sink = await self._stack.enter_async_context(self._fs_token)
        bar = self._stack.enter_context(download_bar(str(self._path)))
        return bar, sink

    pferd_log.print = noop
    CrawlToken._on_aenter = crawl_on_aenter
    DownloadToken._on_aenter = download_on_aenter


def main():
    config = load_config()
    json_args = input()
    pferd = get_pferd(config, json_args)
    patch_log()
    try:
        if os.name == "nt":
            # A "workaround" for the windows event loop somehow crashing after
            # asyncio.run() completes. See:
            # https://bugs.python.org/issue39232
            # https://github.com/encode/httpx/issues/914#issuecomment-780023632
            # TODO Fix this properly
            loop = asyncio.get_event_loop()
            loop.run_until_complete(run(pferd))
            loop.run_until_complete(asyncio.sleep(1))
            loop.close()
        else:
            asyncio.run(run(pferd))
    except (ConfigOptionError, AuthLoadError, RuleParseError) as e:
        die(e)
    # except RuleParseError as e:
    #     TODO: e.pretty_print()
    #     die(e)
    # else:
    #     TODO: pferd.print_report()
    #     pass
