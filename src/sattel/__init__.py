import asyncio
import configparser
import os
import json
import sys

from contextlib import contextmanager
from typing import List, Optional

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


def die(e: Exception, context=None):
    log({"exception": type(e).__name__, "error": str(e), "context": context})
    sys.exit(1)


def log(obj):
    print("sattel: " + json.dumps(obj))


def load_config() -> Config:
    try:
        return Config(load_config_parser())
    except ConfigLoadError as e:
        die(e, message=e.reason)
    except ParserLoadError as e:
        die(e)


def get_pferd(json_input: str) -> Pferd:
    data = json.loads(json_input)

    crawlers: Optional[List[str]] = data.get("crawlers", None)
    skips: Optional[List[str]] = data.get("skips", None)

    config = load_config()
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

        print(f"running crawler {name}")

        try:
            await crawler.run()
        except (CrawlError, AuthError) as e:
            die(e)


class ProgressReport:
    id = 0

    def __init__(self):
        self.progress = 0
        self.total = 0
        self._id = ProgressReport.id
        ProgressReport.id += 1
        print(f"init bar {self._id}")

    def advance(self, amount: float = 1):
        self.progress += amount
        print(f"id={self._id} {self.progress}/{self.total}")

    def set_total(self, total: float):
        self.new_total = total
        print(f"id={self._id} total={self.total}")


def disable_log() -> None:
    def noop(*args, **kwargs):
        pass

    @contextmanager
    def bar(*args):
        yield ProgressReport()

    pferd_log.print = noop
    pferd_log._bar = bar


def main():
    pferd = get_pferd(input())
    disable_log()
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
    except (ConfigOptionError, AuthLoadError) as e:
        die(e)
    except RuleParseError as e:
        # TODO: e.pretty_print()
        die(e)
    else:
        # TODO: pferd.print_report()
        pass
