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


def die(e: Exception):
    log("error", {"exception": type(e).__name__, "error": str(e)})
    sys.exit(1)


def log(kind: str, obj: dict):
    obj["kind"] = kind
    print(json.dumps(obj))


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

        log("crawl", {"crawler": name})

        try:
            await crawler.run()
        except (CrawlError, AuthError) as e:
            die(e)

global_id = 0


class ProgressReport:
    def __init__(self, id: int):
        self.total = 0
        self.id = id
        log("pr_add", {"id": id})

    def advance(self, amount: float = 1):
        log("pr_advance", {"id": self.id, "amount": amount})

    def set_total(self, total: float):
        self.total = total
        log("pr_set_total", {"id": self.id, "total": total})


def disable_log():
    def noop(*args, **kwargs):
        pass

    @contextmanager
    def bar(*args):
        global global_id
        try:
            id = global_id
            global_id += 1
            yield ProgressReport(id)
        finally:
            log("pr_remove", {"id": id})

    pferd_log.print = noop
    pferd_log._bar = bar


def main():
    config = load_config()
    json_args = input()
    pferd = get_pferd(config, json_args)
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
    except (ConfigOptionError, AuthLoadError, RuleParseError) as e:
        die(e)
    # except RuleParseError as e:
    #     TODO: e.pretty_print()
    #     die(e)
    # else:
    #     TODO: pferd.print_report()
    #     pass
