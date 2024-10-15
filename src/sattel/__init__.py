import asyncio
import configparser
import os
import json
import sys
import keyring

from pathlib import Path
from contextlib import contextmanager
from typing import List, Optional, Iterator, Tuple, ContextManager

from PFERD.transformer import RuleParseError
from PFERD.utils import in_daemon_thread
from PFERD.logging import log as pferd_log
from PFERD.auth.keyring import KeyringAuthSection
from PFERD.auth import AUTHENTICATORS, KeyringAuthenticator, AuthLoadError, AuthError
from PFERD.crawl import CrawlError
from PFERD.pferd import Pferd, PferdLoadError
from PFERD.cli import ParserLoadError
from PFERD.config import Config, ConfigLoadError, ConfigOptionError


def die(e: Exception):
    log({"kind": "error", "exception": type(e).__name__, "message": str(e)})
    sys.exit(1)


def log(obj: dict):
    try:
        print(f"{json.dumps(obj, ensure_ascii=False)}", flush=True)
    except Exception:
        pass


def load_config_parser(path: Optional[Path] = None) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    Config.load_parser(parser, path)
    return parser


def load_config(path: Optional[Path] = None) -> Config:
    try:
        return Config(load_config_parser(path))
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
    # These two functions must run inside the same event loop as the crawlers, so that
    # any new objects (like Conditions or Futures) can obtain the correct event loop.
    pferd._load_authenticators()
    pferd._load_crawlers()

    for name in pferd._crawlers_to_run:
        crawler = pferd._crawlers[name]

        crawler_name = name.removeprefix("crawl:")
        log({"kind": "crawl", "name": crawler_name})
        log({"kind": "log", "info": f'run crawler "{crawler_name}"'})

        try:
            await crawler.run()
        except (CrawlError, AuthError, Exception) as e:
            die(e)


class ProgressBar:
    id = 0

    def __init__(self, kind: str, path: str):
        self.id = ProgressBar.id
        ProgressBar.id += 1
        self.total = 0
        self.progress = 0
        log(
            {
                "kind": "progressBar",
                "id": self.id,
                "event": {
                    "kind": "begin",
                    "bar": kind,
                    "path": path[1:-1],
                },
            }
        )

    def advance(self, amount: float = 1):
        self.progress += amount
        log(
            {
                "kind": "progressBar",
                "id": self.id,
                "event": {"kind": "advance", "progress": self.progress},
            }
        )

    def set_total(self, total: float):
        self.total = total
        log(
            {
                "kind": "progressBar",
                "id": self.id,
                "event": {"kind": "setTotal", "total": total},
            }
        )


def quiet_pferd():
    def noop(*args, **kwargs):
        pass

    @contextmanager
    def progress_bar(
        kind: str,
        path: str,
    ) -> Iterator[ProgressBar]:
        bar = ProgressBar(kind, path)
        try:
            yield bar
        finally:
            log({"kind": "progressBar", "id": bar.id, "event": {"kind": "done"}})

    def crawl_bar(self, action: str, path: str) -> ContextManager[ProgressBar]:
        return progress_bar("crawl", path)

    def download_bar(self, action: str, path: str) -> ContextManager[ProgressBar]:
        return progress_bar("download", path)

    def fail(*args, **kwargs):
        raise RuntimeError("Exclusive log output can't be used.")

    pferd_log.print = noop
    pferd_log.download_bar = download_bar
    pferd_log.crawl_bar = crawl_bar
    pferd_log.exclusive_output = fail


def request(subject: str):
    log({"kind": "log", "info": f'requesting "{subject}"'})
    log({"kind": "request", "subject": subject})
    try:
        return input()
    except Exception:
        pass


class SattelAuthenticator(KeyringAuthenticator):
    def __init__(self, name: str, section: KeyringAuthSection):
        super().__init__(name, section)

    async def credentials(self) -> Tuple[str, str]:
        # request the username
        if self._username is None:
            self._username = await in_daemon_thread(lambda: request("username"))

        # first try looking up the password in the keyring. do not look it up if it was
        # invalidated - we want to re-prompt in this case
        if self._password is None and not self._password_invalidated:
            self._password = keyring.get_password(self._keyring_name, self._username)

        # if that fails it wasn't saved in the keyring - we need to read it from the
        # user and store it
        if self._password is None:
            self._password = await in_daemon_thread(lambda: request("password"))
            keyring.set_password(self._keyring_name, self._username, self._password)

        self._password_invalidated = False
        return self._username, self._password

    def invalidate_credentials(self) -> None:
        log({"kind": "loginFailed"})
        super().invalidate_credentials()

    def invalidate_username(self) -> None:
        super().invalidate_username()

    def invalidate_password(self) -> None:
        super().invalidate_password()


AUTHENTICATORS["sattel"] = lambda n, s, c: SattelAuthenticator(n, KeyringAuthSection(s))


def main():
    argc = len(sys.argv)
    if argc <= 1:
        sys.exit(2)
    json_args = sys.argv[1]
    config_file_path = Path(sys.argv[2]) if argc > 2 else None

    log({"kind": "log", "info": f'got "jsonArgs": "{json_args}"'})
    log({"kind": "log", "info": f'got "configFilePath": "{config_file_path}"'})

    config = load_config(config_file_path)
    log({"kind": "log", "info": "loaded pferd config"})

    pferd = get_pferd(config, json_args)
    quiet_pferd()

    log({"kind": "log", "info": "run pferd"})
    try:
        if os.name == "nt":
            # a workaround for the windows event loop somehow crashing after
            # asyncio.run() completes. see:
            # https://bugs.python.org/issue39232
            # https://github.com/encode/httpx/issues/914#issuecomment-780023632
            # TODO: fix this properly
            loop = asyncio.get_event_loop()
            loop.run_until_complete(run(pferd))
            loop.run_until_complete(asyncio.sleep(1))
            loop.close()
        else:
            asyncio.run(run(pferd))
    except (ConfigOptionError, AuthLoadError, RuleParseError, Exception) as e:
        die(e)

    log({"kind": "done"})
    # except RuleParseError as e:
    #     TODO: e.pretty_print()
    #     die(e)
    # else:
    #     TODO: pferd.print_report()
    #     pass
