from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import getpass
import hashlib
import json
import logging
import os
import secrets
import signal
import sqlite3
import sys
import time
from pathlib import Path

from . import __version__
from .api import Telegram, VK
from .bridge import Bridge
from .common import Config, Permanent, Retry, Uncertain
from .store import Store


def validate_secret_values(tg_token: str, vk_token: str) -> None:
    if tg_token.lower().startswith("bot"):
        raise Permanent("Telegram bot token: do not include the 'bot' prefix")
    if tg_token.count(":") != 1:
        raise Permanent("Telegram bot token must contain exactly one ':'")
    bot_id, bot_secret = tg_token.split(":", 1)
    if not bot_id.isdigit() or not bot_secret or any(ch.isspace() for ch in tg_token):
        raise Permanent("Telegram bot token has invalid format")
    if (not vk_token or vk_token.lower() in {"none", "null", "n/a", "na"}
            or any(ch.isspace() for ch in vk_token)):
        raise Permanent("VK access token is empty, a placeholder, or contains whitespace")


def initialize(path: Path) -> None:
    path = path.resolve()
    if path.exists():
        raise Permanent("Configuration already exists; edit it instead of overwriting")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    owner = int(input("Твой числовой Telegram user ID: ").strip())
    group = int(input("ID отдельной Telegram-группы с темами (-100...): ").strip())
    proxy = input("Прокси Telegram (socks5://127.0.0.1:1080 или пусто): ").strip()
    tg_token = getpass.getpass("Токен отдельного Telegram-бота: ").strip()
    vk_token = getpass.getpass("Пользовательский access_token VK с доступом к сообщениям: ").strip()
    state = input("Каталог состояния [state]: ").strip() or "state"
    data = {"TG_OWNER_ID": owner, "TG_GROUP_ID": group, "TG_BOT_TOKEN_FILE": "secrets/telegram.token",
            "VK_ACCESS_TOKEN_FILE": "secrets/vk.token", "TG_PROXY": proxy, "STATE_DIR": state}
    Config(**data, _base=path.parent).validate()
    validate_secret_values(tg_token, vk_token)
    directory = path.parent / "secrets"
    directory.mkdir(exist_ok=True, mode=0o700)
    for name, value in (("telegram.token", tg_token), ("vk.token", vk_token)):
        p = directory / name
        with p.open("x", encoding="utf-8") as f:
            f.write(value + "\n")
        p.chmod(0o600)
    with path.open("x", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    path.chmod(0o600)
    print("Конфигурация создана. Выполни check, затем probe, затем probe --send-test.")


async def execute(args, c: Config, s: Store) -> None:
    vk = VK(c, c.secret("VK_ACCESS_TOKEN_FILE"))
    tg = Telegram(c, c.secret("TG_BOT_TOKEN_FILE"))
    b = Bridge(c, s, vk, tg)
    try:
        if args.action == "probe":
            report = await b.preflight()
            if args.send_test:
                uid = report["vk_user_id"]
                rid = secrets.randbelow(2**31 - 1) + 1
                token_hash = hashlib.sha256(vk.token.encode()).hexdigest()
                # Only the authenticated owner's Saved Messages, never a third party.
                response = await vk.call("messages.send", effect=True, peer_id=uid, random_id=rid,
                                         message="TeleVK: проверка доступа к личным сообщениям. Этот тест отправлен в Избранное моего VK-аккаунта.")
                mid = response if type(response) is int else response.get("message_id", 0)
                if not mid:
                    raise Uncertain("Write probe returned no message ID")
                check = await vk.resolve(uid, mid=mid)
                if not check or check.get("from_id") != uid:
                    raise Permanent("Test send returned an ID but could not be verified by reading it")
                s.set("write_probe_token", token_hash)
                s.set("write_probe_time", time.time())
                report["write_probe_ok"], report["test_message_id"] = True, mid
            if args.photo or args.document:
                if not args.send_test:
                    raise Permanent("Use --send-test together with --photo/--document; tests write to your Saved Messages")
                for field, kind in ((args.photo, "photo"), (args.document, "document")):
                    if field:
                        p = Path(field).expanduser().resolve()
                        if not p.is_file() or p.stat().st_size > c.MAX_FILE_MB * 1024**2:
                            raise Permanent("Invalid or oversized probe file")
                        attachment = await vk.upload(p, report["vk_user_id"], kind, p.name)
                        await vk.call("messages.send", effect=True, peer_id=report["vk_user_id"], random_id=secrets.randbelow(2**31 - 1) + 1,
                                      message=f"TeleVK: {kind} upload test", attachment=attachment)
                        report[kind + "_upload"] = "accepted by VK; check Saved Messages visually"
            print(json.dumps(report, ensure_ascii=False, indent=2))
        elif args.action == "updates":
            from .updates import check_updates
            print((await check_updates(vk.client, c))["text"])
        elif args.action == "run":
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, b.stop.set)
            await b.run()
    finally:
        await vk.close()
        await tg.close()


def main() -> int:
    os.umask(0o077)
    p = argparse.ArgumentParser(description="TeleVK — private single-owner bridge")
    p.add_argument("--config", default="config.json")
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="action", required=True)
    sub.add_parser("init", help="Interactive local config and secret setup")
    sub.add_parser("check", help="Offline config, permissions and database check")
    sub.add_parser("run", help="Run bridge; requires a successful write probe")
    sub.add_parser("updates", help="Check versions, never install updates")
    probe = sub.add_parser("probe", help="Check API access; no writes unless --send-test is provided")
    probe.add_argument("--send-test", action="store_true", help="Send a test to your VK Saved Messages")
    probe.add_argument("--photo", help="Optional photo upload/send test file")
    probe.add_argument("--document", help="Optional document upload/send test file")
    backup = sub.add_parser("backup", help="Offline SQLite backup; stop the service first")
    backup.add_argument("destination")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # HTTPX logs request URLs at INFO; Telegram's token is part of its URL.
    logging.getLogger("httpx").setLevel(logging.CRITICAL)
    logging.getLogger("httpcore").setLevel(logging.CRITICAL)
    store = None
    lock = None
    try:
        if sys.version_info < (3, 11):
            raise Permanent("Python 3.11+ is required")
        if args.action == "init":
            initialize(Path(args.config))
            return 0
        c = Config.load(args.config)
        tg_token = c.secret("TG_BOT_TOKEN_FILE")
        vk_token = c.secret("VK_ACCESS_TOKEN_FILE")
        validate_secret_values(tg_token, vk_token)
        c.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock = (c.state / "process.lock").open("a+")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Permanent("Another TeleVK process uses this STATE_DIR; stop it first or use Telegram commands") from None
        store = Store(c.state / "bridge.sqlite3")
        if args.action == "check":
            import importlib.metadata
            import shutil
            if not shutil.which("curl"):
                raise Permanent("curl is missing")
            if c.TG_PROXY.startswith("socks") or c.VK_PROXY.startswith("socks"):
                try:
                    importlib.metadata.version("socksio")
                except importlib.metadata.PackageNotFoundError:
                    raise Permanent("SOCKS proxy configured, but socksio is missing. Install requirements.txt.") from None
            result = store.db.execute("PRAGMA integrity_check").fetchone()[0]
            if result != "ok":
                raise Permanent("SQLite integrity_check failed")
            print(f"TeleVK {__version__}: config OK, secret permissions OK, SQLite OK. No API access was tested.")
        elif args.action == "backup":
            dest = Path(args.destination).resolve()
            if dest.exists():
                raise Permanent("Backup destination already exists")
            with sqlite3.connect(dest) as out:
                store.db.backup(out)
            dest.chmod(0o600)
            print(f"SQLite backup created: {dest}. Back up config and secrets separately and protect them.")
        else:
            asyncio.run(execute(args, c, store))
        return 0
    except (Permanent, Retry, Uncertain, ValueError, FileNotFoundError) as exc:
        # All provider errors have been normalized without tokens or private payloads.
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"Stopped safely after unexpected {type(exc).__name__}; no raw exception/URL logged. Check database and tests before restarting.", file=sys.stderr)
        return 1
    finally:
        if store:
            store.close()
        if lock:
            lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
