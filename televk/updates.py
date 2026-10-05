from __future__ import annotations

import asyncio
import importlib.metadata
import platform
import re
import sqlite3

import httpx

from .common import Config, subprocess_env

# Update discovery deliberately uses GitHub only. The HTPC does not need PyPI access.
PACKAGE_REPOS = {
    "httpx": "encode/httpx",
    "httpcore": "encode/httpcore",
    "anyio": "agronholm/anyio",
    "h11": "python-hyper/h11",
    "idna": "kjd/idna",
    "certifi": "certifi/python-certifi",
    "socksio": "sethmlarson/socksio",
    "typing_extensions": "python/typing_extensions",
}


def stable_version(value: str) -> tuple[int, ...] | None:
    value = value.lstrip("v")
    if not re.fullmatch(r"\d+(?:\.\d+){1,3}", value):
        return None
    return tuple(map(int, value.split(".")))


def version_from_tag(tag: str) -> str | None:
    """Extract a stable dotted numeric version from common GitHub tag formats."""
    value = str(tag).strip().replace("_", ".")
    match = re.search(r"(\d+(?:\.\d+){1,3})$", value)
    if not match:
        return None
    version = match.group(1)
    return version if stable_version(version) is not None else None


def important_update(current: str, latest: str, *, minor: bool = False) -> bool:
    a, b = stable_version(current), stable_version(latest)
    if a is None or b is None or b <= a:
        return False
    # Before 1.0 a minor change may be breaking; certificate bundles use calendar versions.
    return (b[:2] > a[:2]) if minor or a[0] == 0 else b[0] > a[0]


async def check_updates(client: httpx.AsyncClient, config: Config) -> dict:
    lines, important, failed = [], [], []

    async def get(url: str):
        r = await client.get(url, timeout=15, headers={
            "User-Agent": "TeleVK/0.1.1 version-check",
            "Accept": "application/vnd.github+json" if "api.github.com" in url else "*/*",
        })
        r.raise_for_status()
        return r

    async def github_latest_tag(repo: str) -> str:
        # Tags are used instead of PyPI so update checks work on hosts where PyPI is unreachable.
        obj = (await get(f"https://api.github.com/repos/{repo}/tags?per_page=100")).json()
        if not isinstance(obj, list):
            raise ValueError("GitHub tags response is not a list")
        versions = []
        for item in obj:
            if not isinstance(item, dict):
                continue
            version = version_from_tag(item.get("name", ""))
            if version is not None:
                versions.append((stable_version(version), version))
        if not versions:
            raise ValueError("No stable numeric tags")
        return max(versions, key=lambda x: x[0])[1]

    for name, repository in PACKAGE_REPOS.items():
        try:
            installed = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
        try:
            latest = await github_latest_tag(repository)
            big = important_update(installed, latest) and name != "certifi"
            note = f"{name}: {installed} → GitHub {latest}" + (" [крупное обновление]" if big else "")
            lines.append(note)
            if big:
                important.append(note)
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            failed.append(name)

    try:
        obj = (await get("https://raw.githubusercontent.com/VKCOM/vk-api-schema/master/package.json")).json()
        latest = obj["version"]
        lines.append(f"VK schema: GitHub {latest}; configured API v={config.VK_API_VERSION}")
        ver = stable_version(latest)
        configured = stable_version(config.VK_API_VERSION)
        if ver and configured and ver[:2] > configured[:2]:
            important.append(f"VK schema branch changed: {config.VK_API_VERSION} → {latest}")
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        failed.append("VK schema")

    # Runtime libraries bundled with the OS are reported but never auto-updated by TeleVK.
    lines.append(f"Runtime: Python {platform.python_version()}, SQLite {sqlite3.sqlite_version}")

    for executable, repository, pattern in (
        ("curl", "curl/curl", r"curl ([0-9.]+)"),
        ("systemd", "systemd/systemd", r"systemd (\d+)"),
    ):
        try:
            proc = await asyncio.create_subprocess_exec(
                executable, "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=subprocess_env(),
            )
            try:
                output, _ = await asyncio.wait_for(proc.communicate(), 5)
            finally:
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
            match = re.search(pattern, output.decode(errors="replace"))
            if not match:
                raise ValueError()
            current = match.group(1)
            release = (await get(f"https://api.github.com/repos/{repository}/releases/latest")).json()
            latest = str(release["tag_name"]).removeprefix("curl-").removeprefix("v").replace("_", ".")
            lines.append(f"{executable}: {current} → GitHub latest {latest}")
            if executable == "systemd":
                newer = bool(re.fullmatch(r"\d+(?:\.\d+)*", latest)) and int(latest.split(".")[0]) > int(current)
            else:
                newer = important_update(current, latest)
            if newer:
                important.append(f"{executable}: {current} → upstream {latest}; учитывать пакет дистрибутива")
        except (OSError, asyncio.TimeoutError, httpx.HTTPError, ValueError, KeyError, TypeError):
            failed.append(executable)

    if failed:
        lines.append("НЕ ПРОВЕРЕНО: " + ", ".join(failed) + ". Это не означает отсутствие обновлений.")
    lines.append(
        "Источники проверки: GitHub и локальные версии; PyPI не используется. "
        "Ничего не установлено. Это проверка крупных версий, не аудит уязвимостей. "
        "Python/curl/systemd/системный SQLite обновляются штатным менеджером пакетов ОС."
    )
    return {"text": "Проверка компонентов\n" + "\n".join(lines), "important": important, "failed": failed}
