from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import shutil
import socket
import tempfile
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from .common import Permanent, Retry, subprocess_env

BLOCKED_TRANSITION = [ipaddress.ip_network(n) for n in
                      ("64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32")]


def public_ip(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return False
    if not ip.is_global or ip.is_multicast or ip.is_reserved or ip.is_unspecified:
        return False
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped:
            return public_ip(str(ip.ipv4_mapped))
        if any(ip in net for net in BLOCKED_TRANSITION):
            return False
    return True


def validate_url(url: str) -> tuple[str, int]:
    if any(ord(c) < 33 or ord(c) == 127 for c in url):
        raise Permanent("Unsafe media URL characters")
    try:
        p = urlsplit(url)
        port = p.port
    except ValueError:
        raise Permanent("Malformed media URL") from None
    if p.scheme != "https" or not p.hostname or p.username or p.password or port not in (None, 443):
        raise Permanent("Media URL must be HTTPS on port 443 without credentials")
    host = p.hostname
    if "%" in host or host.lower().rstrip(".") in {"localhost", "localhost.localdomain"}:
        raise Permanent("Non-public media host")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if not public_ip(host):
            raise Permanent("Non-public media IP")
    return host, 443


async def resolve_public(host: str, port: int) -> str:
    try:
        rows = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        raise Retry("Media DNS lookup failed") from None
    addresses = list(dict.fromkeys(r[4][0] for r in rows))
    if not addresses or not all(public_ip(ip) for ip in addresses):
        raise Permanent("Media DNS contains a non-public address")
    return next((x for x in addresses if ":" not in x), addresses[0])


def disk_check(directory: Path, minimum: int, reserve: int = 0) -> None:
    if shutil.disk_usage(directory).free < minimum + reserve:
        raise Retry("Insufficient free disk space", 60)


async def download_public(url: str, dest: Path, limit: int, min_free: int) -> None:
    """curl connects to a vetted address with --resolve (no DNS rebind window).

    Automatic redirects, .curlrc and environment proxies are disabled. Signed
    URLs go through stdin configuration, not command line arguments or logs.
    Response bodies are streamed with an independent hard size cap.
    """
    if not shutil.which("curl"):
        raise Permanent("curl is required")
    current = url
    for _ in range(6):
        host, port = validate_url(current)
        ip = await resolve_public(host, port)
        if ":" in ip:
            ip = f"[{ip}]"
        disk_check(dest.parent, min_free, limit)
        fd, header_name = tempfile.mkstemp(prefix="hdr-", dir=dest.parent)
        os.close(fd)
        proc = None
        try:
            # JSON quoted strings are valid for these ASCII curl-config options.
            config = (f'url = {json.dumps(current)}\n'
                      f'resolve = {json.dumps(f"{host}:{port}:{ip}")}\n')
            proc = await asyncio.create_subprocess_exec(
                "curl", "--disable", "--config", "-", "--silent", "--show-error",
                "--proxy", "", "--noproxy", "*", "--proto", "=https", "--globoff",
                "--connect-timeout", "15", "--max-time", "180", "--max-filesize", str(limit),
                "--dump-header", header_name, "--header", "Accept-Encoding: identity",
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            proc.stdin.write(config.encode())
            await proc.stdin.drain()
            proc.stdin.close()
            size = 0
            with dest.open("wb") as out:
                while block := await asyncio.wait_for(proc.stdout.read(65536), timeout=195):
                    size += len(block)
                    if size > limit:
                        raise Permanent("Attachment exceeds MAX_FILE_MB")
                    disk_check(dest.parent, min_free)
                    out.write(block)
            status = await proc.wait()
            if Path(header_name).stat().st_size > 128 * 1024:
                raise Permanent("Attachment response headers are too large")
            raw = Path(header_name).read_text("iso-8859-1")
            blocks = raw.replace("\r\n", "\n").strip().split("\n\n")
            headers = blocks[-1].splitlines() if blocks else []
            if not headers or not headers[0].startswith("HTTP/"):
                raise Retry("Media download failed before HTTP response")
            code = int(headers[0].split()[1])
            values = {}
            for line in headers[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    values[k.lower().strip()] = v.strip()
            if code in {301, 302, 303, 307, 308}:
                location = values.get("location")
                if not location:
                    raise Permanent("Media redirect has no destination")
                current = urljoin(current, location)
                continue
            if status == 63 or int(values.get("content-length", "0")) > limit:
                raise Permanent("Attachment exceeds MAX_FILE_MB")
            if status or code >= 500 or code == 429:
                raise Retry("Media download interrupted or server temporarily unavailable")
            if code in {401, 403, 404, 410}:
                raise Permanent(f"Attachment no longer accessible (HTTP {code})")
            if code != 200:
                raise Permanent(f"Unexpected attachment response (HTTP {code})")
            if "content-length" in values and int(values["content-length"]) != size:
                raise Retry("Attachment Content-Length mismatch")
            return
        except (ValueError, IndexError):
            raise Permanent("Malformed attachment response headers") from None
        except (asyncio.TimeoutError, OSError):
            raise Retry("Media download failed") from None
        finally:
            if proc and proc.returncode is None:
                proc.kill()
                await proc.wait()
            Path(header_name).unlink(missing_ok=True)
    raise Permanent("Too many media redirects")
