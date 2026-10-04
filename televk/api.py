from __future__ import annotations

import asyncio
import json
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .common import AccessDenied, Config, Permanent, RateGate, Retry, Uncertain, VKError, dumps, safe_name


def fields(data: dict) -> dict[str, str]:
    return {k: dumps(v) if isinstance(v, (dict, list, bool)) else str(v)
            for k, v in data.items() if v is not None}


async def request_json(client: httpx.AsyncClient, url: str, *, effect: bool = False,
                       label: str = "HTTP", data=None, files=None, timeout: float = 60) -> dict:
    """Never include an URL, token, response body or raw transport error in logs."""
    try:
        response = await client.post(url, data=data, files=files, timeout=timeout)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
        raise Retry(f"{label}: connection not established") from None
    except httpx.TransportError:
        if effect:
            raise Uncertain(f"{label}: response lost after request started") from None
        raise Retry(f"{label}: transport failure") from None
    if response.status_code == 429:
        delay = 30.0
        try:
            delay = float(response.json().get("parameters", {}).get("retry_after", 30))
        except (ValueError, AttributeError):
            pass
        raise Retry(f"{label}: rate limit", max(1, delay))
    if response.status_code >= 500:
        if effect:
            raise Uncertain(f"{label}: HTTP {response.status_code}; effect not known")
        raise Retry(f"{label}: HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError:
        if effect:
            raise Uncertain(f"{label}: invalid response; effect not known") from None
        raise Retry(f"{label}: invalid JSON") from None
    if not isinstance(body, dict):
        if effect:
            raise Uncertain(f"{label}: unexpected response shape")
        raise Permanent(f"{label}: unexpected response shape")
    if response.status_code in {401, 403} and "error" not in body and "ok" not in body:
        raise AccessDenied(f"{label}: access denied")
    if response.status_code >= 400 and "error" not in body and "ok" not in body:
        raise Permanent(f"{label}: HTTP {response.status_code}")
    return body


class VK:
    def __init__(self, config: Config, token: str, client: httpx.AsyncClient | None = None):
        self.c, self.token = config, token
        self.client = client or httpx.AsyncClient(proxy=config.VK_PROXY or None, trust_env=False, follow_redirects=False)
        self.gate = RateGate(config.VK_API_INTERVAL)

    async def close(self):
        await self.client.aclose()

    async def call(self, method: str, *, effect: bool = False, **params):
        await self.gate.wait()
        try:
            body = await request_json(self.client, f"https://api.vk.com/method/{method}",
                                      data=fields({**params, "access_token": self.token, "v": self.c.VK_API_VERSION}),
                                      effect=effect, label=f"VK {method}")
        except Retry as exc:
            self.gate.defer(exc.delay)
            raise
        if "error" in body:
            code = int(body["error"].get("error_code", -1))
            if code in {5, 27, 28}:
                raise AccessDenied(f"VK {method}: token rejected (error {code})")
            if code in {6, 9, 10, 29}:
                if effect and code == 10:
                    raise Uncertain(f"VK {method}: internal error after send")
                self.gate.defer(5 if code == 6 else 60)
                raise Retry(f"VK {method}: error {code}", 5 if code == 6 else 60)
            raise VKError(code, method)
        if "response" not in body:
            if effect:
                raise Uncertain(f"VK {method}: no response field")
            raise Permanent(f"VK {method}: no response field")
        return body["response"]

    async def server(self) -> dict:
        response = await self.call("messages.getLongPollServer", need_pts=1, lp_version=3)
        if not isinstance(response, dict) or "pts" not in response:
            raise Permanent("VK did not return a persistent cursor (pts)")
        return {"pts": int(response["pts"])}

    async def changes(self, cursor: dict) -> dict:
        return await self.call("messages.getLongPollHistory", pts=cursor["pts"], lp_version=3,
                               extended=1, msgs_limit=200, events_limit=1000)

    async def history(self, peer: int, anchor: int = 0, count: int = 200) -> dict:
        params = {"peer_id": peer, "count": count, "rev": 0, "extended": 1}
        if anchor:
            # Include anchor and filter it locally: avoids assuming offset semantics
            # when the anchor has just been deleted. Progress is checked by importer.
            params["start_message_id"] = anchor
        return await self.call("messages.getHistory", **params)

    async def resolve(self, peer: int, mid: int = 0, cmid: int = 0) -> dict:
        if cmid:
            r = await self.call("messages.getByConversationMessageId", peer_id=peer, conversation_message_ids=str(cmid), extended=0)
        elif mid:
            r = await self.call("messages.getById", message_ids=str(mid), extended=0)
        else:
            return {}
        return next((m for m in r.get("items", []) if m.get("peer_id") == peer), {})

    async def mark_read(self, peer: int, cmid: int, mid: int = 0):
        if cmid:
            return await self.call("messages.markAsRead", peer_id=peer, up_to_cmid=cmid)
        if mid:
            return await self.call("messages.markAsRead", peer_id=peer, start_message_id=mid)
        raise Permanent("No concrete message boundary; refusing to mark an entire conversation")

    async def upload(self, path: Path, peer: int, kind: str, filename: str) -> str:
        # These URLs come ONLY from the authenticated VK API, not a Telegram message.
        method = "photos.getMessagesUploadServer" if kind == "photo" else "docs.getMessagesUploadServer"
        args = {"peer_id": peer}
        if kind != "photo":
            args["type"] = "doc"
        server = await self.call(method, **args)
        url = server.get("upload_url", "")
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        if (parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443)
                or not any(host == d or host.endswith("." + d) for d in ("vk.com", "vk.ru", "userapi.com", "vkuser.net"))):
            raise Permanent("VK returned an untrusted upload host; review before expanding the allowlist")
        with path.open("rb") as stream:
            data = await request_json(self.client, url, files={"photo" if kind == "photo" else "file":
                                      (safe_name(filename), stream, "image/jpeg" if kind == "photo" else "application/octet-stream")},
                                      label="VK upload", timeout=180)
        if kind == "photo":
            if not all(k in data for k in ("server", "photo", "hash")):
                raise Permanent("VK photo upload returned incomplete data")
            saved = await self.call("photos.saveMessagesPhoto", **{k: data[k] for k in ("server", "photo", "hash")})
            if not isinstance(saved, list) or not saved:
                raise Permanent("VK did not save the photo")
            item, typ = saved[0], "photo"
        else:
            if not data.get("file"):
                raise Permanent("VK document upload did not return a file token")
            saved = await self.call("docs.save", file=data["file"], title=safe_name(filename))
            item, typ = saved.get("doc", {}), "doc"
        if "owner_id" not in item or "id" not in item:
            raise Permanent("VK saved attachment has no ID")
        result = f"{typ}{item['owner_id']}_{item['id']}"
        if item.get("access_key"):
            result += "_" + item["access_key"]
        return result


class Telegram:
    def __init__(self, config: Config, token: str, client: httpx.AsyncClient | None = None):
        self.c, self.token = config, token
        self.client = client or httpx.AsyncClient(proxy=config.TG_PROXY or None, trust_env=False, follow_redirects=False)
        self.gate = RateGate(config.TG_SEND_INTERVAL)
        self.bot_id, self.username = 0, ""

    async def close(self):
        await self.client.aclose()

    async def call(self, method: str, *, effect: bool = False, files=None, **params):
        try:
            body = await request_json(self.client, f"https://api.telegram.org/bot{self.token}/{method}",
                                      data=fields(params), files=files, effect=effect,
                                      label=f"Telegram {method}", timeout=180 if files else 65)
        except Retry as exc:
            self.gate.defer(exc.delay)
            raise
        if not body.get("ok"):
            code = int(body.get("error_code", -1))
            if code == 429:
                delay = int(body.get("parameters", {}).get("retry_after", 30))
                self.gate.defer(delay)
                raise Retry(f"Telegram {method}: rate limit", delay)
            if code == 401:
                raise AccessDenied("Telegram token rejected")
            # Controlled diagnostic enum, not API response text (could contain URLs).
            description = str(body.get("description", "")).lower()
            hint = next((h for h in ("message thread not found", "topic_closed", "not enough rights", "chat not found", "file is too big", "reply message not found", "message to be replied not found", "photo_invalid_dimensions", "image_process_failed") if h in description), "request rejected")
            raise Permanent(f"Telegram {method}: error {code} ({hint})")
        return body.get("result")

    def target(self, thread: int | None = None) -> dict:
        result = {"chat_id": self.c.TG_GROUP_ID}
        if thread and thread != 1:
            result["message_thread_id"] = thread
        return result

    async def send(self, data: dict, path: Path | None = None) -> dict:
        await self.gate.wait()
        params = self.target(data.get("thread"))
        params["disable_notification"] = bool(data.get("historical"))
        if data.get("reply_tg"):
            params["reply_parameters"] = {"message_id": data["reply_tg"], "allow_sending_without_reply": True}
        if path is None:
            return await self.call("sendMessage", effect=True, **params, text=data["text"], link_preview_options={"is_disabled": True})
        kind = "photo" if data.get("kind") == "photo" else "document"
        if path.stat().st_size > 10 * 1024 * 1024:
            kind = "document"
        caption = data.get("caption", "")[:900]
        with path.open("rb") as stream:
            return await self.call("sendPhoto" if kind == "photo" else "sendDocument", effect=True,
                                   **params, caption=caption,
                                   files={kind: (safe_name(data.get("filename", "file.bin")), stream, "application/octet-stream")})

    async def download(self, file_id: str, dest: Path, limit: int) -> None:
        item = await self.call("getFile", file_id=file_id)
        if item.get("file_size", 0) > min(limit, 20 * 1024 * 1024):
            raise Permanent("Telegram file exceeds the cloud getFile limit (20 MiB)")
        part = item.get("file_path", "")
        if not part or part.startswith("/") or ".." in part.split("/") or ":" in part:
            raise Permanent("Invalid Telegram file path")
        try:
            async with self.client.stream("GET", f"https://api.telegram.org/file/bot{self.token}/{part}", timeout=180) as response:
                if response.status_code != 200:
                    raise Retry(f"Telegram file HTTP {response.status_code}")
                total = 0
                with dest.open("wb") as out:
                    async for block in response.aiter_bytes(65536):
                        total += len(block)
                        if total > min(limit, 20 * 1024 * 1024):
                            raise Permanent("Telegram file exceeds the download limit")
                        out.write(block)
        except httpx.TransportError:
            raise Retry("Telegram file download interrupted") from None
