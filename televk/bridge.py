from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import shutil
import socket
import sqlite3
import tempfile
import time
from pathlib import Path

from . import __version__
from .api import Telegram, VK
from .common import AccessDenied, Config, Permanent, Retry, Uncertain, VKError, authorized, dumps, split_text
from .media import disk_check, download_public
from .planner import message_parts, needs_hydration, plan_vk, tg_payload
from .store import Store

LOG = logging.getLogger("televk")
HELP = """TeleVK — VK ↔ Telegram
/status — состояние и очереди
/chats [offset] — диалоги VK и peer_id
/chat PEER_ID [Название] — создать тему
/bind PEER_ID — привязать текущую тему (в том числе после сбоя создания)
/alias Название — переименовать текущую тему
/read — прочитать до последнего доставленного входящего; reply на сообщение — до него
/history all — вся доступная история текущего чата
/history status | cancel — прогресс / остановка импорта
/mute [THREAD_ID] | /unmute [THREAD_ID] | /muted
/dlq — последние ошибки и неопределённые отправки
/retry_dlq ID [force] — повтор; force может создать дубликат
/updates — проверить версии компонентов, ничего не устанавливать
/resync — сверить сообщения после последнего успешного чтения
/help
Обычный текст, фото или документ из привязанной темы отправляются в VK.
Reply на входящее отмечает VK прочитанным только после подтверждения отправки.
Редактирование и удаление не синхронизируются. /mute касается только VK → Telegram."""


def notify_systemd(message: str) -> None:
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    with contextlib.suppress(OSError):
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(address)
            sock.sendall(message.encode())


class Bridge:
    def __init__(self, c: Config, s: Store, vk: VK, tg: Telegram):
        self.c, self.s, self.vk, self.tg = c, s, vk, tg
        self.stop = asyncio.Event()
        self.topic_locks: dict[int, asyncio.Lock] = {}
        self.media_slots = asyncio.Semaphore(2)
        self.vk_send_locks: dict[int, asyncio.Lock] = {}
        self.started = time.time()
        self.media_dir = c.state / "media"
        self.media_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    def notify(self, key: str, text: str, thread: int = 0, reply_tg: int = 0, priority: int = 0) -> None:
        # Independent lanes prevent a failed notice blocking every control message.
        for i, part in enumerate(split_text(text)):
            self.s.add_job(f"notice:{key}:{i}", "notice", {"text": part, "thread": thread, "reply_tg": reply_tg},
                           lane=f"notice:{key}", priority=priority)

    def capacity(self, *, queue: bool = True) -> None:
        disk_check(self.c.state, self.c.MIN_FREE_MB * 1024**2)
        size = sum(p.stat().st_size for p in self.c.state.glob("bridge.sqlite3*") if p.is_file())
        if size > self.c.MAX_DB_MB * 1024**2:
            raise Retry("Database budget reached; stop and increase MAX_DB_MB or review retention", 60)
        if queue and self.s.backlog() >= self.c.MAX_PENDING:
            raise Retry("Queue budget reached; producers paused without advancing cursors", 15)

    async def preflight(self) -> dict:
        me = await self.tg.call("getMe")
        self.tg.bot_id, self.tg.username = int(me["id"]), me.get("username", "")
        chat = await self.tg.call("getChat", chat_id=self.c.TG_GROUP_ID)
        if chat.get("type") != "supergroup" or not chat.get("is_forum"):
            raise Permanent("TG_GROUP_ID must point to a supergroup with topics enabled")
        member = await self.tg.call("getChatMember", chat_id=self.c.TG_GROUP_ID, user_id=me["id"])
        if member.get("status") not in {"administrator", "creator"} or not member.get("can_manage_topics", member.get("status") == "creator"):
            raise Permanent("Telegram bot needs administrator status and Manage Topics permission")
        webhook = await self.tg.call("getWebhookInfo")
        if webhook.get("url"):
            raise Permanent("This bot has a webhook. Use a separate bot or deliberately remove its old webhook first.")
        users = await self.vk.call("users.get")
        if not isinstance(users, list) or not users or not users[0].get("id"):
            raise Permanent("VK token must represent a user, not a community")
        uid = int(users[0]["id"])
        conversations = await self.vk.call("messages.getConversations", count=1, extended=1)
        self.s.names(conversations)
        items = conversations.get("items", [])
        peer = items[0]["conversation"]["peer"]["id"] if items else uid
        await self.vk.history(peer, count=1)
        cursor = await self.vk.server()
        changes = await self.vk.changes(cursor)
        self.validate_changes(changes, cursor)
        self.s.bind_identity(uid, self.tg.bot_id, self.c.TG_GROUP_ID, self.c.TG_OWNER_ID)
        return {"vk_user_id": uid, "telegram_bot_id": self.tg.bot_id,
                "telegram_group_id": self.c.TG_GROUP_ID, "read_access": True,
                "write_probe_ok": self.s.get("write_probe_token") == hashlib.sha256(self.vk.token.encode()).hexdigest()}

    @staticmethod
    def validate_changes(response: dict, previous: dict) -> tuple[list[dict], dict]:
        if not isinstance(response, dict) or "new_pts" not in response:
            raise Permanent("VK changes have no new_pts; cursor was NOT advanced")
        box = response.get("messages", {})
        if not isinstance(box, dict) or not isinstance(box.get("items", []), list):
            raise Permanent("Unsupported VK changes format; cursor was NOT advanced")
        items = box.get("items", [])
        if box.get("count", len(items)) > len(items) and not response.get("more"):
            raise Permanent("VK changes appear truncated; cursor was NOT advanced; use /resync")
        pts = int(response["new_pts"])
        if pts < previous["pts"] or (response.get("more") and pts == previous["pts"]):
            raise Permanent("VK returned a non-progressing cursor; use /resync")
        for m in items:
            Store.vk_key(m)
        return items, {"pts": pts}

    async def topic(self, peer: int) -> int:
        lock = self.topic_locks.setdefault(peer, asyncio.Lock())
        async with lock:
            self.s.ensure_route(peer)
            r = self.s.route(peer)
            if r["thread"]:
                return int(r["thread"])
            if r["topic_state"] == "uncertain":
                raise Retry(f"Topic creation result unknown. In the existing topic run /bind {peer}; or create a topic manually and bind it", 60)
            # Resolve a human-friendly title; title lookup is not required for delivery.
            title = r["title"]
            try:
                info = await self.vk.call("messages.getConversationsById", peer_ids=str(peer), extended=1)
                self.s.names(info)
                it = info.get("items", [])
                if it and title == f"VK {peer}":
                    title = it[0].get("chat_settings", {}).get("title") or self.s.name(peer)
            except (Retry, Permanent):
                pass
            self.s.db.execute("UPDATE routes SET topic_state='creating' WHERE peer=?", (peer,))
            try:
                await self.tg.gate.wait()
                out = await self.tg.call("createForumTopic", effect=True, chat_id=self.c.TG_GROUP_ID, name=title[:128])
                thread = int(out["message_thread_id"])
                with self.s.tx():
                    self.s.bind(peer, thread)
                    self.s.db.execute("UPDATE routes SET title=? WHERE peer=?", (title, peer))
                return thread
            except (Retry, Permanent):
                self.s.db.execute("UPDATE routes SET topic_state='new' WHERE peer=?", (peer,))
                raise
            except Uncertain:
                self.s.db.execute("UPDATE routes SET topic_state='uncertain' WHERE peer=?", (peer,))
                self.notify(f"topic-uncertain:{peer}", f"Создание темы для VK {peer}: результат неизвестен. Проверь группу. В созданной теме выполни /bind {peer}; если темы нет, создай вручную и привяжи.")
                raise Retry("Bind the VK conversation to a topic after ambiguous creation", 60) from None

    async def hydrate_vk_message(self, message: dict) -> dict:
        if not needs_hydration(message):
            return message
        peer = int(message.get("peer_id", 0))
        mid = int(message.get("id", 0))
        cmid = int(message.get("conversation_message_id", 0))
        try:
            if mid:
                full = await self.vk.resolve(peer, mid=mid)
            elif cmid:
                full = await self.vk.resolve(peer, cmid=cmid)
            else:
                return message
        except Permanent as exc:
            # Do not stop text delivery for a permanently unavailable/deleted media object.
            LOG.warning("VK message hydration skipped for peer %s: %s", peer, exc)
            return message
        return {**message, **full} if full else message

    async def poll_vk(self) -> None:
        while not self.stop.is_set():
            try:
                if self.s.get("vk_paused"):
                    await asyncio.sleep(5)
                    continue
                self.capacity()
                cursor = self.s.get("vk_cursor")
                if cursor is None:
                    connected = time.time()
                    cursor = await self.vk.server()
                    with self.s.tx():
                        self.s.set("vk_cursor", cursor)
                        self.s.set("connected_at", connected)
                response = await self.vk.changes(cursor)
                items, updated = self.validate_changes(response, cursor)
                hydrated = [await self.hydrate_vk_message(message) for message in items]
                self.s.ingest_vk_batch(hydrated, updated, response)
                if not response.get("more"):
                    await asyncio.sleep(self.c.VK_POLL_SECONDS)
            except VKError as exc:
                if exc.code == 907:
                    await self.resync("VK cursor expired (907)")
                else:
                    self.s.set("vk_paused", str(exc))
                    self.notify("vk-pause:" + str(time.time_ns()), str(exc) + ". Чтение VK приостановлено; см. /status и руководство.")
            except Retry as exc:
                self.s.set("vk_last_error", str(exc))
                await asyncio.sleep(exc.delay)
            except Permanent as exc:
                self.s.set("vk_paused", str(exc))
                self.notify("vk-pause:" + str(time.time_ns()), str(exc) + ". Проверь доступ/версию API; при смене токена нужен probe и перезапуск.")

    async def poll_tg(self) -> None:
        if self.s.get("tg_connected_at") is None:
            self.s.set("tg_connected_at", int(time.time()))
        while not self.stop.is_set():
            try:
                # Reserve control access when history fills the queue. Disk/DB
                # caps still apply; only the owner can add Telegram inbox records.
                self.capacity(queue=False)
                updates = await self.tg.call("getUpdates", offset=self.s.get("tg_offset", 0), timeout=40,
                                             allowed_updates=["message", "edited_message"])
                with self.s.tx():
                    for update in updates:
                        message = update.get("message") or update.get("edited_message") or {}
                        if authorized(message, self.c.TG_OWNER_ID, self.c.TG_GROUP_ID) and message.get("date", 0) >= self.s.get("tg_connected_at"):
                            self.s.ingest("tg", str(update["update_id"]), update)
                    if updates:
                        self.s.set("tg_offset", max(x["update_id"] for x in updates) + 1)
                    self.s.set("tg_last_ok", time.time())
            except Retry as exc:
                self.s.set("tg_last_error", str(exc))
                await asyncio.sleep(exc.delay)

    async def plan_inbox(self) -> None:
        while not self.stop.is_set():
            rows = self.s.db.execute("SELECT * FROM inbox WHERE processed=0 ORDER BY created,key LIMIT 100").fetchall()
            for row in rows:
                payload = json.loads(row["payload"])
                if (row["source"] == "vk" and payload.get("out") and not self.s.echo(payload)
                        and self.s.db.execute("SELECT 1 FROM jobs WHERE peer=? AND kind='vk_send' AND state='running' AND effect=1", (row["peer"],)).fetchone()):
                    continue
                with self.s.tx():
                    if row["source"] == "vk":
                        plan_vk(self.s, row["key"], payload, self.c.TIMEZONE)
                    else:
                        self.plan_telegram(row["key"], payload)
                    self.s.db.execute("UPDATE inbox SET processed=1 WHERE source=? AND key=?", (row["source"], row["key"]))
            await asyncio.sleep(0.2 if rows else 0.5)

    def plan_telegram(self, key: str, update: dict) -> None:
        m = update.get("message") or update.get("edited_message") or {}
        if not authorized(m, self.c.TG_OWNER_ID, self.c.TG_GROUP_ID):
            return
        thread = int(m.get("message_thread_id") or 0)
        if "edited_message" in update:
            self.notify(f"edit:{key}", "Редактирование не синхронизируется с VK. Исходная отправка не изменена.", thread, m["message_id"])
            return
        # Owner-generated forum housekeeping is not an outgoing VK message.
        if any(k.startswith("forum_topic_") or k in {"new_chat_members", "left_chat_member", "pinned_message"} for k in m):
            return
        text = str(m.get("text") or "")
        if text.startswith("/"):
            first = text.split()[0]
            if "@" in first and first.split("@", 1)[1].lower() != self.tg.username.lower():
                return
            self.s.add_job(f"command:{key}", "command", {"message": m, "update": key}, lane=f"command:{thread}", priority=-10)
            return
        route = self.s.route(thread=thread) if thread else None
        if not route:
            marker = f"no_route_warned:{thread}"
            if not self.s.get(marker):
                self.s.set(marker, True)
                self.notify(f"no-route:{thread}", "Тема не привязана к VK. Используй /chat PEER_ID в General или /bind PEER_ID в этой теме.", thread, m["message_id"])
            return
        try:
            data = tg_payload(m, self.s, route["peer"])
            if data.get("unsupported"):
                self.notify(f"partial-type:{key}", f"Тип Telegram {data['unsupported']} пока не поддерживается. В VK будет передан только текст; файл можно отправить как документ.", thread, m["message_id"])
            self.s.add_job(f"tg:{key}", "vk_send", data, peer=route["peer"], lane="vk_out", priority=-1)
        except Permanent as exc:
            self.notify(f"unsupported:{key}", str(exc), thread, m["message_id"])

    async def resync(self, reason: str = "manual") -> None:
        since = max(float(self.s.get("connected_at", time.time())), float(self.s.get("vk_last_ok", time.time())) - 60)
        cursor = await self.vk.server()
        with self.s.tx():
            self.s.set("vk_cursor", cursor)
            self.s.set("vk_paused", None)
            self.s.set("recovery", {"since": since, "offset": 0})
            self.notify("resync:" + str(time.time_ns()), f"Сверяю историю после разрыва: {reason}. Новые события продолжают приниматься. Удалённые или недоступные сообщения восстановить по истории может не получиться.")

    def start_import(self, peer: int, since: float = 0) -> None:
        row = self.s.db.execute("SELECT * FROM imports WHERE peer=?", (peer,)).fetchone()
        if row and row["state"] not in {"done", "cancelled", "error"}:
            # An explicit all-history request supersedes an in-progress gap scan.
            if since or not row["since"]:
                return
        if row and row["state"] == "cancelled":
            self.s.db.execute("UPDATE jobs SET state='pending',next_at=0,updated=? WHERE peer=? AND lane LIKE 'archive_%' AND state='suppressed'", (time.time(), peer))
        self.s.db.execute("DELETE FROM archive WHERE peer=?", (peer,))
        self.s.db.execute("INSERT OR REPLACE INTO imports(peer,state,since,updated) VALUES(?,'scanning',?,?)", (peer, since, time.time()))

    async def import_page(self, task: dict) -> None:
        peer, anchor = task["peer"], task["anchor"]
        response = await self.vk.history(peer, anchor)
        raw = response.get("items", [])
        rows = [m for m in raw if int(m.get("id", 0)) > 0 and (not anchor or int(m["id"]) < anchor)]
        if raw and not rows and len(raw) > 1:
            raise Permanent("History pagination did not advance; cursor left unchanged")
        if any(int(m.get("id", 0)) <= 0 for m in raw):
            raise Permanent("History contains messages without global IDs; review pagination before importing")
        crossed = bool(task["since"] and any(m.get("date", 0) < task["since"] for m in rows))
        with self.s.tx():
            self.s.names(response)
            for m in rows:
                if task["since"] and m.get("date", 0) < task["since"]:
                    continue
                if int(m.get("peer_id", peer)) != peer:
                    raise Permanent("VK returned history for a different peer")
                m["peer_id"], m["_historical"] = peer, True
                self.s.db.execute("INSERT OR IGNORE INTO archive VALUES(?,?,?,?,?,0)", (peer, m["id"], m.get("conversation_message_id", 0), m.get("date", 0), dumps(m)))
            if rows:
                self.s.db.execute("UPDATE imports SET anchor=?,upper_mid=MAX(upper_mid,?),scanned=scanned+?,updated=? WHERE peer=?", (min(m["id"] for m in rows), max(m["id"] for m in rows), len(rows), time.time(), peer))
            if not raw or not rows or crossed or len(raw) < 200:
                self.s.db.execute("UPDATE imports SET state='enqueueing',updated=? WHERE peer=?", (time.time(), peer))

    async def import_loop(self) -> None:
        while not self.stop.is_set():
            try:
                if self.s.get("vk_paused"):
                    await asyncio.sleep(5)
                    continue
                self.capacity()
                recovery = self.s.get("recovery")
                if recovery:
                    response = await self.vk.call("messages.getConversations", count=200, offset=recovery["offset"], extended=1)
                    with self.s.tx():
                        self.s.names(response)
                        for it in response.get("items", []):
                            if it.get("last_message", {}).get("date", 0) >= recovery["since"]:
                                peer = int(it["conversation"]["peer"]["id"])
                                self.s.ensure_route(peer)
                                self.start_import(peer, recovery["since"])
                        if len(response.get("items", [])) < 200:
                            self.s.set("recovery", None)
                        else:
                            self.s.set("recovery", {**recovery, "offset": recovery["offset"] + 180})
                tasks = self.s.db.execute("SELECT * FROM imports WHERE state IN ('scanning','enqueueing') ORDER BY updated LIMIT 8").fetchall()
                for row in tasks:
                    task = dict(row)
                    route = self.s.route(peer=task["peer"])
                    if route and route["muted"]:
                        continue
                    try:
                        self.capacity()
                        if task["state"] == "scanning":
                            await self.import_page(task)
                        else:
                            rows = self.s.db.execute("SELECT * FROM archive WHERE peer=? AND queued=0 ORDER BY stamp,mid LIMIT 30", (task["peer"],)).fetchall()
                            with self.s.tx():
                                for msg in rows:
                                    m = json.loads(msg["payload"])
                                    plan_vk(self.s, Store.vk_key(m), m, self.c.TIMEZONE)
                                    self.s.db.execute("UPDATE archive SET queued=1 WHERE peer=? AND mid=?", (task["peer"], msg["mid"]))
                                self.s.db.execute("UPDATE imports SET updated=? WHERE peer=?", (time.time(), task["peer"]))
                                if not rows:
                                    self.s.db.execute("UPDATE imports SET state='done',updated=? WHERE peer=?", (time.time(), task["peer"]))
                                    self.notify(f"history-done:{task['peer']}:{task['updated']}", "История прочитана из VK и поставлена в очередь Telegram. Доставка может ещё продолжаться: /status.", route["thread"] if route else 0)
                    except Permanent as exc:
                        self.s.db.execute("UPDATE imports SET state='error',error=?,updated=? WHERE peer=?", (str(exc), time.time(), task["peer"]))
                        self.notify(f"history-error:{task['peer']}:{task['updated']}", str(exc) + ". Импорт остановлен, сохранённая очередь не удалена.", route["thread"] if route else 0)
                await asyncio.sleep(1)
            except Retry as exc:
                await asyncio.sleep(exc.delay)

    async def command(self, job: dict, data: dict) -> None:
        m, key = data["message"], data["update"]
        # Re-check authorization on execution, not only at ingestion.
        if not authorized(m, self.c.TG_OWNER_ID, self.c.TG_GROUP_ID):
            return
        thread, reply_id = int(m.get("message_thread_id") or 0), int(m["message_id"])
        words = m["text"].split()
        cmd, args = words[0].split("@", 1)[0].lower(), words[1:]
        route = self.s.route(thread=thread) if thread else None
        result = ""
        if cmd in {"/help", "/start"}:
            result = HELP
        elif cmd == "/status":
            result = (f"TeleVK {__version__}; uptime {int(time.time() - self.started)}s\n" + self.s.status()
                      + f"\nVK paused: {self.s.get('vk_paused') or 'no'}"
                      + f"\nVK last success: {self.age('vk_last_ok')}\nTG last success: {self.age('tg_last_ok')}"
                      + f"\nVK last error: {self.s.get('vk_last_error', 'none')}\nDatabase: {self.s.path.stat().st_size // 1024**2} MiB")
        elif cmd == "/chats":
            offset = int(args[0]) if args else 0
            if offset < 0:
                raise Permanent("offset must be >= 0")
            response = await self.vk.call("messages.getConversations", count=30, offset=offset, extended=1)
            self.s.names(response)
            lines = []
            for it in response.get("items", []):
                conv = it["conversation"]
                peer = int(conv["peer"]["id"])
                title = conv.get("chat_settings", {}).get("title") or self.s.name(peer)
                lines.append(f"{peer} — {title}")
            result = "\n".join(lines) + f"\nСледующая страница: /chats {offset + 30}\nПодключение: /chat PEER_ID"
        elif cmd == "/chat":
            if not args:
                raise Permanent("/chat PEER_ID [Название]")
            peer = int(args[0])
            if peer == 0:
                raise Permanent("peer_id must not be 0")
            self.s.ensure_route(peer, " ".join(args[1:]))
            target = await self.topic(peer)
            result = f"VK {peer} → тема {target}. Переписка до подключения не импортируется автоматически."
        elif cmd == "/bind":
            if len(args) != 1:
                raise Permanent("/bind PEER_ID в нужной теме")
            self.s.bind(int(args[0]), thread)
            result = "Тема привязана. Очередь использует сохранённое соответствие."
        elif cmd == "/alias":
            if not route or not args:
                raise Permanent("/alias Название в привязанной теме")
            title = " ".join(args)[:128]
            await self.tg.call("editForumTopic", **self.tg.target(thread), name=title)
            self.s.db.execute("UPDATE routes SET title=? WHERE peer=?", (title, route["peer"]))
            result = "Название темы изменено."
        elif cmd in {"/mute", "/unmute"}:
            target = self.s.route(thread=int(args[0])) if args else route
            if not target:
                raise Permanent("Нужна привязанная тема или /mute THREAD_ID")
            self.s.db.execute("UPDATE routes SET muted=? WHERE peer=?", (int(cmd == "/mute"), target["peer"]))
            result = "Входящие VK → Telegram отключены. Ответы в VK доступны." if cmd == "/mute" else "Входящие VK → Telegram включены. Пропущенные во время mute сообщения автоматически не догружаются."
        elif cmd == "/muted":
            result = "\n".join(f"{r['thread']}: {r['title']} (VK {r['peer']})" for r in self.s.db.execute("SELECT * FROM routes WHERE muted=1")) or "Нет отключённых тем."
        elif cmd == "/read":
            if not route:
                raise Permanent("/read работает внутри привязанной темы")
            replied = (m.get("reply_to_message") or {}).get("message_id")
            boundary = self.s.tg_link(replied, route["peer"]) if replied else self.s.db.execute("SELECT * FROM links WHERE peer=? AND incoming=1 ORDER BY cmid DESC,mid DESC LIMIT 1", (route["peer"],)).fetchone()
            if not boundary or not boundary["incoming"]:
                raise Permanent("Нет доставленного входящего сообщения для отметки прочтения")
            self.s.add_job(f"read:{key}", "vk_read", {"cmid": boundary["cmid"], "mid": boundary["mid"], "thread": thread, "ack": True},
                           peer=route["peer"], lane="vk_read", priority=-2)
            result = "Отметка прочтения поставлена в очередь до выбранного сообщения; не до конца чата."
        elif cmd == "/history":
            if not route:
                raise Permanent("/history all выполняется в теме нужного чата; сначала /chat PEER_ID")
            action = args[0].lower() if args else "status"
            peer = route["peer"]
            if action == "all":
                with self.s.tx():
                    self.start_import(peer)
                result = "Запущен импорт всей доступной истории этого чата. /history status — прогресс; /history cancel — остановка. При /mute импорт приостанавливается."
            elif action == "cancel":
                with self.s.tx():
                    self.s.db.execute("UPDATE imports SET state='cancelled',updated=? WHERE peer=?", (time.time(), peer))
                    self.s.db.execute("UPDATE jobs SET state='suppressed',updated=? WHERE peer=? AND lane LIKE 'archive_%' AND state='pending'", (time.time(), peer))
                result = "Импорт остановлен. Уже принятые Telegram сообщения не удаляются; текущая отправка может завершиться."
            elif action == "status":
                row = self.s.db.execute("SELECT * FROM imports WHERE peer=?", (peer,)).fetchone()
                left = self.s.db.execute("SELECT COUNT(*) FROM archive WHERE peer=? AND queued=0", (peer,)).fetchone()[0]
                result = f"{dict(row) if row else 'Импорт не запускался'}\nНе поставлено в очередь: {left}\nДоставка: /status"
            else:
                raise Permanent("/history all | status | cancel")
        elif cmd == "/dlq":
            rows = self.s.db.execute("SELECT id,kind,peer,state,error FROM jobs WHERE state IN ('dead','uncertain') ORDER BY id DESC LIMIT 20").fetchall()
            result = "\n".join(f"#{r['id']} {r['state']} {r['kind']} VK {r['peer']}: {r['error']}" for r in rows) or "Ошибочных и неопределённых задач нет."
        elif cmd == "/retry_dlq":
            if not args:
                raise Permanent("/retry_dlq ID [force]; массовый повтор отключён")
            result = self.s.retry(int(args[0]), len(args) > 1 and args[1] == "force")
        elif cmd == "/resync":
            await self.resync()
            result = "Сверка запущена. Она не отмечает переписку прочитанной."
        elif cmd == "/updates":
            from .updates import check_updates
            report = await check_updates(self.vk.client, self.c)
            result = report["text"]
        else:
            raise Permanent("Неизвестная команда. /help — доступные команды. Команда не отправлена в VK.")
        self.notify(f"command:{key}", result, thread, reply_id)

    def age(self, key: str) -> str:
        value = self.s.get(key)
        return f"{int(time.time() - value)}s ago" if value else "never"

    async def deliver_telegram(self, job: dict, data: dict) -> None:
        peer = job["peer"]
        route = self.s.route(peer)
        if route and route["muted"]:
            self.s.finish(job["id"], "suppressed")
            return
        data["thread"] = await self.topic(peer)
        reply = data.get("reply") or {}
        mapped = self.s.vk_link(peer, mid=int(reply.get("id", 0)), cmid=int(reply.get("conversation_message_id", 0)))
        if mapped:
            data["reply_tg"] = mapped["tg"]
        path = None
        try:
            if job["kind"] == "tg_file":
                async with self.media_slots:
                    disk_check(self.c.state, self.c.MIN_FREE_MB * 1024**2, self.c.MAX_FILE_MB * 1024**2)
                    fd, name = tempfile.mkstemp(prefix=f"job-{job['id']}-", dir=self.media_dir)
                    os.close(fd)
                    path = Path(name)
                    if data.get("size", 0) > self.c.MAX_FILE_MB * 1024**2:
                        raise Permanent("Вложение превышает настроенный лимит размера")
                    try:
                        await download_public(data["url"], path, self.c.MAX_FILE_MB * 1024**2, self.c.MIN_FREE_MB * 1024**2)
                    except Permanent:
                        # Signed URLs can expire during a large history import.
                        current = await self.vk.resolve(peer, data.get("mid", 0), data.get("cmid", 0))
                        if not current:
                            raise
                        current["peer_id"] = peer
                        _, descriptors = message_parts(current, self.s, self.c.TIMEZONE)
                        index = data.get("file_index", 0)
                        if index >= len(descriptors) or descriptors[index].get("url") == data["url"]:
                            raise
                        data.update(descriptors[index])
                        self.s.payload(job["id"], data)
                        await download_public(data["url"], path, self.c.MAX_FILE_MB * 1024**2, self.c.MIN_FREE_MB * 1024**2)
                    # Re-check mute after the potentially slow download.
                    if self.s.route(peer)["muted"]:
                        self.s.finish(job["id"], "suppressed")
                        return
                    self.s.effect(job["id"])
                    try:
                        sent = await self.tg.send(data, path)
                    except Permanent as exc:
                        if data.get("kind") == "photo" and any(x in str(exc) for x in ("photo_invalid_dimensions", "image_process_failed")):
                            self.s.effect(job["id"], False)
                            data["kind"] = "document"
                            self.s.payload(job["id"], data)
                            self.s.effect(job["id"])
                            sent = await self.tg.send(data, path)
                        else:
                            raise
            else:
                self.s.effect(job["id"])
                sent = await self.tg.send(data)
            if not isinstance(sent, dict) or not sent.get("message_id"):
                raise Uncertain("Telegram returned no message ID")
            with self.s.tx():
                self.s.link(int(sent["message_id"]), peer, data.get("mid", 0), data.get("cmid", 0), data.get("incoming", False))
                self.s.finish(job["id"])
        finally:
            if path:
                path.unlink(missing_ok=True)

    async def react_success(self, tg_message_id: int) -> None:
        # Best-effort acknowledgement: a cosmetic reaction must never affect VK delivery state.
        try:
            await self.tg.call(
                "setMessageReaction",
                chat_id=self.c.TG_GROUP_ID,
                message_id=tg_message_id,
                reaction=[{"type": "emoji", "emoji": "⚡"}],
                is_big=False,
            )
        except (Retry, Permanent, Uncertain):
            LOG.warning("Could not set Telegram success reaction for message %s", tg_message_id)

    async def deliver_vk(self, job: dict, data: dict) -> None:
        peer, jid = job["peer"], job["id"]
        if self.s.get("vk_paused"):
            raise Retry("VK is paused. Check /status and re-run probe after fixing access", 60)
        async with self.vk_send_locks.setdefault(peer, asyncio.Lock()):
            path = None
            try:
                if data.get("file_id") and not data.get("attachment"):
                    async with self.media_slots:
                        if data.get("size", 0) > 20 * 1024**2:
                            raise Permanent("Файл Telegram больше 20 MiB: облачный Bot API не позволяет его скачать")
                        disk_check(self.c.state, self.c.MIN_FREE_MB * 1024**2, self.c.MAX_FILE_MB * 1024**2)
                        fd, name = tempfile.mkstemp(prefix=f"out-{jid}-", dir=self.media_dir)
                        os.close(fd)
                        path = Path(name)
                        await self.tg.download(data["file_id"], path, self.c.MAX_FILE_MB * 1024**2)
                        data["attachment"] = await self.vk.upload(path, peer, data["kind"], data["filename"])
                        self.s.payload(jid, data)
                params = {"peer_id": peer, "random_id": job["random_id"], "message": data.get("text", "")}
                if data.get("attachment"):
                    params["attachment"] = data["attachment"]
                reply = data.get("reply") or {}
                if reply.get("cmid"):
                    params["forward"] = dumps({"peer_id": peer, "conversation_message_ids": [reply["cmid"]], "is_reply": True})
                elif reply.get("mid"):
                    params["reply_to"] = reply["mid"]
                self.s.effect(jid)
                response = await self.vk.call("messages.send", effect=True, **params)
                mid = response if type(response) is int else response.get("message_id", response.get("id", 0)) if isinstance(response, dict) else 0
                cmid = response.get("conversation_message_id", 0) if isinstance(response, dict) else 0
                if not mid:
                    raise Uncertain("VK accepted a send but returned no usable message ID")
                with self.s.tx():
                    known = self.s.db.execute("SELECT cmid FROM echoes WHERE peer=? AND random_id=?", (peer, job["random_id"])).fetchone()
                    cmid = cmid or (known["cmid"] if known else 0)
                    self.s.db.execute("UPDATE echoes SET mid=?,cmid=? WHERE peer=? AND random_id=?", (mid, cmid, peer, job["random_id"]))
                    self.s.link(data["tg"], peer, mid, cmid)
                    self.s.finish(jid)
                    if reply.get("incoming"):
                        self.s.add_job(f"read-after:{jid}", "vk_read", {**reply, "thread": data.get("thread", 0), "ack": False}, peer=peer, lane="vk_read", priority=-2)
                if data.get("reply_unmapped"):
                    self.notify(
                        f"reply-unmapped:{jid}",
                        "Ответ передан в VK текстовой цитатой; отметка прочтения не менялась.",
                        data.get("thread", 0),
                        data["tg"],
                        priority=2,
                    )
                await self.react_success(int(data["tg"]))
            finally:
                if path:
                    path.unlink(missing_ok=True)

    async def dispatch(self, job: dict) -> None:
        data = json.loads(job["payload"])
        if job["kind"] in {"tg_text", "tg_file"}:
            await self.deliver_telegram(job, data)
        elif job["kind"] == "vk_send":
            await self.deliver_vk(job, data)
        elif job["kind"] == "command":
            try:
                await self.command(job, data)
            except (ValueError, Permanent) as exc:
                msg = data["message"]
                self.notify(f"command-error:{job['id']}", str(exc) if isinstance(exc, Permanent) else "Неверные аргументы команды. /help", msg.get("message_thread_id", 0), msg["message_id"])
        elif job["kind"] == "vk_read":
            await self.vk.mark_read(job["peer"], data.get("cmid", 0), data.get("mid", 0))
            if data.get("ack"):
                self.notify(f"read-ok:{job['id']}", "✓ VK подтвердил отметку прочтения до выбранного сообщения.", data.get("thread", 0))
        elif job["kind"] == "notice":
            self.s.effect(job["id"])
            await self.tg.send(data)
        else:
            raise Permanent("Unknown stored job type; review the database/application version")

    async def worker(self) -> None:
        while not self.stop.is_set():
            job = self.s.claim()
            if not job:
                await asyncio.sleep(0.25)
                continue
            try:
                await self.dispatch(job)
                # Delivery handlers commit links/read jobs atomically with completion.
                current = self.s.db.execute("SELECT state FROM jobs WHERE id=?", (job["id"],)).fetchone()[0]
                if current == "running":
                    self.s.finish(job["id"])
            except Retry as exc:
                if job["tries"] >= 12 or time.time() - job["created"] > 86400:
                    self.failed(job, "dead", str(exc))
                else:
                    self.s.finish(job["id"], "pending", str(exc), max(exc.delay, min(300, 2 ** min(job["tries"], 8))))
            except Uncertain as exc:
                self.failed(job, "uncertain", str(exc))
            except Permanent as exc:
                if isinstance(exc, AccessDenied):
                    self.s.set("vk_paused", str(exc))
                self.failed(job, "dead", str(exc))
            # Unexpected exceptions, cancellation and storage failures escape.
            # A restart preserves effect=1 as uncertain rather than resending.

    def failed(self, job: dict, state: str, error: str) -> None:
        with self.s.tx():
            self.s.finish(job["id"], state, error)
            if job["kind"] != "notice":
                route = self.s.route(peer=job["peer"])
                data = json.loads(job["payload"])
                thread = route["thread"] if route else data.get("thread", 0)
                text = f"⚠ Задача #{job['id']} ({job['kind']}, VK {job['peer']}): {state}.\n{error}\n"
                if state == "uncertain":
                    text += f"Автоповтор остановлен. Проверь доставку; повтор вручную: /retry_dlq {job['id']} force. Возможен дубликат."
                else:
                    text += f"После исправления: /retry_dlq {job['id']}. Остальные части сообщения не повторяются."
                self.notify(f"failure:{job['id']}:{job['tries']}", text, thread or 0)
        LOG.warning("Job %s %s: %s", job["id"], state, error)

    async def heartbeat(self) -> None:
        while not self.stop.is_set():
            notify_systemd("WATCHDOG=1\nSTATUS=TeleVK event loop responsive")
            await asyncio.sleep(15)

    async def maintenance(self) -> None:
        from .updates import check_updates
        last_clean = 0.0
        while not self.stop.is_set():
            now = time.time()
            if now - last_clean > 86400:
                self.s.cleanup(self.c.RETENTION_DAYS)
                last_clean = now
            if self.c.UPDATE_CHECK_HOURS and now - self.s.get("updates_attempt", 0) > self.c.UPDATE_CHECK_HOURS * 3600:
                report = await check_updates(self.vk.client, self.c)
                with self.s.tx():
                    self.s.set("updates_attempt", now)
                    self.s.set("updates_report", report)
                    fingerprint = hashlib.sha256(dumps(report["important"]).encode()).hexdigest()
                    if report["important"] and fingerprint != self.s.get("updates_notified"):
                        self.notify("updates:" + fingerprint, report["text"])
                        self.s.set("updates_notified", fingerprint)
            notify_systemd("WATCHDOG=1\nSTATUS=Bridge active; " + self.s.status().replace("\n", "; "))
            await asyncio.sleep(15)

    async def run(self) -> None:
        check = await self.preflight()
        if not check["write_probe_ok"]:
            raise Permanent("Run `python -m televk --config config.json probe --send-test` before starting the bridge")
        self.s.recover()
        self.s.set("vk_paused", None)
        # No concurrent process is allowed (CLI flock); leftovers are disposable.
        for p in self.media_dir.iterdir():
            if p.is_file() and not p.is_symlink():
                p.unlink()
        self.notify("startup:" + str(time.time_ns()), f"TeleVK {__version__} запущен. /help — команды. История автоматически не импортируется, получение не означает прочтение.")
        for row in self.s.db.execute("SELECT id FROM jobs WHERE state='uncertain' LIMIT 1"):
            self.notify("recovered-uncertain:" + str(time.time_ns()), "После перезапуска есть неопределённые отправки: /dlq. Автоматически они не повторяются.")
        tasks = [asyncio.create_task(coro, name=name) for coro, name in
                 ((self.poll_vk(), "vk-poll"), (self.poll_tg(), "tg-poll"), (self.plan_inbox(), "inbox"),
                  (self.import_loop(), "history"), (self.maintenance(), "maintenance"), (self.heartbeat(), "watchdog"))]
        tasks += [asyncio.create_task(self.worker(), name=f"worker-{i}") for i in range(self.c.WORKERS)]
        stop_task = asyncio.create_task(self.stop.wait())
        notify_systemd("READY=1\nSTATUS=TeleVK running")
        try:
            done, _ = await asyncio.wait([*tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task == stop_task:
                    continue
                if task.exception():
                    raise task.exception()
                # A signal/explicit stop wakes all loops concurrently. One worker may
                # observe the event and finish a few scheduling ticks before stop_task;
                # that is a normal shutdown, not a crashed required worker.
                if not self.stop.is_set():
                    raise RuntimeError(f"Required worker exited: {task.get_name()}")
        finally:
            notify_systemd("STOPPING=1")
            for task in [*tasks, stop_task]:
                task.cancel()
            await asyncio.gather(*tasks, stop_task, return_exceptions=True)
