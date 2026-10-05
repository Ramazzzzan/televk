from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from .common import Permanent, safe_name, split_text
from .store import Store


def needs_hydration(message: dict) -> bool:
    # Long Poll can carry attachment stubs; fetch the full message before planning media.
    attachments = message.get("attachments")
    if attachments is not None and not isinstance(attachments, list):
        return True
    attachments = attachments or []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        kind = attachment.get("type", "")
        obj = attachment.get(kind) or {}
        if kind == "photo":
            sizes = obj.get("sizes") or []
            orig = obj.get("orig_photo") or {}
            has_url = any(isinstance(size, dict) and size.get("url") for size in sizes)
            if isinstance(orig, dict) and orig.get("url"):
                has_url = True
            if not has_url:
                return True
        elif kind == "doc" and not obj.get("url"):
            return True
    # A completely empty live object may be a media-only Long Poll stub.
    return not attachments and not any((
        message.get("text"),
        message.get("action"),
        message.get("fwd_messages"),
        message.get("geo"),
    ))


def message_parts(message: dict, store: Store, tz: str) -> tuple[str, list[dict]]:
    """Unknown fields/types are visible; never discard a supported text payload."""
    files, notes = [], []

    def visit(m: dict, depth: int = 0) -> None:
        prefix = "> " * min(depth, 4)
        if depth:
            notes.append(f"{prefix}Переслано: {store.name(int(m.get('from_id', 0)))}")
        if m.get("text"):
            notes.append(prefix + str(m["text"]))
        if m.get("action"):
            action = m["action"]
            notes.append(f"{prefix}[Событие VK: {action.get('type', 'unknown')}] {action.get('text', '')}")
        for a in m.get("attachments", []):
            if not isinstance(a, dict):
                notes.append("[Неподдерживаемое вложение VK: неизвестный объект]")
                continue
            kind = a.get("type", "unknown")
            obj = a.get(kind) or {}
            if kind == "photo":
                sizes = [s for s in obj.get("sizes", []) if s.get("url")]
                if obj.get("orig_photo", {}).get("url"):
                    sizes.append(obj["orig_photo"])
                if sizes:
                    best = max(sizes, key=lambda s: s.get("width", 0) * s.get("height", 0))
                    files.append({"kind": "photo", "url": best["url"], "filename": f"photo-{obj.get('id', len(files))}.jpg"})
                else:
                    notes.append("[Фото VK недоступно: API не вернул адрес изображения]")
            elif kind == "doc" and obj.get("url"):
                files.append({"kind": "document", "url": obj["url"], "filename": safe_name(obj.get("title", "document")), "size": obj.get("size", 0)})
            elif kind == "link":
                notes.append("[Ссылка VK]\n" + "\n".join(str(obj[k]) for k in ("title", "url", "description") if obj.get(k)))
            else:
                label = str(obj.get("title") or obj.get("question") or "")[:500]
                notes.append(f"[Неподдерживаемый тип VK: {kind}]" + (" " + label if label else ""))
        if m.get("geo"):
            notes.append("[Неподдерживаемый тип VK: геопозиция]")
        for fwd in m.get("fwd_messages", []):
            if depth < 8 and isinstance(fwd, dict):
                visit(fwd, depth + 1)
            else:
                notes.append("[Вложенная пересылка: откройте VK для продолжения]")

    visit(message)
    stamp = int(message.get("date", 0))
    when = datetime.fromtimestamp(stamp, ZoneInfo(tz)).strftime("%d.%m.%Y %H:%M:%S %Z")
    who = "Я · VK" if message.get("out") else store.name(int(message.get("from_id", 0)))
    prefix = "[История] " if message.get("_historical") else ""
    header = f"{prefix}{who} · {when}"
    reply = message.get("reply_message") or {}
    if reply:
        notes.insert(0, "↪ " + str(reply.get("text") or "[вложение]")[:700])
    if not notes and not files:
        notes.append("[Неподдерживаемое или пустое сообщение VK]")
    if any("Неподдерж" in note or "недоступно" in note for note in notes):
        notes.append(f"Открыть диалог: https://vk.com/im?sel={message['peer_id']}")
    return header + ("\n" + "\n\n".join(notes) if notes else ""), files


def plan_vk(store: Store, key: str, message: dict, timezone: str) -> None:
    """Caller holds a transaction; enqueueing and marking inbox processed are atomic."""
    peer = int(message["peer_id"])
    if store.echo(message):
        # Hydrate links once the echo supplies the conversation-scoped message ID.
        mid, cmid, rid = int(message.get("id", 0)), int(message.get("conversation_message_id", 0)), int(message.get("random_id", 0))
        if rid:
            store.db.execute("UPDATE echoes SET mid=?,cmid=? WHERE peer=? AND random_id=?", (mid, cmid, peer, rid))
        if mid and cmid:
            store.db.execute("UPDATE links SET cmid=? WHERE peer=? AND mid=?", (cmid, peer, mid))
        return
    if not message.get("_historical") and int(message.get("date", 0)) < int(store.get("connected_at", 0)) - 1:
        return  # Old-message edits are outside v0.1 scope, not a history import.
    if store.vk_link(peer, mid=int(message.get("id", 0)), cmid=int(message.get("conversation_message_id", 0))):
        return
    store.ensure_route(peer)
    route = store.route(peer)
    if route["muted"]:
        return
    text, files = message_parts(message, store, timezone)
    meta = {"mid": int(message.get("id", 0)), "cmid": int(message.get("conversation_message_id", 0)),
            "incoming": not bool(message.get("out")), "historical": bool(message.get("_historical")),
            "reply": message.get("reply_message") or {}}
    historical = bool(meta["historical"])
    priority, lane = (30, "archive") if historical else (10, "live")
    for n, part in enumerate(split_text(text)):
        store.add_job(f"vk:{key}:text:{n}", "tg_text", {**meta, "text": part}, peer=peer, lane=lane + "_text", priority=priority)
    for n, file in enumerate(files):
        store.add_job(f"vk:{key}:file:{n}", "tg_file", {**meta, **file, "file_index": n, "caption": text.split("\n")[0]},
                      peer=peer, lane=lane + "_media", priority=priority + 1)


def tg_payload(message: dict, store: Store, peer: int) -> dict:
    data = {"tg": message["message_id"], "thread": message.get("message_thread_id"),
            "text": str(message.get("text") or message.get("caption") or "")}
    photo = message.get("photo")
    doc = message.get("document")
    if photo:
        obj = max(photo, key=lambda x: x.get("width", 0) * x.get("height", 0))
        data.update(file_id=obj["file_id"], kind="photo", filename=f"tg-photo-{message['message_id']}.jpg", size=obj.get("file_size", 0))
    elif doc:
        data.update(file_id=doc["file_id"], kind="document", filename=safe_name(doc.get("file_name", "file.bin")), size=doc.get("file_size", 0))
    elif not data["text"]:
        typ = next((x for x in ("voice", "audio", "video", "animation", "sticker", "video_note", "contact", "location", "poll", "story", "paid_media") if x in message), "unknown")
        raise Permanent(f"Тип Telegram {typ} пока не пересылается. Отправь содержимое как документ или текст.")
    unsupported = next((x for x in ("voice", "audio", "video", "animation", "sticker", "video_note", "contact", "location", "poll", "story", "paid_media", "live_photo", "rich_message") if x in message), None)
    if unsupported and not data.get("file_id"):
        data["unsupported"] = unsupported
    if len(data["text"]) > 9000:
        raise Permanent("Текст длиннее лимита VK в 9000 символов; раздели сообщение")
    reply = message.get("reply_to_message") or {}
    if reply and not reply.get("forum_topic_created"):
        target = store.tg_link(int(reply.get("message_id", 0)), peer)
        if target:
            data["reply"] = {"mid": target["mid"], "cmid": target["cmid"], "incoming": bool(target["incoming"])}
        else:
            quote = str(reply.get("text") or reply.get("caption") or "[вложение]")[:700]
            data["text"] = "↪ " + quote + "\n\n" + data["text"]
            data["reply_unmapped"] = True
    if len(data["text"]) > 9000:
        raise Permanent("Текст вместе с цитатой длиннее 9000 символов; раздели сообщение")
    return data
