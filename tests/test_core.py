from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from televk.api import Telegram, VK, request_json
from televk.bridge import Bridge
from televk.common import Config, Permanent, RateGate, Retry, Uncertain, VKError, authorized, safe_name, split_text
from televk.media import public_ip, validate_url
from televk.planner import message_parts, needs_hydration, plan_vk, tg_payload
from televk.store import Store
from televk.updates import important_update, stable_version, version_from_tag, check_updates


def vk_message(mid=1, peer=42, **extra):
    return {"id": mid, "peer_id": peer, "conversation_message_id": mid,
            "from_id": peer, "date": int(time.time()), "text": f"Message {mid}", "out": 0, **extra}


def tg_message(mid=1, **extra):
    return {"message_id": mid, "chat": {"id": -100123}, "from": {"id": 7},
            "message_thread_id": 55, "date": int(time.time()), "text": "Ответ", **extra}


class Harness:
    def setup_harness(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.c = Config(7, -100123, "telegram.token", "vk.token", _base=self.base, MIN_FREE_MB=1)
        self.s = Store(self.c.state / "bridge.sqlite3")

    def close_harness(self):
        self.s.close()
        self.temp.cleanup()


class TestStore(Harness, unittest.TestCase):
    def setUp(self): self.setup_harness()
    def tearDown(self): self.close_harness()

    def test_transaction_rollback_preserves_cursor(self):
        self.s.set("vk_cursor", {"pts": 10})
        original = self.s.ingest
        def failing(source, key, payload, peer=0):
            if payload["id"] == 2:
                raise RuntimeError("simulated disk failure")
            return original(source, key, payload, peer)
        with patch.object(self.s, "ingest", side_effect=failing):
            with self.assertRaises(RuntimeError):
                self.s.ingest_vk_batch([vk_message(1), vk_message(2)], {"pts": 20}, {})
        self.assertEqual(self.s.get("vk_cursor"), {"pts": 10})
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM inbox").fetchone()[0], 0)

    def test_checkpoint_survives_reopen(self):
        self.s.ingest_vk_batch([vk_message()], {"pts": 20}, {})
        path = self.s.path
        self.s.close()
        self.s = Store(path)
        self.assertEqual(self.s.get("vk_cursor"), {"pts": 20})
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM inbox").fetchone()[0], 1)

    def test_source_dedup(self):
        m = vk_message()
        self.assertTrue(self.s.ingest("vk", Store.vk_key(m), m, 42))
        self.assertFalse(self.s.ingest("vk", Store.vk_key(m), m, 42))

    def test_peer_scoped_cmid(self):
        self.assertNotEqual(Store.vk_key(vk_message(peer=1)), Store.vk_key(vk_message(peer=2)))

    def test_unknown_identifier_rejected(self):
        with self.assertRaises(Permanent): Store.vk_key({"peer_id": 42})

    def test_account_database_binding(self):
        self.s.bind_identity(1, 2, -3, 4)
        self.s.bind_identity(1, 2, -3, 4)
        with self.assertRaises(Permanent): self.s.bind_identity(9, 2, -3, 4)

    def test_random_id_is_stable_for_retry(self):
        jid = self.s.add_job("test", "vk_send", {}, peer=42, lane="out")
        first = dict(self.s.db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone())
        self.s.finish(jid, "dead")
        self.s.retry(jid)
        self.assertEqual(self.s.claim()["random_id"], first["random_id"])
        self.assertNotEqual(first["random_id"], 0)

    def test_uncertain_requires_force(self):
        jid = self.s.add_job("x", "tg_text", {})
        self.s.finish(jid, "uncertain")
        with self.assertRaises(Permanent): self.s.retry(jid)
        self.s.retry(jid, True)
        self.assertEqual(self.s.claim()["id"], jid)

    def test_interrupted_effect_becomes_uncertain(self):
        jid = self.s.add_job("x", "tg_text", {})
        self.s.claim()
        self.s.effect(jid)
        self.s.recover()
        self.assertEqual(self.s.db.execute("SELECT state FROM jobs").fetchone()[0], "uncertain")

    def test_interrupted_preparation_is_retryable(self):
        self.s.add_job("x", "tg_text", {})
        self.s.claim()
        self.s.recover()
        self.assertEqual(self.s.db.execute("SELECT state FROM jobs").fetchone()[0], "pending")

    def test_same_lane_order_and_other_peer_independence(self):
        a = self.s.add_job("a", "x", {}, peer=1, lane="live")
        self.s.add_job("b", "x", {}, peer=1, lane="live")
        c = self.s.add_job("c", "x", {}, peer=2, lane="live")
        self.assertEqual(self.s.claim()["id"], a)
        self.s.finish(a, "uncertain")
        self.assertEqual(self.s.claim()["id"], c)

    def test_media_does_not_block_text_lane(self):
        a = self.s.add_job("a", "x", {}, peer=1, lane="media")
        b = self.s.add_job("b", "x", {}, peer=1, lane="text")
        self.assertEqual(self.s.claim()["id"], a)
        self.assertEqual(self.s.claim()["id"], b)

    def test_muted_history_is_left_pending(self):
        self.s.ensure_route(42)
        self.s.db.execute("UPDATE routes SET muted=1")
        self.s.add_job("a", "tg_text", {}, peer=42, lane="archive_text")
        self.assertIsNone(self.s.claim())
        self.s.db.execute("UPDATE routes SET muted=0")
        self.assertIsNotNone(self.s.claim())

    def test_routes_cannot_crossbind(self):
        self.s.bind(42, 55)
        with self.assertRaises(Permanent): self.s.bind(43, 55)
        with self.assertRaises(Permanent): self.s.bind(42, 56)

    def test_general_cannot_be_bound(self):
        with self.assertRaises(Permanent): self.s.bind(42, 1)

    def test_links_are_one_to_many(self):
        self.s.link(100, 42, 10, 5, True)
        self.s.link(101, 42, 10, 5, True)
        self.assertEqual(self.s.tg_link(101, 42)["mid"], 10)
        self.assertIsNone(self.s.tg_link(101, 43))
        self.assertEqual(self.s.vk_link(42, cmid=5)["tg"], 100)

    def test_echo_matches_peer_and_random_id(self):
        jid = self.s.add_job("out", "vk_send", {}, peer=42)
        rid = self.s.db.execute("SELECT random_id FROM jobs WHERE id=?", (jid,)).fetchone()[0]
        self.assertTrue(self.s.echo(vk_message(out=1, random_id=rid)))
        self.assertFalse(self.s.echo(vk_message(peer=43, out=1, random_id=rid)))
        self.assertFalse(self.s.echo(vk_message(out=1, id=777)))

    def test_cleanup_keeps_failed_and_links(self):
        now = time.time() - 100 * 86400
        for state in ("dead", "uncertain", "done"):
            jid = self.s.add_job(state, "x", {})
            self.s.finish(jid, state)
        self.s.db.execute("UPDATE jobs SET updated=?", (now,))
        self.s.link(100, 42, 1, 1)
        self.s.ingest("vk", "key", {"sensitive": "text"})
        self.s.db.execute("UPDATE inbox SET processed=1,created=?", (now,))
        self.s.cleanup(30)
        self.assertEqual({r[0] for r in self.s.db.execute("SELECT state FROM jobs")}, {"dead", "uncertain"})
        self.assertEqual(self.s.db.execute("SELECT payload FROM inbox").fetchone()[0], "{}")
        self.assertIsNotNone(self.s.tg_link(100, 42))


class TestSecurityAndFormatting(unittest.TestCase):
    def test_owner_and_group_required(self):
        self.assertTrue(authorized(tg_message(), 7, -100123))
        self.assertFalse(authorized(tg_message(), 8, -100123))
        self.assertFalse(authorized(tg_message(), 7, -100999))

    def test_anonymous_admin_rejected(self):
        self.assertFalse(authorized(tg_message(sender_chat={"id": -100123}), 7, -100123))

    def test_bot_and_missing_sender_rejected(self):
        self.assertFalse(authorized(tg_message(**{"from": {"id": 7, "is_bot": True}}), 7, -100123))
        self.assertFalse(authorized({"chat": {"id": -100123}}, 7, -100123))

    def test_utf16_splitting(self):
        text = "😄" * 5000 + "Привет" * 1000
        chunks = split_text(text)
        self.assertEqual("".join(chunks), text)
        self.assertTrue(all(len(x.encode("utf-16-le")) // 2 <= 3500 for x in chunks))

    def test_plain_text_preserved(self):
        self.assertEqual(split_text("<b>x</b> & _test_"), ["<b>x</b> & _test_"])

    def test_filename_sanitized(self):
        self.assertEqual(safe_name("../../passwords.txt"), "passwords.txt")
        self.assertEqual(safe_name("C:\\folder\\a.pdf"), "a.pdf")
        self.assertNotIn("\n", safe_name("hello\nworld"))

    def test_private_and_transition_addresses_blocked(self):
        for ip in ("127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "fc00::1", "fe80::1", "100.64.0.1", "224.0.0.1", "64:ff9b::7f00:1", "2002:7f00:1::1", "::ffff:127.0.0.1"):
            with self.subTest(ip=ip): self.assertFalse(public_ip(ip))
        self.assertTrue(public_ip("8.8.8.8"))
        self.assertTrue(public_ip("2606:4700:4700::1111"))

    def test_url_rejections(self):
        for url in ("file:///etc/passwd", "http://example.com", "https://user:pass@example.com/a", "https://127.0.0.1/a", "https://localhost/a", "https://example.com:22/a", "https://example.com/a\nheader=x"):
            with self.subTest(url=url), self.assertRaises(Permanent): validate_url(url)
        self.assertEqual(validate_url("https://cdn.example.com/a?token=abc")[0], "cdn.example.com")

    def test_major_and_prerelease_detection(self):
        self.assertTrue(important_update("0.28.1", "0.29.0"))
        self.assertTrue(important_update("1.2.3", "2.0.0"))
        self.assertFalse(important_update("1.2.3", "1.3.0"))
        self.assertTrue(important_update("3.11.2", "3.14.1", minor=True))
        for value in ("1.0.dev6", "3.15.0rc1", "4.0.0b2"):
            self.assertIsNone(stable_version(value))
        self.assertEqual(version_from_tag("v3.20"), "3.20")
        self.assertEqual(version_from_tag("curl-8_16_0"), "8.16.0")
        self.assertEqual(version_from_tag("release/0.16.0"), "0.16.0")
        self.assertIsNone(version_from_tag("1.0.dev6"))

    def test_config_bounds(self):
        c = Config(7, -100123, "tg", "vk")
        c.validate()
        for changes in ({"TG_OWNER_ID": 0}, {"TG_GROUP_ID": 1}, {"TG_SEND_INTERVAL": 0.1}, {"WORKERS": 99}, {"VK_POLL_SECONDS": float("nan")}, {"MAX_FILE_MB": 51}):
            with self.subTest(changes=changes), self.assertRaises(ValueError): dataclasses.replace(c, **changes).validate()


class TestPlanner(Harness, unittest.TestCase):
    def setUp(self): self.setup_harness()
    def tearDown(self): self.close_harness()

    def test_text_unknown_attachment_visible(self):
        text, files = message_parts(vk_message(text="Сохранить текст", attachments=[{"type": "audio_message", "audio_message": {}}]), self.s, "UTC")
        self.assertIn("Сохранить текст", text)
        self.assertIn("Неподдерживаемый тип VK: audio_message", text)
        self.assertEqual(files, [])

    def test_photo_and_document_extraction(self):
        m = vk_message(attachments=[{"type": "photo", "photo": {"id": 1, "sizes": [
            {"url": "https://a/small", "width": 10, "height": 10}, {"url": "https://a/large", "width": 500, "height": 500}]}},
            {"type": "doc", "doc": {"url": "https://a/file", "title": "x.pdf", "size": 100}}])
        _, files = message_parts(m, self.s, "UTC")
        self.assertEqual([x["kind"] for x in files], ["photo", "document"])
        self.assertEqual(files[0]["url"], "https://a/large")
        self.assertFalse(needs_hydration(m))

    def test_incomplete_photo_or_doc_needs_hydration(self):
        photo = vk_message(text="", attachments=[{"type": "photo", "photo": {"id": 1}}])
        doc = vk_message(text="", attachments=[{"type": "doc", "doc": {"id": 2}}])
        empty = vk_message(text="", attachments=[])
        self.assertTrue(needs_hydration(photo))
        self.assertTrue(needs_hydration(doc))
        self.assertTrue(needs_hydration(empty))

    def test_forwarded_text_and_document(self):
        _, files = message_parts(vk_message(fwd_messages=[vk_message(text="nested", attachments=[{"type": "doc", "doc": {"url": "https://example.com/f"}}])]), self.s, "UTC")
        self.assertEqual(len(files), 1)

    def test_separate_jobs_for_text_and_file(self):
        m = vk_message(attachments=[{"type": "doc", "doc": {"url": "https://a/file"}}])
        with self.s.tx(): plan_vk(self.s, Store.vk_key(m), m, "UTC")
        rows = self.s.db.execute("SELECT kind,lane FROM jobs").fetchall()
        self.assertEqual({r[0] for r in rows}, {"tg_text", "tg_file"})
        self.assertEqual(len({r[1] for r in rows}), 2)

    def test_mute_drops_incoming_plan_only(self):
        self.s.ensure_route(42)
        self.s.db.execute("UPDATE routes SET muted=1")
        plan_vk(self.s, Store.vk_key(vk_message()), vk_message(), "UTC")
        self.assertEqual(self.s.backlog(), 0)
        self.assertEqual(tg_payload(tg_message(), self.s, 42)["text"], "Ответ")

    def test_no_old_history_without_explicit_flag(self):
        self.s.set("connected_at", time.time())
        m = vk_message(date=1)
        plan_vk(self.s, Store.vk_key(m), m, "UTC")
        self.assertEqual(self.s.backlog(), 0)
        m["_historical"] = True
        plan_vk(self.s, Store.vk_key(m), m, "UTC")
        self.assertEqual(self.s.backlog(), 1)

    def test_native_own_message_not_suppressed(self):
        m = vk_message(out=1)
        plan_vk(self.s, Store.vk_key(m), m, "UTC")
        self.assertEqual(self.s.backlog(), 1)

    def test_bridge_echo_suppressed(self):
        jid = self.s.add_job("out", "vk_send", {}, peer=42)
        rid = self.s.db.execute("SELECT random_id FROM jobs WHERE id=?", (jid,)).fetchone()[0]
        m = vk_message(out=1, random_id=rid)
        plan_vk(self.s, Store.vk_key(m), m, "UTC")
        self.assertEqual(self.s.backlog(), 1)

    def test_reply_maps_to_original_and_not_cross_peer(self):
        self.s.link(100, 42, 123, 5, True)
        data = tg_payload(tg_message(reply_to_message={"message_id": 100, "text": "original"}), self.s, 42)
        self.assertEqual(data["reply"]["cmid"], 5)
        other = tg_payload(tg_message(reply_to_message={"message_id": 100, "text": "original"}), self.s, 43)
        self.assertNotIn("reply", other)
        self.assertTrue(other["reply_unmapped"])

    def test_unsupported_telegram_not_silently_sent(self):
        with self.assertRaises(Permanent): tg_payload(tg_message(text="", voice={"file_id": "v"}), self.s, 42)

    def test_already_delivered_history_not_repeated(self):
        self.s.link(100, 42, 1, 1, True)
        m = vk_message(_historical=True)
        plan_vk(self.s, Store.vk_key(m), m, "UTC")
        self.assertEqual(self.s.backlog(), 0)


class FakeTG:
    token = "TEST_TOKEN"
    username = "testbot"
    bot_id = 999
    def __init__(self):
        self.sent = []
        self.calls = []
        self.gate = RateGate(0)
        self.counter = 100
        self.topics = 0
    async def call(self, method, **params):
        self.calls.append((method, dict(params)))
        if method == "createForumTopic":
            self.topics += 1
            return {"message_thread_id": 55 + self.topics}
        return True
    async def send(self, data, path=None):
        self.counter += 1
        self.sent.append((dict(data), path.read_bytes() if path else None))
        return {"message_id": self.counter}
    def target(self, thread=None): return {"chat_id": -100123, "message_thread_id": thread}
    async def download(self, file_id, dest, limit): dest.write_bytes(b"document bytes")


class FakeVK:
    token = "TEST_VK_TOKEN"
    def __init__(self):
        self.calls, self.reads = [], []
        self.all_history = []
        self.send_error = None
        self.resolved = {}
    async def call(self, method, **params):
        self.calls.append((method, params))
        if method == "messages.send":
            if self.send_error: raise self.send_error
            return 1000
        if method == "messages.getConversationsById": return {"items": []}
        return {"items": []}
    async def mark_read(self, peer, cmid, mid=0):
        self.reads.append((peer, cmid, mid))
        return 1
    async def history(self, peer, anchor=0, count=200):
        rows = [m for m in self.all_history if not anchor or m["id"] <= anchor]
        return {"count": len(rows), "items": rows[:count]}
    async def resolve(self, *args, **kwargs): return dict(self.resolved)
    async def upload(self, *args, **kwargs): return "doc7_100"


class TestBridge(Harness, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.setup_harness()
        self.vk, self.tg = FakeVK(), FakeTG()
        self.b = Bridge(self.c, self.s, self.vk, self.tg)
        self.s.bind(42, 55)
    async def asyncTearDown(self): self.close_harness()

    async def test_incoming_delivery_does_not_mark_read(self):
        m = vk_message()
        plan_vk(self.s, Store.vk_key(m), m, "UTC")
        await self.b.dispatch(self.s.claim())
        self.assertEqual(len(self.tg.sent), 1)
        self.assertEqual(self.vk.reads, [])
        self.assertEqual(self.s.tg_link(101, 42)["cmid"], 1)

    async def test_live_photo_stub_is_hydrated_before_planning(self):
        stub = vk_message(text="", attachments=[{"type": "photo", "photo": {"id": 1}}])
        self.vk.resolved = vk_message(text="", attachments=[{"type": "photo", "photo": {"id": 1, "sizes": [
            {"url": "https://example.com/full.jpg", "width": 1000, "height": 800}
        ]}}])
        hydrated = await self.b.hydrate_vk_message(stub)
        _, files = message_parts(hydrated, self.s, "UTC")
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["url"], "https://example.com/full.jpg")

    async def test_unbound_topic_warning_is_emitted_once(self):
        first = tg_message(mid=501, message_thread_id=999, text="one")
        second = tg_message(mid=502, message_thread_id=999, text="two")
        self.b.plan_telegram("u1", {"message": first})
        self.b.plan_telegram("u2", {"message": second})
        self.assertEqual(
            self.s.db.execute("SELECT COUNT(*) FROM jobs WHERE kind='notice'").fetchone()[0],
            1,
        )

    async def test_successful_vk_send_sets_lightning_reaction_without_success_notice(self):
        self.b.plan_telegram("r1", {"message": tg_message(mid=600, text="out")})
        await self.b.dispatch(self.s.claim())
        reactions = [(method, params) for method, params in self.tg.calls if method == "setMessageReaction"]
        self.assertEqual(len(reactions), 1)
        self.assertEqual(reactions[0][1]["message_id"], 600)
        self.assertEqual(reactions[0][1]["reaction"], [{"type": "emoji", "emoji": "⚡"}])
        notices = [
            row[0] for row in self.s.db.execute("SELECT payload FROM jobs WHERE kind='notice'").fetchall()
        ]
        self.assertFalse(any("VK подтвердил приём сообщения" in payload for payload in notices))

    async def test_text_survives_document_failure(self):
        m = vk_message(attachments=[{"type": "doc", "doc": {"url": "https://example.com/a"}}])
        plan_vk(self.s, Store.vk_key(m), m, "UTC")
        await self.b.dispatch(self.s.claim())
        filejob = self.s.claim()
        with patch("televk.bridge.download_public", new=AsyncMock(side_effect=Permanent("download failed"))):
            with self.assertRaises(Permanent): await self.b.dispatch(filejob)
        self.b.failed(filejob, "dead", "download failed")
        self.assertEqual(len(self.tg.sent), 1)
        self.assertEqual(self.s.db.execute("SELECT state FROM jobs WHERE kind='tg_text'").fetchone()[0], "done")
        self.assertEqual(self.s.db.execute("SELECT state FROM jobs WHERE kind='tg_file'").fetchone()[0], "dead")

    async def test_successful_reply_enqueues_bounded_read(self):
        self.s.link(77, 42, 900, 90, True)
        update = {"update_id": 1, "message": tg_message(reply_to_message={"message_id": 77, "text": "hi"})}
        self.b.plan_telegram("1", update)
        await self.b.dispatch(self.s.claim())
        self.assertEqual(self.vk.reads, [])
        read = self.s.claim()
        self.assertEqual(read["kind"], "vk_read")
        await self.b.dispatch(read)
        self.assertEqual(self.vk.reads, [(42, 90, 900)])
        payload = self.vk.calls[0][1]
        self.assertEqual(json.loads(payload["forward"])["conversation_message_ids"], [90])

    async def test_failed_reply_never_marks_read(self):
        self.s.link(77, 42, 900, 90, True)
        self.vk.send_error = Uncertain("timeout")
        self.b.plan_telegram("1", {"message": tg_message(reply_to_message={"message_id": 77})})
        with self.assertRaises(Uncertain): await self.b.dispatch(self.s.claim())
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM jobs WHERE kind='vk_read'").fetchone()[0], 0)
        self.assertEqual(self.vk.reads, [])

    async def test_plain_outgoing_never_marks_read(self):
        self.b.plan_telegram("1", {"message": tg_message()})
        await self.b.dispatch(self.s.claim())
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM jobs WHERE kind='vk_read'").fetchone()[0], 0)

    async def test_mute_keeps_outgoing_available(self):
        self.s.db.execute("UPDATE routes SET muted=1")
        self.b.plan_telegram("1", {"message": tg_message()})
        await self.b.dispatch(self.s.claim())
        self.assertEqual(self.vk.calls[0][0], "messages.send")

    async def test_mute_suppresses_already_queued_live(self):
        plan_vk(self.s, "42:c1", vk_message(), "UTC")
        self.s.db.execute("UPDATE routes SET muted=1")
        await self.b.dispatch(self.s.claim())
        self.assertEqual(self.tg.sent, [])
        self.assertEqual(self.s.db.execute("SELECT state FROM jobs").fetchone()[0], "suppressed")

    async def test_unauthorized_telegram_is_ignored(self):
        self.b.plan_telegram("1", {"message": tg_message(**{"from": {"id": 999}})})
        self.assertEqual(self.s.backlog(), 0)

    async def test_edit_is_notice_not_new_outgoing(self):
        self.b.plan_telegram("1", {"edited_message": tg_message()})
        self.assertEqual(self.s.db.execute("SELECT kind FROM jobs").fetchone()[0], "notice")

    async def test_read_defaults_to_last_delivered_not_last_vk_message(self):
        self.s.link(50, 42, 20, 20, True)
        self.s.link(51, 42, 100, 100, False)
        self.b.plan_telegram("1", {"message": tg_message(text="/read")})
        await self.b.dispatch(self.s.claim())
        read = self.s.db.execute("SELECT payload FROM jobs WHERE kind='vk_read'").fetchone()
        self.assertEqual(json.loads(read[0])["cmid"], 20)

    async def test_topics_create_once_under_concurrency(self):
        self.s.ensure_route(43)
        results = await asyncio.gather(self.b.topic(43), self.b.topic(43))
        # Fake API must return a previously unused thread for this conversation.
        self.assertEqual(results[0], results[1])
        self.assertEqual(self.tg.topics, 1)

    async def test_topic_ambiguous_creation_not_repeated(self):
        self.s.ensure_route(43)
        self.tg.call = AsyncMock(side_effect=Uncertain("ambiguous"))
        with self.assertRaises(Retry): await self.b.topic(43)
        with self.assertRaises(Retry): await self.b.topic(43)
        self.assertEqual(self.tg.call.await_count, 1)
        self.assertEqual(self.s.route(43)["topic_state"], "uncertain")

    async def test_paged_full_history_is_finite_and_ordered(self):
        self.vk.all_history = [vk_message(i, date=1000 + i) for i in range(451, 0, -1)]
        self.b.start_import(42)
        calls = 0
        while True:
            task = dict(self.s.db.execute("SELECT * FROM imports WHERE peer=42").fetchone())
            if task["state"] != "scanning": break
            await self.b.import_page(task)
            calls += 1
            self.assertLess(calls, 10)
        rows = self.s.db.execute("SELECT mid FROM archive ORDER BY stamp,mid").fetchall()
        self.assertEqual([r[0] for r in rows], list(range(1, 452)))
        self.assertEqual(calls, 3)

    async def test_history_resume_uses_saved_anchor(self):
        self.vk.all_history = [vk_message(i) for i in range(451, 0, -1)]
        self.b.start_import(42)
        await self.b.import_page(dict(self.s.db.execute("SELECT * FROM imports").fetchone()))
        anchor = self.s.db.execute("SELECT anchor FROM imports").fetchone()[0]
        self.assertEqual(anchor, 252)
        path = self.s.path
        self.s.close()
        self.s = Store(path)
        self.b.s = self.s
        await self.b.import_page(dict(self.s.db.execute("SELECT * FROM imports").fetchone()))
        self.assertEqual(self.s.db.execute("SELECT anchor FROM imports").fetchone()[0], 53)
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM archive").fetchone()[0], 399)

    async def test_invalid_changes_never_accepted(self):
        for value in ({}, {"new_pts": 9}, {"new_pts": 10, "more": True}, {"new_pts": 11, "messages": {"count": 4, "items": []}}):
            with self.subTest(value=value), self.assertRaises(Permanent): Bridge.validate_changes(value, {"pts": 10})

    async def test_valid_paginated_changes(self):
        messages, cur = Bridge.validate_changes({"new_pts": 11, "more": True, "messages": {"items": [vk_message()], "count": 1}}, {"pts": 10})
        self.assertEqual(cur, {"pts": 11})
        self.assertEqual(len(messages), 1)


class TestHTTP(unittest.IsolatedAsyncioTestCase):
    async def test_read_timeout_on_send_is_uncertain(self):
        async def handler(request): raise httpx.ReadTimeout("token=SECRET", request=request)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            with self.assertRaises(Uncertain) as error:
                await request_json(c, "https://example.com/SECRET", effect=True)
            self.assertNotIn("SECRET", str(error.exception))

    async def test_connect_failure_is_retryable(self):
        async def handler(request): raise httpx.ConnectError("failed", request=request)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            with self.assertRaises(Retry): await request_json(c, "https://example.com", effect=True)

    async def test_http_5xx_on_send_is_uncertain(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503))) as c:
            with self.assertRaises(Uncertain): await request_json(c, "https://example.com", effect=True)
            with self.assertRaises(Retry): await request_json(c, "https://example.com", effect=False)

    async def test_rate_limit_delay_preserved(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(429, json={"parameters": {"retry_after": 17}}))) as c:
            with self.assertRaises(Retry) as error: await request_json(c, "https://example.com", effect=True)
            self.assertEqual(error.exception.delay, 17)

    async def test_vk_error_does_not_leak_token(self):
        config = Config(7, -100123, "x", "y")
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"error": {"error_code": 15, "error_msg": "SECRET"}}))) as client:
            v = VK(config, "SECRET", client)
            with self.assertRaises(VKError) as err: await v.call("messages.getHistory")
            self.assertNotIn("SECRET", str(err.exception))

    async def test_vk_read_uses_up_to_cmid(self):
        calls = []
        def handler(r):
            calls.append(r.content.decode())
            return httpx.Response(200, json={"response": 1})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            v = VK(Config(7, -100123, "x", "y"), "test", client)
            await v.mark_read(42, 99)
            self.assertIn("up_to_cmid=99", calls[0])
            self.assertNotIn("mark_conversation_as_read", calls[0])

    async def test_upload_rejects_untrusted_host(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"response": {"upload_url": "https://evil.example/upload"}}))) as client:
            v = VK(Config(7, -100123, "x", "y"), "test", client)
            with self.assertRaises(Permanent): await v.upload(Path("unused"), 42, "document", "a")

    async def test_telegram_photo_and_document_forms(self):
        seen = []
        def handler(r):
            seen.append((str(r.url), r.content))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 10}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            tg = Telegram(Config(7, -100123, "x", "y"), "TEST", client)
            tg.gate.interval = 0
            with tempfile.TemporaryDirectory() as d:
                p = Path(d) / "f"
                p.write_bytes(b"file bytes")
                await tg.send({"thread": 55, "kind": "document", "filename": "a.pdf"}, p)
            self.assertTrue(seen[0][0].endswith("sendDocument"))
            self.assertIn(b'name="document"', seen[0][1])
            self.assertIn(b"file bytes", seen[0][1])

    async def test_failed_update_check_is_not_uptodate(self):
        seen = []
        def handler(r):
            seen.append(str(r.url))
            raise httpx.ConnectError("offline", request=r)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            report = await check_updates(client, Config(7, -100123, "x", "y"))
        self.assertTrue(report["failed"])
        self.assertIn("НЕ ПРОВЕРЕНО", report["text"])
        self.assertTrue(any("github.com" in url for url in seen))
        self.assertFalse(any("pypi.org" in url for url in seen))

    async def test_rate_gate_observes_late_deferral(self):
        gate = RateGate(0)
        gate.defer(0.01)
        start = time.monotonic()
        task = asyncio.create_task(gate.wait())
        await asyncio.sleep(0.005)
        gate.defer(0.03)
        await task
        self.assertGreaterEqual(time.monotonic() - start, 0.03)


if __name__ == "__main__":
    unittest.main()
