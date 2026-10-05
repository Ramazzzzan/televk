"""Offline integration tests. Every provider and attachment response is simulated."""
from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs

import httpx

from televk.api import Telegram, VK
from televk.bridge import Bridge
from televk.common import Config, Permanent, Retry, Uncertain
from televk.media import validate_url
from televk.store import Store


class TestServiceCycle(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.config = Config(7, -100123, 'tg.token', 'vk.token', _base=self.base,
                             MIN_FREE_MB=1, VK_POLL_SECONDS=.01, VK_API_INTERVAL=0,
                             TG_SEND_INTERVAL=0, UPDATE_CHECK_HOURS=0)
        self.store = Store(self.config.state / 'bridge.sqlite3')
        self.store.bind(42, 55)
        self.provider_calls, self.telegram_sends, self.vk_sends, self.reads = [], [], [], []
        self.reply_update_returned = False
        self.received_text_id = 0
        self.telegram_message_id = 500
        self.live_batch_sent = False
        self.message = {'id': 80, 'conversation_message_id': 12, 'peer_id': 42,
                        'from_id': 42, 'date': int(time.time()) + 1, 'out': 0,
                        'text': 'INTEGRATION: входящий текст',
                        'attachments': [{'type': 'doc', 'doc': {'id': 1, 'owner_id': 42,
                            'url': 'https://example.org/integration.txt', 'title': 'integration.txt', 'size': 8}},
                            {'type': 'audio_message', 'audio_message': {'id': 3}}]}
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(self.provider))
        self.vk = VK(self.config, 'TEST_ONLY_VK_TOKEN', self.client)
        self.tg = Telegram(self.config, 'TEST_ONLY_TG_TOKEN', self.client)
        self.bridge = Bridge(self.config, self.store, self.vk, self.tg)

    async def asyncTearDown(self):
        self.bridge.stop.set()
        await self.client.aclose()
        self.store.close()
        self.temp.cleanup()

    async def provider(self, request):
        method = request.url.path.rsplit('/', 1)[-1]
        self.provider_calls.append(method)
        form = {k: v[-1] for k, v in parse_qs(request.content.decode(errors='replace')).items()}
        vk = request.url.host == 'api.vk.com'
        if vk:
            if method == 'users.get':
                result = [{'id': 777, 'first_name': 'Test'}]
            elif method == 'messages.getConversations':
                result = {'count': 1, 'items': [{'conversation': {'peer': {'id': 42}}, 'last_message': self.message}]}
            elif method == 'messages.getHistory':
                result = {'count': 1, 'items': [self.message]}
            elif method == 'messages.getLongPollServer':
                result = {'pts': 10, 'ts': '10', 'key': 'TEST', 'server': 'example.org'}
            elif method == 'messages.getLongPollHistory':
                live = self.store.get('connected_at') is not None and not self.live_batch_sent
                result = {'new_pts': 11 if live or self.live_batch_sent else 10,
                          'messages': {'count': int(live), 'items': [self.message] if live else []}, 'more': False}
                self.live_batch_sent = self.live_batch_sent or live
                await asyncio.sleep(.01)
            elif method == 'messages.send':
                self.vk_sends.append(form)
                result = 1000
            elif method == 'messages.markAsRead':
                self.reads.append(form)
                result = 1
            else:
                raise AssertionError(f'Unexpected VK call: {method}')
            return httpx.Response(200, json={'response': result})
        if method == 'getMe':
            result = {'id': 99, 'is_bot': True, 'username': 'integration_bot'}
        elif method == 'getChat':
            result = {'id': -100123, 'type': 'supergroup', 'is_forum': True}
        elif method == 'getChatMember':
            result = {'status': 'administrator', 'can_manage_topics': True}
        elif method == 'getWebhookInfo':
            result = {'url': ''}
        elif method == 'getUpdates':
            await asyncio.sleep(.02)
            result = []
            if self.received_text_id and not self.reply_update_returned:
                result = [{'update_id': 1234, 'message': {'message_id': 900,
                    'date': int(time.time()) + 1, 'chat': {'id': -100123}, 'from': {'id': 7},
                    'message_thread_id': 55, 'text': 'INTEGRATION: мой ответ',
                    'reply_to_message': {'message_id': self.received_text_id}}}]
                self.reply_update_returned = True
        elif method == 'setMessageReaction':
            self.telegram_sends.append((method, form, request.content))
            result = True
        elif method in {'sendMessage', 'sendDocument', 'sendPhoto'}:
            self.telegram_message_id += 1
            self.telegram_sends.append((method, form, request.content))
            result = {'message_id': self.telegram_message_id}
            if method == 'sendMessage' and 'INTEGRATION: входящий' in form.get('text', ''):
                self.received_text_id = self.telegram_message_id
        else:
            raise AssertionError(f'Unexpected Telegram call: {method}')
        return httpx.Response(200, json={'ok': True, 'result': result})

    async def test_start_poll_persist_deliver_reply_read_and_shutdown(self):
        self.store.set('write_probe_token', hashlib.sha256(self.vk.token.encode()).hexdigest())
        async def download(url, dest, limit, reserve):
            dest.write_bytes(b'fixture\n')
        with patch('televk.bridge.download_public', side_effect=download):
            task = asyncio.create_task(self.bridge.run())
            try:
                async with asyncio.timeout(8):
                    while not self.reads or not any(x[0] == 'sendDocument' for x in self.telegram_sends):
                        if task.done():
                            await task
                        await asyncio.sleep(.02)
            finally:
                self.bridge.stop.set()
                await asyncio.wait_for(task, 2)
        self.assertEqual(len(self.vk_sends), 1)
        self.assertEqual(self.vk_sends[0]['message'], 'INTEGRATION: мой ответ')
        reply = json.loads(self.vk_sends[0]['forward'])
        self.assertEqual(reply['conversation_message_ids'], [12])
        self.assertEqual(self.reads[0]['up_to_cmid'], '12')
        self.assertTrue(any(x[0] == 'setMessageReaction' for x in self.telegram_sends))
        self.assertEqual(self.store.get('tg_offset'), 1235)
        self.assertEqual(self.store.get('vk_cursor'), {'pts': 11})
        sent_text = '\n'.join(x[1].get('text', '') for x in self.telegram_sends)
        self.assertIn('audio_message', sent_text)
        self.assertIsNotNone(self.store.tg_link(900, 42))
        self.assertEqual(len(list(self.bridge.media_dir.iterdir())), 0)
        self.assertFalse(any('delete' in m.lower() for m in self.provider_calls))

    async def test_run_requires_token_specific_write_probe(self):
        with self.assertRaises(Permanent):
            await self.bridge.run()
        self.store.set('write_probe_token', hashlib.sha256(b'OLD_TOKEN').hexdigest())
        with self.assertRaises(Permanent):
            await self.bridge.run()
        self.assertEqual(self.vk_sends, [])
        self.assertEqual(self.reads, [])

    async def test_worker_holds_ambiguous_send_and_creates_notice(self):
        jid = self.store.add_job('ambiguous', 'vk_send', {'text': 'test', 'tg': 1, 'thread': 55}, peer=42, lane='vk_out')
        async def ambiguous(job):
            self.store.effect(job['id'])
            self.bridge.stop.set()
            raise Uncertain('simulated lost response')
        with patch.object(self.bridge, 'dispatch', side_effect=ambiguous):
            await self.bridge.worker()
        row = self.store.db.execute('SELECT state FROM jobs WHERE id=?', (jid,)).fetchone()
        self.assertEqual(row[0], 'uncertain')
        self.assertIsNotNone(self.store.db.execute("SELECT 1 FROM jobs WHERE kind='notice'").fetchone())

    async def test_worker_safe_failure_is_retryable(self):
        jid = self.store.add_job('retry', 'tg_text', {}, peer=42, lane='text')
        async def unavailable(job):
            self.bridge.stop.set()
            raise Retry('simulated refusal before send', 1)
        with patch.object(self.bridge, 'dispatch', side_effect=unavailable):
            await self.bridge.worker()
        row = self.store.db.execute('SELECT state,next_at FROM jobs WHERE id=?', (jid,)).fetchone()
        self.assertEqual(row['state'], 'pending')
        self.assertGreater(row['next_at'], time.time())

    async def test_unsupported_media_with_caption_notifies_and_sends_only_text(self):
        m = {'message_id': 1, 'chat': {'id': -100123}, 'from': {'id': 7}, 'message_thread_id': 55,
             'caption': 'Сохранить текст', 'video': {'file_id': 'VID'}}
        self.bridge.plan_telegram('vid1', {'message': m})
        row = self.store.db.execute("SELECT payload FROM jobs WHERE kind='vk_send'").fetchone()
        data = json.loads(row[0])
        self.assertEqual(data['text'], 'Сохранить текст')
        self.assertNotIn('file_id', data)
        note = self.store.db.execute("SELECT payload FROM jobs WHERE kind='notice'").fetchone()
        self.assertIn('video', note[0])

    async def test_cancelled_history_reactivates_only_suppressed_archive(self):
        self.bridge.start_import(42)
        self.store.db.execute("UPDATE imports SET state='cancelled'")
        old = self.store.add_job('archive-old', 'tg_text', {}, peer=42, lane='archive_text')
        live = self.store.add_job('live-old', 'tg_text', {}, peer=42, lane='live_text')
        self.store.finish(old, 'suppressed')
        self.store.finish(live, 'suppressed')
        self.bridge.start_import(42)
        self.assertEqual(self.store.db.execute('SELECT state FROM jobs WHERE id=?', (old,)).fetchone()[0], 'pending')
        self.assertEqual(self.store.db.execute('SELECT state FROM jobs WHERE id=?', (live,)).fetchone()[0], 'suppressed')


    async def test_all_history_upgrades_active_gap_recovery(self):
        self.bridge.start_import(42, since=12345)
        self.store.db.execute("UPDATE imports SET anchor=99")
        self.bridge.start_import(42, since=0)
        row = self.store.db.execute('SELECT * FROM imports WHERE peer=42').fetchone()
        self.assertEqual(row['since'], 0)
        self.assertEqual(row['anchor'], 0)
        self.assertEqual(row['state'], 'scanning')

    async def test_control_poll_can_survive_queue_budget(self):
        from dataclasses import replace
        self.bridge.c = replace(self.config, MAX_PENDING=1)
        self.store.add_job('full', 'tg_text', {}, peer=42)
        with self.assertRaises(Retry):
            self.bridge.capacity()
        self.bridge.capacity(queue=False)

    async def test_early_echo_cmid_preserved_by_send_ack(self):
        jid = self.store.add_job('early', 'vk_send', {'text': 'out', 'tg': 9, 'thread': 55}, peer=42, lane='vk_out')
        job = dict(self.store.db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone())
        self.store.db.execute('UPDATE echoes SET mid=1000,cmid=42 WHERE peer=42')
        await self.bridge.deliver_vk(job, json.loads(job['payload']))
        self.assertEqual(self.store.tg_link(9, 42)['cmid'], 42)


class TestURLRegression(unittest.TestCase):
    def test_malformed_port_rejected_without_raw_exception(self):
        for url in ('https://example.org:BAD/a', 'https://[invalid/a'):
            with self.assertRaises(Permanent):
                validate_url(url)
