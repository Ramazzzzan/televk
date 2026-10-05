"""Streaming/downloader control tests with a fake curl process (no real network)."""
from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from televk.common import Permanent, Retry
from televk.media import download_public, resolve_public


class FakeStdin:
    def __init__(self): self.value = b''
    def write(self, data): self.value += data
    async def drain(self): pass
    def close(self): pass


class FakeProcess:
    def __init__(self, body, status=0):
        self.stdin = FakeStdin()
        self.stdout = asyncio.StreamReader()
        self.stdout.feed_data(body)
        self.stdout.feed_eof()
        self.returncode, self.status = None, status
    async def wait(self):
        if self.returncode is None:
            self.returncode = self.status
        return self.returncode
    def kill(self): self.returncode = -9


class TestDownloader(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dest = Path(self.temp.name) / 'file'
        self.calls, self.processes = [], []

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def run_download(self, replies, limit=64):
        async def create(*args, **kwargs):
            self.calls.append(args)
            header, body, code = replies[min(len(self.calls) - 1, len(replies) - 1)]
            Path(args[args.index('--dump-header') + 1]).write_bytes(header)
            proc = FakeProcess(body, code)
            self.processes.append(proc)
            return proc
        with patch('televk.media.shutil.which', return_value='/usr/bin/curl'), \
             patch('televk.media.resolve_public', new=AsyncMock(return_value='1.1.1.1')), \
             patch('televk.media.disk_check'), \
             patch('televk.media.asyncio.create_subprocess_exec', side_effect=create):
            await download_public('https://example.org/a?secret=SIGNED', self.dest, limit, 1)

    async def test_stream_download_and_no_signed_url_in_argv(self):
        await self.run_download([(b'HTTP/2 200\r\ncontent-length: 4\r\n\r\n', b'DATA', 0)])
        self.assertEqual(self.dest.read_bytes(), b'DATA')
        self.assertFalse(any('SIGNED' in arg for arg in self.calls[0]))
        self.assertIn(b'SIGNED', self.processes[0].stdin.value)
        self.assertIn(b'resolve = "example.org:443:1.1.1.1"', self.processes[0].stdin.value)
        self.assertIn('--disable', self.calls[0])
        self.assertEqual(list(Path(self.temp.name).glob('hdr-*')), [])

    async def test_redirect_revalidates_private_target(self):
        with self.assertRaises(Permanent):
            await self.run_download([(b'HTTP/2 302\r\nlocation: https://127.0.0.1/secrets\r\n\r\n', b'', 0)])
        self.assertEqual(len(self.calls), 1)

    async def test_streaming_hard_limit_without_content_length(self):
        with self.assertRaises(Permanent):
            await self.run_download([(b'HTTP/2 200\r\n\r\n', b'ABCDE', 0)], limit=4)
        self.assertEqual(self.processes[0].returncode, -9)

    async def test_empty_attachment_is_permanent(self):
        with self.assertRaisesRegex(Permanent, "empty"):
            await self.run_download([(b'HTTP/2 200\r\ncontent-length: 0\r\n\r\n', b'', 0)])

    async def test_incomplete_file_is_retryable(self):
        with self.assertRaises(Retry):
            await self.run_download([(b'HTTP/2 200\r\ncontent-length: 5\r\n\r\n', b'ABC', 0)])

    async def test_malformed_size_is_controlled_failure(self):
        with self.assertRaises(Permanent):
            await self.run_download([(b'HTTP/2 200\r\ncontent-length: xyz\r\n\r\n', b'A', 0)])

    async def test_redirect_loop_is_bounded(self):
        with self.assertRaises(Permanent):
            await self.run_download([(b'HTTP/2 302\r\nlocation: /next\r\n\r\n', b'', 0)])
        self.assertEqual(len(self.calls), 6)

    async def test_dns_mixed_public_private_rejected(self):
        loop = asyncio.get_running_loop()
        rows = [(2, 1, 6, '', ('1.1.1.1', 443)), (2, 1, 6, '', ('10.0.0.1', 443))]
        with patch.object(loop, 'getaddrinfo', new=AsyncMock(return_value=rows)):
            with self.assertRaises(Permanent):
                await resolve_public('example.org', 443)
