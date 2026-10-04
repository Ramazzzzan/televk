from __future__ import annotations

import contextlib
import fcntl
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from televk.__main__ import main
from televk.store import Store


class TestLocalCLI(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.config = self.base / 'config.json'
        self.old_umask = os.umask(0o077)

    def tearDown(self):
        os.umask(self.old_umask)
        self.temp.cleanup()

    def invoke(self, *args):
        output = io.StringIO()
        with patch('sys.argv', ['televk', '--config', str(self.config), *args]), \
             contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            result = main()
        return result, output.getvalue()

    def init_config(self):
        with patch('builtins.input', side_effect=['7', '-100123', '', 'state']), \
             patch('getpass.getpass', side_effect=['FAKE_TG_SECRET', 'FAKE_VK_SECRET']):
            return self.invoke('init')

    def test_init_secrets_are_private_and_not_printed(self):
        code, out = self.init_config()
        self.assertEqual(code, 0)
        self.assertNotIn('FAKE_', out)
        for name in ('telegram.token', 'vk.token'):
            p = self.base / 'secrets' / name
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(self.config.read_text())['TG_OWNER_ID'], 7)

    def test_init_refuses_overwrite(self):
        self.init_config()
        before = self.config.read_bytes()
        code, out = self.invoke('init')
        self.assertEqual(code, 2)
        self.assertEqual(self.config.read_bytes(), before)

    def test_offline_check(self):
        self.init_config()
        code, out = self.invoke('check')
        self.assertEqual(code, 0)
        self.assertIn('No API access was tested', out)

    def test_offline_check_rejects_exposed_token(self):
        self.init_config()
        (self.base / 'secrets' / 'vk.token').chmod(0o644)
        code, out = self.invoke('check')
        self.assertEqual(code, 2)
        self.assertIn('permissions', out)
        self.assertNotIn('FAKE_VK_SECRET', out)

    def test_unknown_config_setting_rejected(self):
        self.init_config()
        obj = json.loads(self.config.read_text())
        obj['TYPO_SETTING'] = 5
        self.config.write_text(json.dumps(obj))
        code, out = self.invoke('check')
        self.assertEqual(code, 2)
        self.assertIn('TYPO_SETTING', out)

    def test_backup_and_no_overwrite(self):
        self.init_config()
        s = Store(self.base / 'state' / 'bridge.sqlite3')
        s.bind(42, 55)
        s.close()
        dest = self.base / 'backup.sqlite3'
        code, _ = self.invoke('backup', str(dest))
        self.assertEqual(code, 0)
        copied = Store(dest)
        self.assertEqual(copied.route(42)['thread'], 55)
        copied.close()
        code, _ = self.invoke('backup', str(dest))
        self.assertEqual(code, 2)

    def test_second_process_lock_rejected(self):
        self.init_config()
        state = self.base / 'state'
        state.mkdir()
        with (state / 'process.lock').open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            code, out = self.invoke('check')
        self.assertEqual(code, 2)
        self.assertIn('Another TeleVK process', out)
