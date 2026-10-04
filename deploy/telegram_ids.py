#!/usr/bin/env python3
"""Read IDs from a NEW bot's pending updates; never run beside a live poller."""
from __future__ import annotations
import getpass
import json
import sys
import httpx


def main() -> int:
    print('Для НОВОГО бота: добавь его администратором группы с темами и напиши там /ids.')
    print('Сервис TeleVK и другие получатели getUpdates этого бота должны быть остановлены.')
    token = getpass.getpass('Токен бота (ввод скрыт): ').strip()
    proxy = input('Прокси Telegram или пусто: ').strip()
    try:
        with httpx.Client(proxy=proxy or None, trust_env=False, follow_redirects=False, timeout=45) as client:
            r = client.post(f'https://api.telegram.org/bot{token}/getUpdates',
                            data={'timeout': '0', 'allowed_updates': json.dumps(['message'])})
            r.raise_for_status()
            body = r.json()
        if not body.get('ok'):
            print('Telegram отклонил запрос; проверь токен и отсутствие webhook. Данные ответа не выводятся.', file=sys.stderr)
            return 2
        seen = set()
        for update in body.get('result', []):
            message = update.get('message') or {}
            pair = (message.get('from', {}).get('id'), message.get('chat', {}).get('id'))
            if pair not in seen and all(pair):
                seen.add(pair)
                print(f'user_id={pair[0]}  chat_id={pair[1]}  thread_id={message.get("message_thread_id", 0)}')
        if not seen:
            print('Сообщений нет: напиши /ids от своего аккаунта в новой группе и повтори.')
        return 0
    except (httpx.HTTPError, ValueError, KeyError):
        print('Проверка не выполнена: ошибка сети/API. URL и токен не выводятся.', file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
