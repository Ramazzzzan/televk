# Источники и база совместимости

Публичные документы проверялись при подготовке 4 октября 2026 года. Источники описывают методы и ограничения платформ, но не подтверждают выданные права конкретному VK-приложению или успешность живого подключения.

| Источник | Что использовано |
|---|---|
| https://github.com/Ramazzzzan/telemax/blob/main/README.md | Telemax 3.5.2: темы, односторонний mute, очередь, неопределённые отправки, эксплуатационная модель |
| https://github.com/Ramazzzzan/telemax/blob/main/telemax.py | Изучение структуры текущего проекта; не является зависимостью TeleVK |
| https://github.com/VKCOM/vk-api-schema/blob/master/messages/methods.json | Методы истории, диалогов, отправки, replies, getLongPollHistory, markAsRead с up_to_cmid |
| https://github.com/VKCOM/vk-api-schema/blob/master/messages/responses.json | Формат сообщений, new_pts и more |
| https://github.com/VKCOM/vk-api-schema/blob/master/messages/objects.json | Идентификаторы сообщений и объект forward |
| https://github.com/VKCOM/vk-api-schema/blob/master/docs/methods.json | Загрузка и сохранение документов |
| https://github.com/VKCOM/vk-api-schema/blob/master/photos/methods.json | Загрузка фотографий в сообщения |
| https://github.com/VKCOM/vk-api-schema/blob/master/errors.json | Коды отказов, включая 907 — старый pts |
| https://github.com/VKCOM/vk-api-schema/blob/master/package.json | Семейство опубликованной схемы; не разрешение на доступ к сообщениям |
| https://core.telegram.org/bots/api | Telegram Bot API; база документации 10.3, методы тем, ответы и обновления |
| https://core.telegram.org/bots/faq | Доступ сообщений боту-администратору; файлы около 20/50 МБ; общий предел группы 20 сообщений/мин |
| https://pypi.org/project/httpx/ | Закреплён стабильный HTTPX 0.28.1 с extras socks; серия 1.0.dev не выбиралась как стабильная |

Снимки, прочитанные в репозитории VK: `messages/methods.json` blob `f9273ff8dbdb17bddd56efd46435031e175652f1`; `messages/responses.json` blob `11712ee08e7a42404784e97b05edf9c742386d7b`. Версия вызовов в конфигурации — 5.199. Заявлять, что все разрешения и методы будут работать с произвольным пользовательским токеном, нельзя.

## Источники встроенной проверки обновлений

Начиная с 0.1.1 запущенный сервис **не обращается к PyPI**. Для Python-зависимостей используются GitHub tags официальных upstream-репозиториев: `encode/httpx`, `encode/httpcore`, `agronholm/anyio`, `python-hyper/h11`, `kjd/idna`, `certifi/python-certifi`, `sethmlarson/socksio`, `python/typing_extensions`. VK schema читается из `VKCOM/vk-api-schema`; для curl и systemd используются GitHub Releases. Python и SQLite показываются как локальные runtime-версии и должны обновляться пакетным менеджером ОС.

Это источники, к которым **будет обращаться запущенный сервис**, а не утверждение, что найдено и проверено любое актуальное обновление всех перечисленных компонентов в среде пользователя. При недоступности GitHub отчёт прямо указывает неполную проверку. Проверка версий не заменяет security advisory/CVE feed и не сравнивает набор патчей пакета конкретного Linux-дистрибутива.

## Источники offline-wheelhouse

Закреплённые версии и SHA-256 wheel-файлов release-манифеста сверены с публикациями PyPI. Для 0.1.1 фиксированы `httpx 0.28.1`, `httpcore 1.0.9`, `anyio 4.13.0`, `h11 0.16.0`, `idna 3.17`, `certifi 2026.5.20`, `socksio 1.0.0`, `typing_extensions 4.16.0`. `tools/prepare-wheelhouse.*` не доверяет только факту успешной загрузки: каждый файл должен совпасть с `wheelhouse/EXPECTED_SHA256SUMS` до коммита в GitHub.

Страницы релизов: https://pypi.org/project/httpx/0.28.1/ ; https://pypi.org/project/httpcore/1.0.9/ ; https://pypi.org/project/anyio/4.13.0/ ; https://pypi.org/project/h11/0.16.0/ ; https://pypi.org/project/idna/3.17/ ; https://pypi.org/project/certifi/2026.5.20/ ; https://pypi.org/project/socksio/1.0.0/ ; https://pypi.org/project/typing-extensions/4.16.0/ .
