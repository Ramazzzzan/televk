# GitHub-first развёртывание и offline-зависимости

TeleVK 0.1.1 исходит из того, что **GitHub-репозиторий — источник истины**, а HTPC не обязан иметь доступ к PyPI. Закреплённые Python wheel-файлы один раз скачиваются на ПК, проверяются по SHA-256, коммитятся в `wheelhouse/`, после чего HTPC устанавливает их локально через `pip --no-index`.

## 1. Подготовить репозиторий на ПК

Скопируй содержимое релиза 0.1.1 **в корень нового репозитория**, а не в подкаталог `televk-0.1.1/`. Не коммить реальные токены, `config.json`, базы, логи или `.env`; штатный `.gitignore` их исключает.

На Windows PowerShell, из корня репозитория:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\prepare-wheelhouse.ps1
```

Если Windows не знает команду `py`, предварительно задай:

```powershell
$env:PYTHON="python"
```

или полный путь к установленному `python.exe`, затем повтори команду.

Скрипт использует доступ ПК к PyPI только для скачивания **точно закреплённых** версий из `requirements.txt` + `constraints.txt`. Он:

1. разрешает только binary wheel-файлы;
2. отбрасывает платформенно-зависимые сборки — для этого релиза допустимы только `py3-none-any`;
3. проверяет, что число wheel-файлов совпадает с constraints;
4. сверяет SHA-256 каждого файла с заранее закреплённым `wheelhouse/EXPECTED_SHA256SUMS`;
5. только после успешной проверки создаёт `wheelhouse/SHA256SUMS`.

Ожидаемый набор 0.1.1:

```text
httpx 0.28.1
httpcore 1.0.9
anyio 4.13.0
h11 0.16.0
idna 3.17
certifi 2026.5.20
socksio 1.0.0
typing_extensions 4.16.0
```

Вручную искать восемь ссылок в браузере не требуется: `pip download` корректно разрешает extra `httpx[socks]` с нашими constraints.

После успешного формирования wheelhouse:

```powershell
git status
git add .
git commit -m "TeleVK 0.1.1: offline wheelhouse and GitHub deployment"
git push
```

Wheel-файлы маленькие и платформенно-независимые, Git LFS для них не нужен.

## 2. Проверка GitHub Actions

После коммита `wheelhouse/*.whl` и `wheelhouse/SHA256SUMS` workflow `.github/workflows/tests.yml`:

- сравнивает `SHA256SUMS` с release-манифестом `EXPECTED_SHA256SUMS`;
- повторно проверяет хеш каждого wheel;
- устанавливает зависимости **только** из wheelhouse;
- выполняет `pip check`;
- запускает весь unit/integration test suite на Python 3.11 и 3.12.

Зелёный workflow подтверждает, что GitHub-репозиторий содержит самодостаточный набор Python-зависимостей для проверенных версий Python. Он не подтверждает права реального VK-токена и настройки Telegram-группы — это проверяется `probe` на HTPC.

## 3. Продолжить уже прерванную установку на HTPC

Твой запуск 0.1.0 остановился на `pip install`. На этой стадии старый installer обычно уже успел создать `/opt/televk`, `/etc/televk/` и `/var/lib/televk/`, но **ещё не создал** `config.json`, SQLite-базу и systemd unit.

После того как новый репозиторий с wheelhouse доступен на HTPC, используй рабочую копию Git:

```bash
git pull --ff-only
sudo bash deploy/install.sh --resume
```

`--resume` специально сделан узким и безопасным. Он удаляет и создаёт заново только незавершённый `/opt/televk`, но сохраняет `/etc/televk` и `/var/lib/televk`. Если уже существует реальная конфигурация, база или systemd unit, он **откажется** продолжать — такой случай считается обновлением живого сервиса, а не восстановлением первой установки.

По умолчанию installer вообще не обращается к PyPI. Он требует совпадения двух SHA-256 manifest-файлов, проверяет wheel-файлы, создаёт новый venv и вызывает pip с:

```text
--no-index --find-links=/opt/televk/wheelhouse
```

`--online` оставлен только как явный дополнительный режим для машин, где package index действительно доступен; для твоего HTPC он не нужен.

## 4. Настроить и выполнить probe

После успешного installer и тестов:

```bash
cd /opt/televk
sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json init
```

Для `STATE_DIR` укажи:

```text
/var/lib/televk
```

Затем:

```bash
sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json check
sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json probe
sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json probe --send-test
```

И только после успешных проверок:

```bash
sudo systemctl enable --now televk
```

## 5. Проверка крупных обновлений без PyPI

В 0.1.1 `/updates` также не требует PyPI. Upstream-версии Python-зависимостей читаются по stable numeric tags официальных GitHub-репозиториев; VK schema, curl и systemd — тоже через GitHub. Локальные Python и SQLite показываются как runtime-информация и должны обновляться средствами ОС.

TeleVK ничего не устанавливает автоматически. Эта проверка предназначена для обнаружения крупных изменений версий, а не для полноценного CVE/security-аудита.
