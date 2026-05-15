# Telegram Worker

Один воркер на Node.js, держит **несколько Telegram-аккаунтов** одновременно. Логин аккаунтов — через админку Lovable (`/admin/telegram`), без редактирования файлов.

## Архитектура

- Каждые 3 сек воркер пулит список аккаунтов из Lovable (`/api/public/worker/accounts`)
- Для каждого аккаунта смотрит на `status`:
  - `login_requested` → шлёт код в Telegram (`sendCode`), пишет назад `pending_phone_code_hash` + статус `code_sent`
  - `code_submitted` (админ ввёл код) → пытается залогиниться. Если просит 2FA → `password_required`, иначе сохраняет `session_string` и `connected`
  - `password_submitted` → проверяет пароль, сохраняет сессию
  - `connected` → пулит очередь сообщений и слушает входящие
- Все события логируются в `/admin/telegram`

## Локальный запуск (для тестов)

```bash
cd worker
cp .env.example .env
# Заполни WORKER_SECRET (тот же, что в Lovable)
npm install
npm start
```

После запуска иди в `/admin/telegram` → добавь аккаунт → введи код в открывшемся окне.


