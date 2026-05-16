import "dotenv/config";
import { TelegramClient, Api } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import { computeCheck } from "telegram/Password.js";
import { api } from "./api.js";

const {
  TELEGRAM_API_ID,
  TELEGRAM_API_HASH,
  LOVABLE_BASE_URL,
  WORKER_SECRET,
} = process.env;

const SYNC_INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS || 3000);
const PULL_INTERVAL_MS = Number(process.env.PULL_INTERVAL_MS || 5000);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 30000);
const MIN_DELAY = Number(process.env.MIN_SEND_DELAY_MS || 8000);
const MAX_DELAY = Number(process.env.MAX_SEND_DELAY_MS || 20000);

for (const [k, v] of Object.entries({
  TELEGRAM_API_ID, TELEGRAM_API_HASH, LOVABLE_BASE_URL, WORKER_SECRET,
})) {
  if (!v) { console.error(`Missing env: ${k}`); process.exit(1); }
}

const apiId = Number(TELEGRAM_API_ID);
const apiHash = TELEGRAM_API_HASH;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = () => MIN_DELAY + Math.floor(Math.random() * (MAX_DELAY - MIN_DELAY));

/**
 * State per account:
 *   { client, status, busy, pulling, heartbeat }
 */
const accounts = new Map();

/**
 * Pending login clients: accountId -> { client, expiresAt }
 * Sign-in нужно завершить на ТОМ ЖЕ клиенте (auth_key), что и SendCode/SignIn,
 * иначе CheckPassword не пройдёт. Держим клиент живым между шагами.
 */
const pendingLoginClients = new Map();
const PENDING_LOGIN_TTL_MS = 10 * 60 * 1000;

async function getOrCreateLoginClient(accountId, sessionString = "") {
  const existing = pendingLoginClients.get(accountId);
  if (existing && existing.expiresAt > Date.now()) {
    return existing.client;
  }
  if (existing) {
    await existing.client.disconnect().catch(() => {});
    pendingLoginClients.delete(accountId);
  }
  const client = await newClient(sessionString || "");
  await client.connect();
  pendingLoginClients.set(accountId, {
    client,
    expiresAt: Date.now() + PENDING_LOGIN_TTL_MS,
  });
  return client;
}

async function disposeLoginClient(accountId) {
  const entry = pendingLoginClients.get(accountId);
  if (!entry) return;
  pendingLoginClients.delete(accountId);
  await entry.client.disconnect().catch(() => {});
}

function log(accountId, level, event, data) {
  const tag = accountId ? accountId.slice(0, 8) : "----";
  console.log(`[${level}][${tag}] ${event}`, data || "");
  if (accountId) {
    api.log(accountId, level, event, data || null).catch(() => {});
  }
}

async function newClient(sessionString = "") {
  const session = new StringSession(sessionString);
  const client = new TelegramClient(session, apiId, apiHash, {
    connectionRetries: 5,
    autoReconnect: true,
  });
  return client;
}

async function startConnectedClient(state, account) {
  const client = await newClient(account.session_string);
  await client.connect();
  const me = await client.getMe();
  state.client = client;
  state.me = me;

  // listen for incoming messages
  client.addEventHandler(async (event) => {
    const m = event.message;
    if (!m || m.out) return;
    const sender = await m.getSender().catch(() => null);
    try {
      await api.inbound({
        account_id: account.id,
        telegram_user_id: sender?.id ? String(sender.id) : null,
        username: sender?.username || null,
        first_name: sender?.firstName || null,
        last_name: sender?.lastName || null,
        text: m.message || "",
        telegram_message_id: String(m.id),
      });
    } catch (e) {
      log(account.id, "error", "inbound_failed", { error: e.message });
    }
  }, new NewMessage({ incoming: true }));

  // listen for reactions on our messages in private chats (raw updates)
  client.addEventHandler(async (update) => {
    if (!update || update.className !== "UpdateMessageReactions") return;
    const peer = update.peer;
    if (!peer || peer.className !== "PeerUser") return; // только личка
    const userIdStr = String(peer.userId);
    const results = update.reactions?.results || [];
    // Берём последнюю реакцию (обычно она и есть только что поставленная)
    const emojis = results
      .map((r) => {
        const reaction = r.reaction;
        if (!reaction) return null;
        if (reaction.emoticon) return reaction.emoticon;
        if (reaction.documentId) return "✨"; // кастомный эмодзи / стикер-реакция
        return null;
      })
      .filter(Boolean);
    if (emojis.length === 0) {
      // Реакция снята — игнорируем
      return;
    }
    const emoji = emojis[emojis.length - 1];
    let sender = null;
    try { sender = await client.getEntity(BigInt(userIdStr)); } catch {}
    try {
      await api.inboundReaction({
        account_id: account.id,
        telegram_user_id: userIdStr,
        username: sender?.username || null,
        first_name: sender?.firstName || null,
        last_name: sender?.lastName || null,
        emoji,
        msg_id: update.msgId,
      });
      log(account.id, "info", "reaction_received", { from: userIdStr, emoji });
    } catch (e) {
      log(account.id, "error", "reaction_inbound_failed", { error: e.message });
    }
  });

  log(account.id, "info", "connected", {
    username: me.username,
    user_id: String(me.id),
  });

  // Update display_name on first connect
  await api.updateAccount({
    account_id: account.id,
    status: "connected",
    display_name: me.username || me.firstName || account.phone,
  });
}

async function handleLoginRequested(account) {
  log(account.id, "info", "sending_code", { phone: account.phone });
  // Сбрасываем предыдущую попытку логина (если была)
  await disposeLoginClient(account.id);
  const client = await getOrCreateLoginClient(account.id);
  try {
    const result = await client.invoke(
      new Api.auth.SendCode({
        phoneNumber: account.phone,
        apiId,
        apiHash,
        settings: new Api.CodeSettings({
          allowFlashcall: false,
          currentNumber: true,
          allowAppHash: false,
        }),
      }),
    );
    await api.updateAccount({
      account_id: account.id,
      status: "code_sent",
      session_string: client.session.save(),
      pending_phone_code_hash: result.phoneCodeHash,
      last_error: null,
    });
    log(account.id, "info", "code_sent", null);
  } catch (e) {
    await disposeLoginClient(account.id);
    await api.updateAccount({
      account_id: account.id,
      status: "error",
      last_error: `sendCode: ${e.message}`,
    });
    log(account.id, "error", "send_code_failed", { error: e.message });
  }
}

async function handleCodeSubmitted(account) {
  if (!account.pending_phone_code_hash) {
    await api.updateAccount({
      account_id: account.id,
      status: "error",
      last_error: "Нет phone_code_hash, перезапусти логин",
      clear_pending_code: true,
    });
    return;
  }
  // ВАЖНО: используем тот же auth_key, что слал код. Если воркер перезапускался,
  // восстанавливаем временную StringSession из БД, а не создаём пустую сессию.
  const entry = pendingLoginClients.get(account.id);
  if ((!entry || entry.expiresAt <= Date.now()) && !account.session_string) {
    await api.updateAccount({
      account_id: account.id,
      status: "error",
      last_error: "Сессия логина истекла, нажми «Заново»",
      clear_pending_code: true,
      clear_phone_code_hash: true,
    });
    return;
  }
  const client = await getOrCreateLoginClient(account.id, account.session_string || "");
  try {
    await client.invoke(
      new Api.auth.SignIn({
        phoneNumber: account.phone,
        phoneCodeHash: account.pending_phone_code_hash,
        phoneCode: account.pending_code,
      }),
    );
    // Success без 2FA
    const sessionString = client.session.save();
    await api.updateAccount({
      account_id: account.id,
      status: "connected",
      session_string: sessionString,
      clear_pending_code: true,
      clear_phone_code_hash: true,
      last_error: null,
    });
    log(account.id, "info", "logged_in", null);
    await disposeLoginClient(account.id);
  } catch (e) {
    if (String(e.message || e.errorMessage || "").includes("SESSION_PASSWORD_NEEDED")) {
      await api.updateAccount({
        account_id: account.id,
        status: "password_required",
        session_string: client.session.save(),
        clear_pending_code: true,
        last_error: null,
      });
      log(account.id, "info", "password_required", null);
      // Клиент НЕ закрываем — нужен для CheckPassword
    } else {
      await disposeLoginClient(account.id);
      await api.updateAccount({
        account_id: account.id,
        status: "error",
        last_error: `signIn: ${e.message || e.errorMessage}`,
        clear_pending_code: true,
      });
      log(account.id, "error", "sign_in_failed", { error: e.message });
    }
  }
}

async function handlePasswordSubmitted(account) {
  const entry = pendingLoginClients.get(account.id);
  if ((!entry || entry.expiresAt <= Date.now()) && !account.session_string) {
    await api.updateAccount({
      account_id: account.id,
      status: "error",
      last_error: "Сессия логина истекла, нажми «Заново»",
      clear_pending_password: true,
    });
    return;
  }
  const client = await getOrCreateLoginClient(account.id, account.session_string || "");
  try {
    const pwd = await client.invoke(new Api.account.GetPassword());
    const check = await computeCheck(pwd, account.pending_password);
    await client.invoke(new Api.auth.CheckPassword({ password: check }));
    const sessionString = client.session.save();
    await api.updateAccount({
      account_id: account.id,
      status: "connected",
      session_string: sessionString,
      clear_pending_password: true,
      clear_phone_code_hash: true,
      last_error: null,
    });
    log(account.id, "info", "logged_in_2fa", null);
    await disposeLoginClient(account.id);
  } catch (e) {
    // Не закрываем клиент — пусть пользователь введёт пароль ещё раз
    await api.updateAccount({
      account_id: account.id,
      status: "password_required",
      session_string: client.session.save(),
      last_error: `2fa: ${e.message || e.errorMessage}`,
      clear_pending_password: true,
    });
    log(account.id, "error", "password_failed", { error: e.message });
  }
}

async function resolveContact(client, c) {
  if (!c) throw new Error("contact is missing");
  if (c.username) return await client.getEntity(c.username);
  if (c.telegramUserId) return await client.getEntity(BigInt(c.telegramUserId));
  if (c.phone) return await client.getEntity(c.phone);
  throw new Error("contact has no username/user_id/phone");
}

async function processSendQueue(account, state) {
  if (state.pulling || !state.client) return;
  state.pulling = true;
  try {
    const { items } = await api.pull(account.id, 5);
    for (const item of items || []) {
      try {
        const entity = await resolveContact(state.client, item.target);
        // Показываем "печатает..." и держим его, пока имитируем набор текста.
        // Скорость ~ как у обычного человека: ~70 мс на символ, 1.5..8 сек.
        const text = item.content || "";
        const typingMs = Math.min(8000, Math.max(1500, text.length * 70));
        try {
          await state.client.invoke(new Api.messages.SetTyping({
            peer: entity,
            action: new Api.SendMessageTypingAction(),
          }));
        } catch {}
        await sleep(typingMs);
        const sendOpts = { message: text };
        if (item.replyToMessageId) {
          // Отвечаем цитатой на исходное сообщение собеседника
          sendOpts.replyTo = Number(item.replyToMessageId);
        }
        const sent = await state.client.sendMessage(entity, sendOpts);
        await api.ack(item.queueId, true, null, entity?.id ? String(entity.id) : null);
        log(account.id, "info", "sent", {
          to: item.target?.username || item.target?.telegramUserId || item.target?.phone,
        });
      } catch (err) {
        await api.ack(item.queueId, false, err.message || String(err), null);
        log(account.id, "error", "send_failed", { id: item.queueId, error: err.message });
      }
    }
  } catch (e) {
    log(account.id, "error", "pull_failed", { error: e.message });
  } finally {
    state.pulling = false;
  }
}

async function syncOnce() {
  let resp;
  try {
    resp = await api.accounts();
  } catch (e) {
    console.error("accounts sync failed:", e.message);
    return;
  }
  const list = resp.accounts || [];
  const seen = new Set();
  for (const account of list) {
    seen.add(account.id);
    let state = accounts.get(account.id);
    if (!state) {
      state = { busy: false, pulling: false, lastHeartbeat: 0 };
      accounts.set(account.id, state);
    }
    if (state.busy) continue;
    state.busy = true;

    try {
      // State machine
      if (account.status === "login_requested") {
        await handleLoginRequested(account);
      } else if (account.status === "code_submitted" && account.pending_code) {
        await handleCodeSubmitted(account);
      } else if (account.status === "password_submitted" && account.pending_password) {
        await handlePasswordSubmitted(account);
      } else if (
        (account.status === "connected" || account.status === "active") &&
        account.session_string &&
        !state.client
      ) {
        await startConnectedClient(state, account);
      } else if (account.status === "paused" && state.client) {
        // Disconnect client when paused
        await state.client.disconnect().catch(() => {});
        state.client = null;
        log(account.id, "info", "paused", null);
      } else if (account.status === "disconnected" && state.client) {
        await state.client.disconnect().catch(() => {});
        state.client = null;
      }
    } catch (e) {
      log(account.id, "error", "state_handler_failed", { status: account.status, error: e.message });
    } finally {
      state.busy = false;
    }

    // Heartbeat for connected
    if (state.client && Date.now() - state.lastHeartbeat > HEARTBEAT_INTERVAL_MS) {
      state.lastHeartbeat = Date.now();
      api.heartbeat(account.id, "connected").catch(() => {});
    }
  }

  // Cleanup removed accounts
  for (const [id, state] of accounts.entries()) {
    if (!seen.has(id)) {
      if (state.client) await state.client.disconnect().catch(() => {});
      accounts.delete(id);
    }
  }
}

async function pullAllQueues() {
  for (const account of (await api.accounts().catch(() => ({ accounts: [] }))).accounts || []) {
    const state = accounts.get(account.id);
    if (state?.client && (account.status === "connected" || account.status === "active")) {
      processSendQueue(account, state).catch(() => {});
    }
  }
}

async function main() {
  console.log("Telegram worker started.");
  console.log("  base:", LOVABLE_BASE_URL);

  // Sync loop
  (async () => {
    while (true) {
      await syncOnce().catch((e) => console.error("sync error:", e));
      await sleep(SYNC_INTERVAL_MS);
    }
  })();

  // Pull-queue loop
  (async () => {
    while (true) {
      await pullAllQueues().catch(() => {});
      await sleep(PULL_INTERVAL_MS);
    }
  })();
}

main();

process.on("SIGTERM", async () => {
  console.log("Shutting down...");
  for (const [id, state] of accounts.entries()) {
    if (state.client) {
      await api.heartbeat(id, "disconnected").catch(() => {});
      await state.client.disconnect().catch(() => {});
    }
  }
  process.exit(0);
});
