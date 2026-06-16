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
const CONNECT_TIMEOUT_MS = Number(process.env.CONNECT_TIMEOUT_MS || 25000);
const TELEGRAM_REQUEST_TIMEOUT_MS = Number(process.env.TELEGRAM_REQUEST_TIMEOUT_MS || 30000);
const ACCOUNT_STEP_TIMEOUT_MS = Number(process.env.ACCOUNT_STEP_TIMEOUT_MS || 45000);

for (const [k, v] of Object.entries({
  TELEGRAM_API_ID, TELEGRAM_API_HASH, LOVABLE_BASE_URL, WORKER_SECRET,
})) {
  if (!v) { console.error(`Missing env: ${k}`); process.exit(1); }
}

const apiId = Number(TELEGRAM_API_ID);
const apiHash = TELEGRAM_API_HASH;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = () => MIN_DELAY + Math.floor(Math.random() * (MAX_DELAY - MIN_DELAY));

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timeout after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

async function connectClient(client, label = "telegram connect") {
  try {
    await withTimeout(client.connect(), CONNECT_TIMEOUT_MS, label);
  } catch (e) {
    await client.disconnect().catch(() => {});
    throw e;
  }
}

function telegramRequest(promise, label) {
  return withTimeout(promise, TELEGRAM_REQUEST_TIMEOUT_MS, label);
}

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

async function getOrCreateLoginClient(accountId, sessionString = "", proxy = null) {
  const existing = pendingLoginClients.get(accountId);
  if (existing && existing.expiresAt > Date.now()) {
    return existing.client;
  }
  if (existing) {
    await existing.client.disconnect().catch(() => {});
    pendingLoginClients.delete(accountId);
  }
  const client = await newClient(sessionString || "", proxy);
  await connectClient(client, `login connect ${accountId.slice(0, 8)}`);
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

/**
 * Сигнал о состоянии соединения аккаунта (вероятная пропажа/возврат прокси).
 * Шлём на сервер только на ПЕРЕХОДАХ (down<->restored), чтобы не спамить
 * HTTP-ом каждый цикл синхронизации. Идемпотентность дополнительно держится
 * на сервере.
 */
function setConnAlert(account, state, kind, error) {
  if (state.connAlert === kind) return;
  state.connAlert = kind;
  api.connectionAlert(account.id, kind, error).catch(() => {});
}

// Преобразует прокси из БД (host/port/username/password) в формат gramjs SOCKS5.
function toGramProxy(proxy) {
  if (!proxy || !proxy.host || !proxy.port) return undefined;
  const p = {
    ip: proxy.host,
    port: Number(proxy.port),
    socksType: 5,
  };
  if (proxy.username) p.username = proxy.username;
  if (proxy.password) p.password = proxy.password;
  return p;
}

async function newClient(sessionString = "", proxy = null) {
  const session = new StringSession(sessionString);
  const gramProxy = toGramProxy(proxy);
  const opts = {
    connectionRetries: 5,
    autoReconnect: true,
  };
  if (gramProxy) opts.proxy = gramProxy;
  const client = new TelegramClient(session, apiId, apiHash, opts);
  return client;
}

async function startConnectedClient(state, account) {
  const client = await newClient(account.session_string, account.proxy);
  let me;
  try {
    await connectClient(client, `connect ${account.id.slice(0, 8)}`);
    me = await telegramRequest(client.getMe(), `getMe ${account.id.slice(0, 8)}`);
  } catch (e) {
    // Не удалось подключиться — почти всегда это умерший/кончившийся прокси
    // (трафик идёт через него). Сообщаем админу и пробуем ещё раз позже.
    await client.disconnect().catch(() => {});
    setConnAlert(account, state, "down", e.message);
    throw e;
  }
  // Подключение живо — если до этого падало, сообщаем что прокси снова работает.
  setConnAlert(account, state, "restored");
  state.client = client;
  state.me = me;


  // Прогреваем поток updates И populate-им entity-кэш (access_hash) для всех
  // диалогов аккаунта. Без этого getEntity(BigInt(userId)) для контактов, с
  // которыми бот ещё не переписывался через эту сессию, падает с
  // "Could not find the input entity for PeerUser" — и ссылка не уходит.
  try {
    await telegramRequest(client.getDialogs({ limit: 200 }), `getDialogs ${account.id.slice(0, 8)}`);
  } catch (e) {
    log(account.id, "warn", "warmup_dialogs_failed", { error: e.message });
  }
  state.lastDialogsRefreshAt = Date.now();


  // Определяет тип медиа у сообщения без текста (голосовое/фото/стикер/кружок),
  // чтобы сервер не ронял пустой текст и бот отвечал по-человечески.
  function detectMediaType(m) {
    if (!m || m.message) return null;
    try {
      if (m.voice) return "voice";
      if (m.videoNote) return "video_note";
      if (m.sticker) return "sticker";
      if (m.gif) return "video";
      if (m.video) return "video";
      if (m.photo) return "photo";
      if (m.document) return "document";
      const media = m.media;
      if (media) {
        const cn = media.className || "";
        if (cn === "MessageMediaPhoto") return "photo";
        if (cn === "MessageMediaDocument") return "document";
      }
    } catch {}
    return null;
  }

  // listen for incoming messages

  client.addEventHandler(async (event) => {
    const m = event.message;
    if (!m || m.out) return;
    // ЖЁСТКОЕ ПРАВИЛО: отвечаем ТОЛЬКО на сообщения в ЛИЧКЕ (PeerUser).
    // Сообщения в группах/каналах (PeerChat/PeerChannel) — где аккаунт сидит
    // ради сбора контактов — НИКОГДА не обрабатываем как входящие. Иначе бот
    // видит, как люди переписываются в публичном чате, и начинает писать им
    // первым. Спамим строго тех, кого собрали в базу (source='harvest'),
    // и только по расписанию (утро/вечер). Здесь — отсечка чужих чатов.
    const peerCn = m.peerId?.className;
    if (peerCn && peerCn !== "PeerUser") return;
    const sender = await m.getSender().catch(() => null);
    // Дополнительная защита: отправитель должен быть обычным пользователем,
    // а не каналом/группой/ботом.
    if (!sender || sender.className !== "User" || sender.bot) return;
    // Detect reply to OUR Telegram story (MessageReplyStoryHeader has storyId)
    const replyTo = m.replyTo;
    const isStoryReply = !!(replyTo && (replyTo.className === "MessageReplyStoryHeader" || replyTo.storyId));
    const storyId = isStoryReply ? String(replyTo.storyId ?? "") : null;
    try {
      await api.inbound({
        account_id: account.id,
        telegram_user_id: sender?.id ? String(sender.id) : null,
        username: sender?.username || null,
        first_name: sender?.firstName || null,
        last_name: sender?.lastName || null,
        text: m.message || "",
        media_type: detectMediaType(m),
        telegram_message_id: String(m.id),
        reply_to_story: isStoryReply,
        story_id: storyId,
      });
    } catch (e) {
      log(account.id, "error", "inbound_failed", { error: e.message });
    }
  }, new NewMessage({ incoming: true }));

  // listen for OUTGOING messages in private chats — to catch messages the admin
  // sends manually from Telegram (cold first-touch spam, manual takeover, etc).
  // Our own bot-sent messages are deduplicated via state.selfSentMsgIds, set
  // right after client.sendMessage() in processSendQueue.
  client.addEventHandler(async (event) => {
    const m = event.message;
    if (!m || !m.out) return;
    if (!m.message) return; // skip non-text (stickers, media without caption)
    // Private chat only — recipient is a single user (PeerUser).
    const peerId = m.peerId;
    if (!peerId || peerId.className !== "PeerUser") return;
    // Dedupe: skip messages our worker itself just sent through the queue.
    if (state.selfSentMsgIds && state.selfSentMsgIds.has(Number(m.id))) {
      state.selfSentMsgIds.delete(Number(m.id));
      return;
    }
    const userIdStr = String(peerId.userId);
    let recipient = null;
    try { recipient = await client.getEntity(BigInt(userIdStr)); } catch {}
    try {
      await api.outbound({
        account_id: account.id,
        telegram_user_id: userIdStr,
        username: recipient?.username || null,
        first_name: recipient?.firstName || null,
        last_name: recipient?.lastName || null,
        text: m.message,
        telegram_message_id: String(m.id),
      });
      log(account.id, "info", "manual_outbound_captured", {
        to: recipient?.username || userIdStr,
        len: m.message.length,
      });
    } catch (e) {
      log(account.id, "error", "outbound_capture_failed", { error: e.message });
    }
  }, new NewMessage({ outgoing: true }));


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
  await telegramRequest(api.updateAccount({
    account_id: account.id,
    status: "connected",
    display_name: me.username || me.firstName || account.phone,
  }), `updateAccount connected ${account.id.slice(0, 8)}`);
}

async function handleLoginRequested(account) {
  log(account.id, "info", "sending_code", { phone: account.phone });
  // Сбрасываем предыдущую попытку логина (если была)
  await disposeLoginClient(account.id);
  const client = await getOrCreateLoginClient(account.id, "", account.proxy);
  try {
    const result = await telegramRequest(client.invoke(
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
    ), `sendCode ${account.id.slice(0, 8)}`);
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
  const client = await getOrCreateLoginClient(account.id, account.session_string || "", account.proxy);
  try {
    await telegramRequest(client.invoke(
      new Api.auth.SignIn({
        phoneNumber: account.phone,
        phoneCodeHash: account.pending_phone_code_hash,
        phoneCode: account.pending_code,
      }),
    ), `signIn ${account.id.slice(0, 8)}`);
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
  const client = await getOrCreateLoginClient(account.id, account.session_string || "", account.proxy);
  try {
    const pwd = await telegramRequest(
      client.invoke(new Api.account.GetPassword()),
      `getPassword ${account.id.slice(0, 8)}`,
    );
    const check = await computeCheck(pwd, account.pending_password);
    await telegramRequest(
      client.invoke(new Api.auth.CheckPassword({ password: check })),
      `checkPassword ${account.id.slice(0, 8)}`,
    );
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

async function resolveContact(client, c, state) {
  if (!c) throw new Error("contact is missing");
  // 1. По username — самый надёжный путь, не зависит от access_hash в кэше.
  if (c.username) {
    try { return await client.getEntity(c.username); } catch (e) {
      if (!c.telegramUserId && !c.phone) throw e;
    }
  }
  // 2. По user_id — требует access_hash в InMemorySession. Если его нет —
  // обновляем диалоги (не чаще раза в 5 мин) и пробуем ещё раз.
  if (c.telegramUserId) {
    try {
      return await client.getEntity(BigInt(c.telegramUserId));
    } catch (e) {
      const last = state?.lastDialogsRefreshAt || 0;
      if (Date.now() - last > 5 * 60_000) {
        try {
          await client.getDialogs({ limit: 200 });
          if (state) state.lastDialogsRefreshAt = Date.now();
          return await client.getEntity(BigInt(c.telegramUserId));
        } catch (e2) {
          if (!c.phone) throw e2;
        }
      } else if (!c.phone) {
        throw e;
      }
    }
  }
  if (c.phone) return await client.getEntity(c.phone);
  throw new Error("contact has no username/user_id/phone");
}

/** Признаки спам-блока в ошибке от Telegram */
function isSpamBlockError(err) {
  if (!err) return false;
  const s = String(err).toUpperCase();
  return (
    s.includes("PEER_FLOOD") ||
    s.includes("FLOOD_WAIT") ||
    s.includes("USERS_TOO_MUCH") ||
    s.includes("SPAM") ||
    s.includes("PRIVACY_RESTRICTED")
  );
}

const SPAMBOT_FREE_RE = /(no limits|no longer limited|now free|good news|all restrictions.*lifted|больше не ограничен|снят[оы]|свободен)/i;
const SPAMBOT_BLOCKED_RE = /(unfortunately|sorry|our system|temporarily restricted|cannot send|ограничен|временно)/i;
const SPAMBOT_COOLDOWN_MS = 30 * 60 * 1000; // не дёргаем @SpamBot чаще раза в 30 мин

async function readLastSpamBotText(client, bot) {
  try {
    const msgs = await client.getMessages(bot, { limit: 1 });
    return msgs?.[0]?.message || "";
  } catch {
    return "";
  }
}

/**
 * Пытаемся снять спам-блок через @SpamBot: /start, ждём ответ, если бот говорит
 * что ограничения есть — пробуем второй раз. Больше двух раз не спамим.
 * Возвращает: "unblocked" | "still_blocked" | "unknown" | "cooldown".
 */
async function tryUnblockViaSpamBot(state, accountId) {
  const now = Date.now();
  if (state.lastSpamBotAt && now - state.lastSpamBotAt < SPAMBOT_COOLDOWN_MS) {
    return "cooldown";
  }
  state.lastSpamBotAt = now;
  const client = state.client;
  if (!client) return "unknown";

  let bot;
  try {
    bot = await client.getEntity("SpamBot");
  } catch (e) {
    log(accountId, "warn", "spambot_resolve_failed", { error: e.message });
    return "unknown";
  }

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      await client.sendMessage(bot, { message: "/start" });
      log(accountId, "info", "spambot_start_sent", { attempt });
    } catch (e) {
      log(accountId, "error", "spambot_start_failed", { attempt, error: e.message });
      return "unknown";
    }
    // Ждём ответ — поллим до 8 секунд
    let text = "";
    for (let i = 0; i < 8; i++) {
      await sleep(1000);
      text = await readLastSpamBotText(client, bot);
      if (text) break;
    }
    log(accountId, "info", "spambot_reply", { attempt, text: text.slice(0, 200) });
    if (!text) return "unknown";
    if (SPAMBOT_FREE_RE.test(text)) return "unblocked";
    if (!SPAMBOT_BLOCKED_RE.test(text)) {
      // Текст непонятный — не повторяем, считаем неизвестным
      return "unknown";
    }
    // Если первый /start не помог — пробуем ещё раз. После второго — стоп.
  }
  return "still_blocked";
}

async function processSendQueue(account, state) {
  if (state.pulling || !state.client) return;
  state.pulling = true;
  try {
    const { items } = await api.pull(account.id, 5);
    for (const item of items || []) {
      try {
        const entity = await resolveContact(state.client, item.target, state);
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
        // Remember this msg id so the outgoing-handler dedupes it
        if (sent?.id != null) {
          if (!state.selfSentMsgIds) state.selfSentMsgIds = new Set();
          state.selfSentMsgIds.add(Number(sent.id));
          // safety TTL: drop after 60s in case the outgoing event never fires
          setTimeout(() => state.selfSentMsgIds?.delete(Number(sent.id)), 60_000);
        }
        await api.ack(item.queueId, true, null, entity?.id ? String(entity.id) : null);
        log(account.id, "info", "sent", {
          to: item.target?.username || item.target?.telegramUserId || item.target?.phone,
        });
      } catch (err) {
        const errText = err.message || String(err);
        let spamBotResult = null;
        if (isSpamBlockError(errText)) {
          log(account.id, "warn", "spam_block_detected", { error: errText });
          spamBotResult = await tryUnblockViaSpamBot(state, account.id);
          log(account.id, "info", "spambot_result", { result: spamBotResult });
        }
        await api.ack(item.queueId, false, errText, null, spamBotResult);
        log(account.id, "error", "send_failed", { id: item.queueId, error: errText, spamBotResult });
        // Если @SpamBot подтвердил блок — нет смысла долбить остальные item-ы в пуле
        if (spamBotResult === "still_blocked" || spamBotResult === "cooldown") {
          break;
        }
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
      state = { busy: false, pulling: false, lastHeartbeat: 0, stepStartedAt: 0 };
      accounts.set(account.id, state);
    }
    if (state.busy && Date.now() - (state.stepStartedAt || 0) > ACCOUNT_STEP_TIMEOUT_MS * 2) {
      log(account.id, "warn", "state_unstuck", { status: account.status });
      state.busy = false;
    }
    if (state.busy) continue;
    state.busy = true;
    state.stepStartedAt = Date.now();

    try {
      // State machine
      if (account.status === "login_requested") {
        await withTimeout(handleLoginRequested(account), ACCOUNT_STEP_TIMEOUT_MS, `login_requested ${account.id.slice(0, 8)}`);
      } else if (account.status === "code_submitted" && account.pending_code) {
        await withTimeout(handleCodeSubmitted(account), ACCOUNT_STEP_TIMEOUT_MS, `code_submitted ${account.id.slice(0, 8)}`);
      } else if (account.status === "password_submitted" && account.pending_password) {
        await withTimeout(handlePasswordSubmitted(account), ACCOUNT_STEP_TIMEOUT_MS, `password_submitted ${account.id.slice(0, 8)}`);
      } else if (
        (account.status === "connected" || account.status === "active") &&
        account.session_string &&
        !state.client
      ) {
        await withTimeout(startConnectedClient(state, account), ACCOUNT_STEP_TIMEOUT_MS, `start_connected ${account.id.slice(0, 8)}`);
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
      state.stepStartedAt = 0;
    }

    // Heartbeat for connected
    if (state.client) {
      // Если соединение разорвалось посреди сессии (умер прокси) — gramjs
      // выставляет client.connected=false. Это тоже "пропажа прокси".
      if (state.client.connected === false) {
        setConnAlert(account, state, "down", "connection dropped");
      } else if (state.connAlert === "down") {
        // Авто-reconnect поднял соединение обратно.
        setConnAlert(account, state, "restored");
      }
      if (Date.now() - state.lastHeartbeat > HEARTBEAT_INTERVAL_MS) {
        state.lastHeartbeat = Date.now();
        api.heartbeat(account.id, "connected").catch(() => {});
      }
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

const HARVEST_INTERVAL_MS = Number(process.env.HARVEST_INTERVAL_MS || 120000);
let harvesting = false;

/** Извлекаем invite-hash из t.me/+hash или t.me/joinchat/hash */
function parseInviteHash(link) {
  const m = link.match(/(?:t\.me\/\+|t\.me\/joinchat\/|joinchat\/|^\+)([A-Za-z0-9_-]+)/);
  return m ? m[1] : null;
}

/** Нормализуем @username / t.me/username → username */
function parsePublicUsername(link) {
  const m = link.match(/(?:t\.me\/|@)?([A-Za-z][A-Za-z0-9_]{3,})$/);
  return m ? m[1].replace(/^@/, "") : null;
}

/** Резолвим группу по ссылке; при инвайте пытаемся вступить, если ещё не внутри */
async function resolveGroupEntity(client, link) {
  const inviteHash = parseInviteHash(link);
  if (inviteHash) {
    try {
      const info = await client.invoke(
        new Api.messages.CheckChatInvite({ hash: inviteHash }),
      );
      if (info.chat) return info.chat; // уже участник
    } catch {}
    try {
      const updates = await client.invoke(
        new Api.messages.ImportChatInvite({ hash: inviteHash }),
      );
      const chat = updates?.chats?.[0];
      if (chat) return chat;
    } catch (e) {
      throw new Error(`invite resolve failed: ${e.message}`);
    }
  }
  const username = parsePublicUsername(link);
  if (username) {
    return await client.getEntity(username);
  }
  return await client.getEntity(link);
}

/** Имя содержит кириллицу (русское имя) */
function hasRussianName(s) {
  const name = `${s.firstName || ""} ${s.lastName || ""}`;
  return /[а-яёА-ЯЁ]/.test(name);
}

/**
 * Онлайн / был недавно (в пределах 2 суток).
 * Telegram-статусы: UserStatusOnline, UserStatusRecently, UserStatusOffline (wasOnline),
 * UserStatusLastWeek / UserStatusLastMonth / UserStatusEmpty.
 */
function isRecentlyOnline(s) {
  const st = s?.status;
  if (!st) return false;
  const cn = st.className;
  if (cn === "UserStatusOnline" || cn === "UserStatusRecently") return true;
  if (cn === "UserStatusOffline" && st.wasOnline) {
    const wasMs = Number(st.wasOnline) * 1000;
    return Date.now() - wasMs <= 2 * 24 * 60 * 60 * 1000;
  }
  // UserStatusLastWeek / UserStatusLastMonth / UserStatusEmpty → не собираем
  return false;
}

/** Приоритетный фильтр: русское имя + есть username + онлайн/недавно в сети */
function passesHarvestFilter(s) {
  if (!s.username) return false;
  if (!hasRussianName(s)) return false;
  if (!isRecentlyOnline(s)) return false;
  return true;
}

async function harvestGroup(account, state, group) {
  const client = state.client;
  if (!client) return;
  let title = null;
  try {
    const entity = await resolveGroupEntity(client, group.link);
    title = entity?.title || group.link;

    const limit = Math.min(3000, Math.max(50, group.messagesLimit || 500));
    const seen = new Map(); // userId -> {username, first_name, last_name}
    const messages = await client.getMessages(entity, { limit });
    let scanned = 0;
    let skipped = 0;
    for (const m of messages) {
      const s = m?.sender;
      if (!s || s.className !== "User") continue;
      if (s.bot || s.self) continue;
      const uid = String(s.id);
      if (seen.has(uid)) continue;
      scanned++;
      if (!passesHarvestFilter(s)) {
        skipped++;
        continue;
      }
      seen.set(uid, {
        telegram_user_id: uid,
        username: s.username || null,
        first_name: s.firstName || null,
        last_name: s.lastName || null,
      });
    }


    const users = Array.from(seen.values());
    // батчим по 500
    for (let i = 0; i < users.length; i += 500) {
      const chunk = users.slice(i, i + 500);
      await api.harvestSubmit({
        groupId: group.id,
        title,
        users: chunk,
        done: i + 500 >= users.length,
      });
    }
    if (users.length === 0) {
      await api.harvestSubmit({ groupId: group.id, title, users: [], done: true });
    }
    log(account.id, "info", "harvest_done", { group: title, collected: users.length, scanned, skipped });
  } catch (e) {
    log(account.id, "error", "harvest_failed", { link: group.link, error: e.message });
    await api.harvestSubmit({
      groupId: group.id,
      title,
      users: [],
      error: e.message.slice(0, 480),
    }).catch(() => {});
  }
}

async function harvestOnce() {
  if (harvesting) return;
  harvesting = true;
  try {
    const { groups } = await api.harvestTargets().catch(() => ({ groups: [] }));
    for (const group of groups || []) {
      const state = accounts.get(group.accountId);
      if (!state?.client) continue;
      await harvestGroup({ id: group.accountId }, state, group);
      await sleep(jitter());
    }
  } catch (e) {
    console.error("harvest error:", e.message);
  } finally {
    harvesting = false;
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

  // Harvest loop — собираем активных авторов из указанных групп
  (async () => {
    while (true) {
      await harvestOnce().catch((e) => console.error("harvest loop error:", e));
      await sleep(HARVEST_INTERVAL_MS);
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
