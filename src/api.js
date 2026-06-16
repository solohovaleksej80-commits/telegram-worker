import fetch from "node-fetch";

const BASE = process.env.LOVABLE_BASE_URL;
const SECRET = process.env.WORKER_SECRET;
const API_TIMEOUT_MS = Number(process.env.API_TIMEOUT_MS || 20000);

async function call(path, body) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), API_TIMEOUT_MS);
  let res;
  let text;
  try {
    res = await fetch(`${BASE}${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${SECRET}`,
      },
      body: JSON.stringify(body || {}),
      signal: controller.signal,
    });
    text = await res.text();
  } catch (e) {
    if (e?.name === "AbortError") {
      throw new Error(`API ${path} timeout after ${API_TIMEOUT_MS}ms`);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) {
    throw new Error(`API ${path} ${res.status}: ${text.slice(0, 300)}`);
  }
  return data;
}

export const api = {
  accounts: () => call("/api/public/worker/accounts", {}),
  updateAccount: (payload) => call("/api/public/worker/account-update", payload),
  heartbeat: (accountId, status = "connected") =>
    call("/api/public/worker/heartbeat", { accountId, status }),
  pull: (accountId, limit = 5) =>
    call("/api/public/worker/pull", { accountId, limit }),
  ack: (queueId, success, error, resolvedTelegramUserId, spamBotResult) =>
    call("/api/public/worker/ack", {
      queueId,
      success,
      error,
      resolvedTelegramUserId,
      spamBotResult: spamBotResult || undefined,
    }),
  inbound: (payload) => call("/api/public/worker/inbound", {
    accountId: payload.account_id,
    telegramUserId: payload.telegram_user_id,
    username: payload.username,
    firstName: payload.first_name,
    lastName: payload.last_name,
    text: payload.text,
    mediaType: payload.media_type || null,
    telegramMessageId: payload.telegram_message_id != null ? String(payload.telegram_message_id) : null,
    reply_to_story: payload.reply_to_story,
    story_id: payload.story_id,
  }),
  outbound: (payload) => call("/api/public/worker/outbound", {
    accountId: payload.account_id,
    telegramUserId: payload.telegram_user_id,
    username: payload.username,
    firstName: payload.first_name,
    lastName: payload.last_name,
    text: payload.text,
    telegramMessageId: payload.telegram_message_id != null ? String(payload.telegram_message_id) : null,
  }),
  inboundReaction: (payload) => call("/api/public/worker/inbound", {
    kind: "reaction",
    accountId: payload.account_id,
    telegramUserId: payload.telegram_user_id,
    username: payload.username,
    firstName: payload.first_name,
    lastName: payload.last_name,
    emoji: payload.emoji,
    msgId: payload.msg_id != null ? String(payload.msg_id) : null,
  }),
  log: (accountId, level, event, data) =>
    call("/api/public/worker/log", { accountId, level, event, data }),
  connectionAlert: (accountId, state, error) =>
    call("/api/public/worker/connection-alert", {
      account_id: accountId,
      state,
      error: error ? String(error).slice(0, 500) : undefined,
    }),
  harvestTargets: () => call("/api/public/worker/harvest-targets", {}),
  harvestSubmit: (payload) => call("/api/public/worker/harvest-submit", payload),
};
