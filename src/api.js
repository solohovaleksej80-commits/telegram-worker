import fetch from "node-fetch";

const BASE = process.env.LOVABLE_BASE_URL;
const SECRET = process.env.WORKER_SECRET;

async function call(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${SECRET}`,
    },
    body: JSON.stringify(body || {}),
  });
  const text = await res.text();
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
  ack: (queueId, success, error, resolvedTelegramUserId) =>
    call("/api/public/worker/ack", {
      queueId,
      success,
      error,
      resolvedTelegramUserId,
    }),
  inbound: (payload) => call("/api/public/worker/inbound", {
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
};
