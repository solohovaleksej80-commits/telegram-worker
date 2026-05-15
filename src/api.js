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
    call("/api/public/worker/heartbeat", { account_id: accountId, status }),
  pull: (accountId, limit = 5) =>
    call("/api/public/worker/pull", { account_id: accountId, limit }),
  ack: (messageId, success, error, telegramMessageId) =>
    call("/api/public/worker/ack", {
      message_id: messageId,
      success,
      error,
      telegram_message_id: telegramMessageId,
    }),
  inbound: (payload) => call("/api/public/worker/inbound", payload),
  log: (accountId, level, event, data) =>
    call("/api/public/worker/log", { account_id: accountId, level, event, data }),
};
