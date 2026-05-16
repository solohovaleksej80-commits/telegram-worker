import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";
import { supabaseAdmin } from "@/integrations/supabase/client.server";
import { authorizeWorker } from "@/lib/worker-auth";
import { VLADIMIR_SYSTEM_PROMPT } from "@/lib/vladimir-prompt";

const Schema = z
  .object({
    accountId: z.string().uuid().optional(),
    account_id: z.string().uuid().optional(),
    telegramUserId: z.string().regex(/^\d+$/).optional(),
    telegram_user_id: z.string().regex(/^\d+$/).optional().nullable(),
    username: z.string().max(64).optional().nullable(),
    firstName: z.string().max(128).optional().nullable(),
    first_name: z.string().max(128).optional().nullable(),
    lastName: z.string().max(128).optional().nullable(),
    last_name: z.string().max(128).optional().nullable(),
    text: z.string().min(1).max(4000),
  })
  .transform((data) => ({
    accountId: data.accountId ?? data.account_id,
    telegramUserId: data.telegramUserId ?? data.telegram_user_id,
    username: data.username,
    firstName: data.firstName ?? data.first_name,
    lastName: data.lastName ?? data.last_name,
    text: data.text,
  }))
  .pipe(
    z.object({
      accountId: z.string().uuid(),
      telegramUserId: z.string().regex(/^\d+$/),
      username: z.string().max(64).optional().nullable(),
      firstName: z.string().max(128).optional().nullable(),
      lastName: z.string().max(128).optional().nullable(),
      text: z.string().min(1).max(4000),
    }),
  );

const AI_URL = "https://ai.gateway.lovable.dev/v1/chat/completions";
const MODEL = "google/gemini-3-flash-preview";

async function callAI(messages: Array<{ role: string; content: string }>) {
  const apiKey = process.env.LOVABLE_API_KEY;
  if (!apiKey) throw new Error("LOVABLE_API_KEY is not configured");
  const res = await fetch(AI_URL, {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ model: MODEL, messages }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`AI gateway error ${res.status}: ${text}`);
  }
  const json: { choices?: Array<{ message?: { content?: string } }> } = await res.json();
  return json.choices?.[0]?.message?.content ?? "";
}

/**
 * Воркер передаёт входящее сообщение от пользователя.
 * Мы: апсертим контакт, апсертим conversation, сохраняем входящее, генерим
 * ответ Владимира AI и кладём его в outbound_queue (воркер заберёт через /pull).
 */
export const Route = createFileRoute("/api/public/worker/inbound")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const auth = authorizeWorker(request);
        if (auth) return auth;

        const parsed = Schema.safeParse(await request.json().catch(() => null));
        if (!parsed.success) {
          return Response.json({ error: parsed.error.flatten() }, { status: 400 });
        }
        const { accountId, telegramUserId, username, firstName, lastName, text } = parsed.data;

        // 1. Контакт
        const tgUidNum = Number(telegramUserId);
        const { data: existingContact } = await supabaseAdmin
          .from("contacts")
          .select("id, conversation_id, status, first_message_at")
          .eq("account_id", accountId)
          .eq("telegram_user_id", tgUidNum)
          .maybeSingle();

        let contactId: string;
        let conversationId: string | null = existingContact?.conversation_id ?? null;

        if (existingContact) {
          contactId = existingContact.id;
          await supabaseAdmin
            .from("contacts")
            .update({
              status: existingContact.status === "pending" ? "replied" : existingContact.status,
              last_message_at: new Date().toISOString(),
              username: username ?? undefined,
              first_name: firstName ?? undefined,
              last_name: lastName ?? undefined,
            })
            .eq("id", contactId);
        } else {
          const { data: created, error: insErr } = await supabaseAdmin
            .from("contacts")
            .insert({
              account_id: accountId,
              telegram_user_id: tgUidNum,
              username: username ?? null,
              first_name: firstName ?? null,
              last_name: lastName ?? null,
              status: "replied",
              last_message_at: new Date().toISOString(),
            })
            .select("id")
            .single();
          if (insErr || !created) {
            return Response.json({ error: insErr?.message ?? "insert failed" }, { status: 500 });
          }
          contactId = created.id;
        }

        // 2. Conversation
        if (!conversationId) {
          const anonId = `tg:${accountId}:${telegramUserId}`.slice(0, 64);
          const { data: existingConv } = await supabaseAdmin
            .from("conversations")
            .select("id")
            .eq("anon_id", anonId)
            .maybeSingle();
          if (existingConv) {
            conversationId = existingConv.id;
          } else {
            const { data: newConv, error: convErr } = await supabaseAdmin
              .from("conversations")
              .insert({ anon_id: anonId })
              .select("id")
              .single();
            if (convErr || !newConv) {
              return Response.json({ error: convErr?.message ?? "conv failed" }, { status: 500 });
            }
            conversationId = newConv.id;
            await supabaseAdmin.from("leads").insert({
              conversation_id: conversationId,
              telegram_username: username ?? null,
              name: firstName ?? null,
            });
          }
          await supabaseAdmin
            .from("contacts")
            .update({ conversation_id: conversationId })
            .eq("id", contactId);
        }

        // 3. Сохраняем входящее
        await supabaseAdmin.from("messages").insert({
          conversation_id: conversationId,
          role: "user",
          content: text,
        });

        // 3b. Анти-спам: если для контакта уже есть отложенный ответ в очереди,
        // не дёргаем AI повторно и не плодим сообщения. Когда тот ответ уйдёт,
        // следующее входящее снова поднимет диалог свежим.
        const { data: alreadyQueued } = await supabaseAdmin
          .from("outbound_queue")
          .select("id")
          .eq("contact_id", contactId)
          .in("status", ["pending", "claimed"])
          .limit(1);
        if (alreadyQueued && alreadyQueued.length > 0) {
          return Response.json({ ok: true, queued: false, reason: "already_queued" });
        }

        // 3c. Анти-спам: если последний наш ответ ушёл меньше 90 секунд назад,
        // пропускаем (имитация: человек не залипает в чате каждую секунду).
        const ninetySecAgo = new Date(Date.now() - 90_000).toISOString();
        const { data: recentSent } = await supabaseAdmin
          .from("outbound_queue")
          .select("id")
          .eq("contact_id", contactId)
          .eq("status", "sent")
          .gte("sent_at", ninetySecAgo)
          .limit(1);
        if (recentSent && recentSent.length > 0) {
          return Response.json({ ok: true, queued: false, reason: "cooldown" });
        }

        // 4. История + память для AI
        const [{ data: history }, { data: memory }] = await Promise.all([
          supabaseAdmin
            .from("messages")
            .select("role, content")
            .eq("conversation_id", conversationId)
            .order("created_at", { ascending: false })
            .limit(30),
          supabaseAdmin
            .from("memory_snippets")
            .select("key, value")
            .eq("conversation_id", conversationId),
        ]);

        let memoryContext = "";
        if (memory && memory.length > 0) {
          memoryContext =
            "\n\n# ЧТО ТЫ ПОМНИШЬ О СОБЕСЕДНИКЕ\n" +
            memory.map((m) => `- ${m.key}: ${m.value}`).join("\n");
        }

        const aiMessages = [
          { role: "system", content: VLADIMIR_SYSTEM_PROMPT + memoryContext },
          ...(history ?? []).reverse().map((m) => ({ role: m.role, content: m.content })),
        ];

        let reply = "";
        try {
          reply = await callAI(aiMessages);
        } catch (e) {
          console.error("AI failed in inbound:", e);
          return Response.json({ ok: true, queued: false, error: "ai_failed" });
        }

        if (!reply.trim()) return Response.json({ ok: true, queued: false });

        // 5. Кладём ответ в очередь с задержкой 5-10 минут (имитация живого
        // ответа + защита от спам-атак и расхода токенов).
        const delayMs = 5 * 60_000 + Math.floor(Math.random() * 5 * 60_000);
        const scheduledFor = new Date(Date.now() + delayMs).toISOString();
        await supabaseAdmin.from("outbound_queue").insert({
          contact_id: contactId,
          account_id: accountId,
          content: reply,
          scheduled_for: scheduledFor,
        });

        return Response.json({ ok: true, queued: true, scheduled_for: scheduledFor });
      },
    },
  },
});
