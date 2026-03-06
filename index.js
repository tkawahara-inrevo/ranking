require("dotenv").config();

const { WebClient } = require("@slack/web-api");
const { google } = require("googleapis");

const botClient = new WebClient(process.env.SLACK_BOT_TOKEN);
const userClient = new WebClient(process.env.SLACK_USER_TOKEN);

const CHAT_CHANNEL_ID = process.env.CHAT_CHANNEL_ID;
const REPORT_CHANNELS = [
  { id: process.env.REPORT_IN_CHANNEL_ID, name: "all-出勤日報" },
  { id: process.env.REPORT_OUT_CHANNEL_ID, name: "all-退勤日報" }
].filter(x => x.id);

const REPORT_BOT_ID = process.env.REPORT_BOT_ID || "";
const REPORT_APP_ID = process.env.REPORT_APP_ID || "";
const REPORT_TEXT_KEYWORD = process.env.REPORT_TEXT_KEYWORD || "";

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const RAW_DAILY_SHEET = "raw_daily";
const RAW_AUDIT_SHEET = "raw_reply_audit";

const PARENT_LOOKBACK_DAYS = Number(process.env.PARENT_LOOKBACK_DAYS || 2);
const DEBUG_AUDIT_LIMIT = Number(process.env.DEBUG_AUDIT_LIMIT || 500);

function nowJstString() {
  return new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date()).replace(" ", "T");
}

function makeRunId() {
  return `run_${nowJstString().replace(/[:]/g, "-")}_${Math.random().toString(36).slice(2, 8)}`;
}

function formatDateJST(date) {
  const parts = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(date);

  const y = parts.find(p => p.type === "year")?.value;
  const m = parts.find(p => p.type === "month")?.value;
  const d = parts.find(p => p.type === "day")?.value;
  return `${y}-${m}-${d}`;
}

function formatDateTimeJSTFromSlackTs(ts) {
  if (!ts) return "";
  const ms = Number(ts) * 1000;
  if (Number.isNaN(ms)) return "";
  return new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(ms));
}

function getYesterdayRangeJST() {
  const now = new Date();
  const jstNow = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Tokyo" }));

  const from = new Date(jstNow);
  from.setDate(from.getDate() - 1);
  from.setHours(0, 0, 0, 0);

  const to = new Date(jstNow);
  to.setDate(to.getDate() - 1);
  to.setHours(23, 59, 59, 999);

  return { from, to };
}

function toSlackTs(date) {
  return String(Math.floor(date.getTime() / 1000));
}

function truncateText(text, max = 120) {
  if (!text) return "";
  return text.length > max ? `${text.slice(0, max)}...` : text;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getGoogleAuth() {
  const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
  return new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
}

async function getSheetsClient() {
  const auth = await getGoogleAuth().getClient();
  return google.sheets({ version: "v4", auth });
}

async function appendRows(sheetName, rows) {
  if (!rows.length) return;

  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A:Z`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows }
  });
}

async function fetchAllHistory(channel, oldest, latest) {
  let cursor;
  const all = [];

  do {
    const res = await botClient.conversations.history({
      channel,
      oldest,
      latest,
      inclusive: true,
      limit: 200,
      cursor
    });

    all.push(...(res.messages || []));
    cursor = res.response_metadata?.next_cursor || undefined;
  } while (cursor);

  return all;
}

async function fetchAllReplies(channel, ts) {
  let cursor;
  const all = [];

  do {
    const res = await userClient.conversations.replies({
      channel,
      ts,
      limit: 200,
      cursor
    });

    all.push(...(res.messages || []));
    cursor = res.response_metadata?.next_cursor || undefined;
  } while (cursor);

  return all;
}

async function safeFetchReplies(channel, ts) {
  try {
    return await fetchAllReplies(channel, ts);
  } catch (err) {
    console.error("[replies error]", {
      channel,
      ts,
      error: err?.data?.error || err?.message || "unknown_error"
    });
    await sleep(1500);
    return [];
  }
}

async function fetchUsersMap() {
  let cursor;
  const members = [];

  do {
    const res = await botClient.users.list({
      limit: 200,
      cursor
    });

    members.push(...(res.members || []));
    cursor = res.response_metadata?.next_cursor || undefined;
  } while (cursor);

  const map = {};
  for (const user of members) {
    map[user.id] = {
      id: user.id,
      userName:
        user.profile?.display_name ||
        user.real_name ||
        user.name ||
        user.id
    };
  }
  return map;
}

function isHumanMessage(msg) {
  if (!msg) return false;
  if (!msg.user) return false;
  if (msg.bot_id) return false;
  if (msg.app_id) return false;
  if (msg.subtype === "bot_message") return false;
  if (msg.subtype === "channel_join") return false;
  if (msg.subtype === "channel_leave") return false;
  return true;
}

function isTargetReportParent(msg) {
  if (!msg) return false;

  const looksBotLike =
    msg.subtype === "bot_message" ||
    !!msg.bot_id ||
    !!msg.app_id;

  if (!looksBotLike) return false;

  if (REPORT_BOT_ID && msg.bot_id !== REPORT_BOT_ID) return false;
  if (REPORT_APP_ID && msg.app_id !== REPORT_APP_ID) return false;

  if (REPORT_TEXT_KEYWORD) {
    const text = msg.text || "";
    if (!text.includes(REPORT_TEXT_KEYWORD)) return false;
  }

  return true;
}

function inRangeSlackTs(ts, fromDate, toDate) {
  const n = Number(ts);
  const fromSec = fromDate.getTime() / 1000;
  const toSec = toDate.getTime() / 1000;
  return n >= fromSec && n <= toSec;
}

async function collectChatRanking({ runId, from, to, usersMap }) {
  const oldest = toSlackTs(from);
  const latest = toSlackTs(to);
  const counter = {};
  const targetDate = formatDateJST(from);

  console.log(`[chat] targetDate=${targetDate} channel=${CHAT_CHANNEL_ID} oldest=${oldest} latest=${latest}`);

  const messages = await fetchAllHistory(CHAT_CHANNEL_ID, oldest, latest);
  console.log(`[chat] parent message count=${messages.length}`);

  let parentCounted = 0;
  let replyCounted = 0;

  for (const msg of messages) {
    if (isHumanMessage(msg)) {
      counter[msg.user] = (counter[msg.user] || 0) + 1;
      parentCounted += 1;
    }

    if (msg.reply_count && msg.ts) {
      const replies = await safeFetchReplies(CHAT_CHANNEL_ID, msg.ts);

      for (const reply of replies.slice(1)) {
        if (!isHumanMessage(reply)) continue;
        if (!inRangeSlackTs(reply.ts, from, to)) continue;

        counter[reply.user] = (counter[reply.user] || 0) + 1;
        replyCounted += 1;
      }

      await sleep(250);
    }
  }

  console.log(`[chat] counted parent=${parentCounted} reply=${replyCounted} unique_users=${Object.keys(counter).length}`);

  return Object.entries(counter)
    .map(([userId, count]) => ({
      run_id: runId,
      date: targetDate,
      category: "chat",
      user_id: userId,
      user_name: usersMap[userId]?.userName || userId,
      count,
      channel_ids: CHAT_CHANNEL_ID,
      channel_names: "all-雑談",
      note: `parent+reply aggregated on ${targetDate}`
    }))
    .sort((a, b) => b.count - a.count);
}

async function collectReportReplyRanking({ runId, from, to, usersMap }) {
  const targetDate = formatDateJST(from);
  const counter = {};
  const auditRows = [];

  const parentSearchFrom = new Date(from);
  parentSearchFrom.setDate(parentSearchFrom.getDate() - PARENT_LOOKBACK_DAYS);

  const oldest = toSlackTs(parentSearchFrom);
  const latest = toSlackTs(to);

  console.log(`[report] targetDate=${targetDate}`);
  console.log(`[report] parent search window=${formatDateJST(parentSearchFrom)}..${formatDateJST(to)} lookbackDays=${PARENT_LOOKBACK_DAYS}`);

  for (const channel of REPORT_CHANNELS) {
    console.log(`[report] scanning channel=${channel.name}(${channel.id}) oldest=${oldest} latest=${latest}`);

    const messages = await fetchAllHistory(channel.id, oldest, latest);
    console.log(`[report] fetched parent candidates channel=${channel.name} count=${messages.length}`);

    let matchedParentCount = 0;
    let replyTotalSeen = 0;
    let replyCounted = 0;

    for (const msg of messages) {
      const parentMatched = isTargetReportParent(msg);
      if (!parentMatched) continue;

      matchedParentCount += 1;

      const parentSummary = {
        channel: channel.name,
        parentTs: msg.ts,
        parentTsJst: formatDateTimeJSTFromSlackTs(msg.ts),
        bot_id: msg.bot_id || "",
        app_id: msg.app_id || "",
        subtype: msg.subtype || "",
        text: truncateText(msg.text || "", 100)
      };

      console.log("[report][parent matched]", parentSummary);

      if (!msg.ts) continue;

      const replies = await safeFetchReplies(channel.id, msg.ts);

      console.log(`[report][thread] channel=${channel.name} parentTs=${msg.ts} replies=${Math.max(replies.length - 1, 0)}`);

      for (const reply of replies.slice(1)) {
        replyTotalSeen += 1;

        const human = isHumanMessage(reply);
        const rangeOk = inRangeSlackTs(reply.ts, from, to);

        const auditRow = [
          runId,
          targetDate,
          channel.id,
          channel.name,
          msg.ts || "",
          truncateText(msg.text || "", 200),
          msg.user || "",
          msg.bot_id || "",
          msg.app_id || "",
          reply.ts || "",
          reply.user || "",
          usersMap[reply.user]?.userName || reply.user || "",
          truncateText(reply.text || "", 200),
          rangeOk ? "Y" : "N",
          human ? "Y" : "N",
          parentMatched ? "Y" : "N",
          `parent=${formatDateTimeJSTFromSlackTs(msg.ts)} reply=${formatDateTimeJSTFromSlackTs(reply.ts)}`
        ];

        if (auditRows.length < DEBUG_AUDIT_LIMIT) {
          auditRows.push(auditRow);
        }

        console.log("[report][reply inspect]", {
          channel: channel.name,
          parentTs: msg.ts,
          replyTs: reply.ts,
          replyTsJst: formatDateTimeJSTFromSlackTs(reply.ts),
          replyUser: reply.user || "",
          replyUserName: usersMap[reply.user]?.userName || "",
          human,
          rangeOk,
          text: truncateText(reply.text || "", 50)
        });

        if (!human) continue;
        if (!rangeOk) continue;

        counter[reply.user] = (counter[reply.user] || 0) + 1;
        replyCounted += 1;
      }

      await sleep(300);
    }

    console.log(
      `[report][summary] channel=${channel.name} matchedParents=${matchedParentCount} repliesSeen=${replyTotalSeen} repliesCounted=${replyCounted} uniqueUsers=${Object.keys(counter).length}`
    );
  }

  const rankingRows = Object.entries(counter)
    .map(([userId, count]) => ({
      run_id: runId,
      date: targetDate,
      category: "report_reply",
      user_id: userId,
      user_name: usersMap[userId]?.userName || userId,
      count,
      channel_ids: REPORT_CHANNELS.map(x => x.id).join(","),
      channel_names: REPORT_CHANNELS.map(x => x.name).join(","),
      note: `reply_ts filtered on ${targetDate}; parent lookback=${PARENT_LOOKBACK_DAYS}d`
    }))
    .sort((a, b) => b.count - a.count);

  return { rankingRows, auditRows };
}

async function main() {
  const runId = makeRunId();
  const { from, to } = getYesterdayRangeJST();
  const targetDate = formatDateJST(from);

  console.log("==================================================");
  console.log(`[start] runId=${runId}`);
  console.log(`[target] date=${targetDate}`);
  console.log(`[range] from=${from.toISOString()} to=${to.toISOString()}`);
  console.log(`[config] chatChannel=${CHAT_CHANNEL_ID}`);
  console.log(`[config] reportChannels=${REPORT_CHANNELS.map(x => `${x.name}(${x.id})`).join(", ")}`);
  console.log(`[config] reportBotId=${REPORT_BOT_ID || "(not set)"}`);
  console.log(`[config] reportAppId=${REPORT_APP_ID || "(not set)"}`);
  console.log(`[config] reportTextKeyword=${REPORT_TEXT_KEYWORD || "(not set)"}`);
  console.log("==================================================");

  const usersMap = await fetchUsersMap();
  console.log(`[users] loaded=${Object.keys(usersMap).length}`);

  const chatRows = CHAT_CHANNEL_ID
    ? await collectChatRanking({ runId, from, to, usersMap })
    : [];

  const { rankingRows: reportRows, auditRows } =
    await collectReportReplyRanking({ runId, from, to, usersMap });

  const dailyRows = [...chatRows, ...reportRows];

  console.log(`[result] chatRows=${chatRows.length} reportRows=${reportRows.length} auditRows=${auditRows.length}`);

  if (dailyRows.length) {
    await appendRows(
      RAW_DAILY_SHEET,
      dailyRows.map(r => [
        r.run_id,
        r.date,
        r.category,
        r.user_id,
        r.user_name,
        r.count,
        r.channel_ids,
        r.channel_names,
        r.note
      ])
    );
    console.log(`[sheets] appended to ${RAW_DAILY_SHEET}: ${dailyRows.length} rows`);
  } else {
    console.log(`[sheets] no rows for ${RAW_DAILY_SHEET}`);
  }

  if (auditRows.length) {
    await appendRows(RAW_AUDIT_SHEET, auditRows);
    console.log(`[sheets] appended to ${RAW_AUDIT_SHEET}: ${auditRows.length} rows`);
  } else {
    console.log(`[sheets] no rows for ${RAW_AUDIT_SHEET}`);
  }

  console.log(`[done] runId=${runId} targetDate=${targetDate}`);
}

main().catch(err => {
  console.error("[fatal]", err?.data || err);
  process.exit(1);
});