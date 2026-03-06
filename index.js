require("dotenv").config();

const { WebClient } = require("@slack/web-api");
const { google } = require("googleapis");

const botClient = new WebClient(process.env.SLACK_BOT_TOKEN);
const userClient = new WebClient(process.env.SLACK_USER_TOKEN);

const CHAT_CHANNEL_ID = process.env.CHAT_CHANNEL_ID;
const REPORT_CHANNEL_IDS = [
  process.env.REPORT_IN_CHANNEL_ID,
  process.env.REPORT_OUT_CHANNEL_ID
].filter(Boolean);

const REPORT_BOT_ID = process.env.REPORT_BOT_ID || "";
const REPORT_APP_ID = process.env.REPORT_APP_ID || "";
const REPORT_TEXT_KEYWORD = process.env.REPORT_TEXT_KEYWORD || "";

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const RAW_SHEET_NAME = "raw_daily";

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

function getYesterdayRangeJST() {
  const now = new Date();

  const todayJst = new Date(
    now.toLocaleString("en-US", { timeZone: "Asia/Tokyo" })
  );

  const from = new Date(todayJst);
  from.setDate(from.getDate() - 1);
  from.setHours(0, 0, 0, 0);

  const to = new Date(todayJst);
  to.setDate(to.getDate() - 1);
  to.setHours(23, 59, 59, 999);

  return { from, to };
}

function toSlackTs(date) {
  return String(Math.floor(date.getTime() / 1000));
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function safeFetchReplies(channel, ts) {
  try {
    return await fetchAllReplies(channel, ts);
  } catch (err) {
    const status = err?.data?.error || err?.message || "unknown_error";
    console.error(`[replies error] channel=${channel} ts=${ts} error=${status}`);
    await sleep(1500);
    return [];
  }
}

async function collectChatRanking({ from, to, usersMap }) {
  const oldest = toSlackTs(from);
  const latest = toSlackTs(to);
  const counter = {};

  const messages = await fetchAllHistory(CHAT_CHANNEL_ID, oldest, latest);

  for (const msg of messages) {
    if (isHumanMessage(msg)) {
      counter[msg.user] = (counter[msg.user] || 0) + 1;
    }

    if (msg.reply_count && msg.ts) {
      const replies = await safeFetchReplies(CHAT_CHANNEL_ID, msg.ts);

      for (const reply of replies.slice(1)) {
        if (isHumanMessage(reply)) {
          counter[reply.user] = (counter[reply.user] || 0) + 1;
        }
      }

      await sleep(300);
    }
  }

  return Object.entries(counter)
    .map(([userId, count]) => ({
      date: formatDateJST(from),
      category: "chat",
      user_id: userId,
      user_name: usersMap[userId]?.userName || userId,
      count,
      channel_ids: CHAT_CHANNEL_ID,
      channel_names: "all-雑談"
    }))
    .sort((a, b) => b.count - a.count);
}

async function collectReportReplyRanking({ from, to, usersMap }) {
  const oldest = toSlackTs(from);
  const latest = toSlackTs(to);
  const counter = {};

  for (const channelId of REPORT_CHANNEL_IDS) {
    const messages = await fetchAllHistory(channelId, oldest, latest);

    for (const msg of messages) {
      if (!isTargetReportParent(msg)) continue;
      if (!msg.reply_count || !msg.ts) continue;

      const replies = await safeFetchReplies(channelId, msg.ts);

      for (const reply of replies.slice(1)) {
        if (isHumanMessage(reply)) {
          counter[reply.user] = (counter[reply.user] || 0) + 1;
        }
      }

      await sleep(300);
    }
  }

  return Object.entries(counter)
    .map(([userId, count]) => ({
      date: formatDateJST(from),
      category: "report_reply",
      user_id: userId,
      user_name: usersMap[userId]?.userName || userId,
      count,
      channel_ids: REPORT_CHANNEL_IDS.join(","),
      channel_names: "all-出勤日報,all-退勤日報"
    }))
    .sort((a, b) => b.count - a.count);
}

async function deleteExistingRowsForDate(targetDate) {
  // 最小構成では重複削除をしない版にしている
  // 再実行で重複を避けたい場合は Apps Script 側でユニーク化するか、
  // 後で batchUpdate で削除処理を追加してね
  console.log(`[info] duplicate delete skipped for date=${targetDate}`);
}

async function appendRows(rows) {
  if (!rows.length) {
    console.log("[info] no rows to append");
    return;
  }

  const sheets = await getSheetsClient();

  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${RAW_SHEET_NAME}!A:G`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: {
      values: rows.map(r => [
        r.date,
        r.category,
        r.user_id,
        r.user_name,
        r.count,
        r.channel_ids,
        r.channel_names
      ])
    }
  });
}

async function main() {
  const { from, to } = getYesterdayRangeJST();
  const targetDate = formatDateJST(from);

  console.log(`[start] targetDate=${targetDate}`);
  console.log(`[range] from=${from.toISOString()} to=${to.toISOString()}`);

  const usersMap = await fetchUsersMap();

  const chatRows = await collectChatRanking({ from, to, usersMap });
  const reportRows = await collectReportReplyRanking({ from, to, usersMap });

  const allRows = [...chatRows, ...reportRows];

  await deleteExistingRowsForDate(targetDate);
  await appendRows(allRows);

  console.log(`[done] appended=${allRows.length}`);
}

main().catch(err => {
  console.error("[fatal]", err?.data || err);
  process.exit(1);
});