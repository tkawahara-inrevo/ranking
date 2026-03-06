require("dotenv").config();

const { WebClient } = require("@slack/web-api");
const { google } = require("googleapis");

const botClient = new WebClient(process.env.SLACK_BOT_TOKEN);
const userClient = new WebClient(process.env.SLACK_USER_TOKEN);

const CHAT_CHANNEL = {
  id: process.env.CHAT_CHANNEL_ID,
  name: "all-雑談"
};

const REPORT_IN_CHANNEL = {
  id: process.env.REPORT_IN_CHANNEL_ID,
  name: "all-出勤日報",
  category: "report_in_reply"
};

const REPORT_OUT_CHANNEL = {
  id: process.env.REPORT_OUT_CHANNEL_ID,
  name: "all-退勤日報",
  category: "report_out_reply"
};

const REPORT_CHANNELS = [REPORT_IN_CHANNEL, REPORT_OUT_CHANNEL].filter(x => x.id);

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const RAW_DAILY_SHEET = "raw_daily";

const REPORT_BOT_ID = process.env.REPORT_BOT_ID || "";
const REPORT_APP_ID = process.env.REPORT_APP_ID || "";
const REPORT_TEXT_KEYWORD = process.env.REPORT_TEXT_KEYWORD || "";
const PARENT_LOOKBACK_DAYS = Number(process.env.PARENT_LOOKBACK_DAYS || 2);

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
    range: `${sheetName}!A:H`,
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
    await sleep(1200);
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

function buildDailyRows({ runId, date, category, channelId, channelName, counter, usersMap }) {
  return Object.entries(counter)
    .map(([userId, count]) => [
      runId,
      date,
      category,
      userId,
      usersMap[userId]?.userName || userId,
      count,
      channelId,
      channelName
    ])
    .sort((a, b) => b[5] - a[5]);
}

async function collectChatRows({ runId, from, to, usersMap }) {
  if (!CHAT_CHANNEL.id) return [];

  const oldest = toSlackTs(from);
  const latest = toSlackTs(to);
  const targetDate = formatDateJST(from);
  const counter = {};

  const messages = await fetchAllHistory(CHAT_CHANNEL.id, oldest, latest);

  let countedPosts = 0;
  let countedReplies = 0;

  for (const msg of messages) {
    if (isHumanMessage(msg) && inRangeSlackTs(msg.ts, from, to)) {
      counter[msg.user] = (counter[msg.user] || 0) + 1;
      countedPosts += 1;
    }

    if (msg.reply_count && msg.ts) {
      const replies = await safeFetchReplies(CHAT_CHANNEL.id, msg.ts);

      for (const reply of replies.slice(1)) {
        if (!isHumanMessage(reply)) continue;
        if (!inRangeSlackTs(reply.ts, from, to)) continue;

        counter[reply.user] = (counter[reply.user] || 0) + 1;
        countedReplies += 1;
      }

      await sleep(250);
    }
  }

  console.log("[chat]", {
    date: targetDate,
    channel: CHAT_CHANNEL.name,
    parentMessagesFetched: messages.length,
    postsCounted: countedPosts,
    repliesCounted: countedReplies,
    users: Object.keys(counter).length
  });

  return buildDailyRows({
    runId,
    date: targetDate,
    category: "chat",
    channelId: CHAT_CHANNEL.id,
    channelName: CHAT_CHANNEL.name,
    counter,
    usersMap
  });
}

async function collectReportRowsForChannel({ runId, from, to, usersMap, channel }) {
  const targetDate = formatDateJST(from);
  const counter = {};

  const parentSearchFrom = new Date(from);
  parentSearchFrom.setDate(parentSearchFrom.getDate() - PARENT_LOOKBACK_DAYS);

  const oldest = toSlackTs(parentSearchFrom);
  const latest = toSlackTs(to);

  const messages = await fetchAllHistory(channel.id, oldest, latest);

  let matchedParents = 0;
  let repliesCounted = 0;

  for (const msg of messages) {
    if (!isTargetReportParent(msg)) continue;
    matchedParents += 1;

    if (!msg.ts) continue;

    const replies = await safeFetchReplies(channel.id, msg.ts);

    for (const reply of replies.slice(1)) {
      if (!isHumanMessage(reply)) continue;
      if (!inRangeSlackTs(reply.ts, from, to)) continue;

      counter[reply.user] = (counter[reply.user] || 0) + 1;
      repliesCounted += 1;
    }

    await sleep(300);
  }

  console.log("[report]", {
    date: targetDate,
    channel: channel.name,
    parentCandidatesFetched: messages.length,
    matchedParents,
    repliesCounted,
    users: Object.keys(counter).length
  });

  return buildDailyRows({
    runId,
    date: targetDate,
    category: channel.category,
    channelId: channel.id,
    channelName: channel.name,
    counter,
    usersMap
  });
}

async function main() {
  const runId = makeRunId();
  const { from, to } = getYesterdayRangeJST();
  const targetDate = formatDateJST(from);

  console.log("[start]", {
    runId,
    targetDate,
    from: from.toISOString(),
    to: to.toISOString()
  });

  const usersMap = await fetchUsersMap();

  const chatRows = await collectChatRows({ runId, from, to, usersMap });

  const reportInRows = REPORT_IN_CHANNEL.id
    ? await collectReportRowsForChannel({
        runId,
        from,
        to,
        usersMap,
        channel: REPORT_IN_CHANNEL
      })
    : [];

  const reportOutRows = REPORT_OUT_CHANNEL.id
    ? await collectReportRowsForChannel({
        runId,
        from,
        to,
        usersMap,
        channel: REPORT_OUT_CHANNEL
      })
    : [];

  const allRows = [...chatRows, ...reportInRows, ...reportOutRows];

  if (allRows.length) {
    await appendRows(RAW_DAILY_SHEET, allRows);
  }

  console.log("[done]", {
    runId,
    targetDate,
    rows: allRows.length
  });
}

main().catch(err => {
  console.error("[fatal]", err?.data || err);
  process.exit(1);
});