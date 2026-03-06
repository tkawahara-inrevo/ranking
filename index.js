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
const RAW_RUN_LOG_SHEET = "raw_run_log";

const PARENT_LOOKBACK_DAYS = Number(process.env.PARENT_LOOKBACK_DAYS || 2);

function nowJst() {
  return new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Tokyo" }));
}

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
  const jstNow = nowJst();

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

  const messages = await fetchAllHistory(CHAT_CHANNEL_ID, oldest, latest);

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

  console.log("[chat summary]", {
    targetDate,
    channelId: CHAT_CHANNEL_ID,
    parentCandidates: messages.length,
    parentsCounted: parentCounted,
    repliesCounted: replyCounted,
    uniqueUsers: Object.keys(counter).length
  });

  const dailyRows = Object.entries(counter)
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

  const runLogRows = [[
    runId,
    targetDate,
    "chat",
    CHAT_CHANNEL_ID,
    "all-雑談",
    messages.length,
    parentCounted,
    replyCounted,
    replyCounted,
    Object.keys(counter).length,
    "chat aggregated",
    nowJstString()
  ]];

  return { dailyRows, runLogRows };
}

async function collectReportReplyRanking({ runId, from, to, usersMap }) {
  const targetDate = formatDateJST(from);
  const counter = {};
  const runLogRows = [];

  const parentSearchFrom = new Date(from);
  parentSearchFrom.setDate(parentSearchFrom.getDate() - PARENT_LOOKBACK_DAYS);

  const oldest = toSlackTs(parentSearchFrom);
  const latest = toSlackTs(to);

  for (const channel of REPORT_CHANNELS) {
    const messages = await fetchAllHistory(channel.id, oldest, latest);

    let matchedParentCount = 0;
    let repliesSeen = 0;
    let repliesCounted = 0;

    for (const msg of messages) {
      if (!isTargetReportParent(msg)) continue;
      matchedParentCount += 1;

      if (!msg.ts) continue;

      const replies = await safeFetchReplies(channel.id, msg.ts);

      for (const reply of replies.slice(1)) {
        repliesSeen += 1;

        if (!isHumanMessage(reply)) continue;
        if (!inRangeSlackTs(reply.ts, from, to)) continue;

        counter[reply.user] = (counter[reply.user] || 0) + 1;
        repliesCounted += 1;
      }

      await sleep(300);
    }

    console.log("[report summary]", {
      targetDate,
      channelId: channel.id,
      channelName: channel.name,
      parentCandidates: messages.length,
      matchedParents: matchedParentCount,
      repliesSeen,
      repliesCounted,
      uniqueUsersSoFar: Object.keys(counter).length
    });

    runLogRows.push([
      runId,
      targetDate,
      "report_reply",
      channel.id,
      channel.name,
      messages.length,
      matchedParentCount,
      repliesSeen,
      repliesCounted,
      Object.keys(counter).length,
      `reply_ts filtered; parent lookback=${PARENT_LOOKBACK_DAYS}d`,
      nowJstString()
    ]);
  }

  const dailyRows = Object.entries(counter)
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

  return { dailyRows, runLogRows };
}

async function main() {
  const runId = makeRunId();
  const { from, to } = getYesterdayRangeJST();
  const targetDate = formatDateJST(from);

  console.log("==================================================");
  console.log("[start]", {
    runId,
    targetDate,
    from: from.toISOString(),
    to: to.toISOString(),
    chatChannel: CHAT_CHANNEL_ID,
    reportChannels: REPORT_CHANNELS.map(x => `${x.name}(${x.id})`).join(", "),
    reportBotId: REPORT_BOT_ID || "(not set)",
    reportAppId: REPORT_APP_ID || "(not set)",
    reportTextKeyword: REPORT_TEXT_KEYWORD || "(not set)",
    parentLookbackDays: PARENT_LOOKBACK_DAYS
  });
  console.log("==================================================");

  const usersMap = await fetchUsersMap();
  console.log("[users loaded]", Object.keys(usersMap).length);

  const chatResult = CHAT_CHANNEL_ID
    ? await collectChatRanking({ runId, from, to, usersMap })
    : { dailyRows: [], runLogRows: [] };

  const reportResult = await collectReportReplyRanking({ runId, from, to, usersMap });

  const dailyRows = [...chatResult.dailyRows, ...reportResult.dailyRows];
  const runLogRows = [...chatResult.runLogRows, ...reportResult.runLogRows];

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
  }

  if (runLogRows.length) {
    await appendRows(RAW_RUN_LOG_SHEET, runLogRows);
  }

  console.log("[done]", {
    runId,
    targetDate,
    dailyRows: dailyRows.length,
    runLogRows: runLogRows.length
  });
}

main().catch(err => {
  console.error("[fatal]", err?.data || err);
  process.exit(1);
});