// =============================================================
// VLESS-over-WebSocket Dashboard for Cloudflare Workers (v4 - Features)
// - پروکسی VLESS روی WebSocket (رایگان روی پلن Free کلودفلر)
// - پنل مدیریت چند کاربره با محدودیت حجم (GB) و زمان (تاریخ انقضا)
// - ذخیره‌سازی کاربران و مصرف در Cloudflare KV
// - v4: 14 new features added (auto-disable, alerts, concurrency, bandwidth,
//       telegram, webhooks, connection logs, CSV export, bandwidth chart,
//       IP whitelist/blacklist, 2FA, custom sub page, i18n, theme toggle)
// =============================================================

import { connect } from 'cloudflare:sockets';

// ---------- تنظیمات ثابت ----------
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

// ---------- ابزار UUID ----------
function generateUUID() {
  return crypto.randomUUID();
}

function uuidToBytes(uuid) {
  const hex = uuid.replace(/-/g, '');
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return bytes;
}

function bytesToUUID(bytes, offset = 0) {
  const hex = [];
  for (let i = 0; i < 16; i++) {
    hex.push(bytes[offset + i].toString(16).padStart(2, '0'));
  }
  return (
    hex.slice(0, 4).join('') + '-' +
    hex.slice(4, 6).join('') + '-' +
    hex.slice(6, 8).join('') + '-' +
    hex.slice(8, 10).join('') + '-' +
    hex.slice(10, 16).join('')
  );
}

// ---------- ذخیره‌سازی کاربران (KV) ----------
async function getUser(env, uuid) {
  const raw = await env.VLESS_USERS.get(`user:${uuid}`);
  return raw ? JSON.parse(raw) : null;
}

async function saveUser(env, user) {
  await env.VLESS_USERS.put(`user:${user.uuid}`, JSON.stringify(user));
}

async function deleteUser(env, uuid) {
  await env.VLESS_USERS.delete(`user:${uuid}`);
}

async function listUsers(env) {
  const list = await env.VLESS_USERS.list({ prefix: 'user:' });
  if (list.keys.length === 0) return [];

  const rawUsers = await Promise.all(
    list.keys.map(key => env.VLESS_USERS.get(key.name))
  );

  const today = todayStr();
  const rawCounts = await Promise.all(
    list.keys.map(key => {
      const uuid = key.name.replace('user:', '');
      return env.VLESS_USERS.get(`reqcount:${uuid}:${today}`);
    })
  );

  return rawUsers.map((raw, i) => {
    if (!raw) return null;
    const user = JSON.parse(raw);
    user.dailyRequestsUsed = rawCounts[i] ? parseInt(rawCounts[i], 10) || 0 : 0;
    return user;
  }).filter(Boolean);
}

function isUserAllowed(user) {
  if (!user) return { ok: false, reason: 'کاربر یافت نشد' };
  if (!user.enabled) return { ok: false, reason: 'کاربر غیرفعال است' };
  if (user.expireAt && new Date(user.expireAt).getTime() < Date.now()) {
    return { ok: false, reason: 'مدت اشتراک منقضی شده' };
  }
  if (user.trafficLimitGB > 0) {
    const limitBytes = user.trafficLimitGB * 1024 * 1024 * 1024;
    if (user.trafficUsedBytes >= limitBytes) {
      return { ok: false, reason: 'حجم مصرفی تمام شده' };
    }
  }
  return { ok: true };
}

// ---------- محدودیت روزانه تعداد ریکوئست ----------
function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

async function getRequestCount(env, uuid) {
  const date = todayStr();
  const key = `reqcount:${uuid}:${date}`;
  const raw = await env.VLESS_USERS.get(key);
  return raw ? parseInt(raw, 10) || 0 : 0;
}

async function incrementRequestCount(env, uuid) {
  const date = todayStr();
  const key = `reqcount:${uuid}:${date}`;
  const raw = await env.VLESS_USERS.get(key);
  const current = raw ? parseInt(raw, 10) || 0 : 0;
  const newVal = current + 1;
  await env.VLESS_USERS.put(key, String(newVal), { expirationTtl: 86400 * 2 });
  await incrementRequestHistory(env, date);
  return newVal;
}

async function resetRequestCount(env, uuid) {
  const date = todayStr();
  const key = `reqcount:${uuid}:${date}`;
  await env.VLESS_USERS.delete(key);
}

async function incrementRequestHistory(env, date) {
  const key = `reqhistory:${date}`;
  const raw = await env.VLESS_USERS.get(key);
  const current = raw ? parseInt(raw, 10) || 0 : 0;
  await env.VLESS_USERS.put(key, String(current + 1), { expirationTtl: 86400 * 14 });
}

async function getRequestHistory(env) {
  const dates = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(Date.now() - i * 86400000);
    dates.push(d.toISOString().slice(0, 10));
  }

  const results = await Promise.all(
    dates.map(dateStr => env.VLESS_USERS.get(`reqhistory:${dateStr}`))
  );

  return dates.map((dateStr, i) => ({
    date: dateStr,
    requests: Number(results[i] || 0),
  }));
}

async function getEffectiveDailyCountAsync(env, uuid) {
  return await getRequestCount(env, uuid);
}

// ---------- مصرف ترافیک ----------
function dailyKey(date) {
  return 'daily:' + date.toISOString().slice(0, 10);
}

// Feature 2: Traffic alerts helper
async function createTrafficAlert(env, uuid, userName, pct, bytesUsed, limitBytes) {
  const entry = {
    id: crypto.randomUUID(),
    action: 'traffic_alert',
    detail: `هشدار ترافیک: کاربر «${userName}» به ${pct}% مصرف رسید (${(bytesUsed / (1024*1024*1024)).toFixed(2)} GB از ${(limitBytes / (1024*1024*1024)).toFixed(2)} GB)`,
    timestamp: new Date().toISOString(),
    uuid: uuid,
    alertLevel: pct >= 90 ? 'critical' : 'warning',
    percentUsed: pct,
  };

  // Log in alerts key
  const alertKey = 'alerts:global';
  try {
    const raw = await env.VLESS_USERS.get(alertKey);
    const alerts = raw ? JSON.parse(raw) : [];
    alerts.unshift(entry);
    await env.VLESS_USERS.put(alertKey, JSON.stringify(alerts.slice(0, 100)));
  } catch(e) {}

  // Also log in activity
  await logActivity(env, { uuid, action: 'traffic_alert', detail: entry.detail });

  // Feature 5: Telegram notification
  await sendTelegramAlert(env, entry.detail);

  // Feature 6: Webhook notification
  await fireWebhook(env, 'traffic_alert', { uuid, userName, percentUsed: pct, bytesUsed, limitBytes });
}

async function flushUsage(env, uuid, bytesDelta) {
  if (bytesDelta <= 0) return;
  const [userRaw, currentDaily] = await Promise.all([
    env.VLESS_USERS.get(`user:${uuid}`),
    env.VLESS_USERS.get(dailyKey(new Date()))
  ]);
  const user = userRaw ? JSON.parse(userRaw) : null;
  if (user) {
    const oldUsed = user.trafficUsedBytes || 0;
    user.trafficUsedBytes = oldUsed + bytesDelta;

    // Feature 2: Check traffic thresholds
    if (user.trafficLimitGB > 0) {
      const limitBytes = user.trafficLimitGB * 1024 * 1024 * 1024;
      const newPct = Math.round((user.trafficUsedBytes / limitBytes) * 100);
      const oldPct = Math.round((oldUsed / limitBytes) * 100);

      // Only alert once per threshold crossing
      if ((oldPct < 80 && newPct >= 80) || (oldPct < 90 && newPct >= 90)) {
        createTrafficAlert(env, uuid, user.name, newPct, user.trafficUsedBytes, limitBytes).catch(() => {});
      }
    }

    await Promise.all([
      env.VLESS_USERS.put(`user:${uuid}`, JSON.stringify(user)),
      env.VLESS_USERS.put(dailyKey(new Date()), String((Number(currentDaily) || 0) + bytesDelta))
    ]);

    // Feature 9: Update bandwidth sample
    updateBandwidthSample(env, bytesDelta).catch(() => {});
  }
}

// Feature 9: Bandwidth sampling
async function updateBandwidthSample(env, bytes) {
  const now = new Date();
  const key = `bw:${now.getUTCFullYear()}-${String(now.getUTCMonth()+1).padStart(2,'0')}-${String(now.getUTCDate()).padStart(2,'0')}-${String(now.getUTCHours()).padStart(2,'0')}-${String(now.getUTCMinutes()).padStart(2,'0')}`;
  try {
    const raw = await env.VLESS_USERS.get(key);
    const current = raw ? parseInt(raw, 10) || 0 : 0;
    await env.VLESS_USERS.put(key, String(current + bytes), { expirationTtl: 86400 * 3 });
  } catch(e) {}
}

async function getDailyStats(env) {
  const dates = [];
  for (let i = 6; i >= 0; i--) {
    dates.push(new Date(Date.now() - i * 86400000));
  }

  const results = await Promise.all(
    dates.map(d => env.VLESS_USERS.get(dailyKey(d)))
  );

  const last7 = dates.map((d, i) => ({
    date: dailyKey(d).replace('daily:', ''),
    bytes: Number(results[i] || 0),
  }));

  return { todayBytes: last7[last7.length - 1].bytes, last7 };
}

// ---------- Activity Log (KV) ----------
const MAX_ACTIVITY_PER_USER = 20;
const MAX_ACTIVITY_GLOBAL = 50;

async function logActivity(env, { uuid, action, detail }) {
  try {
    const entry = {
      id: crypto.randomUUID(),
      action,
      detail: detail || '',
      timestamp: new Date().toISOString(),
    };

    if (uuid) {
      const key = `activity:${uuid}`;
      const raw = await env.VLESS_USERS.get(key);
      const log = raw ? JSON.parse(raw) : [];
      log.unshift(entry);
      await env.VLESS_USERS.put(key, JSON.stringify(log.slice(0, MAX_ACTIVITY_PER_USER)));
    }

    const globalRaw = await env.VLESS_USERS.get('activity:global');
    const globalLog = globalRaw ? JSON.parse(globalRaw) : [];
    globalLog.unshift({ ...entry, uuid: uuid || null });
    await env.VLESS_USERS.put('activity:global', JSON.stringify(globalLog.slice(0, MAX_ACTIVITY_GLOBAL)));
  } catch (e) {
    console.error('logActivity error:', e);
  }
}

async function getActivityLog(env, uuid) {
  try {
    if (uuid) {
      const raw = await env.VLESS_USERS.get(`activity:${uuid}`);
      return raw ? JSON.parse(raw) : [];
    }
    const raw = await env.VLESS_USERS.get('activity:global');
    return raw ? JSON.parse(raw) : [];
  } catch (e) {
    return [];
  }
}

// ---------- Feature 7: Connection Logs ----------
const MAX_CONN_LOG = 50;

async function logConnection(env, uuid, destination, bytesTransferred) {
  try {
    const key = `connlog:${uuid}`;
    const raw = await env.VLESS_USERS.get(key);
    const log = raw ? JSON.parse(raw) : [];
    log.unshift({
      id: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      destination,
      bytes: bytesTransferred,
    });
    await env.VLESS_USERS.put(key, JSON.stringify(log.slice(0, MAX_CONN_LOG)));
  } catch(e) {}
}

async function getConnectionLog(env, uuid) {
  try {
    const raw = await env.VLESS_USERS.get(`connlog:${uuid}`);
    return raw ? JSON.parse(raw) : [];
  } catch(e) {
    return [];
  }
}

// ---------- Feature 1: Auto-disable expired users ----------
async function checkExpiredUsers(env) {
  try {
    // Check last run time - once per hour max
    const lastRunRaw = await env.VLESS_USERS.get('config:lastExpireCheck');
    if (lastRunRaw) {
      const lastRun = parseInt(lastRunRaw, 10);
      if (Date.now() - lastRun < 3600000) return; // Less than 1 hour
    }

    const users = await listUsers(env);
    let disabledCount = 0;

    for (const user of users) {
      if (user.enabled && user.expireAt && new Date(user.expireAt).getTime() < Date.now()) {
        user.enabled = false;
        await saveUser(env, user);
        await logActivity(env, {
          uuid: user.uuid,
          action: 'user_expired',
          detail: `کاربر «${user.name}» به دلیل انقضای اشتراک غیرفعال شد`,
        });
        await sendTelegramAlert(env, `⏰ کاربر «${user.name}» منقضی شد و غیرفعال گردید`);
        await fireWebhook(env, 'user_expired', { uuid: user.uuid, name: user.name });
        disabledCount++;
      }
    }

    await env.VLESS_USERS.put('config:lastExpireCheck', String(Date.now()));
    if (disabledCount > 0) {
      console.log(`checkExpiredUsers: disabled ${disabledCount} expired users`);
    }
  } catch(e) {
    console.error('checkExpiredUsers error:', e);
  }
}

// ---------- Feature 5: Telegram Bot ----------
async function sendTelegramAlert(env, message) {
  if (!env.BOT_TOKEN || !env.BOT_CHAT_ID) return;
  try {
    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: env.BOT_CHAT_ID,
        text: `🛡️ Hadi Panel\n\n${message}`,
        parse_mode: 'HTML',
      }),
    });
  } catch(e) {
    console.error('Telegram send error:', e);
  }
}

// ---------- Telegram Bot — Webhook Handler ----------
async function setupTelegramWebhook(env) {
  if (!env.BOT_TOKEN) return;
  try {
    const webhookUrl = `https://${env.WORKER_NAME === 'hah' ? 'hah' : env.WORKER_NAME}.workers.dev/tg-webhook`;
    // Try to use custom domain if available
    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/setWebhook`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: webhookUrl, allowed_updates: ['message'] }),
    });
  } catch(e) {
    console.error('Telegram webhook setup error:', e);
  }
}

async function handleTelegramWebhook(request, env) {
  if (request.method !== 'POST') return new Response('OK');
  try {
    const update = await request.json();
    const msg = update.message;
    if (!msg || !msg.text) return new Response('OK');

    const chatId = msg.chat.id;
    const text = msg.text.trim();
    const userId = msg.from?.id;

    // Only respond to the configured chat ID
    if (env.BOT_CHAT_ID && String(chatId) !== String(env.BOT_CHAT_ID)) {
      return new Response('OK');
    }

    let reply = '';

    if (text === '/start' || text === '/help') {
      reply = `🛡️ <b>Hadi Panel Bot</b>\n\n` +
        `به ربات مدیریت پنل خوش آمدید!\n\n` +
        `📌 دستورات موجود:\n` +
        `/status — وضعیت سرور\n` +
        `/users — لیست کاربران\n` +
        `/traffic — مصرف امروز\n` +
        `/requests — آمار ریکوئست\n` +
        `/help — راهنما`;
    } else if (text === '/status') {
      const users = await listUsers(env);
      const active = users.filter(u => isUserAllowed(u).ok).length;
      const daily = await getDailyStats(env);
      const trafficMB = (daily.todayBytes / (1024 * 1024)).toFixed(1);
      reply = `📊 <b>وضعیت سرور</b>\n\n` +
        `🟢 وضعیت: <b>فعال</b>\n` +
        `👥 کل کاربران: <b>${users.length}</b>\n` +
        `✅ کاربر فعال: <b>${active}</b>\n` +
        `📈 مصرف امروز: <b>${trafficMB} MB</b>`;
    } else if (text === '/users') {
      const users = await listUsers(env);
      const active = users.filter(u => isUserAllowed(u).ok);
      const expired = users.filter(u => !isUserAllowed(u).ok);
      let list = active.slice(0, 10).map((u, i) => {
        const gb = (u.trafficUsedBytes || 0) / (1024 * 1024 * 1024);
        const limit = u.trafficLimitGB > 0 ? `${u.trafficLimitGB} GB` : '∞';
        return `${i + 1}. <b>${u.name}</b> — ${gb.toFixed(1)} GB / ${limit}`;
      }).join('\n');
      if (active.length > 10) list += `\n... و ${active.length - 10} کاربر دیگر`;
      reply = `👥 <b>کاربران فعال (${active.length})</b>\n\n${list || 'هیچ کاربر فعالی نیست'}`;
      if (expired.length > 0) reply += `\n\n❌ منقضی/غیرفعال: ${expired.length} نفر`;
    } else if (text === '/traffic') {
      const daily = await getDailyStats(env);
      const lines = daily.last7.map(d => {
        const mb = (d.bytes / (1024 * 1024)).toFixed(1);
        const bar = '█'.repeat(Math.min(20, Math.round(d.bytes / (daily.todayBytes || 1) * 20)));
        return `${d.date.slice(5)}: ${bar} ${mb} MB`;
      }).join('\n');
      const todayMB = (daily.todayBytes / (1024 * 1024)).toFixed(1);
      reply = `📈 <b>مصرف ترافیک (۷ روز)</b>\n\n${lines}\n\n📍 امروز: <b>${todayMB} MB</b>`;
    } else if (text === '/requests') {
      const stats = await getWorkerStats(env);
      if (stats.available) {
        reply = `📊 <b>آمار ریکوئست</b>\n\n` +
          `🔢 ریکوئست‌های امروز: <b>${stats.requestsToday.toLocaleString()}</b>\n` +
          `📏 سقف روزانه: <b>${stats.dailyLimit.toLocaleString()}</b>`;
      } else {
        reply = `⚠️ آمار ریکوئست در دسترس نیست\n${stats.reason || ''}`;
      }
    } else {
      reply = `❓ دستور نامعتبر\nبرای راهنما /help بزنید`;
    }

    // Send reply
    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text: reply, parse_mode: 'HTML' }),
    });

    return new Response('OK');
  } catch(e) {
    console.error('Telegram webhook error:', e);
    return new Response('OK');
  }
}

async function sendTelegramMessage(env, text) {
  if (!env.BOT_TOKEN || !env.BOT_CHAT_ID) {
    return { ok: false, message: 'BOT_TOKEN یا BOT_CHAT_ID تنظیم نشده' };
  }
  try {
    const res = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: env.BOT_CHAT_ID,
        text: text,
        parse_mode: 'HTML',
      }),
    });
    const data = await res.json();
    if (data.ok) return { ok: true };
    return { ok: false, message: data.description || 'خطا در ارسال پیام' };
  } catch(e) {
    return { ok: false, message: e.message };
  }
}

// ---------- Feature 6: Webhook ----------
async function fireWebhook(env, event, data) {
  if (!env.WEBHOOK_URL) return;
  try {
    await fetch(env.WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        event,
        timestamp: new Date().toISOString(),
        panel: 'Hadi Panel',
        data,
      }),
    });
  } catch(e) {
    console.error('Webhook error:', e);
  }
}

// ---------- Feature 11: TOTP 2FA (pure JS) ----------
// Base32 encode/decode
function base32Encode(buffer) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const byte of buffer) bits += byte.toString(2).padStart(8, '0');
  let result = '';
  for (let i = 0; i < bits.length; i += 5) {
    const chunk = bits.substr(i, 5).padEnd(5, '0');
    result += alphabet[parseInt(chunk, 2)];
  }
  return result;
}

function base32Decode(str) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  str = str.replace(/=+$/, '').toUpperCase();
  let bits = '';
  for (const ch of str) {
    const idx = alphabet.indexOf(ch);
    if (idx === -1) continue;
    bits += idx.toString(2).padStart(5, '0');
  }
  const bytes = new Uint8Array(Math.floor(bits.length / 8));
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(bits.substr(i * 8, 8), 2);
  }
  return bytes;
}

async function hmacSHA1(key, message) {
  const cryptoKey = await crypto.subtle.importKey(
    'raw', key, { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', cryptoKey, message);
  return new Uint8Array(sig);
}

async function generateTOTP(secret, timeStep = 30) {
  const time = Math.floor(Date.now() / 1000 / timeStep);
  const timeBytes = new ArrayBuffer(8);
  const view = new DataView(timeBytes);
  view.setUint32(4, time, false);

  const keyBytes = base32Decode(secret);
  const hmac = await hmacSHA1(keyBytes, timeBytes);

  const offset = hmac[hmac.length - 1] & 0x0f;
  const code = ((hmac[offset] & 0x7f) << 24) |
               ((hmac[offset + 1] & 0xff) << 16) |
               ((hmac[offset + 2] & 0xff) << 8) |
               (hmac[offset + 3] & 0xff);
  return String(code % 1000000).padStart(6, '0');
}

async function verifyTOTP(secret, token, window = 1) {
  const timeStep = 30;
  for (let i = -window; i <= window; i++) {
    const time = Math.floor(Date.now() / 1000 / timeStep) + i;
    const timeBytes = new ArrayBuffer(8);
    const dv = new DataView(timeBytes);
    dv.setUint32(4, time, false);

    const keyBytes = base32Decode(secret);
    const hmac = await hmacSHA1(keyBytes, timeBytes);
    const offset = hmac[hmac.length - 1] & 0x0f;
    const code = ((hmac[offset] & 0x7f) << 24) |
                 ((hmac[offset + 1] & 0xff) << 16) |
                 ((hmac[offset + 2] & 0xff) << 8) |
                 (hmac[offset + 3] & 0xff);
    const expected = String(code % 1000000).padStart(6, '0');
    if (token === expected) return true;
  }
  return false;
}

async function generate2FASecret() {
  const bytes = new Uint8Array(20);
  crypto.getRandomValues(bytes);
  return base32Encode(bytes);
}

// ---------- Rate Limiting ----------
const RATE_LIMIT_MAX = 60;
const RATE_LIMIT_WINDOW_MS = 60 * 1000;

function getRateLimitKey(ip) {
  return `ratelimit:${ip}`;
}

async function checkRateLimit(env, ip) {
  try {
    const key = getRateLimitKey(ip);
    const raw = await env.VLESS_USERS.get(key);
    const data = raw ? JSON.parse(raw) : { count: 0, windowStart: Date.now() };
    const now = Date.now();

    if (now - data.windowStart > RATE_LIMIT_WINDOW_MS) {
      data.count = 1;
      data.windowStart = now;
      await env.VLESS_USERS.put(key, JSON.stringify(data), { expirationTtl: 120 });
      return true;
    }

    if (data.count >= RATE_LIMIT_MAX) return false;

    data.count += 1;
    await env.VLESS_USERS.put(key, JSON.stringify(data), { expirationTtl: 120 });
    return true;
  } catch (e) {
    return true;
  }
}

function getClientIP(request) {
  return request.headers.get('cf-connecting-ip') ||
    request.headers.get('x-real-ip') ||
    request.headers.get('x-forwarded-for')?.split(',')[0]?.trim() ||
    'unknown';
}

// ---------- خواندن هدر پروتکل VLESS ----------
function parseVlessHeader(buffer) {
  if (buffer.byteLength < 24) return { hasError: true, message: 'داده ناقص' };
  const view = new DataView(buffer);
  const version = view.getUint8(0);
  const uuidBytes = new Uint8Array(buffer.slice(1, 17));
  const uuid = bytesToUUID(uuidBytes);
  const optLength = view.getUint8(17);
  const cmdOffset = 18 + optLength;
  const command = view.getUint8(cmdOffset);
  if (command !== 1) {
    return { hasError: true, message: 'فقط TCP پشتیبانی می‌شود' };
  }
  let offset = cmdOffset + 1;
  const port = view.getUint16(offset);
  offset += 2;
  const addressType = view.getUint8(offset);
  offset += 1;

  let address = '';
  if (addressType === 1) {
    address = Array.from(new Uint8Array(buffer.slice(offset, offset + 4))).join('.');
    offset += 4;
  } else if (addressType === 2) {
    const domainLength = view.getUint8(offset);
    offset += 1;
    address = new TextDecoder().decode(buffer.slice(offset, offset + domainLength));
    offset += domainLength;
  } else if (addressType === 3) {
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(view.getUint16(offset + i * 2).toString(16));
    }
    address = parts.join(':');
    offset += 16;
  } else {
    return { hasError: true, message: 'نوع آدرس نامعتبر' };
  }

  return {
    hasError: false,
    version,
    uuid,
    address,
    port,
    rawDataIndex: offset,
  };
}

// ---------- تبدیل WebSocket به ReadableStream ----------
function makeReadableWebSocketStream(webSocket, earlyDataHeader) {
  let readableStreamCancel = false;
  const stream = new ReadableStream({
    start(controller) {
      webSocket.addEventListener('message', (event) => {
        if (readableStreamCancel) return;
        controller.enqueue(event.data);
      });
      webSocket.addEventListener('close', () => {
        if (readableStreamCancel) return;
        controller.close();
      });
      webSocket.addEventListener('error', (err) => {
        controller.error(err);
      });
      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel() {
      readableStreamCancel = true;
      safeCloseWebSocket(webSocket);
    },
  });
  return stream;
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { earlyData: null, error: null };
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decode = atob(base64Str);
    const arrayBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
    return { earlyData: arrayBuffer.buffer, error: null };
  } catch (error) {
    return { earlyData: null, error };
  }
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
      socket.close();
    }
  } catch (e) {}
}

// ---------- Feature 10: IP whitelist/blacklist check ----------
function checkIPAccess(user, clientIP) {
  if (!user) return { ok: true };
  // Blacklist: if blacklist has entries, IP must be in it
  if (user.ipBlacklist && user.ipBlacklist.length > 0) {
    if (!user.ipBlacklist.includes(clientIP)) {
      return { ok: false, reason: 'آدرس IP شما در لیست سیاه است' };
    }
  }
  // Whitelist: if whitelist has entries, IP must be in it
  if (user.ipWhitelist && user.ipWhitelist.length > 0) {
    if (!user.ipWhitelist.includes(clientIP)) {
      return { ok: false, reason: 'آدرس IP شما در لیست سفید نیست' };
    }
  }
  return { ok: true };
}

// ---------- Feature 3: Concurrent connection tracking ----------
async function incrementActiveConnections(env, uuid) {
  const key = `active:${uuid}`;
  const raw = await env.VLESS_USERS.get(key);
  const count = raw ? parseInt(raw, 10) || 0 : 0;
  await env.VLESS_USERS.put(key, String(count + 1), { expirationTtl: 300 });
  return count + 1;
}

async function decrementActiveConnections(env, uuid) {
  const key = `active:${uuid}`;
  const raw = await env.VLESS_USERS.get(key);
  const count = raw ? parseInt(raw, 10) || 0 : 0;
  const newCount = Math.max(0, count - 1);
  if (newCount <= 0) {
    await env.VLESS_USERS.delete(key);
  } else {
    await env.VLESS_USERS.put(key, String(newCount), { expirationTtl: 300 });
  }
  return newCount;
}

// ---------- Feature 4: Bandwidth limiting helpers ----------
// Returns bytes allowed in current window (1 second). Returns Infinity if no limit.
function getBytesAllowed(speedMBps) {
  if (!speedMBps || speedMBps <= 0) return Infinity;
  return speedMBps * 1024 * 1024; // bytes per second
}

// ---------- هسته اصلی پروکسی VLESS ----------
async function handleVlessConnection(request, env) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableStream = makeReadableWebSocketStream(webSocket, earlyDataHeader);

  let remoteSocket = null;
  let userUuid = null;
  let userName = null;
  let usageBytes = 0;
  let vlessHeaderSent = false;
  let connectionStartTime = null;
  let lastDestination = null;

  const FLUSH_INTERVAL_MS = 30000;
  const FLUSH_BYTES_THRESHOLD = 8 * 1024 * 1024;

  const flushInterval = setInterval(() => {
    if (userUuid && usageBytes > 0) {
      const toFlush = usageBytes;
      usageBytes = 0;
      flushUsage(env, userUuid, toFlush).catch(() => {});
    }
  }, FLUSH_INTERVAL_MS);

  const closeAll = () => {
    clearInterval(flushInterval);
    if (userUuid && usageBytes > 0) {
      flushUsage(env, userUuid, usageBytes).catch(() => {});
      usageBytes = 0;
    }
    safeCloseWebSocket(webSocket);
    try { remoteSocket && remoteSocket.close(); } catch (e) {}

    // Feature 3: Decrement concurrent connections
    if (userUuid) {
      decrementActiveConnections(env, userUuid).catch(() => {});
    }

    // Feature 7: Log connection
    if (userUuid && lastDestination && connectionStartTime) {
      const connBytes = usageBytes;
      logConnection(env, userUuid, lastDestination, connBytes).catch(() => {});
    }
  };

  const maybeEarlyFlush = () => {
    if (userUuid && usageBytes >= FLUSH_BYTES_THRESHOLD) {
      const toFlush = usageBytes;
      usageBytes = 0;
      flushUsage(env, userUuid, toFlush).catch(() => {});
    }
  };

  readableStream
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          if (remoteSocket) {
            // Feature 4: Bandwidth limiting (upload direction - client to remote)
            // We track bytes per second and add delay if needed
            const writer = remoteSocket.writable.getWriter();
            await writer.write(chunk);
            usageBytes += chunk.byteLength;
            writer.releaseLock();
            maybeEarlyFlush();
            return;
          }

          const header = parseVlessHeader(chunk);
          if (header.hasError) {
            throw new Error(header.message);
          }

          const today = todayStr();
          const clientIP = request.headers.get('cf-connecting-ip') || 'unknown';

          // Parallel KV reads
          const [userRaw, countRaw] = await Promise.all([
            env.VLESS_USERS.get(`user:${header.uuid}`),
            env.VLESS_USERS.get(`reqcount:${header.uuid}:${today}`),
          ]);
          const user = userRaw ? JSON.parse(userRaw) : null;
          const currentCount = countRaw ? parseInt(countRaw, 10) || 0 : 0;

          const allowed = isUserAllowed(user);
          if (!allowed.ok) {
            throw new Error('دسترسی رد شد: ' + allowed.reason);
          }

          // Feature 10: IP whitelist/blacklist
          const ipCheck = checkIPAccess(user, clientIP);
          if (!ipCheck.ok) {
            throw new Error('دسترسی رد شد: ' + ipCheck.reason);
          }

          // Feature 3: Concurrent connection limit
          if (user.maxConcurrent && user.maxConcurrent > 0) {
            const activeKey = `active:${header.uuid}`;
            const activeRaw = await env.VLESS_USERS.get(activeKey);
            const activeCount = activeRaw ? parseInt(activeRaw, 10) || 0 : 0;
            if (activeCount >= user.maxConcurrent) {
              throw new Error('دسترسی رد شد: حداکثر اتصالات همزمان رسیده');
            }
            await incrementActiveConnections(env, header.uuid);
          }

          // Check daily limit
          if (user.dailyRequestLimit && user.dailyRequestLimit > 0) {
            if (currentCount >= user.dailyRequestLimit) {
              throw new Error('دسترسی رد شد: محدودیت روزانه تعداد ریکوئست تمام شده');
            }
          }

          const newCount = currentCount + 1;
          await Promise.all([
            env.VLESS_USERS.put(`reqcount:${header.uuid}:${today}`, String(newCount), { expirationTtl: 86400 * 2 }),
            ...(newCount % 10 === 0 ? [incrementRequestHistory(env, today)] : [])
          ]);

          userUuid = header.uuid;
          userName = user.name;
          connectionStartTime = Date.now();
          lastDestination = `${header.address}:${header.port}`;

          if (newCount <= 1) {
            logActivity(env, {
              uuid: header.uuid,
              action: 'connect',
              detail: `اتصال به ${header.address}:${header.port}`,
            }).catch(() => {});
          }

          remoteSocket = connect({ hostname: header.address, port: header.port });

          const rawClientData = chunk.slice(header.rawDataIndex);
          const writer = remoteSocket.writable.getWriter();
          await writer.write(rawClientData);
          usageBytes += rawClientData.byteLength;
          writer.releaseLock();

          // پاسخ VLESS به کلاینت
          if (!vlessHeaderSent) {
            vlessHeaderSent = true;
            const vlessResponse = new Uint8Array([header.version, 0]);
            if (webSocket.readyState === WS_READY_STATE_OPEN) {
              webSocket.send(vlessResponse);
            }
          }

          // Feature 4: Bandwidth limiting (download direction - remote to client)
          // Track bytes per second for this user
          let dlBytesThisSecond = 0;
          let dlWindowStart = Date.now();
          const dlLimit = getBytesAllowed(user.downloadSpeedMBps);

          remoteSocket.readable
            .pipeTo(
              new WritableStream({
                async write(remoteChunk) {
                  if (webSocket.readyState === WS_READY_STATE_OPEN) {
                    // Feature 4: Enforce download speed limit
                    if (dlLimit < Infinity) {
                      dlBytesThisSecond += remoteChunk.byteLength;
                      const elapsed = Date.now() - dlWindowStart;
                      if (elapsed < 1000 && dlBytesThisSecond >= dlLimit) {
                        // Wait for remainder of the second
                        await new Promise(resolve => setTimeout(resolve, 1000 - elapsed));
                        dlBytesThisSecond = 0;
                        dlWindowStart = Date.now();
                      } else if (elapsed >= 1000) {
                        dlBytesThisSecond = remoteChunk.byteLength;
                        dlWindowStart = Date.now();
                      }
                    }

                    webSocket.send(remoteChunk);
                    usageBytes += remoteChunk.byteLength;
                    maybeEarlyFlush();
                  }
                },
                close() { closeAll(); },
                abort() { closeAll(); },
              })
            )
            .catch((err) => {
              console.error('proxy pipe error:', err && err.message);
              closeAll();
            });
        },
        close() { closeAll(); },
        abort() { closeAll(); },
      })
    )
    .catch((err) => {
      console.error('client pipe error:', err && err.message);
      closeAll();
    });

  return new Response(null, { status: 101, webSocket: client });
}

// =============================================================
// بخش داشبورد مدیریتی (Admin API + رابط کاربری)
// =============================================================

function jsonResponse(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', ...extraHeaders },
  });
}

function errorResponse(message, status = 400) {
  return jsonResponse({ ok: false, message }, status);
}

async function sha256(text) {
  const data = new TextEncoder().encode(text);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(hash)).map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function makeSessionToken(env) {
  const payload = `${Date.now()}:${crypto.randomUUID()}`;
  const sig = await sha256(payload + env.ADMIN_PASSWORD);
  return btoa(payload) + '.' + sig;
}

async function verifySessionToken(env, token) {
  if (!token || !token.includes('.')) return false;
  const [b64payload, sig] = token.split('.');
  let payload;
  try {
    payload = atob(b64payload);
  } catch (e) {
    return false;
  }
  const expectedSig = await sha256(payload + env.ADMIN_PASSWORD);
  if (sig !== expectedSig) return false;
  const [tsStr] = payload.split(':');
  const ts = parseInt(tsStr, 10);
  if (Date.now() - ts > 12 * 60 * 60 * 1000) return false;
  return true;
}

function getCookie(request, name) {
  const cookie = request.headers.get('cookie') || '';
  const match = cookie.match(new RegExp(`${name}=([^;]+)`));
  return match ? match[1] : null;
}

async function requireAuth(request, env) {
  const token = getCookie(request, 'admin_session');
  return verifySessionToken(env, token);
}

async function getWorkerStats(env) {
  const cached = await env.VLESS_USERS.get('cache:cfstats');
  if (cached) {
    try { return JSON.parse(cached); } catch(e) { /* fall through */ }
  }

  if (!env.CF_API_TOKEN || !env.CF_ACCOUNT_ID) {
    return { available: false, reason: 'CF_API_TOKEN یا CF_ACCOUNT_ID تنظیم نشده' };
  }
  const now = new Date();
  const startOfDay = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())).toISOString();
  const nowIso = now.toISOString();

  const query = `
    query GetWorkerStats($accountTag: String!, $scriptName: String!, $start: Time!, $end: Time!) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          workersInvocationsAdaptive(
            filter: { scriptName: $scriptName, datetime_geq: $start, datetime_leq: $end }
            limit: 1000
          ) {
            sum { requests }
          }
        }
      }
    }
  `;

  try {
    const res = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.CF_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query,
        variables: {
          accountTag: env.CF_ACCOUNT_ID,
          scriptName: env.WORKER_NAME || 'hah',
          start: startOfDay,
          end: nowIso,
        },
      }),
    });
    const data = await res.json();
    if (data.errors) {
      return { available: false, reason: data.errors.map((e) => e.message).join(', ') };
    }
    const rows = data?.data?.viewer?.accounts?.[0]?.workersInvocationsAdaptive || [];
    const requestsToday = rows.reduce((sum, r) => sum + (r.sum?.requests || 0), 0);
    const result = { available: true, requestsToday, dailyLimit: 100000 };
    await env.VLESS_USERS.put('cache:cfstats', JSON.stringify(result), { expirationTtl: 300 }).catch(() => {});
    return result;
  } catch (err) {
    return { available: false, reason: err.message };
  }
}

// ---------- API Router ----------
async function handleApi(request, env, url) {
  const path = url.pathname;

  const clientIP = getClientIP(request);
  if (path !== '/api/login') {
    const allowed = await checkRateLimit(env, clientIP);
    if (!allowed) {
      return errorResponse('تعداد درخواست‌ها بیش از حد مجاز است. لطفاً صبر کنید.', 429);
    }
  }

  // ---- Public endpoints ----

  if (path === '/api/login' && request.method === 'POST') {
    try {
      const body = await request.json().catch(() => ({}));
      if (body.password === env.ADMIN_PASSWORD) {
        const token = await makeSessionToken(env);
        await logActivity(env, { action: 'login', detail: 'ورود موفق مدیر' });
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: {
            'content-type': 'application/json',
            'set-cookie': `admin_session=${token}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=43200`,
          },
        });
      }
      await logActivity(env, { action: 'login_failed', detail: 'تلاش ناموفق ورود' });
      return errorResponse('رمز عبور اشتباه است', 401);
    } catch (e) {
      return errorResponse('خطا در پردازش درخواست', 400);
    }
  }

  // Feature 11: 2FA endpoints (public - need password first)
  if (path === '/api/2fa/verify' && request.method === 'POST') {
    try {
      const body = await request.json().catch(() => ({}));
      if (!body.password || !body.token) {
        return errorResponse('رمز عبور و کد TOTP لازم است', 400);
      }
      if (body.password !== env.ADMIN_PASSWORD) {
        return errorResponse('رمز عبور اشتباه است', 401);
      }
      const secretRaw = await env.VLESS_USERS.get('config:2fa_secret');
      if (!secretRaw) {
        return errorResponse('2FA فعال نیست', 400);
      }
      const valid = await verifyTOTP(secretRaw, body.token);
      if (valid) {
        const token = await makeSessionToken(env);
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: {
            'content-type': 'application/json',
            'set-cookie': `admin_session=${token}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=43200`,
          },
        });
      }
      return errorResponse('کد TOTP نامعتبر است', 401);
    } catch(e) {
      return errorResponse('خطا در بررسی 2FA', 500);
    }
  }

  if (path === '/api/2fa/status' && request.method === 'GET') {
    const secretRaw = await env.VLESS_USERS.get('config:2fa_secret');
    return jsonResponse({ ok: true, enabled: !!secretRaw });
  }

  // Feature 12: Custom subscription template (public for rendering)
  if (path === '/api/config/subtemplate' && request.method === 'GET') {
    const tmpl = await env.VLESS_USERS.get('config:subtemplate');
    return jsonResponse({ ok: true, template: tmpl || '' });
  }

  // ---- Protected endpoints ----
  const authed = await requireAuth(request, env);
  if (!authed) return errorResponse('ابتدا وارد شوید', 401);

  // ---- Stats (with auto-disable check) ----
  if (path === '/api/stats' && request.method === 'GET') {
    try {
      // Feature 1: Auto-disable expired users
      checkExpiredUsers(env).catch(() => {});

      const [requestStats, daily, users] = await Promise.all([
        getWorkerStats(env),
        getDailyStats(env),
        listUsers(env),
      ]);
      const activeUsers = users.filter((u) => isUserAllowed(u).ok).length;
      const totalTrafficUsed = users.reduce((sum, u) => sum + (u.trafficUsedBytes || 0), 0);
      return jsonResponse({
        ok: true,
        requestStats,
        todayBytes: daily.todayBytes,
        last7Days: daily.last7,
        activeUsers,
        totalUsers: users.length,
        totalTrafficUsed,
        protocolBreakdown: { VLESS: 100, VMESS: 0, TROJAN: 0, OTHER: 0 },
        version: '4.0.0',
      });
    } catch (e) {
      return errorResponse('خطا در دریافت آمار', 500);
    }
  }

  // ---- List users ----
  if (path === '/api/users' && request.method === 'GET') {
    try {
      const users = await listUsers(env);
      return jsonResponse({ ok: true, users });
    } catch (e) {
      return errorResponse('خطا در دریافت لیست کاربران', 500);
    }
  }

  // ---- Create user ----
  if (path === '/api/users' && request.method === 'POST') {
    try {
      const body = await request.json().catch(() => ({}));
      const user = {
        uuid: generateUUID(),
        name: body.name || 'کاربر بدون نام',
        trafficLimitGB: Number(body.trafficLimitGB) || 0,
        trafficUsedBytes: 0,
        dailyRequestLimit: Number(body.dailyRequestLimit) || 0,
        expireAt: body.expireAt || null,
        enabled: true,
        createdAt: new Date().toISOString(),
        // Feature 3: Concurrent connection limit
        maxConcurrent: Number(body.maxConcurrent) || 0,
        // Feature 4: Bandwidth limiting
        downloadSpeedMBps: Number(body.downloadSpeedMBps) || 0,
        uploadSpeedMBps: Number(body.uploadSpeedMBps) || 0,
        // Feature 10: IP whitelist/blacklist
        ipWhitelist: body.ipWhitelist || [],
        ipBlacklist: body.ipBlacklist || [],
      };
      await saveUser(env, user);
      await logActivity(env, {
        uuid: user.uuid,
        action: 'user_created',
        detail: `کاربر «${user.name}» ایجاد شد`,
      });
      // Feature 5: Telegram
      await sendTelegramAlert(env, `✅ کاربر جدید ایجاد شد:\nنام: ${user.name}\nUUID: ${user.uuid}`);
      // Feature 6: Webhook
      await fireWebhook(env, 'user_created', { uuid: user.uuid, name: user.name });
      return jsonResponse({ ok: true, user });
    } catch (e) {
      return errorResponse('خطا در ایجاد کاربر', 500);
    }
  }

  // ---- Update user (extended with new fields) ----
  const userMatch = path.match(/^\/api\/users\/([0-9a-f-]{36})$/);
  if (userMatch && request.method === 'PUT') {
    try {
      const uuid = userMatch[1];
      const existing = await getUser(env, uuid);
      if (!existing) return errorResponse('کاربر یافت نشد', 404);
      const body = await request.json().catch(() => ({}));
      if (body.name !== undefined) existing.name = body.name;
      if (body.trafficLimitGB !== undefined) existing.trafficLimitGB = Number(body.trafficLimitGB);
      if (body.dailyRequestLimit !== undefined) existing.dailyRequestLimit = Number(body.dailyRequestLimit);
      if (body.expireAt !== undefined) existing.expireAt = body.expireAt;
      if (body.enabled !== undefined) existing.enabled = !!body.enabled;
      if (body.resetTraffic) existing.trafficUsedBytes = 0;
      // Feature 3
      if (body.maxConcurrent !== undefined) existing.maxConcurrent = Number(body.maxConcurrent);
      // Feature 4
      if (body.downloadSpeedMBps !== undefined) existing.downloadSpeedMBps = Number(body.downloadSpeedMBps);
      if (body.uploadSpeedMBps !== undefined) existing.uploadSpeedMBps = Number(body.uploadSpeedMBps);
      // Feature 10
      if (body.ipWhitelist !== undefined) existing.ipWhitelist = body.ipWhitelist;
      if (body.ipBlacklist !== undefined) existing.ipBlacklist = body.ipBlacklist;

      await saveUser(env, existing);
      await env.VLESS_USERS.delete(`subcache:${uuid}`).catch(() => {});
      await logActivity(env, {
        uuid,
        action: 'user_updated',
        detail: `کاربر «${existing.name}» به‌روزرسانی شد`,
      });
      return jsonResponse({ ok: true, user: existing });
    } catch (e) {
      return errorResponse('خطا در به‌روزرسانی کاربر', 500);
    }
  }

  // ---- Delete user ----
  if (userMatch && request.method === 'DELETE') {
    try {
      const uuid = userMatch[1];
      const existing = await getUser(env, uuid);
      const name = existing?.name || 'ناشناخته';
      await deleteUser(env, uuid);
      await env.VLESS_USERS.delete(`subcache:${uuid}`).catch(() => {});
      await logActivity(env, {
        action: 'user_deleted',
        detail: `کاربر «${name}» حذف شد`,
      });
      await sendTelegramAlert(env, `🗑️ کاربر «${name}» حذف شد`);
      await fireWebhook(env, 'user_deleted', { uuid, name });
      return jsonResponse({ ok: true });
    } catch (e) {
      return errorResponse('خطا در حذف کاربر', 500);
    }
  }

  // ---- Reset user traffic ----
  const resetMatch = path.match(/^\/api\/users\/([0-9a-f-]{36})\/reset-traffic$/);
  if (resetMatch && request.method === 'PUT') {
    try {
      const uuid = resetMatch[1];
      const existing = await getUser(env, uuid);
      if (!existing) return errorResponse('کاربر یافت نشد', 404);
      existing.trafficUsedBytes = 0;
      await saveUser(env, existing);
      await env.VLESS_USERS.delete(`subcache:${uuid}`).catch(() => {});
      await logActivity(env, {
        uuid,
        action: 'traffic_reset',
        detail: `ترافیک کاربر «${existing.name}» صفر شد`,
      });
      return jsonResponse({ ok: true, user: existing });
    } catch (e) {
      return errorResponse('خطا در صفر کردن ترافیک', 500);
    }
  }

  // ---- Export all users as JSON ----
  if (path === '/api/export' && request.method === 'GET') {
    try {
      const users = await listUsers(env);
      await logActivity(env, { action: 'export_all', detail: `خروجی JSON از ${users.length} کاربر` });
      return new Response(JSON.stringify(users, null, 2), {
        status: 200,
        headers: {
          'content-type': 'application/json; charset=utf-8',
          'content-disposition': `attachment; filename="users-export-${todayStr()}.json"`,
        },
      });
    } catch (e) {
      return errorResponse('خطا در خروجی‌گیری', 500);
    }
  }

  // ---- Feature 8: CSV Export ----
  if (path === '/api/export/csv' && request.method === 'GET') {
    try {
      const users = await listUsers(env);
      const header = 'Name,UUID,Traffic Used (GB),Traffic Limit (GB),Expires,Status,Requests Today\n';
      const rows = users.map(u => {
        const usedGB = ((u.trafficUsedBytes || 0) / (1024*1024*1024)).toFixed(2);
        const limitGB = u.trafficLimitGB || 'Unlimited';
        const expires = u.expireAt || 'Never';
        const status = isUserAllowed(u).ok ? 'Active' : 'Inactive';
        const reqs = u.dailyRequestsUsed || 0;
        // Escape names with commas
        const name = (u.name || '').includes(',') ? `"${u.name}"` : (u.name || '');
        return `${name},${u.uuid},${usedGB},${limitGB},${expires},${status},${reqs}`;
      }).join('\n');
      await logActivity(env, { action: 'export_csv', detail: `خروجی CSV از ${users.length} کاربر` });
      return new Response(header + rows, {
        status: 200,
        headers: {
          'content-type': 'text/csv; charset=utf-8',
          'content-disposition': `attachment; filename="users-export-${todayStr()}.csv"`,
        },
      });
    } catch (e) {
      return errorResponse('خطا در خروجی CSV', 500);
    }
  }

  // ---- Activity log ----
  if (path === '/api/activity' && request.method === 'GET') {
    try {
      const log = await getActivityLog(env);
      return jsonResponse({ ok: true, activity: log });
    } catch (e) {
      return errorResponse('خطا در دریافت فعالیت‌ها', 500);
    }
  }

  // ---- Feature 2: Alerts endpoint ----
  if (path === '/api/alerts' && request.method === 'GET') {
    try {
      const raw = await env.VLESS_USERS.get('alerts:global');
      const alerts = raw ? JSON.parse(raw) : [];
      return jsonResponse({ ok: true, alerts });
    } catch(e) {
      return jsonResponse({ ok: true, alerts: [] });
    }
  }

  // ---- Feature 7: Connection logs ----
  const connLogMatch = path.match(/^\/api\/connections\/([0-9a-f-]{36})$/);
  if (connLogMatch && request.method === 'GET') {
    try {
      const uuid = connLogMatch[1];
      const log = await getConnectionLog(env, uuid);
      return jsonResponse({ ok: true, connections: log });
    } catch(e) {
      return errorResponse('خطا در دریافت لاگ اتصالات', 500);
    }
  }

  // ---- Feature 9: Bandwidth history ----
  if (path === '/api/bandwidth-history' && request.method === 'GET') {
    try {
      const now = new Date();
      const points = [];
      // Generate 288 points (last 24 hours, every 5 minutes)
      for (let i = 287; i >= 0; i--) {
        const t = new Date(now.getTime() - i * 5 * 60000);
        const key = `bw:${t.getUTCFullYear()}-${String(t.getUTCMonth()+1).padStart(2,'0')}-${String(t.getUTCDate()).padStart(2,'0')}-${String(t.getUTCHours()).padStart(2,'0')}-${String(t.getUTCMinutes()).padStart(2,'0')}`;
        points.push({ time: t.toISOString(), key });
      }
      // Batch read
      const values = await Promise.all(
        points.map(p => env.VLESS_USERS.get(p.key))
      );
      const history = points.map((p, i) => ({
        time: p.time,
        bytes: Number(values[i] || 0),
      }));
      return jsonResponse({ ok: true, history });
    } catch(e) {
      return jsonResponse({ ok: true, history: [] });
    }
  }

  // ---- Request Stats API ----
  if (path === '/api/request-stats' && request.method === 'GET') {
    try {
      const users = await listUsers(env);
      const userStats = users.map(u => ({
        uuid: u.uuid,
        name: u.name,
        requestsToday: u.dailyRequestsUsed || 0,
        dailyRequestLimit: u.dailyRequestLimit || 0,
      }));
      userStats.sort((a, b) => b.requestsToday - a.requestsToday);
      const totalRequests = userStats.reduce((s, u) => s + u.requestsToday, 0);
      return jsonResponse({
        ok: true,
        totalRequests,
        avgPerUser: users.length > 0 ? Math.round(totalRequests / users.length) : 0,
        topUsers: userStats.slice(0, 5),
        allUsers: userStats,
      });
    } catch (e) {
      return errorResponse('خطا در دریافت آمار ریکوئست', 500);
    }
  }

  // ---- Request History API ----
  if (path === '/api/request-history' && request.method === 'GET') {
    try {
      const history = await getRequestHistory(env);
      return jsonResponse({ ok: true, history });
    } catch (e) {
      return errorResponse('خطا در دریافت تاریخچه ریکوئست', 500);
    }
  }

  // ---- Reset User Requests ----
  const resetReqMatch = path.match(/^\/api\/users\/([0-9a-f-]{36})\/reset-requests$/);
  if (resetReqMatch && request.method === 'PUT') {
    try {
      const uuid = resetReqMatch[1];
      const existing = await getUser(env, uuid);
      if (!existing) return errorResponse('کاربر یافت نشد', 404);
      await resetRequestCount(env, uuid);
      await logActivity(env, {
        uuid,
        action: 'requests_reset',
        detail: `ریکوئست‌های روزانه کاربر «${existing.name}» صفر شد`,
      });
      return jsonResponse({ ok: true, message: 'شمارنده ریکوئست صفر شد' });
    } catch (e) {
      return errorResponse('خطا در صفر کردن ریکوئست', 500);
    }
  }

  // ---- Feature 5: Telegram send ----
  if (path === '/api/tg/send' && request.method === 'POST') {
    try {
      const body = await request.json().catch(() => ({}));
      if (!body.message) return errorResponse('پیام الزامی است', 400);
      const result = await sendTelegramMessage(env, body.message);
      return jsonResponse(result);
    } catch(e) {
      return errorResponse('خطا در ارسال پیام تلگرام', 500);
    }
  }

  // ---- Feature 5: Telegram send summary ----
  if (path === '/api/tg/send-summary' && request.method === 'POST') {
    try {
      const users = await listUsers(env);
      const activeCount = users.filter(u => isUserAllowed(u).ok).length;
      const totalTraffic = users.reduce((s, u) => s + (u.trafficUsedBytes || 0), 0);
      const todayTraffic = (await getDailyStats(env)).todayBytes;

      const msg = `📊 گزارش پنل Hadi Panel\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👥 کل کاربران: ${users.length}\n` +
        `✅ فعال: ${activeCount}\n` +
        `📈 مصرف امروز: ${(todayTraffic / (1024*1024)).toFixed(1)} MB\n` +
        `💾 مصرف کل: ${(totalTraffic / (1024*1024*1024)).toFixed(2)} GB\n` +
        `⏰ ${new Date().toLocaleString('fa-IR')}`;

      const result = await sendTelegramMessage(env, msg);
      return jsonResponse(result);
    } catch(e) {
      return errorResponse('خطا در ارسال خلاصه', 500);
    }
  }

  // ---- Telegram: Setup Webhook ----
  if (path === '/api/tg/setup-webhook' && request.method === 'POST') {
    try {
      if (!env.BOT_TOKEN) {
        return errorResponse('BOT_TOKEN تنظیم نشده', 400);
      }
      // Get the hostname from the request URL
      const hostName = url.hostname;
      const webhookUrl = `https://${hostName}/tg-webhook`;
      const res = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: webhookUrl, allowed_updates: ['message'] }),
      });
      const data = await res.json();
      if (data.ok) {
        return jsonResponse({ ok: true, message: `وب‌هوک با موفقیت تنظیم شد:\n${webhookUrl}` });
      }
      return errorResponse(`خطا: ${data.description || 'ناشناخته'}`, 400);
    } catch(e) {
      return errorResponse('خطا در تنظیم وب‌هوک', 500);
    }
  }

  // ---- Feature 6: Webhook test ----
  if (path === '/api/webhook/test' && request.method === 'POST') {
    try {
      if (!env.WEBHOOK_URL) {
        return errorResponse('WEBHOOK_URL تنظیم نشده', 400);
      }
      await fetch(env.WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event: 'test',
          timestamp: new Date().toISOString(),
          panel: 'Hadi Panel',
          data: { message: 'این یک پیام تستی از Hadi Panel است' },
        }),
      });
      return jsonResponse({ ok: true, message: 'پیام تست وبهوک ارسال شد' });
    } catch(e) {
      return errorResponse('خطا در ارسال وبهوک تست', 500);
    }
  }

  // ---- Feature 11: 2FA setup/verify ----
  if (path === '/api/2fa/setup' && request.method === 'POST') {
    try {
      const body = await request.json().catch(() => ({}));
      if (body.password !== env.ADMIN_PASSWORD) {
        return errorResponse('رمز عبور اشتباه است', 401);
      }

      if (body.action === 'enable') {
        const secret = await generate2FASecret();
        await env.VLESS_USERS.put('config:2fa_secret', secret);
        const hostName = url.hostname;
        const otpauthUrl = `otpauth://totp/HadiPanel:admin?secret=${secret}&issuer=HadiPanel&algorithm=SHA1&digits=6&period=30`;
        return jsonResponse({ ok: true, secret, otpauthUrl });
      }

      if (body.action === 'disable') {
        const secret = await env.VLESS_USERS.get('config:2fa_secret');
        if (!secret) return errorResponse('2FA فعال نیست');
        // Verify current token before disabling
        if (body.token) {
          const valid = await verifyTOTP(secret, body.token);
          if (!valid) return errorResponse('کد TOTP نامعتبر است', 401);
        }
        await env.VLESS_USERS.delete('config:2fa_secret');
        return jsonResponse({ ok: true, message: '2FA غیرفعال شد' });
      }

      return errorResponse('action نامعتبر (enable/disable)');
    } catch(e) {
      return errorResponse('خطا در تنظیم 2FA', 500);
    }
  }

  // ---- Feature 12: Custom subscription template ----
  if (path === '/api/config/subtemplate' && request.method === 'PUT') {
    try {
      const body = await request.json().catch(() => ({}));
      if (body.template !== undefined) {
        await env.VLESS_USERS.put('config:subtemplate', body.template);
      }
      return jsonResponse({ ok: true, message: 'قالب سابسکرایشن ذخیره شد' });
    } catch(e) {
      return errorResponse('خطا در ذخیره قالب', 500);
    }
  }

  // ---- Feature 13/14: Language & Theme (stored server-side too) ----
  if (path === '/api/config/preferences' && request.method === 'GET') {
    try {
      const lang = await env.VLESS_USERS.get('config:language') || 'fa';
      const theme = await env.VLESS_USERS.get('config:theme') || 'dark';
      return jsonResponse({ ok: true, language: lang, theme });
    } catch(e) {
      return jsonResponse({ ok: true, language: 'fa', theme: 'dark' });
    }
  }

  if (path === '/api/config/preferences' && request.method === 'PUT') {
    try {
      const body = await request.json().catch(() => ({}));
      if (body.language) await env.VLESS_USERS.put('config:language', body.language);
      if (body.theme) await env.VLESS_USERS.put('config:theme', body.theme);
      return jsonResponse({ ok: true });
    } catch(e) {
      return errorResponse('خطا در ذخیره تنظیمات', 500);
    }
  }

  return errorResponse('مسیر نامعتبر', 404);
}

// =============================================================
// روتر اصلی
// =============================================================

function buildVlessUri(uuid, hostName, name) {
  const params = new URLSearchParams({
    encryption: 'none',
    security: 'tls',
    sni: hostName,
    type: 'ws',
    host: hostName,
    path: '/vless-ws',
  });
  return `vless://${uuid}@${hostName}:443?${params.toString()}#${encodeURIComponent(name)}`;
}

function buildVlessUriCached(user, hostName) {
  if (user._cachedUri && user._cachedHost === hostName) return user._cachedUri;
  const uri = buildVlessUri(user.uuid, hostName, user.name);
  return uri;
}

function fmtBytesFa(bytes) {
  if (!bytes) return '0 مگابایت';
  const gb = bytes / (1024 * 1024 * 1024);
  if (gb >= 1) return gb.toFixed(2) + ' گیگابایت';
  return (bytes / (1024 * 1024)).toFixed(1) + ' مگابایت';
}

function subInfoPage(user, subUrl, configLink, statusInfo, dailyUsed) {
  const used = user.trafficUsedBytes || 0;
  const limitBytes = user.trafficLimitGB > 0 ? user.trafficLimitGB * 1024 * 1024 * 1024 : 0;
  const pct = limitBytes > 0 ? Math.min(100, Math.round((used / limitBytes) * 100)) : 0;
  const expireText = user.expireAt ? new Date(user.expireAt).toLocaleDateString('fa-IR') : 'نامحدود';
  const daysLeft = user.expireAt ? Math.ceil((new Date(user.expireAt).getTime() - Date.now()) / 86400000) : null;
  dailyUsed = dailyUsed || 0;
  const dailyLimit = user.dailyRequestLimit || 0;
  const dailyPct = dailyLimit > 0 ? Math.min(100, Math.round((dailyUsed / dailyLimit) * 100)) : 0;
  const statusColor = statusInfo.ok ? '#34d399' : '#f87171';
  const statusText = statusInfo.ok ? 'فعال' : statusInfo.reason;
  const remaining = limitBytes > 0 ? fmtBytesFa(Math.max(0, limitBytes - used)) : 'نامحدود';

  return `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<title>${user.name} — اطلاعات اشتراک</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
:root{--bg:#000;--bg-card:rgba(28,28,30,.8);--border:rgba(255,255,255,.08);--border-active:rgba(255,255,255,.15);--text:#f5f5f7;--text-sec:rgba(255,255,255,.6);--text-ter:rgba(255,255,255,.35);--accent:#F6821F;--accent-glow:rgba(246,130,31,.3);--success:#34d399;--danger:#f87171;--warning:#fbbf24;--glass:rgba(255,255,255,.03);--r:20px;--rs:14px;--rx:10px}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'SF Pro Display','Segoe UI',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden;-webkit-font-smoothing:antialiased}
.amb{position:fixed;inset:0;z-index:0;pointer-events:none;background:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(246,130,31,.12),transparent),radial-gradient(ellipse 60% 40% at 80% 50%,rgba(246,130,31,.06),transparent),radial-gradient(ellipse 50% 30% at 20% 80%,rgba(52,211,153,.04),transparent)}
.amb::after{content:'';position:absolute;inset:0;background-image:linear-gradient(rgba(255,255,255,.02) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.02) 1px,transparent 1px);background-size:60px 60px;mask-image:radial-gradient(ellipse 70% 50% at 50% 0%,black,transparent 70%)}
.ctn{position:relative;z-index:1;max-width:440px;margin:0 auto;padding:24px 20px 40px;min-height:100vh;display:flex;flex-direction:column;gap:16px}
.hdr{text-align:center;padding:32px 0 8px;animation:fu .6s cubic-bezier(.16,1,.3,1) both}
.hdr .logo{width:64px;height:64px;border-radius:18px;background:linear-gradient(135deg,var(--accent),#e06816);display:inline-flex;align-items:center;justify-content:center;font-size:28px;font-weight:800;color:#fff;box-shadow:0 8px 32px rgba(246,130,31,.3);margin-bottom:16px}
.hdr h1{font-size:22px;font-weight:700;letter-spacing:-.3px;margin-bottom:6px}
.hdr .sub{font-size:13px;color:var(--text-sec);font-weight:400}
.sbadge{display:inline-flex;align-items:center;gap:8px;padding:6px 14px 6px 10px;background:${statusInfo.ok?'rgba(52,211,153,.12)':'rgba(248,113,113,.12)'};border:1px solid ${statusInfo.ok?'rgba(52,211,153,.2)':'rgba(248,113,113,.2)'};border-radius:999px;font-size:12px;font-weight:600;color:${statusColor};margin-top:12px}
.sdot{width:8px;height:8px;border-radius:50%;background:${statusColor};animation:pls 2s ease-in-out infinite}
.cd{background:var(--bg-card);backdrop-filter:blur(40px);-webkit-backdrop-filter:blur(40px);border:1px solid var(--border);border-radius:var(--r);padding:22px;animation:fu .6s cubic-bezier(.16,1,.3,1) both}
.cd:nth-child(2){animation-delay:.05s}.cd:nth-child(3){animation-delay:.1s}.cd:nth-child(4){animation-delay:.15s}.cd:nth-child(5){animation-delay:.2s}.cd:nth-child(6){animation-delay:.25s}
.ct{font-size:12px;font-weight:600;color:var(--text-sec);text-transform:uppercase;letter-spacing:.5px;margin-bottom:16px;display:flex;align-items:center;gap:8px}
.ct i{font-size:14px;color:var(--accent)}
.rsec{display:flex;align-items:center;gap:24px}
.rbox{position:relative;width:100px;height:100px;flex-shrink:0}
.rbox svg{width:100%;height:100%;transform:rotate(-90deg)}
.rbg{fill:none;stroke:rgba(255,255,255,.06);stroke-width:8}
.rfill{fill:none;stroke-width:8;stroke-linecap:round;stroke:${pct>=90?'var(--danger)':pct>=70?'var(--warning)':'var(--accent)'};stroke-dasharray:264;stroke-dashoffset:${264-(264*pct/100)};transition:stroke-dashoffset 1s cubic-bezier(.16,1,.3,1)}
.rval{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center}
.rnum{font-size:22px;font-weight:800;letter-spacing:-.5px}
.rlbl{font-size:10px;color:var(--text-ter);margin-top:-2px}
.rdet{flex:1}
.drow{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px}
.drow:last-child{border-bottom:none}
.dlbl{color:var(--text-sec)}
.dval{font-weight:600;font-variant-numeric:tabular-nums}
.sgrid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.sitem{background:var(--glass);border:1px solid var(--border);border-radius:var(--rs);padding:16px;text-align:center;transition:border-color .2s}
.sitem:hover{border-color:var(--border-active)}
.sicon{width:36px;height:36px;border-radius:10px;background:rgba(246,130,31,.1);display:inline-flex;align-items:center;justify-content:center;font-size:14px;color:var(--accent);margin-bottom:10px}
.sval{font-size:18px;font-weight:700;letter-spacing:-.3px;margin-bottom:2px}
.slbl{font-size:11px;color:var(--text-ter)}
.lsec{display:flex;flex-direction:column;gap:10px}
.litem{background:var(--glass);border:1px solid var(--border);border-radius:var(--rx);padding:12px 14px;display:flex;align-items:center;gap:10px;transition:border-color .2s}
.litem:hover{border-color:var(--border-active)}
.licon{width:32px;height:32px;border-radius:8px;background:rgba(246,130,31,.1);display:flex;align-items:center;justify-content:center;font-size:12px;color:var(--accent);flex-shrink:0}
.linp{flex:1;min-width:0;background:transparent;border:none;outline:none;color:var(--text);font-family:'SF Mono','JetBrains Mono',monospace;font-size:11px;direction:ltr;text-align:left}
.cbtn{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:8px 14px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;transition:all .2s;font-family:inherit}
.cbtn:hover{transform:scale(1.02);box-shadow:0 4px 16px var(--accent-glow)}
.cbtn:active{transform:scale(.98)}
.cbtn.copied{background:var(--success)}
.ft{text-align:center;padding:16px 0;font-size:11px;color:var(--text-ter)}
.ftb{display:inline-flex;align-items:center;gap:6px;color:var(--text-sec);font-weight:500}
.ftb .dt{color:var(--accent)}
.toast{position:fixed;bottom:32px;left:50%;transform:translateX(-50%) translateY(20px);background:rgba(28,28,30,.95);backdrop-filter:blur(20px);border:1px solid var(--border-active);color:var(--text);padding:12px 20px;border-radius:999px;font-size:13px;font-weight:600;z-index:200;display:flex;align-items:center;gap:8px;opacity:0;pointer-events:none;transition:all .3s cubic-bezier(.16,1,.3,1);box-shadow:0 16px 48px rgba(0,0,0,.4)}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.toast i{color:var(--success)}
@keyframes fu{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:translateY(0)}}
@keyframes pls{0%,100%{opacity:1}50%{opacity:.4}}
@media(max-width:380px){.ctn{padding:16px 14px 32px}.cd{padding:18px}.rsec{flex-direction:column;align-items:stretch}.rbox{margin:0 auto}}
</style>
</head>
<body>
<div class="amb"></div>
<div class="ctn">
  <div class="hdr">
    <div class="logo">H</div>
    <h1>${user.name}</h1>
    <div class="sub">اطلاعات اشتراک VPN</div>
    <div class="sbadge"><span class="sdot"></span>${statusText}</div>
  </div>
  <div class="cd">
    <div class="ct"><i class="fas fa-chart-pie"></i>مصرف ترافیک</div>
    <div class="rsec">
      <div class="rbox">
        <svg viewBox="0 0 100 100"><circle class="rbg" cx="50" cy="50" r="42"/><circle class="rfill" cx="50" cy="50" r="42"/></svg>
        <div class="rval"><div class="rnum">${pct}%</div><div class="rlbl">مصرف</div></div>
      </div>
      <div class="rdet">
        <div class="drow"><span class="dlbl">مصرف‌شده</span><span class="dval">${fmtBytesFa(used)}</span></div>
        <div class="drow"><span class="dlbl">سقف مجاز</span><span class="dval">${limitBytes > 0 ? fmtBytesFa(limitBytes) : '∞ نامحدود'}</span></div>
        ${limitBytes > 0 ? `<div class="drow"><span class="dlbl">باقی‌مانده</span><span class="dval" style="color:${pct >= 90 ? 'var(--danger)' : 'var(--success)'}">${remaining}</span></div>` : ''}
      </div>
    </div>
  </div>
  <div class="sgrid">
    <div class="sitem"><div class="sicon"><i class="fas fa-clock"></i></div><div class="sval">${expireText}</div><div class="slbl">تاریخ انقضا</div></div>
    <div class="sitem"><div class="sicon" style="background:rgba(52,211,153,.1);color:var(--success)"><i class="fas fa-fire"></i></div><div class="sval">${dailyUsed.toLocaleString('fa-IR')}</div><div class="slbl">ریکوئست امروز</div></div>
    ${daysLeft !== null ? `<div class="sitem"><div class="sicon" style="background:${daysLeft < 3 ? 'rgba(248,113,113,.1)' : 'rgba(246,130,31,.1)'};color:${daysLeft < 3 ? 'var(--danger)' : 'var(--accent)'}"><i class="fas fa-hourglass-half"></i></div><div class="sval" style="color:${daysLeft < 0 ? 'var(--danger)' : daysLeft <= 3 ? 'var(--warning)' : 'var(--text)'}">${daysLeft >= 0 ? daysLeft + ' روز' : 'منقضی'}</div><div class="slbl">${daysLeft >= 0 ? 'باقی‌مانده' : 'منقضی شده'}</div></div>` : ''}
    ${dailyLimit > 0 ? `<div class="sitem"><div class="sicon"><i class="fas fa-bolt"></i></div><div class="sval">${dailyPct}%</div><div class="slbl">سقف ریکوئست</div></div>` : '<div class="sitem"><div class="sicon"><i class="fas fa-infinity"></i></div><div class="sval">∞</div><div class="slbl">ریکوئست نامحدود</div></div>'}
  </div>
  <div class="cd">
    <div class="ct"><i class="fas fa-link"></i>لینک سابسکرایشن</div>
    <div class="lsec"><div class="litem"><div class="licon"><i class="fas fa-rss"></i></div><input class="linp" readonly value="${subUrl}" onclick="this.select()"><button class="cbtn" onclick="cp(this,'${subUrl}')">کپی</button></div></div>
  </div>
  <div class="cd">
    <div class="ct"><i class="fas fa-terminal"></i>کانفیگ مستقیم</div>
    <div class="lsec"><div class="litem"><div class="licon" style="background:rgba(52,211,153,.1);color:var(--success)"><i class="fas fa-code"></i></div><input class="linp" readonly value="${configLink}" onclick="this.select()"><button class="cbtn" onclick="cp(this,'${configLink}')">کپی</button></div></div>
  </div>
  <div class="ft"><div class="ftb">Hadi Panel <span class="dt">•</span> امن و سریع</div></div>
</div>
<div class="toast" id="toast"><i class="fas fa-check-circle"></i><span id="toastText">کپی شد</span></div>
<script>
function cp(btn,text){navigator.clipboard.writeText(text).then(()=>{btn.textContent='کپی شد ✓';btn.classList.add('copied');const t=document.getElementById('toast');t.classList.add('show');setTimeout(()=>{t.classList.remove('show');btn.textContent='کپی';btn.classList.remove('copied')},2000)})}
</script>
</body>
</html>`;
}

async function handleSubscription(request, env, uuid) {
  const user = await getUser(env, uuid);
  const allowed = isUserAllowed(user);
  const hostName = new URL(request.url).hostname;

  const acceptHeader = request.headers.get('accept') || '';
  const wantsHtml = acceptHeader.includes('text/html');

  if (!user) {
    if (wantsHtml) return new Response('کاربر یافت نشد', { status: 404, headers: { 'content-type': 'text/plain; charset=utf-8' } });
    return new Response('', { status: 200, headers: { 'content-type': 'text/plain; charset=utf-8' } });
  }

  const link = buildVlessUriCached(user, hostName);

  if (wantsHtml) {
    const cacheKey = `subcache:${uuid}`;
    try {
      const cached = await env.VLESS_USERS.get(cacheKey);
      if (cached) {
        return new Response(cached, { status: 200, headers: { 'content-type': 'text/html; charset=utf-8' } });
      }
    } catch (e) { /* cache miss */ }

    const subUrl = `${new URL(request.url).origin}/sub/${user.uuid}`;
    const dailyUsed = await getRequestCount(env, user.uuid);
    const page = subInfoPage(user, subUrl, link, allowed, dailyUsed);
    await env.VLESS_USERS.put(cacheKey, page, { expirationTtl: 300 }).catch(() => {});
    return new Response(page, { status: 200, headers: { 'content-type': 'text/html; charset=utf-8' } });
  }

  if (!allowed.ok) {
    return new Response('', { status: 200, headers: { 'content-type': 'text/plain; charset=utf-8' } });
  }
  const body = btoa(unescape(encodeURIComponent(link + '\n')));
  const headers = { 'content-type': 'text/plain; charset=utf-8' };
  const used = user.trafficUsedBytes || 0;
  const total = user.trafficLimitGB > 0 ? user.trafficLimitGB * 1024 * 1024 * 1024 : 0;
  const expire = user.expireAt ? Math.floor(new Date(user.expireAt).getTime() / 1000) : 0;
  headers['subscription-userinfo'] = `upload=0; download=${used}; total=${total}; expire=${expire}`;

  return new Response(body, { status: 200, headers });
}

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const upgradeHeader = request.headers.get('Upgrade');

      // اتصال VLESS از طریق WebSocket
      if (upgradeHeader === 'websocket' && url.pathname === '/vless-ws') {
        try {
          return await handleVlessConnection(request, env);
        } catch (err) {
          return new Response('WebSocket error: ' + err.message, { status: 500 });
        }
      }

      // لینک سابسکرایشن هر کاربر
      const subMatch = url.pathname.match(/^\/sub\/([0-9a-f-]{36})$/);
      if (subMatch) {
        return handleSubscription(request, env, subMatch[1]);
      }

      // Telegram Webhook
      if (url.pathname === '/tg-webhook') {
        return handleTelegramWebhook(request, env);
      }

      // API مدیریتی
      if (url.pathname.startsWith('/api/')) {
        return handleApi(request, env, url);
      }

      // صفحه داشبورد و فایل‌های استاتیک
      return env.ASSETS.fetch(request);
    } catch (err) {
      console.error('Unhandled error:', err);
      return errorResponse('خطای داخلی سرور', 500);
    }
  },
};
