// =============================================================
// VLESS-over-WebSocket Dashboard for Cloudflare Workers
// - پروکسی VLESS روی WebSocket (رایگان روی پلن Free کلودفلر)
// - پنل مدیریت چند کاربره با محدودیت حجم (GB) و زمان (تاریخ انقضا)
// - ذخیره‌سازی کاربران و مصرف در Cloudflare KV
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
// ساختار هر کاربر در KV با کلید user:<uuid>
// { uuid, name, trafficLimitGB, trafficUsedBytes, expireAt (ISO یا null), enabled, createdAt }

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
  const users = [];
  for (const key of list.keys) {
    const raw = await env.VLESS_USERS.get(key.name);
    if (raw) {
      const user = JSON.parse(raw);
      user.dailyRequestsUsed = getEffectiveDailyCount(user); // بدون هیچ KV read اضافه
      users.push(user);
    }
  }
  return users;
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

// ---------- محدودیت روزانه تعداد ریکوئست (اتصال) هر کاربر ----------
// بهینه‌سازی: به‌جای یک KV key جداگانه (که هر اتصال یک read + یک write اضافه می‌کرد)،
// شمارنده روز داخل همان آبجکت کاربر ذخیره می‌شود؛ چون کاربر را برای بررسی مجوز
// (isUserAllowed) در هر حالت می‌خوانیم، این کار صفر KV-operation اضافه دارد.
function todayStr() {
  return new Date().toISOString().slice(0, 10);
}
function getEffectiveDailyCount(user) {
  return user.dailyRequestDate === todayStr() ? (user.dailyRequestCount || 0) : 0;
}
// روی آبجکت user در حافظه تغییر می‌دهد (mutate)؛ ذخیره‌سازی نهایی به عهده‌ی صدازننده است.
// اگر سقف پر شده باشد false برمی‌گرداند و چیزی را تغییر نمی‌دهد.
function bumpDailyRequest(user) {
  if (!user.dailyRequestLimit || user.dailyRequestLimit <= 0) return true; // نامحدود
  const today = todayStr();
  const current = user.dailyRequestDate === today ? (user.dailyRequestCount || 0) : 0;
  if (current >= user.dailyRequestLimit) return false;
  user.dailyRequestDate = today;
  user.dailyRequestCount = current + 1;
  return true;
}

// شمارنده مصرف: برای جلوگیری از نوشتن مکرر روی KV (که نرخ محدود دارد)
// مصرف هر اتصال را در حافظه جمع می‌زنیم و فقط هر چند وقت یک‌بار (یا وقتی حجم قابل‌توجهی رد و بدل شد) در KV ذخیره می‌کنیم.
function dailyKey(date) {
  return 'daily:' + date.toISOString().slice(0, 10); // daily:YYYY-MM-DD
}

async function flushUsage(env, uuid, bytesDelta) {
  if (bytesDelta <= 0) return;
  const user = await getUser(env, uuid);
  if (user) {
    user.trafficUsedBytes = (user.trafficUsedBytes || 0) + bytesDelta;
    await saveUser(env, user);
  }
  // مصرف کل روزانه (برای کارت «مصرف امروز» و نمودار ۷ روز گذشته)
  const key = dailyKey(new Date());
  const current = Number((await env.VLESS_USERS.get(key)) || 0);
  await env.VLESS_USERS.put(key, String(current + bytesDelta));
}

async function getDailyStats(env) {
  const last7 = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(Date.now() - i * 86400000);
    const key = dailyKey(d);
    const bytes = Number((await env.VLESS_USERS.get(key)) || 0);
    last7.push({ date: key.replace('daily:', ''), bytes });
  }
  const todayBytes = last7[last7.length - 1].bytes;
  return { todayBytes, last7 };
}

// ---------- خواندن هدر پروتکل VLESS ----------
// مرجع فرمت: https://xtls.github.io/development/protocols/vless.html
function parseVlessHeader(buffer) {
  if (buffer.byteLength < 24) return { hasError: true, message: 'داده ناقص' };
  const view = new DataView(buffer);
  const version = view.getUint8(0);
  const uuidBytes = new Uint8Array(buffer.slice(1, 17));
  const uuid = bytesToUUID(uuidBytes);
  const optLength = view.getUint8(17);
  const cmdOffset = 18 + optLength;
  const command = view.getUint8(cmdOffset); // 1 = TCP, 2 = UDP
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
    // IPv4
    address = Array.from(new Uint8Array(buffer.slice(offset, offset + 4))).join('.');
    offset += 4;
  } else if (addressType === 2) {
    // Domain
    const domainLength = view.getUint8(offset);
    offset += 1;
    address = new TextDecoder().decode(buffer.slice(offset, offset + domainLength));
    offset += domainLength;
  } else if (addressType === 3) {
    // IPv6
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

// ---------- هسته اصلی پروکسی VLESS ----------
async function handleVlessConnection(request, env) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableStream = makeReadableWebSocketStream(webSocket, earlyDataHeader);

  let remoteSocket = null;
  let userUuid = null;
  let usageBytes = 0;
  let vlessHeaderSent = false;

  // بهینه‌سازی: قبلاً هر ۵ ثانیه، حالا هر ۳۰ ثانیه (یا فوراً اگر حجم قابل‌توجهی رد و بدل شد).
  // چون سقف رایگان KV کلودفلر روزی فقط ۱۰۰۰ نوشتن است، این تغییر تعداد نوشتن‌ها را ~۶ برابر کاهش می‌دهد.
  const FLUSH_INTERVAL_MS = 30000;
  const FLUSH_BYTES_THRESHOLD = 8 * 1024 * 1024; // اگر ۸ مگابایت رد و بدل شد، زودتر از موعد ذخیره کن

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
  };

  // اگر حجم رد و بدل‌شده از آستانه رد شد، زودتر از تایمر ۳۰ ثانیه‌ای ذخیره کن
  // (برای اتصال‌های پرترافیک، تا مصرف ثبت‌شده خیلی از واقعیت عقب نماند).
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

          const user = await getUser(env, header.uuid);
          const allowed = isUserAllowed(user);
          if (!allowed.ok) {
            throw new Error('دسترسی رد شد: ' + allowed.reason);
          }
          if (!bumpDailyRequest(user)) {
            throw new Error('دسترسی رد شد: محدودیت روزانه تعداد ریکوئست تمام شده');
          }
          // همان یک write که در پایان اتصال/هر چند ثانیه برای مصرف ترافیک انجام می‌شود،
          // شمارنده روزانه به‌روزشده را هم با خودش ذخیره می‌کند (بدون write جداگانه).
          await saveUser(env, user);
          userUuid = header.uuid;

          remoteSocket = connect({ hostname: header.address, port: header.port });

          const rawClientData = chunk.slice(header.rawDataIndex);
          const writer = remoteSocket.writable.getWriter();
          await writer.write(rawClientData);
          usageBytes += rawClientData.byteLength;
          writer.releaseLock();

          // پاسخ VLESS به کلاینت (نسخه + بدون آپشن اضافه)
          if (!vlessHeaderSent) {
            vlessHeaderSent = true;
            const vlessResponse = new Uint8Array([header.version, 0]);
            if (webSocket.readyState === WS_READY_STATE_OPEN) {
              webSocket.send(vlessResponse);
            }
          }

          // انتقال داده از سرور مقصد به کلاینت
          remoteSocket.readable
            .pipeTo(
              new WritableStream({
                write(remoteChunk) {
                  if (webSocket.readyState === WS_READY_STATE_OPEN) {
                    webSocket.send(remoteChunk);
                    usageBytes += remoteChunk.byteLength;
                    maybeEarlyFlush();
                  }
                },
                close() { closeAll(); },
                abort() { closeAll(); },
              })
            )
            .catch(() => closeAll());
        },
        close() { closeAll(); },
        abort() { closeAll(); },
      })
    )
    .catch(() => closeAll());

  return new Response(null, { status: 101, webSocket: client });
}

// =============================================================
// بخش داشبورد مدیریتی (Admin API + رابط کاربری)
// =============================================================

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
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
  // اعتبار سشن: ۱۲ ساعت
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

// ---------- آمار واقعی ریکوئست از Cloudflare Analytics API ----------
async function getWorkerStats(env) {
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
    return { available: true, requestsToday, dailyLimit: 100000 };
  } catch (err) {
    return { available: false, reason: err.message };
  }
}

async function handleApi(request, env, url) {
  const path = url.pathname;

  if (path === '/api/login' && request.method === 'POST') {
    const body = await request.json().catch(() => ({}));
    if (body.password === env.ADMIN_PASSWORD) {
      const token = await makeSessionToken(env);
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: {
          'content-type': 'application/json',
          'set-cookie': `admin_session=${token}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=43200`,
        },
      });
    }
    return jsonResponse({ ok: false, message: 'رمز عبور اشتباه است' }, 401);
  }

  const authed = await requireAuth(request, env);
  if (!authed) return jsonResponse({ ok: false, message: 'ابتدا وارد شوید' }, 401);

  if (path === '/api/stats' && request.method === 'GET') {
    const requestStats = await getWorkerStats(env);
    const daily = await getDailyStats(env);
    const users = await listUsers(env);
    const activeUsers = users.filter((u) => isUserAllowed(u).ok).length;
    return jsonResponse({
      ok: true,
      requestStats,
      todayBytes: daily.todayBytes,
      last7Days: daily.last7,
      activeUsers,
      totalUsers: users.length,
      // این پنل فقط پروتکل VLESS را پیاده‌سازی کرده، بنابراین واقعیت همین است:
      protocolBreakdown: { VLESS: 100, VMESS: 0, TROJAN: 0, OTHER: 0 },
    });
  }

  if (path === '/api/users' && request.method === 'GET') {
    const users = await listUsers(env);
    return jsonResponse({ ok: true, users });
  }

  if (path === '/api/users' && request.method === 'POST') {
    const body = await request.json().catch(() => ({}));
    const user = {
      uuid: generateUUID(),
      name: body.name || 'کاربر بدون نام',
      trafficLimitGB: Number(body.trafficLimitGB) || 0, // 0 = نامحدود
      trafficUsedBytes: 0,
      dailyRequestLimit: Number(body.dailyRequestLimit) || 0, // 0 = نامحدود
      expireAt: body.expireAt || null, // ISO date یا null برای نامحدود
      enabled: true,
      createdAt: new Date().toISOString(),
    };
    await saveUser(env, user);
    return jsonResponse({ ok: true, user });
  }

  const userMatch = path.match(/^\/api\/users\/([0-9a-f-]{36})$/);
  if (userMatch && request.method === 'PUT') {
    const uuid = userMatch[1];
    const existing = await getUser(env, uuid);
    if (!existing) return jsonResponse({ ok: false, message: 'یافت نشد' }, 404);
    const body = await request.json().catch(() => ({}));
    if (body.name !== undefined) existing.name = body.name;
    if (body.trafficLimitGB !== undefined) existing.trafficLimitGB = Number(body.trafficLimitGB);
    if (body.dailyRequestLimit !== undefined) existing.dailyRequestLimit = Number(body.dailyRequestLimit);
    if (body.expireAt !== undefined) existing.expireAt = body.expireAt;
    if (body.enabled !== undefined) existing.enabled = !!body.enabled;
    if (body.resetTraffic) existing.trafficUsedBytes = 0;
    await saveUser(env, existing);
    return jsonResponse({ ok: true, user: existing });
  }

  if (userMatch && request.method === 'DELETE') {
    await deleteUser(env, userMatch[1]);
    return jsonResponse({ ok: true });
  }

  return jsonResponse({ ok: false, message: 'مسیر نامعتبر' }, 404);
}


// =============================================================
// روتر اصلی
// =============================================================

// ---------- لینک سابسکرایشن (برای اپلیکیشن‌های کلاینت مثل v2rayNG / NekoBox / Streisand) ----------
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

// ---------- صفحه گرافیکی اطلاعات سابسکرایشن (برای زمانی که لینک در مرورگر باز شود) ----------
function fmtBytesFa(bytes) {
  if (!bytes) return '0 مگابایت';
  const gb = bytes / (1024 * 1024 * 1024);
  if (gb >= 1) return gb.toFixed(2) + ' گیگابایت';
  return (bytes / (1024 * 1024)).toFixed(1) + ' مگابایت';
}

function subInfoPage(user, subUrl, configLink, statusInfo) {
  const used = user.trafficUsedBytes || 0;
  const limitBytes = user.trafficLimitGB > 0 ? user.trafficLimitGB * 1024 * 1024 * 1024 : 0;
  const pct = limitBytes > 0 ? Math.min(100, Math.round((used / limitBytes) * 100)) : 0;
  const expireText = user.expireAt
    ? new Date(user.expireAt).toLocaleDateString('fa-IR')
    : 'نامحدود';
  const daysLeft = user.expireAt
    ? Math.ceil((new Date(user.expireAt).getTime() - Date.now()) / 86400000)
    : null;
  const statusColor = statusInfo.ok ? '#3ddc97' : '#e25858';
  const statusText = statusInfo.ok ? 'فعال' : statusInfo.reason;

  return `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${user.name} — اطلاعات اشتراک</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: Tahoma, Vazir, sans-serif; background:#0a0a0a; color:#e9e9e9; margin:0; padding:20px 14px; min-height:100vh; }
  .wrap { max-width: 420px; margin: 0 auto; }
  .card { background:#181818; border:1px solid #2d2d2d; border-radius:16px; padding:20px; margin-bottom:14px; }
  .top { display:flex; align-items:center; gap:12px; margin-bottom:6px; }
  .avatar { width:46px; height:46px; border-radius:50%; background:linear-gradient(135deg,#FFD54F,#e0b13a); display:flex; align-items:center; justify-content:center; font-weight:800; color:#0a0a0a; font-size:16px; }
  .name { font-weight:700; font-size:16px; }
  .badge { display:inline-flex; align-items:center; gap:6px; padding:4px 10px; border-radius:999px; font-size:12px; margin-top:4px; }
  .dot { width:7px; height:7px; border-radius:50%; background:${statusColor}; }
  .row { display:flex; justify-content:space-between; align-items:center; padding:10px 0; border-bottom:1px solid #262626; font-size:13px; }
  .row:last-child { border-bottom:none; }
  .row .l { color:#9a9a9a; }
  .row .v { font-family:'JetBrains Mono',monospace; font-weight:600; }
  .bar { height:8px; border-radius:99px; background:#262626; overflow:hidden; margin-top:10px; }
  .bar span { display:block; height:100%; background:${pct>=90?'#e25858':'#FFD54F'}; width:${pct}%; }
  .barlabel { display:flex; justify-content:space-between; font-size:11px; color:#9a9a9a; margin-top:6px; }
  h3 { font-size:13px; color:#9a9a9a; margin:0 0 10px; font-weight:600; }
  .linkbox { display:flex; gap:8px; align-items:center; background:#101010; border:1px solid #2d2d2d; border-radius:10px; padding:10px 12px; }
  .linkbox input { flex:1; background:transparent; border:none; color:#e9e9e9; font-family:monospace; font-size:11px; direction:ltr; text-align:left; outline:none; }
  .copybtn { background:#FFD54F; color:#0a0a0a; border:none; border-radius:8px; padding:8px 12px; font-weight:700; font-size:12px; cursor:pointer; white-space:nowrap; }
  .foot { text-align:center; color:#666; font-size:11px; margin-top:6px; }
  .toast { position:fixed; bottom:24px; left:50%; transform:translateX(-50%) translateY(20px); background:#FFD54F; color:#0a0a0a; padding:9px 18px; border-radius:999px; font-size:13px; font-weight:700; opacity:0; transition:.25s; }
  .toast.show { opacity:1; transform:translateX(-50%) translateY(0); }
</style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <div class="top">
      <div class="avatar">${(user.name||'ک').trim()[0] || 'ک'}</div>
      <div>
        <div class="name">${user.name}</div>
        <span class="badge" style="background:${statusColor}22;color:${statusColor}"><span class="dot"></span>${statusText}</span>
      </div>
    </div>
  </div>

  <div class="card">
    <h3>مصرف ترافیک</h3>
    <div class="row"><span class="l">مصرف‌شده</span><span class="v">${fmtBytesFa(used)}</span></div>
    <div class="row"><span class="l">سقف مجاز</span><span class="v">${limitBytes > 0 ? fmtBytesFa(limitBytes) : 'نامحدود'}</span></div>
    ${limitBytes > 0 ? `<div class="bar"><span></span></div><div class="barlabel"><span>${pct}٪ مصرف‌شده</span><span>${fmtBytesFa(Math.max(0,limitBytes-used))} باقی‌مانده</span></div>` : ''}
  </div>

  <div class="card">
    <h3>اعتبار زمانی</h3>
    <div class="row"><span class="l">تاریخ انقضا</span><span class="v">${expireText}</span></div>
    ${daysLeft !== null ? `<div class="row"><span class="l">${daysLeft >= 0 ? 'روزهای باقی‌مانده' : 'منقضی شده'}</span><span class="v" style="color:${daysLeft < 0 ? '#e25858' : (daysLeft <= 3 ? '#FFD54F' : '#e9e9e9')}">${daysLeft >= 0 ? daysLeft + ' روز' : Math.abs(daysLeft) + ' روز پیش'}</span></div>` : ''}
  </div>

  <div class="card">
    <h3>لینک سابسکرایشن (برای اپلیکیشن VPN)</h3>
    <div class="linkbox">
      <input readonly value="${subUrl}" onclick="this.select()">
      <button class="copybtn" onclick="copyText('${subUrl}')">کپی</button>
    </div>
  </div>

  <div class="card">
    <h3>کانفیگ مستقیم (تک‌کاربره)</h3>
    <div class="linkbox">
      <input readonly value="${configLink}" onclick="this.select()">
      <button class="copybtn" onclick="copyText('${configLink}')">کپی</button>
    </div>
  </div>

  <div class="foot">این لینک را در اختیار دیگران قرار ندهید</div>
</div>
<div class="toast" id="toast">کپی شد</div>
<script>
function copyText(t){
  navigator.clipboard.writeText(t).then(()=>{
    const el = document.getElementById('toast');
    el.classList.add('show');
    setTimeout(()=>el.classList.remove('show'), 1500);
  });
}
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

  const link = buildVlessUri(user.uuid, hostName, user.name);

  // مرورگر → صفحه گرافیکی وضعیت اشتراک
  if (wantsHtml) {
    const subUrl = `${new URL(request.url).origin}/sub/${user.uuid}`;
    const page = subInfoPage(user, subUrl, link, allowed);
    return new Response(page, { status: 200, headers: { 'content-type': 'text/html; charset=utf-8' } });
  }

  // اپلیکیشن VPN → خروجی استاندارد Base64
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

    // لینک سابسکرایشن هر کاربر: /sub/<uuid>  (بدون نیاز به لاگین، فقط با uuid خودش)
    const subMatch = url.pathname.match(/^\/sub\/([0-9a-f-]{36})$/);
    if (subMatch) {
      return handleSubscription(request, env, subMatch[1]);
    }

    // API مدیریتی
    if (url.pathname.startsWith('/api/')) {
      return handleApi(request, env, url);
    }

    // صفحه داشبورد و هر فایل استاتیک دیگر (از پوشه public/ سرو می‌شود)
    return env.ASSETS.fetch(request);
  },
};
