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
    if (raw) users.push(JSON.parse(raw));
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

// شمارنده مصرف: برای جلوگیری از نوشتن مکرر روی KV (که نرخ محدود دارد)
// مصرف هر اتصال را در حافظه جمع می‌زنیم و فقط در پایان اتصال (یا هر چند ثانیه) در KV ذخیره می‌کنیم.
async function flushUsage(env, uuid, bytesDelta) {
  if (bytesDelta <= 0) return;
  const user = await getUser(env, uuid);
  if (!user) return;
  user.trafficUsedBytes = (user.trafficUsedBytes || 0) + bytesDelta;
  await saveUser(env, user);
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

  const flushInterval = setInterval(() => {
    if (userUuid && usageBytes > 0) {
      const toFlush = usageBytes;
      usageBytes = 0;
      flushUsage(env, userUuid, toFlush).catch(() => {});
    }
  }, 5000);

  const closeAll = () => {
    clearInterval(flushInterval);
    if (userUuid && usageBytes > 0) {
      flushUsage(env, userUuid, usageBytes).catch(() => {});
      usageBytes = 0;
    }
    safeCloseWebSocket(webSocket);
    try { remoteSocket && remoteSocket.close(); } catch (e) {}
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
    const stats = await getWorkerStats(env);
    return jsonResponse({ ok: true, stats });
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

function buildVlessLink(uuid, hostName, name) {
  // VLESS + WebSocket + TLS روی پورت 443 (استاندارد برای عبور از فیلترینگ)
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


// =============================================================
// روتر اصلی
// =============================================================

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

    // API مدیریتی
    if (url.pathname.startsWith('/api/')) {
      return handleApi(request, env, url);
    }

    // صفحه داشبورد و هر فایل استاتیک دیگر (از پوشه public/ سرو می‌شود)
    return env.ASSETS.fetch(request);
  },
};
