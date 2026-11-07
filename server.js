const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const { encode, decode } = require('@msgpack/msgpack');

const ROOT = __dirname;
let cfg = {};
try { cfg = JSON.parse(fs.readFileSync(path.join(ROOT, 'config.json'), 'utf8')); } catch (e) { cfg = {}; }
cfg.ID_BASE = cfg.ID_BASE || 'https://api.azhkthg1.net/id/';
cfg.WS_BASE = cfg.WS_BASE || 'wss://websocket.azhkthg1.net/wsbinary';
if (process.env.REFRESH_TOKEN) cfg.REFRESH_TOKEN = process.env.REFRESH_TOKEN;
if (process.env.ID_BASE) cfg.ID_BASE = process.env.ID_BASE;
if (process.env.WS_BASE) cfg.WS_BASE = process.env.WS_BASE;
if (process.env.PORT) cfg.PORT = process.env.PORT;
if (!cfg.REFRESH_TOKEN) console.error('WARN: thiếu REFRESH_TOKEN (đặt biến môi trường hoặc config.json)');
const DB_FILE = path.join(ROOT, 'sessions.json');
const BET_LEN = 50;
const GAP_LEN = 20;
const WATCHDOG_MS = 15000;

let sessions = {};
try { sessions = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch (e) { sessions = {}; }
let saveTimer = null;
function saveDB() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; fs.writeFile(DB_FILE, JSON.stringify(sessions), () => {}); }, 800);
}

let round = null;
const sse = new Set();
let lastMsgAt = 0;
let lastLogin = null;

function resultOf(d1, d2, d3) {
  const total = d1 + d2 + d3;
  return { total, result: total <= 10 ? 'XIU' : 'TAI' };
}

function broadcast(type, data) {
  const msg = 'data: ' + JSON.stringify({ type, data }) + '\n\n';
  for (const res of sse) { try { res.write(msg); } catch (e) {} }
}

function pushHtr(htr) {
  const added = [];
  for (const s of htr) {
    if (s && s.sid != null && s.d1 != null && sessions[s.sid] == null) {
      const r = resultOf(s.d1, s.d2, s.d3);
      sessions[s.sid] = { sid: s.sid, d1: s.d1, d2: s.d2, d3: s.d3, total: r.total, result: r.result, ts: Date.now() };
      added.push(sessions[s.sid]);
    }
  }
  if (!added.length) return;
  added.sort((a, b) => a.sid - b.sid);
  saveDB();
  for (const s of added) {
    console.log('[RESULT] ' + s.sid + '  ' + s.d1 + ',' + s.d2 + ',' + s.d3 + '  =' + s.total + '  ' + s.result);
    broadcast('result', s);
  }
}

function addSession(sid, d1, d2, d3) {
  if (sid == null || d1 == null) return;
  if (sessions[sid] != null) return;
  const r = resultOf(d1, d2, d3);
  sessions[sid] = { sid, d1, d2, d3, total: r.total, result: r.result, ts: Date.now() };
  saveDB();
  console.log('[RESULT] ' + sid + '  ' + d1 + ',' + d2 + ',' + d3 + '  =' + r.total + '  ' + r.result);
  broadcast('result', sessions[sid]);
}

function handleMsg(raw) {
  lastMsgAt = Date.now();
  let a;
  try { a = decode(raw instanceof Uint8Array ? raw : new Uint8Array(raw)); } catch (e) { return; }
  if (!Array.isArray(a) || !a[1] || typeof a[1] !== 'object') return;
  const o = a[1];
  const now = Date.now();
  if (o.cmd === 1002) {
    round = { sid: o.sid, openTs: now, revealTs: null, dice: null, total: null, result: null };
    console.log('[OPEN]  ' + o.sid);
    broadcast('open', round);
  } else if ((o.cmd === 1003 || o.cmd === 1004) && o.d1 !== undefined) {
    const cur = round && round.sid != null ? round.sid : null;
    if (cur != null) {
      const r = resultOf(o.d1, o.d2, o.d3);
      round = Object.assign({}, round, { revealTs: now, dice: [o.d1, o.d2, o.d3], total: r.total, result: r.result });
      addSession(cur, o.d1, o.d2, o.d3);
      broadcast('reveal', round);
    }
  } else if (Array.isArray(o.htr)) {
    pushHtr(o.htr);
  }
}

async function refreshAccessToken() {
  const u = cfg.ID_BASE + '?command=refreshToken&refreshToken=' + encodeURIComponent(cfg.REFRESH_TOKEN);
  const j = await (await fetch(u)).json();
  if (j.status !== 0 || !j.data || !j.data.accessToken) throw new Error('refreshToken failed: ' + JSON.stringify(j).slice(0, 200));
  return j.data.accessToken;
}

async function getFreshLogin() {
  const accessToken = await refreshAccessToken();
  const res = await fetch(cfg.ID_BASE + '?command=loginAccessToken', { headers: { authorization: accessToken } });
  const j = await res.json();
  if (j.status !== 0 || !j.data) throw new Error('loginAccessToken failed: ' + JSON.stringify(j).slice(0, 200));
  const d = j.data;
  return {
    wsToken: d.wsToken,
    info: JSON.stringify(d.info),
    signature: d.signature,
    expireIn: d.expireIn || null,
  };
}

function buildUrl(wsToken) {
  return cfg.WS_BASE + '?token=' + wsToken;
}

function send(obj) { try { ws.send(Buffer.from(encode(obj))); } catch (e) {} }

let ws = null;
let pollTimer = null;
let watchdog = null;
let reconnectTimer = null;
let connecting = false;

async function connect() {
  if (connecting) return;
  connecting = true;
  clearInterval(pollTimer);
  clearInterval(watchdog);
  clearTimeout(reconnectTimer);
  try {
    console.log('Fetching fresh login...');
    lastLogin = await getFreshLogin();
    console.log('Login OK (wsToken ' + lastLogin.wsToken.length + 'b, exp ' + lastLogin.expireIn + ')');
  } catch (e) {
    console.log('Login fetch failed: ' + e.message + ' -> retry 5s');
    connecting = false;
    reconnectTimer = setTimeout(connect, 5000);
    return;
  }

  const url = buildUrl(lastLogin.wsToken);
  ws = new WebSocket(url, { handshakeTimeout: 20000 });
  ws.binaryType = 'arraybuffer';

  ws.on('open', () => {
    lastMsgAt = Date.now();
    console.log('WS open -> login');
    send([1, 'MiniGame', '', '', { info: lastLogin.info, signature: lastLogin.signature }]);
    clearInterval(pollTimer);
    pollTimer = setInterval(() => { send([6, 'MiniGame', 'taixiuPlugin', { cmd: 1005 }]); }, 2000);
    setTimeout(() => send([6, 'MiniGame', 'taixiuPlugin', { cmd: 1005 }]), 300);
    clearInterval(watchdog);
    watchdog = setInterval(() => {
      if (Date.now() - lastMsgAt > WATCHDOG_MS) {
        console.log('WATCHDOG: no data ' + Math.round((Date.now() - lastMsgAt) / 1000) + 's -> reconnect');
        try { ws.terminate(); } catch (e) {}
      }
    }, 5000);
    connecting = false;
  });

  ws.on('message', (data) => handleMsg(data));

  ws.on('close', () => {
    console.log('WS closed -> reconnect 3s (fresh login)');
    clearInterval(pollTimer);
    clearInterval(watchdog);
    ws = null;
    connecting = false;
    reconnectTimer = setTimeout(connect, 3000);
  });

  ws.on('error', (e) => { console.log('WS error: ' + e.message); try { ws.terminate(); } catch (e2) {} });
}

function calcStats() {
  const arr = Object.values(sessions).sort((a, b) => a.sid - b.sid);
  const seq = arr.map((s) => s.result);
  const last100 = arr.slice(-100);
  const last60 = arr.slice(-60);
  const last50 = arr.slice(-50);

  const countsOf = (a) => ({ tai: a.filter((s) => s.result === 'TAI').length, xiu: a.filter((s) => s.result === 'XIU').length });

  let curType = null, curLen = 0;
  for (let i = seq.length - 1; i >= 0; i--) {
    if (curType === null) { curType = seq[i]; curLen = 1; }
    else if (seq[i] === curType) curLen++;
    else break;
  }

  let maxT = 0, maxX = 0, rt = 0, rx = 0;
  for (const r of seq) {
    if (r === 'TAI') { rt++; rx = 0; if (rt > maxT) maxT = rt; }
    else { rx++; rt = 0; if (rx > maxX) maxX = rx; }
  }

  const dist = {};
  for (let s = 3; s <= 18; s++) dist[s] = 0;
  for (const s of last100) if (s.total >= 3 && s.total <= 18) dist[s.total]++;

  let bal = 0;
  const balance = last60.map((s) => { bal += (s.result === 'TAI' ? 1 : -1); return bal; });

  const sums = last60.map((s) => s.total);
  const avgLast20 = last60.slice(-20).reduce((a, s) => a + s.total, 0) / Math.max(1, Math.min(20, last60.length));

  return {
    updatedAt: Date.now(),
    totalSessions: arr.length,
    counts: { all: countsOf(arr), last50: countsOf(last50), last100: countsOf(last100) },
    currentStreak: { type: curType === 'TAI' ? 'TAI' : curType === 'XIU' ? 'XIU' : null, length: curLen },
    longest: { tai: maxT, xiu: maxX },
    distribution: dist,
    averageTotal: Number(avgLast20.toFixed(2)),
    sequence: last100.map((s) => (s.result === 'TAI' ? 'T' : 'X')).join(''),
    sums,
    balance,
    recent: last100.slice(-20).reverse().map((s) => ({ sid: s.sid, d: [s.d1, s.d2, s.d3], total: s.total, result: s.result })),
  };
}

function aiSnapshot() {
  const s = state();
  const st = s.stats;
  const c = st.counts.last50;
  const summary =
    'Phá đảo mặt trời — phiên ' + (s.round && s.round.sid != null ? s.round.sid : '?') +
    ' | pha: ' + (s.phase ? s.phase.name : 'N/A') +
    (s.phase && s.phase.secondsLeft != null ? ' (' + s.phase.secondsLeft + 's)' : '') +
    ' | 50 phiên: TÀI ' + c.tai + ' / XỈU ' + c.xiu +
    ' | chuỗi hiện tại: ' + st.currentStreak.length + 'x ' + (st.currentStreak.type || '?') +
    ' | TB tổng: ' + st.averageTotal +
    ' | luật: tổng 3-10 = XỈU, 11-18 = TÀI';
  return {
    ok: true,
    brand: 'Phá đảo mặt trời',
    updatedAt: st.updatedAt,
    rule: 'Tổng 3 xúc xắc 3..10 = XỈU, 11..18 = TÀI',
    connected: s.connected,
    current: s.round ? { sid: s.round.sid, phase: s.phase ? s.phase.name : null, secondsLeft: s.phase ? s.phase.secondsLeft : null, result: s.round.result || null, dice: s.round.dice || null, total: s.round.total || null } : null,
    last: st.recent,
    stats: {
      counts: st.counts,
      currentStreak: st.currentStreak,
      longest: st.longest,
      distribution: st.distribution,
      averageTotal: st.averageTotal,
      sequence: st.sequence,
    },
    note: 'Kết quả là RNG độc lập phía server; không thể dự đoán phiên kế tiếp từ chuỗi trước.',
    summary,
  };
}

function phaseOf() {
  if (!round || round.sid == null) return null;
  const now = Date.now();
  if (round.revealTs == null) {
    const elapsed = (now - round.openTs) / 1000;
    if (elapsed > BET_LEN + 15) return { name: 'WAITING', sid: round.sid, secondsLeft: 0, hint: 'đang đồng bộ...' };
    const rem = Math.max(0, Math.round(BET_LEN - elapsed));
    return { name: 'BETTING', sid: round.sid, secondsLeft: rem, hint: 'ket qua sau ~' + rem + 's' };
  }
  const g = (now - round.revealTs) / 1000;
  const rem = Math.max(0, Math.round(GAP_LEN - g));
  return { name: 'SETTLE', sid: round.sid, result: round.result, dice: round.dice, secondsLeft: rem, hint: 'phien moi sau ~' + rem + 's' };
}

function state() {
  const arr = Object.values(sessions).sort((a, b) => a.sid - b.sid);
  const T = arr.filter((s) => s.result === 'TAI').length;
  return {
    connected: !!(ws && ws.readyState === 1) && (Date.now() - lastMsgAt < WATCHDOG_MS),
    lastMsgAge: lastMsgAt ? Math.round((Date.now() - lastMsgAt) / 1000) : null,
    count: arr.length,
    tai: T,
    xiu: arr.length - T,
    round,
    phase: phaseOf(),
    latest: arr.slice(-30),
    stats: calcStats(),
  };
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (url.pathname === '/api/state') { res.setHeader('content-type', 'application/json'); return res.end(JSON.stringify(state())); }
  if (url.pathname === '/api/ai' || url.pathname === '/api/agent') { res.setHeader('content-type', 'application/json'); return res.end(JSON.stringify(aiSnapshot())); }
  if (url.pathname === '/api/latest') { const arr = Object.values(sessions).sort((a, b) => a.sid - b.sid); res.setHeader('content-type', 'application/json'); return res.end(JSON.stringify(arr[arr.length - 1] || null)); }
  if (url.pathname === '/api/sessions') { const limit = Math.min(2000, +(url.searchParams.get('limit') || 100)); const arr = Object.values(sessions).sort((a, b) => a.sid - b.sid).slice(-limit); res.setHeader('content-type', 'application/json'); return res.end(JSON.stringify(arr)); }
  if (url.pathname === '/events') {
    res.setHeader('content-type', 'text/event-stream'); res.setHeader('cache-control', 'no-cache'); res.setHeader('connection', 'keep-alive');
    res.write('data: ' + JSON.stringify({ type: 'hello', data: state() }) + '\n\n');
    sse.add(res);
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 15000);
    req.on('close', () => { clearInterval(ping); sse.delete(res); });
    return;
  }
  let p = url.pathname === '/' ? '/index.html' : url.pathname;
  const rel = p.replace(/^\/+/, '');
  const candidates = [path.join(ROOT, 'public', rel), path.join(ROOT, rel)];
  const serveNext = (i) => {
    if (i >= candidates.length) { res.statusCode = 404; return res.end('not found'); }
    fs.readFile(candidates[i], (err, data) => {
      if (err) return serveNext(i + 1);
      const ext = path.extname(candidates[i]);
      res.setHeader('content-type', ext === '.html' ? 'text/html; charset=utf-8' : ext === '.js' ? 'text/javascript' : 'text/plain');
      res.end(data);
    });
  };
  serveNext(0);
});
const PORT = cfg.PORT || 3000;
server.listen(PORT, () => console.log('HTTP listening on http://localhost:' + PORT));

connect();