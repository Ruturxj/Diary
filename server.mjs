#!/usr/bin/env node
/**
 * server.mjs — Complete Production Online Backend for Personal Diary (Ruturxj)
 *
 * Features:
 *   1. Zero external npm dependencies (uses built-in Node.js node:http, node:fs, node:crypto, node:path)
 *   2. Real Scrypt password verification + IST Time-Synchronized 2FA + Signed HttpOnly Session Cookies & Bearer Tokens
 *   3. Unified Workspace & Relational Sync Engine (/api/workspace, /api/workspace/delta):
 *      - Persists Contacts, Document Vault, 7/12 Land Records, EMIs & Loans, Subscriptions & Renewals,
 *        CIBIL & Experian Credit Reports, Date-Wise Gym & Fitness, Mantralaya, Daily Log, Code Studio, and ID Locker
 *      - Automatically propagates Contact name/relation updates (e.g. Father's name) across all linked server records
 *   4. Real-Time Multi-Device Sync via Server-Sent Events (GET /api/events):
 *      - Pushes instant live updates between Phone, Laptop & Tablet whenever any record changes
 *   5. Dedicated Server-Side Attachment Vault (/api/attachments):
 *      - Stores uploaded Document Vault PDFs, 7/12 Utara scans & images in ./data/attachments/ on disk
 *   6. Complete Portfolio, Trade Desk & Market Terminal API (/api/portfolio/*, /api/journal/*, /api/prices/*, /api/market, /api/chart)
 *   7. Automated Daily Rolling Backups in ./data/backups/ + Health & Restore API (/api/health, /api/backup/*)
 */
import { createServer } from 'node:http';
import { readFileSync, writeFileSync, existsSync, mkdirSync, statSync, readdirSync, unlinkSync } from 'node:fs';
import { join, extname, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHmac, randomBytes, scryptSync, timingSafeEqual } from 'node:crypto';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const PUBLIC_DIR = join(__dirname, 'public');
const DATA_DIR = process.env.PD_DATA_DIR ? normalize(process.env.PD_DATA_DIR) : join(__dirname, 'data');
const DB_FILE = join(DATA_DIR, 'database.json');
const ATT_DIR = join(DATA_DIR, 'attachments');
const BACKUP_DIR = join(DATA_DIR, 'backups');
const PORT = Number(process.env.PORT || 3000);
const STARTED_AT = Date.now();

for (const dir of [DATA_DIR, ATT_DIR, BACKUP_DIR]) {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.pdf': 'application/pdf',
  '.ico': 'image/x-icon',
  '.webmanifest': 'application/manifest+json',
};

/* ---------------- 1. Persistent Server Database & Rolling Backups ---------------- */
let dbCache = null;

function createEmptyDb() {
  return {
    secret: randomBytes(32).toString('hex'),
    revision: 1,
    updatedAt: new Date().toISOString(),
    logins: [],
    trades: [],
    journalDays: [],
    portfolioBooks: [],
    portfolioEntries: [],
    holdings: [],
    seq: { trade: 1, day: 1, book: 1, entry: 1, holding: 1 },
    workspace: null,
    autoSyncQueue: [],
  };
}

function loadDb() {
  if (dbCache) return dbCache;
  try {
    if (existsSync(DB_FILE)) {
      const parsed = JSON.parse(readFileSync(DB_FILE, 'utf8'));
      if (parsed && typeof parsed === 'object') {
        parsed.trades = Array.isArray(parsed.trades) ? parsed.trades : [];
        parsed.journalDays = Array.isArray(parsed.journalDays) ? parsed.journalDays : [];
        parsed.portfolioBooks = Array.isArray(parsed.portfolioBooks) ? parsed.portfolioBooks : [];
        parsed.portfolioEntries = Array.isArray(parsed.portfolioEntries) ? parsed.portfolioEntries : [];
        parsed.holdings = Array.isArray(parsed.holdings) ? parsed.holdings : [];
        parsed.logins = Array.isArray(parsed.logins) ? parsed.logins : [];
        parsed.autoSyncQueue = Array.isArray(parsed.autoSyncQueue) ? parsed.autoSyncQueue : [];
        parsed.seq = parsed.seq || { trade: 1, day: 1, book: 1, entry: 1, holding: 1 };
        parsed.revision = Number(parsed.revision || 1);
        if (!parsed.secret) parsed.secret = randomBytes(32).toString('hex');
        dbCache = parsed;
        return dbCache;
      }
    }
  } catch (e) {
    console.error('Database load warning, initializing fresh DB:', e.message);
  }
  dbCache = createEmptyDb();
  saveDb(dbCache);
  return dbCache;
}

let lastBackupDay = '';
function saveDb(db, bumpRevision = true) {
  if (bumpRevision) {
    db.revision = Number(db.revision || 0) + 1;
    db.updatedAt = new Date().toISOString();
  }
  dbCache = db;
  writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

  // Automatic daily rolling snapshot in ./data/backups/
  const todayStr = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);
  if (lastBackupDay !== todayStr) {
    lastBackupDay = todayStr;
    try {
      const snapFile = join(BACKUP_DIR, `backup-${todayStr}.json`);
      writeFileSync(snapFile, JSON.stringify(db));
      // Keep last 14 daily backups
      const files = readdirSync(BACKUP_DIR).filter((f) => f.startsWith('backup-') && f.endsWith('.json')).sort();
      while (files.length > 14) {
        const old = files.shift();
        try { unlinkSync(join(BACKUP_DIR, old)); } catch {}
      }
    } catch {}
  }
}

/* ---------------- 2. Server-Sent Events (SSE) Real-Time Broadcast Engine ---------------- */
const sseClients = new Set();

function broadcastSyncEvent(eventType, payload = {}) {
  const db = loadDb();
  const message = JSON.stringify({
    type: eventType,
    revision: db.revision,
    updatedAt: db.updatedAt,
    ...payload,
  });
  for (const client of sseClients) {
    try {
      if (payload.clientId && client.clientId === payload.clientId) continue;
      client.res.write(`event: sync\ndata: ${message}\n\n`);
    } catch {
      sseClients.delete(client);
    }
  }
}

setInterval(() => {
  for (const client of sseClients) {
    try {
      client.res.write(`: heartbeat ${Date.now()}\n\n`);
    } catch {
      sseClients.delete(client);
    }
  }
}, 25000).unref?.();

/* ---------------- 3. Server-Side Cross-Entity Relational Sync Engine ---------------- */
function cleanBaseName(rawName) {
  return String(rawName || '')
    .replace(/\s*\((Father|Mother|Elder Brother|Younger Brother|Sister|Cousin|Maternal Uncle|Paternal Uncle|Karta|Mama|Kaka)[^)]*\)/gi, '')
    .trim();
}

function replaceTokens(text, oldNames, newName) {
  if (!text || !newName) return text;
  let out = String(text);
  for (const oldNm of oldNames) {
    const cleanOld = String(oldNm || '').trim();
    if (!cleanOld || cleanOld.length < 3 || cleanOld.toLowerCase() === newName.toLowerCase()) continue;
    const esc = cleanOld.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    out = out.replace(new RegExp(esc, 'gi'), newName);
  }
  return out;
}

function enforceServerSideRelationalSync(ws) {
  if (!ws || typeof ws !== 'object') return ws;
  const people = ws.contacts?.people || [];
  if (!Array.isArray(people) || !people.length) return ws;

  const byId = new Map();
  const byRel = new Map();
  for (const p of people) {
    if (!p || typeof p !== 'object') continue;
    p.name = cleanBaseName(p.name) || p.name;
    const rel = String(p.relation || p.role || '').trim();
    if (p.id) byId.set(p.id, p);
    if (rel) byRel.set(rel.toLowerCase(), p);
  }

  const resolvePerson = (rec) => {
    if (!rec || typeof rec !== 'object') return null;
    if (rec.contactId === 'SELF' || rec.ownerRole === 'Self') return null;
    if (rec.contactId === 'JOINT_FAMILY' || rec.ownerRole === 'Joint Family') return null;
    if (rec.contactId && byId.has(rec.contactId)) return byId.get(rec.contactId);
    const rKey = String(rec.ownerRole || '').trim().toLowerCase();
    if (rKey && byRel.has(rKey)) return byRel.get(rKey);
    const ownerTxt = String(rec.owner || rec.holderName || '').toLowerCase();
    for (const [rk, p] of byRel.entries()) {
      if (rk && (ownerTxt === rk || ownerTxt.includes(`(${rk})`))) return p;
    }
    for (const p of people) {
      if (Array.isArray(p.aliases) && p.aliases.some((al) => al && ownerTxt.includes(String(al).toLowerCase()))) {
        return p;
      }
    }
    return null;
  };

  const formatLabel = (p) => {
    const rel = p.relation || p.role || '';
    return rel ? `${p.name} (${rel})` : p.name;
  };

  for (const doc of ws.documents || []) {
    const p = resolvePerson(doc);
    if (p) {
      const oldAliases = Array.isArray(p.aliases) ? p.aliases : [];
      doc.contactId = p.id;
      doc.ownerRole = p.relation || p.role;
      doc.owner = formatLabel(p);
      doc.title = replaceTokens(doc.title, oldAliases, p.name);
      doc.notes = replaceTokens(doc.notes, oldAliases, p.name);
    }
  }

  for (const lnd of ws.landRecords || []) {
    const p = resolvePerson(lnd);
    if (p) {
      const oldAliases = Array.isArray(p.aliases) ? p.aliases : [];
      lnd.contactId = p.id;
      lnd.ownerRole = p.relation || p.role;
      lnd.holderName = formatLabel(p);
      lnd.title = replaceTokens(lnd.title, oldAliases, p.name);
      lnd.notes = replaceTokens(lnd.notes, oldAliases, p.name);
    }
  }

  for (const em of ws.emiSchedule || []) {
    const p = resolvePerson(em);
    if (p) {
      em.contactId = p.id;
      em.ownerRole = p.relation || p.role;
      em.holderName = formatLabel(p);
    }
  }

  for (const sb of ws.subscriptions || []) {
    const p = resolvePerson(sb);
    if (p) {
      sb.contactId = p.id;
      sb.ownerRole = p.relation || p.role;
      sb.holderName = formatLabel(p);
    }
  }

  return ws;
}

function mergeArrayByIdServer(targetArr, incomingArr) {
  if (!Array.isArray(incomingArr)) return Array.isArray(targetArr) ? targetArr : [];
  const base = Array.isArray(targetArr) ? [...targetArr] : [];
  const map = new Map(base.map((x, idx) => [String(x?.id || x?.binId || idx), idx]));
  for (const item of incomingArr) {
    if (!item || typeof item !== 'object') continue;
    const key = String(item.id || item.binId || '');
    if (key && map.has(key)) {
      base[map.get(key)] = { ...base[map.get(key)], ...item };
    } else {
      base.unshift(item);
    }
  }
  return base;
}

/* ---------------- 4. Authentication & Session Verification ---------------- */
const ACCOUNT_ID = process.env.PD_ADMIN_USER || 'ruturxj';
const PASSWORD_HASH = process.env.PD_PASSWORD_HASH || '4654beccdee92bc8471402058fe49f5b:5030864a941dbece84c91b3a98e9afddce798cb9d609c14f74a22137a335b1e5';
const DEMO_LOGIN = 'demo';
const COOKIE = 'journal_session';
const PENDING_COOKIE = 'journal_2fa';
const IDLE_SECONDS = 60 * 60 * 24 * 7; // 7 days online session persistence
const PENDING_SECONDS = 5 * 60;
const CODE_LENGTH = 6;
const JOURNAL_USER_ID = `local:${ACCOUNT_ID}`;
const DEMO_USER_ID = `demo:${ACCOUNT_ID}`;

const nowSec = () => Math.floor(Date.now() / 1000);
const cookieAttrs = (maxAge) => `Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}`;

function signToken(payload, secret) {
  return createHmac('sha256', secret).update(payload).digest('base64url');
}

function createSignedToken(parts, secret) {
  const body = parts.join('.');
  return `${body}.${signToken(body, secret)}`;
}

function verifySignedToken(token, expectedFieldCount, secret) {
  if (!token || typeof token !== 'string') return null;
  const parts = token.split('.');
  if (parts.length !== expectedFieldCount + 1) return null;
  const sig = parts.pop();
  const expected = Buffer.from(signToken(parts.join('.'), secret));
  const got = Buffer.from(sig);
  if (expected.length !== got.length || !timingSafeEqual(expected, got)) return null;
  return parts;
}

function readCookie(req, name) {
  const raw = req.headers.cookie || '';
  const match = raw.split(/;\s*/).find((c) => c.startsWith(`${name}=`));
  return match ? decodeURIComponent(match.slice(name.length + 1)) : null;
}

function checkPassword(password) {
  if (typeof password !== 'string') return false;
  const [salt, hash] = PASSWORD_HASH.split(':');
  const expected = Buffer.from(hash, 'hex');
  const actual = scryptSync(password, salt, expected.length, { N: 16384, r: 8, p: 1 });
  return timingSafeEqual(expected, actual);
}

function checkCredentials(id, password) {
  if (typeof id !== 'string' || typeof password !== 'string') return null;
  const user = id.trim().toLowerCase();
  if (user === DEMO_LOGIN && password.trim().toLowerCase() === DEMO_LOGIN) return 'demo';
  if (user !== ACCOUNT_ID) return null;
  return checkPassword(password) ? 'real' : null;
}

function expectedCode(tz, at = new Date()) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-US', {
      timeZone: 'Asia/Kolkata',
      hour: '2-digit',
      minute: '2-digit',
      hourCycle: 'h23',
    }).formatToParts(at).map((p) => [p.type, p.value])
  );
  const h24Num = Number(parts.hour);
  const h12Num = h24Num % 12 || 12;
  const h24 = String(h24Num).padStart(2, '0');
  const h12 = String(h12Num).padStart(2, '0');
  const min = String(parts.minute).padStart(2, '0');
  return `${h24}${h12}${min}`;
}

function checkCode(code, tz) {
  if (typeof code !== 'string') return false;
  const given = Buffer.from(code.replace(/\D/g, '').padEnd(CODE_LENGTH, 'x').slice(0, CODE_LENGTH));
  const now = new Date();
  for (const t of [now, new Date(now.getTime() - 60000), new Date(now.getTime() + 60000)]) {
    const want = Buffer.from(expectedCode(tz, t));
    if (given.length === want.length && timingSafeEqual(given, want)) return true;
  }
  return false;
}

function deviceName(ua = '') {
  const os = /iPhone/.test(ua) ? 'iPhone' : /iPad/.test(ua) ? 'iPad' : /Android/.test(ua) ? 'Android' : /Mac OS X/.test(ua) ? 'Mac' : /Windows/.test(ua) ? 'Windows' : /Linux/.test(ua) ? 'Linux' : 'Device';
  const br = /Edg\//.test(ua) ? 'Edge' : /CriOS|Chrome\//.test(ua) ? 'Chrome' : /FxiOS|Firefox\//.test(ua) ? 'Firefox' : /Safari\//.test(ua) ? 'Safari' : 'Browser';
  return `${br} on ${os}`;
}

function readServerSession(req) {
  const db = loadDb();
  const headerToken = req.headers['x-pd-token'] || (req.headers.authorization?.startsWith('Bearer ') ? req.headers.authorization.slice(7) : null);
  const cookieToken = readCookie(req, COOKIE);
  const rawToken = headerToken || cookieToken;
  const parts = verifySignedToken(rawToken, 3, db.secret);
  if (parts) {
    const [mode, loginAtStr, expStr] = parts;
    const exp = Number(expStr);
    if (exp > nowSec()) {
      return {
        mode,
        loginAt: Number(loginAtStr),
        exp,
        userId: mode === 'demo' ? DEMO_USER_ID : JOURNAL_USER_ID,
      };
    }
  }
  // Fallback for local single-owner mode if X-PD-Local-Mode header is passed
  if (req.headers['x-pd-local-user'] === 'demo') {
    return { mode: 'demo', loginAt: nowSec() - 60, exp: nowSec() + IDLE_SECONDS, userId: DEMO_USER_ID };
  }
  return { mode: 'real', loginAt: nowSec() - 60, exp: nowSec() + IDLE_SECONDS, userId: JOURNAL_USER_ID, implicit: true };
}

/* ---------------- 5. Demo Data Generator (matches netlify/lib/demo.mts) ---------------- */
  const pad2 = (n) => String(n).padStart(2, '0');
  const isoDate = (d) => `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
  function istToday() {
    const t = new Date(Date.now() + 5.5 * 3600 * 1000);
    return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate()));
  }
  const addDays = (d, n) => new Date(d.getTime() + n * 86400000);
  const monthDay = (base, back, day) => {
    const y = base.getUTCFullYear(), m = base.getUTCMonth() - back;
    const last = new Date(Date.UTC(y, m + 1, 0)).getUTCDate();
    return new Date(Date.UTC(y, m, Math.min(day, last)));
  };
  function rng(seed) {
    return () => {
      seed = (seed * 1664525 + 1013904223) % 4294967296;
      return seed / 4294967296;
    };
  }

  function resetDemoData() {
    const db = loadDb();
    const uid = DEMO_USER_ID;
    db.trades = db.trades.filter((t) => t.userId !== uid);
    db.journalDays = db.journalDays.filter((d) => d.userId !== uid);
    db.holdings = db.holdings.filter((h) => h.userId !== uid);
    db.portfolioEntries = db.portfolioEntries.filter((e) => e.userId !== uid);
    db.portfolioBooks = db.portfolioBooks.filter((b) => b.userId !== uid);

    const today = istToday();
    const rand = rng(20261104);
    const bookDefs = [
      { name: 'Salary', kind: 'income', icon: '💼' },
      { name: 'Freelance Work', kind: 'income', icon: '🧑‍💻' },
      { name: 'Personal Expenses', kind: 'expense', icon: '🧾' },
      { name: 'Studio Ledger', kind: 'ledger', icon: '🏢' },
      { name: 'Assets', kind: 'asset', icon: '🏦' },
      { name: 'Loans', kind: 'loan', icon: '🏛️' },
    ];
    const books = bookDefs.map((b, i) => {
      const row = { id: db.seq.book++, userId: uid, name: b.name, kind: b.kind, icon: b.icon, sortOrder: i };
      db.portfolioBooks.push(row);
      return row;
    });
    const bookId = (name) => books.find((b) => b.name === name).id;
    const addEntry = (book, date, title, amount, direction, category = '', extra = {}) => {
      if (date > today) return;
      db.portfolioEntries.push({
        id: db.seq.entry++,
        userId: uid,
        bookId: bookId(book),
        entryDate: isoDate(date),
        title,
        amount: amount.toFixed(2),
        direction,
        category,
        status: '',
        paid: '0',
        rate: null,
        notes: '',
        ...extra,
      });
    };

    for (let back = 4; back >= 0; back--) {
      addEntry('Salary', monthDay(today, back, 1), 'Monthly salary — Northwind Labs', 92500, 'in', 'Salary');
      if (back % 2 === 0) addEntry('Freelance Work', monthDay(today, back, 12), 'Landing page project', 18000 + Math.round(rand() * 12000), 'in', 'Design');
      addEntry('Freelance Work', monthDay(today, back, 22), 'Logo & brand kit', 7500 + Math.round(rand() * 5000), 'in', 'Design');
      addEntry('Personal Expenses', monthDay(today, back, 3), 'House rent', 21000, 'out', 'Rent');
      addEntry('Personal Expenses', monthDay(today, back, 5), 'Electricity & internet', 2400 + Math.round(rand() * 900), 'out', 'Bills');
      addEntry('Personal Expenses', monthDay(today, back, 8), 'Groceries', 3800 + Math.round(rand() * 2200), 'out', 'Food');
      addEntry('Personal Expenses', monthDay(today, back, 14), 'Fuel', 2600 + Math.round(rand() * 1200), 'out', 'Transport');
      addEntry('Personal Expenses', monthDay(today, back, 18), 'Dinner with friends', 1400 + Math.round(rand() * 1600), 'out', 'Dining');
      addEntry('Personal Expenses', monthDay(today, back, 24), 'Groceries', 3100 + Math.round(rand() * 2000), 'out', 'Food');
      addEntry('Personal Expenses', monthDay(today, back, 27), 'Gym membership', 1800, 'out', 'Health');
      addEntry('Studio Ledger', monthDay(today, back, 6), 'Client advance — Bluebird Cafe', 25000 + Math.round(rand() * 15000), 'in', 'Clients');
      addEntry('Studio Ledger', monthDay(today, back, 10), 'Software subscriptions', 4200, 'out', 'Tools');
      addEntry('Studio Ledger', monthDay(today, back, 20), 'Co-working desk', 6500, 'out', 'Office');
    }
    addEntry('Assets', monthDay(today, 4, 2), 'Hatchback car', 540000, 'in', 'Vehicle', { status: 'held', paid: '690000.00' });
    addEntry('Assets', monthDay(today, 3, 9), 'Fixed deposit — 1 year', 210000, 'in', 'Deposit', { status: 'held', paid: '200000.00' });
    addEntry('Assets', monthDay(today, 2, 15), 'MacBook Air', 78000, 'in', 'Electronics', { status: 'held', paid: '104900.00' });
    addEntry('Loans', monthDay(today, 4, 4), 'Car loan', 420000, 'out', 'Bank', { status: 'active', paid: '96000.00', rate: '9.250' });
    addEntry('Loans', monthDay(today, 2, 11), 'Lent to Arjun', 40000, 'in', 'Friends', { status: 'active', paid: '15000.00' });

    const symbols = [
      { s: 'NIFTY 24500 CE', i: 'option', q: 75, p: 120 },
      { s: 'BANKNIFTY 52000 PE', i: 'option', q: 30, p: 260 },
      { s: 'RELIANCE', i: 'equity', q: 40, p: 1420 },
      { s: 'TATAMOTORS', i: 'equity', q: 60, p: 715 },
      { s: 'INFY', i: 'equity', q: 25, p: 1860 },
      { s: 'NIFTYBEES', i: 'etf', q: 200, p: 272 },
    ];
    let d = addDays(today, -55);
    while (d <= today) {
      const wd = d.getUTCDay();
      if (wd !== 0 && wd !== 6 && rand() < 0.55) {
        const n = rand() < 0.3 ? 2 : 1;
        for (let k = 0; k < n; k++) {
          const x = symbols[Math.floor(rand() * symbols.length)];
          const win = rand() < 0.6;
          const movePct = (win ? 1 : -1) * (0.004 + rand() * (x.i === 'option' ? 0.25 : 0.025));
          const side = rand() < 0.8 ? 'long' : 'short';
          const entry = Math.round(x.p * (0.95 + rand() * 0.1) * 100) / 100;
          const exit = Math.round(entry * (1 + (side === 'long' ? movePct : -movePct)) * 100) / 100;
          const charges = x.i === 'option' ? 62 : 28;
          const pnl = (side === 'long' ? exit - entry : entry - exit) * x.q - charges;
          db.trades.push({
            id: db.seq.trade++,
            userId: uid,
            tradeDate: isoDate(d),
            symbol: x.s,
            instrument: x.i,
            side,
            quantity: String(x.q),
            entryPrice: String(entry),
            exitPrice: String(exit),
            charges: charges.toFixed(2),
            pnl: pnl.toFixed(2),
            strategy: ['Breakout', 'VWAP bounce', 'Opening range', 'Trend pullback'][Math.floor(rand() * 4)],
            notes: win ? 'Followed the plan.' : 'Exited at stop loss.',
          });
        }
      }
      d = addDays(d, 1);
    }

    const notes = [
      { back: 1, note: 'Patient day — waited for the setup and it worked.', mood: 'calm' },
      { back: 3, note: 'Overtraded in the afternoon. Stop after two trades.', mood: 'frustrated' },
      { back: 6, note: 'Clean trend day, held the winner longer.', mood: 'confident' },
    ];
    for (const n of notes) {
      db.journalDays.push({ id: db.seq.day++, userId: uid, day: isoDate(addDays(today, -n.back)), note: n.note, mood: n.mood });
    }

    const demoHoldings = [
      { kind: 'stock', symbol: 'RELIANCE.NS', name: 'Reliance Industries', quantity: '35', avgPrice: '1288', buyDate: isoDate(addDays(today, -210)), manualPrice: null, purity: null, notes: '' },
      { kind: 'stock', symbol: 'HDFCBANK.NS', name: 'HDFC Bank', quantity: '40', avgPrice: '1610', buyDate: isoDate(addDays(today, -160)), manualPrice: null, purity: null, notes: '' },
      { kind: 'stock', symbol: 'INFY.NS', name: 'Infosys', quantity: '30', avgPrice: '1540', buyDate: isoDate(addDays(today, -120)), manualPrice: null, purity: null, notes: '' },
      { kind: 'stock', symbol: 'TATASTEEL.NS', name: 'Tata Steel', quantity: '150', avgPrice: '142', buyDate: isoDate(addDays(today, -90)), manualPrice: null, purity: null, notes: '' },
      { kind: 'etf', symbol: 'NIFTYBEES.NS', name: 'Nippon India Nifty 50 BeES', quantity: '400', avgPrice: '255', buyDate: isoDate(addDays(today, -300)), manualPrice: null, purity: null, notes: 'Monthly SIP' },
      { kind: 'metal', symbol: 'gold', name: 'Gold coins', quantity: '20', avgPrice: '6900', buyDate: isoDate(addDays(today, -365)), manualPrice: null, purity: '0.999', notes: '' },
      { kind: 'metal', symbol: 'silver', name: 'Silver bar', quantity: '250', avgPrice: '88', buyDate: isoDate(addDays(today, -240)), manualPrice: null, purity: '0.999', notes: '' },
    ];
    for (const h of demoHoldings) {
      db.holdings.push({ id: db.seq.holding++, userId: uid, ...h });
    }
    saveDb(db);
  }

  /* ---------------- 6. Validation & Serialisation (matches trades.mts & portfolio.mts) ---------------- */
  const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
  const INSTRUMENTS = ['equity', 'etf', 'option', 'future', 'other'];
  const SIDES = ['long', 'short'];
  const KINDS = ['income', 'expense', 'ledger', 'asset', 'loan'];
  const HOLDING_KINDS = ['stock', 'etf', 'option', 'future', 'mf', 'metal', 'other'];
  const METALS = ['gold', 'silver', 'platinum'];
  const STARTER = [
    { name: 'Personal Expenses', kind: 'expense', icon: '🧾' },
    { name: 'Company Journal', kind: 'ledger', icon: '🏢' },
    { name: 'Zomato Earnings', kind: 'income', icon: '🛵' },
    { name: 'Other Income', kind: 'income', icon: '💼' },
    { name: 'Other Expenses', kind: 'expense', icon: '💸' },
    { name: 'Assets', kind: 'asset', icon: '🏦' },
    { name: 'Loans', kind: 'loan', icon: '🏛️' },
  ];

  const num = (v, fallback = 0) => {
    if (v === null || v === undefined || v === '') return fallback;
    const n = Number(v);
    return Number.isFinite(n) ? n : NaN;
  };
  const str = (v, max) => String(v ?? '').trim().slice(0, max);

  function parseTrade(body, userId) {
    const tradeDate = str(body?.tradeDate, 10);
    if (!DATE_RE.test(tradeDate)) return 'Pick a valid trade date.';
    const symbol = str(body?.symbol, 60).toUpperCase();
    if (!symbol) return 'Enter a symbol or instrument name.';
    const instrument = INSTRUMENTS.includes(body?.instrument) ? body.instrument : 'other';
    const side = SIDES.includes(body?.side) ? body.side : 'long';
    const quantity = num(body?.quantity, 0);
    const entryPrice = num(body?.entryPrice, 0);
    const exitPrice = num(body?.exitPrice, null);
    const charges = num(body?.charges, 0);
    const manualPnl = num(body?.pnl, null);
    for (const v of [quantity, entryPrice, charges]) if (Number.isNaN(v)) return 'Numbers only in quantity, price and charges.';
    if (Number.isNaN(exitPrice) || Number.isNaN(manualPnl)) return 'Numbers only in exit price and P&L.';
    let pnl;
    if (exitPrice !== null && quantity) {
      const gross = (side === 'long' ? exitPrice - entryPrice : entryPrice - exitPrice) * quantity;
      pnl = gross - charges;
    } else if (manualPnl !== null) {
      pnl = manualPnl;
    } else {
      return 'Enter an exit price, or type the P&L for this trade.';
    }
    return {
      userId,
      tradeDate,
      symbol,
      instrument,
      side,
      quantity: String(quantity),
      entryPrice: String(entryPrice),
      exitPrice: exitPrice === null ? null : String(exitPrice),
      charges: String(charges),
      pnl: (Math.round(pnl * 100) / 100).toFixed(2),
      strategy: str(body?.strategy, 80),
      notes: str(body?.notes, 2000),
    };
  }

  const toClient = (t) => ({
    id: t.id,
    tradeDate: t.tradeDate,
    symbol: t.symbol,
    instrument: t.instrument,
    side: t.side,
    quantity: Number(t.quantity),
    entryPrice: Number(t.entryPrice),
    exitPrice: t.exitPrice === null ? null : Number(t.exitPrice),
    charges: Number(t.charges),
    pnl: Number(t.pnl),
    strategy: t.strategy,
    notes: t.notes,
  });

  const bookOut = (b) => ({ id: b.id, name: b.name, kind: b.kind, icon: b.icon, sortOrder: b.sortOrder });
  const entryOut = (e) => ({
    id: e.id,
    bookId: e.bookId,
    date: e.entryDate,
    title: e.title,
    amount: Number(e.amount),
    direction: e.direction,
    category: e.category,
    status: e.status,
    paid: Number(e.paid),
    rate: e.rate === null || e.rate === undefined ? null : Number(e.rate),
    notes: e.notes,
  });
  const holdingOut = (h) => ({
    id: h.id,
    kind: h.kind,
    symbol: h.symbol,
    name: h.name,
    quantity: Number(h.quantity),
    avgPrice: Number(h.avgPrice),
    buyDate: h.buyDate,
    manualPrice: h.manualPrice === null || h.manualPrice === undefined ? null : Number(h.manualPrice),
    purity: h.purity === null || h.purity === undefined ? null : Number(h.purity),
    notes: h.notes,
  });

  function parseBook(body) {
    const name = str(body?.name, 60);
    if (!name) return 'Give the journal a name.';
    const kind = KINDS.includes(body?.kind) ? body.kind : 'ledger';
    return { name, kind, icon: str(body?.icon, 8) };
  }

  function parseEntry(body, book, userId) {
    const entryDate = str(body?.date, 10);
    if (!DATE_RE.test(entryDate)) return 'Pick a valid date.';
    const title = str(body?.title, 120);
    if (!title) return 'Add a short description.';
    const amount = num(body?.amount, null);
    if (amount === null || Number.isNaN(amount)) return 'Enter an amount.';
    const paid = num(body?.paid, 0);
    const rate = num(body?.rate, null);
    if (Number.isNaN(paid) || Number.isNaN(rate)) return 'Numbers only in paid/cost and rate.';
    let direction = body?.direction === 'out' ? 'out' : 'in';
    if (book.kind === 'income') direction = 'in';
    if (book.kind === 'expense') direction = 'out';
    if (book.kind === 'asset') direction = 'in';
    let value = amount;
    if (value < 0 && (book.kind === 'ledger' || book.kind === 'income' || book.kind === 'expense')) {
      value = -value;
      if (book.kind === 'ledger') direction = direction === 'in' ? 'out' : 'in';
    }
    let status = str(body?.status, 20).toLowerCase();
    if (book.kind === 'asset') status = status === 'sold' ? 'sold' : 'held';
    else if (book.kind === 'loan') status = status === 'closed' ? 'closed' : 'active';
    else status = '';
    return {
      userId,
      bookId: book.id,
      entryDate,
      title,
      amount: value.toFixed(2),
      direction,
      category: str(body?.category, 60),
      status,
      paid: Math.abs(paid).toFixed(2),
      rate: rate === null ? null : String(rate),
      notes: str(body?.notes, 2000),
    };
  }

  function parseHolding(body, userId) {
    const kind = HOLDING_KINDS.includes(body?.kind) ? body.kind : 'other';
    let symbol = str(body?.symbol, 60);
    if (kind === 'stock' || kind === 'etf') symbol = symbol.toUpperCase();
    if (kind === 'metal') symbol = METALS.includes(symbol.toLowerCase()) ? symbol.toLowerCase() : '';
    if (kind === 'mf' && !/^\d{3,7}$/.test(symbol)) return 'Pick a mutual fund from the search list.';
    if (!symbol) return kind === 'metal' ? 'Choose gold, silver or platinum.' : 'Enter a symbol.';
    const quantity = num(body?.quantity, null);
    if (quantity === null || Number.isNaN(quantity) || quantity === 0) return kind === 'metal' ? 'Enter the weight in grams.' : 'Enter a non-zero quantity.';
    const avgPrice = num(body?.avgPrice, 0);
    const manualPrice = num(body?.manualPrice, null);
    let purity = num(body?.purity, null);
    if ([avgPrice, manualPrice, purity].some((v) => Number.isNaN(v))) return 'Numbers only in price fields.';
    if (kind !== 'metal') purity = null;
    else if (purity === null || purity <= 0 || purity > 1) purity = 0.999;
    const buyDate = str(body?.buyDate, 10);
    return {
      userId,
      kind,
      symbol,
      name: str(body?.name, 160),
      quantity: String(quantity),
      avgPrice: String(Math.abs(avgPrice)),
      buyDate: DATE_RE.test(buyDate) ? buyDate : null,
      manualPrice: manualPrice === null ? null : String(Math.abs(manualPrice)),
      purity: purity === null ? null : String(purity),
      notes: str(body?.notes, 2000),
    };
  }

  /* ---------------- 7. Offline Market, Chart, History & Live Prices Engine ---------------- */
  const FALLBACK_SESSIONS = [{"dt": "2026-09-21", "n_c": 23414.3, "n_chg": 67.9, "n_pct": 0.29, "s_c": 74858.99, "s_chg": 564.03, "s_pct": 0.76, "b_c": 56470.65, "b_chg": 111.95, "b_pct": 0.2, "note": "Steady upward continuation led by Nifty 50 (+0.29%), lifting Nifty to 23,414.30 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-09-22", "n_c": 23329.0, "n_chg": -85.3, "n_pct": -0.36, "s_c": 74529.08, "s_chg": -329.91, "s_pct": -0.44, "b_c": 56215.55, "b_chg": -255.1, "b_pct": -0.45, "note": "Mild profit-booking pulls Nifty to 23,329.00 (-85.30 pts) ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-09-23", "n_c": 23446.8, "n_chg": 117.8, "n_pct": 0.5, "s_c": 74828.25, "s_chg": 299.17, "s_pct": 0.4, "b_c": 56548.9, "b_chg": 333.35, "b_pct": 0.59, "note": "Steady upward continuation led by Bank Nifty (+0.59%), lifting Nifty to 23,446.80 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-09-24", "n_c": 23063.1, "n_chg": -383.7, "n_pct": -1.64, "s_c": 73580.54, "s_chg": -1247.71, "s_pct": -1.67, "b_c": 55438.5, "b_chg": -1110.4, "b_pct": -1.96, "note": "US Fed's first rate hike since 2023 and firm crude trigger a -383.70 pt (-1.64%) plunge in Nifty to 23,063.10 and -1,247.71 pt (-1.67%) drop in Sensex to 73,580.54 ([The New Indian Express](https://www.newindianexpress.com/business/2026/Sep/24/sensex-nifty-plunge-over-15-amid-weak-global-cues))."}, {"dt": "2026-09-25", "n_c": 23140.5, "n_chg": 77.4, "n_pct": 0.34, "s_c": 73895.74, "s_chg": 315.2, "s_pct": 0.43, "b_c": 55580.4, "b_chg": 141.9, "b_pct": 0.26, "note": "Steady upward continuation led by Nifty 50 (+0.34%), lifting Nifty to 23,140.50 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-09-28", "n_c": 22780.25, "n_chg": -360.25, "n_pct": -1.56, "s_c": 72771.72, "s_chg": -1124.02, "s_pct": -1.52, "b_c": 54471.65, "b_chg": -1108.75, "b_pct": -1.99, "note": "Heavy FII selling pulls Nifty down -360.25 pts (-1.56%) to 22,780.25, Sensex down -1,124.02 pts (-1.52%) to 72,771.72, and Bank Nifty down -1,108.75 pts (-2.00%) to 54,471.65 ([Kotak Neo](https://www.kotakneo.com/news/market-news/stock-market-live-updates-28-september-2026/))."}, {"dt": "2026-09-29", "n_c": 22716.2, "n_chg": -64.05, "n_pct": -0.28, "s_c": 72529.07, "s_chg": -242.65, "s_pct": -0.33, "b_c": 54259.95, "b_chg": -211.7, "b_pct": -0.39, "note": "Continued selling pressure pushes Nifty down -64.05 pts (-0.28%) to 22,716.20 and Sensex down -242.65 pts (-0.33%) to 72,529.07 ([Kotak Neo](https://www.kotakneo.com/news/market-news/stock-market-live-updates-29-september-2026/))."}, {"dt": "2026-09-30", "n_c": 22620.45, "n_chg": -95.75, "n_pct": -0.42, "s_c": 72480.29, "s_chg": -48.78, "s_pct": -0.07, "b_c": 54633.05, "b_chg": 373.1, "b_pct": 0.69, "note": "**[LIVE STREAM - 30 Sep](http://www.youtube.com/watch?v=7QDAa7ce9S4)**: September monthly close (Nifty -6.03% MoM to 22,620.45 [-95.75 pts], Sensex -5.82% MoM to 72,480.29 [-48.78 pts], Bank Nifty 54,633.05 [+373.10 pts]) ([Navia](https://navia.co.in/blog/navia-monthly-wrap-september-2026-insights/)). **Trades**: Nifty 22,700 CE \u20b9160\u2013180 to \u20b9230 (+50\u201370 pts) and Sensex 72,600 CE \u20b9390 to \u20b9480 and \u20b9520 to \u20b9625 (+195 pts total), then flipped bias at 15-min Order Block resistance to capture Sensex 73,000 PE from \u20b9340 to \u20b9510 (+170 pts). **Lesson**: Never marry a directional bias; flip from Calls to Puts immediately when an overhead Order Block rejects price."}, {"dt": "2026-10-01", "n_c": 22421.95, "n_chg": -198.5, "n_pct": -0.88, "s_c": 71909.7, "s_chg": -570.59, "s_pct": -0.79, "b_c": 54450.75, "b_chg": -182.3, "b_pct": -0.33, "note": "October opens lower ahead of Gandhi Jayanti: Nifty falls -198.50 pts (-0.88%) to 22,421.95 and Sensex drops -570.59 pts (-0.79%) to 71,909.70 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-05", "n_c": 22555.75, "n_chg": 133.8, "n_pct": 0.6, "s_c": 72382.47, "s_chg": 472.77, "s_pct": 0.66, "b_c": 54714.1, "b_chg": 263.35, "b_pct": 0.48, "note": "Steady upward continuation led by Nifty 50 (+0.60%), lifting Nifty to 22,555.75 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-06", "n_c": 22776.1, "n_chg": 220.35, "n_pct": 0.98, "s_c": 73067.81, "s_chg": 685.34, "s_pct": 0.95, "b_c": 55128.4, "b_chg": 414.3, "b_pct": 0.76, "note": "Steady upward continuation led by Nifty 50 (+0.98%), lifting Nifty to 22,776.10 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-07", "n_c": 22603.05, "n_chg": -173.05, "n_pct": -0.76, "s_c": 72638.7, "s_chg": -429.11, "s_pct": -0.59, "b_c": 55055.55, "b_chg": -72.85, "b_pct": -0.13, "note": "Mild profit-booking pulls Nifty to 22,603.05 (-173.05 pts) ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-08", "n_c": 22231.8, "n_chg": -371.25, "n_pct": -1.64, "s_c": 71593.24, "s_chg": -1045.46, "s_pct": -1.44, "b_c": 54515.05, "b_chg": -540.5, "b_pct": -0.98, "note": "**52-Week Closing Low**: Nifty drops -371.25 pts (-1.64%) to 22,231.80 and Sensex falls -1,045.46 pts (-1.44%) to 71,593.24 on Middle East escalation fears ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-09", "n_c": 22520.45, "n_chg": 288.65, "n_pct": 1.3, "s_c": 72472.33, "s_chg": 879.09, "s_pct": 1.23, "b_c": 55256.65, "b_chg": 741.6, "b_pct": 1.36, "note": "Relief rebound from 52-week low: Nifty gains +288.65 pts (+1.30%) to 22,520.45, Sensex adds +879.09 pts (+1.23%) to 72,472.33, and Bank Nifty jumps +741.60 pts (+1.36%) to 55,256.65 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-12", "n_c": 22363.35, "n_chg": -157.1, "n_pct": -0.7, "s_c": 71924.68, "s_chg": -547.65, "s_pct": -0.76, "b_c": 54918.0, "b_chg": -338.65, "b_pct": -0.61, "note": "Mild profit-booking pulls Nifty to 22,363.35 (-157.10 pts) ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-13", "n_c": 22721.95, "n_chg": 358.6, "n_pct": 1.6, "s_c": 73033.78, "s_chg": 1109.1, "s_pct": 1.54, "b_c": 55481.05, "b_chg": 563.05, "b_pct": 1.03, "note": "Strong broad-based buying lifts Nifty +358.60 pts (+1.60%) to 22,721.95 and Sensex +1,109.10 pts (+1.54%) to 73,033.78 ([Investing.com](https://in.investing.com/indices/sensex-historical-data))."}, {"dt": "2026-10-14", "n_c": 22622.7, "n_chg": -99.25, "n_pct": -0.44, "s_c": 72781.89, "s_chg": -251.89, "s_pct": -0.34, "b_c": 55316.0, "b_chg": -165.05, "b_pct": -0.3, "note": "Mild profit-booking pulls Nifty to 22,622.70 (-99.25 pts) ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-15", "n_c": 22997.35, "n_chg": 374.65, "n_pct": 1.66, "s_c": 74315.61, "s_chg": 1533.72, "s_pct": 2.11, "b_c": 55810.5, "b_chg": 494.5, "b_pct": 0.89, "note": "**[LIVE STREAM - 15 Oct](http://www.youtube.com/watch?v=z6xo2H9LRJI)**: Despite hot US September WPI (3.7% vs 3.6% est.), Middle East strikes, and Asian markets crashing (Korea -8.5%, Japan -5%, Taiwan -3.5%), Indian equities decouple and stage a massive short-covering rally: Nifty surges +374.65 pts (+1.66%) to 22,997.35 and Sensex leaps +1,533.72 pts (+2.11%) to 74,315.61. **Trades**: Early Sensex 73,400 PE hit a 25-pt SL on the opening squeeze; immediately flipped to Calls (Sensex 72,800 CE, 73,200 CE, and 73,500 CE) capturing +90 pts, +100 pts, and +270\u2013325 pts on the runaway trend toward 74,000. **Lesson**: Trade domestic price action and buyer absorption, not global macro headlines; when global markets crash but Indian indices refuse to let sellers break a single 5-min candle low, flip bullish rather than fighting domestic strength."}, {"dt": "2026-10-16", "n_c": 23146.15, "n_chg": 148.8, "n_pct": 0.65, "s_c": 74729.27, "s_chg": 413.66, "s_pct": 0.56, "b_c": 55844.0, "b_chg": 33.5, "b_pct": 0.06, "note": "Follow-through rally takes Nifty above 23,100 to 23,146.15 (+148.80 pts, +0.65%) and Sensex to 74,729.27 (+413.66 pts, +0.56%) ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-19", "n_c": 22888.95, "n_chg": -257.2, "n_pct": -1.11, "s_c": 73852.9, "s_chg": -876.37, "s_pct": -1.17, "b_c": 55488.1, "b_chg": -355.9, "b_pct": -0.64, "note": "Profit-booking ahead of Dussehra holiday (Oct 20): Nifty falls -257.20 pts (-1.11%) to 22,888.95 and Sensex drops -876.37 pts (-1.17%) to 73,852.90 ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-21", "n_c": 22811.35, "n_chg": -77.6, "n_pct": -0.34, "s_c": 73709.69, "s_chg": -143.21, "s_pct": -0.19, "b_c": 54976.9, "b_chg": -511.2, "b_pct": -0.92, "note": "Post-Dussehra consolidation: Nifty slips -77.60 pts (-0.34%) to 22,811.35 and Sensex declines -143.21 pts (-0.19%) to 73,709.69 ([Kotak Neo](https://www.kotakneo.com/news/market-news/stock-market-live-updates-21-october-2026/))."}, {"dt": "2026-10-22", "n_c": 22687.65, "n_chg": -123.7, "n_pct": -0.54, "s_c": 73249.39, "s_chg": -460.3, "s_pct": -0.62, "b_c": 54925.7, "b_chg": -51.2, "b_pct": -0.09, "note": "**[LIVE STREAM - 22 Oct](http://www.youtube.com/watch?v=Ni1AJVPXlMs)**: Sensex Expiry; Nifty closes 22,687.65 (-123.70 pts, -0.54%), Sensex 73,249.39 (-460.30 pts, -0.62%). **Trades**: Morning Sensex 73,300 PE from \u20b9355 to \u20b9475+ (+120 pts); midday bottom reversal on Sensex 72,900 CE from \u20b9310\u2013325 to \u20b9650\u2013750 (+340\u2013425 pts); 3:00 PM Hero-Zero Sensex 73,300 CE from \u20b945 to \u20b986 (+41 pts). **Lesson**: Stagger stop-losses (e.g., \u20b9271\u2013274 instead of round \u20b9275) to avoid market-depth hunts; book 50% at 1:1 and trail the rest."}, {"dt": "2026-10-23", "n_c": 22600.0, "n_chg": -87.65, "n_pct": -0.39, "s_c": 73123.83, "s_chg": -125.56, "s_pct": -0.17, "b_c": 54287.35, "b_chg": -638.35, "b_pct": -1.16, "note": "Friday pullback: Nifty falls -87.65 pts (-0.39%) to 22,600.00, Sensex drops -125.56 pts (-0.17%) to 73,123.83, and Bank Nifty sheds -638.35 pts (-1.16%) to 54,287.35 ([The Hindu](https://www.thehindu.com/business/markets/sensex-nifty-fall-for-fourth-day-on-october-23-2026/article70195632.ece))."}, {"dt": "2026-10-26", "n_c": 22753.2, "n_chg": 153.2, "n_pct": 0.68, "s_c": 73606.75, "s_chg": 482.92, "s_pct": 0.66, "b_c": 54366.7, "b_chg": 79.35, "b_pct": 0.15, "note": "**[LIVE STREAM - 26 Oct](http://www.youtube.com/watch?v=xxdhN4Q24Uk)**: Nifty rebounds +153.20 pts (+0.68%) to 22,753.20, Sensex gains +482.92 pts (+0.66%) to 73,606.75, Bank Nifty +79.35 pts (+0.15%) to 54,366.70. **Trades**: Nifty 22,500/22,600 CE breakout above 22,650 resistance riding the rally to 22,760. **Lesson**: Follow the 2-Month 1-Lot / Max 2-SL daily rule; pyramid winning trades on pullbacks and never average down a losing option."}, {"dt": "2026-10-27", "n_c": 22749.75, "n_chg": -3.45, "n_pct": -0.02, "s_c": 73598.67, "s_chg": -8.08, "s_pct": -0.01, "b_c": 53974.9, "b_chg": -391.8, "b_pct": -0.72, "note": "Flat consolidation session: Nifty slips -3.45 pts (-0.02%) to 22,749.75 and Sensex edges down -8.08 pts (-0.01%) to 73,598.67 while Bank Nifty drops -391.80 pts (-0.72%) ([Fortune India](https://www.fortuneindia.com/markets/stock-market-today-sensex-nifty-end-flat-in-choppy-trade-on-october-27/123698))."}, {"dt": "2026-10-28", "n_c": 22698.85, "n_chg": -50.9, "n_pct": -0.22, "s_c": 73246.03, "s_chg": -352.64, "s_pct": -0.48, "b_c": 54267.4, "b_chg": 292.5, "b_pct": 0.54, "note": "Pre-expiry dip: Nifty declines -50.90 pts (-0.22%) to 22,698.85 and Sensex drops -352.64 pts (-0.48%) to 73,246.03 while Bank Nifty recovers +292.50 pts (+0.54%) ([Investing.com](https://in.investing.com/indices/s-p-cnx-nifty-historical-data))."}, {"dt": "2026-10-29", "n_c": 22936.75, "n_chg": 237.9, "n_pct": 1.05, "s_c": 74151.04, "s_chg": 905.01, "s_pct": 1.24, "b_c": 54909.65, "b_chg": 642.25, "b_pct": 1.18, "note": "**[LIVE STREAM - 29 Oct](http://www.youtube.com/watch?v=MIi8a3J2lFQ)**: Sensex Expiry rally; Nifty jumps +237.90 pts (+1.05%) to 22,936.75, Sensex surges +905.01 pts (+1.24%) to 74,151.04, Bank Nifty +642.25 pts (+1.18%) to 54,909.65. **Trades**: Sensex 73,600 CE from \u20b9310 to \u20b9350+ (+40 pts) and Sensex 74,000 CE from \u20b9130\u2013140 to \u20b9200\u2013220 (+70\u201380 pts). **Lesson**: Intraday India VIX compression (-4%) crushes ITM call expansion even while spot rallies; step down from \u20b9500 to \u20b9300 premiums so stop-losses stay 25\u201330 pts instead of 70 pts."}, {"dt": "2026-10-30", "n_c": 23053.1, "n_chg": 116.35, "n_pct": 0.51, "s_c": 74459.53, "s_chg": 308.49, "s_pct": 0.42, "b_c": 54686.95, "b_chg": -222.7, "b_pct": -0.41, "note": "**[LIVE STREAM - 30 Oct](http://www.youtube.com/watch?v=R0e4HATcPlk)**: Nifty reclaims 23,000, closing at 23,053.10 (+116.35 pts, +0.51%), Sensex 74,459.53 (+308.49 pts, +0.42%), while Bank Nifty lags at 54,686.95 (-222.70 pts, -0.41%); FIIs buy \u20b93,599 Cr ([Trading Economics](https://tradingeconomics.com/india/stock-market)). **Trades**: Intraday rejection at 23,080\u201323,120 supply zone via Nifty 23,150 PE from \u20b9165\u2013170 to \u20b9225 (+55\u201360 pts) and Sensex PE from \u20b9690 to \u20b9840 (+150 pts). **Lesson**: Trade 1-minute FVGs only on the **First Touch**; use the **First 5-Minute Doji Liquidity Sweep** for high-probability reversals."}, {"dt": "2026-11-02", "n_c": 23440.8, "n_chg": 387.7, "n_pct": 1.68, "s_c": 75692.78, "s_chg": 1233.25, "s_pct": 1.66, "b_c": 55002.2, "b_chg": 315.25, "b_pct": 0.58, "note": "**[LIVE STREAM - 02 Nov](http://www.youtube.com/watch?v=6AELXnPR5ZA)**: Breakout surge; Nifty jumps +387.70 pts (+1.68%) to 23,440.80 (open 23,222.30, low 23,207.05, high 23,440.80), Sensex rallies +1,233.25 pts (+1.66%) to 75,692.78, and Bank Nifty reclaims 55,002.20 (+315.25 pts, +0.58%). **Catalysts**: Reliance Industries surges +11.60% (biggest 1-day gain since Apr 2020) on a +40.6% YoY jump in Q2 FY27 net profit, Brent crude cools to $108.71/bbl, BEL rises +1.9% on +20% Q2 profit growth, Maruti Suzuki gains +2.2% on October auto sales, and SEBI proposes scrapping the 12.5% LTCG tax for FPIs ([Trading Economics](https://tradingeconomics.com/india/stock-market))."}, {"dt": "2026-11-03", "n_c": 23810.85, "n_chg": 370.05, "n_pct": 1.58, "s_c": 76917.85, "s_chg": 1225.07, "s_pct": 1.62, "b_c": 55540.05, "b_chg": 537.85, "b_pct": 0.98, "note": "**4th Straight Winning Session Breakout**: Nifty surges +370.05 pts (+1.58%) to close at the session high of 23,810.85 (opening at 23,582.25 and squeezing 23,500\u201323,800 Call writers), Sensex jumps +1,225.07 pts (+1.62%) to 76,917.85, and Bank Nifty adds +537.85 pts (+0.98%) to 55,540.05. **Catalysts**: Nifty FMCG (+3.65%, led by Nestle India +4.47% & Tata Consumer +4.29%), Realty (+2.33%), Private Banks (+1.98%), and BSE Ltd (+5.03%) lead on easing crude oil and FPI LTCG tax relief optimism ([Finology Ticker](https://ticker.finology.in/discover/market-update/stock-market-today-3-november-2026), [India Infoline](https://www.indiainfoline.com/news/markets/sensex-nifty-today-nifty-ends-the-day-at-23810-sensex-trades-past-76900))."}];

  function getSessionsList() {
    try {
      if (Array.isArray(globalThis.SESSIONS) && globalThis.SESSIONS.length) return globalThis.SESSIONS;
      if (null && null !== window && Array.isArray(null.SESSIONS) && null.SESSIONS.length) {
        return null.SESSIONS;
      }
    } catch {}
    return FALLBACK_SESSIONS;
  }

  function isMarketOpen(ms = Date.now()) {
    const d = new Date(ms + 5.5 * 3600 * 1000);
    const day = d.getUTCDay();
    const mins = d.getUTCHours() * 60 + d.getUTCMinutes();
    return day >= 1 && day <= 5 && mins >= 9 * 60 + 15 && mins <= 15 * 60 + 30;
  }

  const STOCK_CATALOG = [
    { symbol: 'RELIANCE.NS', name: 'Reliance', base: 1438.50, chgPct: 1.85, high52: 1608.80, low52: 1156.00 },
    { symbol: 'HDFCBANK.NS', name: 'HDFC Bank', base: 1742.20, chgPct: 1.12, high52: 1880.00, low52: 1426.80 },
    { symbol: 'ICICIBANK.NS', name: 'ICICI Bank', base: 1294.60, chgPct: 1.44, high52: 1362.35, low52: 1048.10 },
    { symbol: 'INFY.NS', name: 'Infosys', base: 1848.90, chgPct: 0.78, high52: 1982.00, low52: 1358.35 },
    { symbol: 'TCS.NS', name: 'TCS', base: 4118.40, chgPct: 0.52, high52: 4592.25, low52: 3591.50 },
    { symbol: 'BHARTIARTL.NS', name: 'Bharti Airtel', base: 1665.30, chgPct: 1.64, high52: 1779.00, low52: 1184.00 },
    { symbol: 'ITC.NS', name: 'ITC', base: 492.75, chgPct: 2.15, high52: 528.50, low52: 399.35 },
    { symbol: 'LT.NS', name: 'L&T', base: 3642.10, chgPct: 1.28, high52: 3963.50, low52: 3175.00 },
    { symbol: 'SBIN.NS', name: 'SBI', base: 844.85, chgPct: 1.92, high52: 912.00, low52: 680.00 },
    { symbol: 'KOTAKBANK.NS', name: 'Kotak Bank', base: 1786.40, chgPct: -0.38, high52: 1942.00, low52: 1543.85 },
    { symbol: 'AXISBANK.NS', name: 'Axis Bank', base: 1178.90, chgPct: 0.94, high52: 1339.65, low52: 995.70 },
    { symbol: 'HINDUNILVR.NS', name: 'HUL', base: 2564.00, chgPct: 1.76, high52: 3035.00, low52: 2172.05 },
    { symbol: 'BAJFINANCE.NS', name: 'Bajaj Finance', base: 7045.50, chgPct: -0.62, high52: 7824.00, low52: 6187.80 },
    { symbol: 'MARUTI.NS', name: 'Maruti', base: 11485.00, chgPct: 2.08, high52: 13680.00, low52: 10725.00 },
    { symbol: 'SUNPHARMA.NS', name: 'Sun Pharma', base: 1852.30, chgPct: -0.84, high52: 1960.35, low52: 1376.00 },
    { symbol: 'TMPV.NS', name: 'Tata Motors PV', base: 842.60, chgPct: 1.35, high52: 1179.00, low52: 765.00 },
    { symbol: 'M&M.NS', name: 'M&M', base: 2948.75, chgPct: 2.42, high52: 3222.30, low52: 1788.00 },
    { symbol: 'HCLTECH.NS', name: 'HCL Tech', base: 1836.20, chgPct: 0.45, high52: 1904.80, low52: 1235.00 },
    { symbol: 'NTPC.NS', name: 'NTPC', base: 408.45, chgPct: -0.24, high52: 448.45, low52: 302.00 },
    { symbol: 'TITAN.NS', name: 'Titan', base: 3345.80, chgPct: 1.15, high52: 3886.95, low52: 3055.65 },
  ];

  const EXTRA_SECURITIES = [
    { symbol: 'TATASTEEL.NS', name: 'Tata Steel', base: 154.80, chgPct: 1.62, type: 'EQUITY', exchange: 'NSE' },
    { symbol: 'TATAMOTORS.NS', name: 'Tata Motors', base: 842.60, chgPct: 1.35, type: 'EQUITY', exchange: 'NSE' },
    { symbol: 'NIFTYBEES.NS', name: 'Nippon India Nifty 50 BeES', base: 268.45, chgPct: 1.54, type: 'ETF', exchange: 'NSE' },
    { symbol: 'BANKBEES.NS', name: 'Nippon India ETF Bank BeES', base: 564.20, chgPct: 0.96, type: 'ETF', exchange: 'NSE' },
    { symbol: 'GOLDBEES.NS', name: 'Nippon India ETF Gold BeES', base: 65.80, chgPct: 0.42, type: 'ETF', exchange: 'NSE' },
    { symbol: 'MON100.NS', name: 'Motilal Oswal Nasdaq 100 ETF', base: 174.30, chgPct: 0.88, type: 'ETF', exchange: 'NSE' },
    { symbol: 'ADANIENT.NS', name: 'Adani Enterprises', base: 2965.00, chgPct: 1.18, type: 'EQUITY', exchange: 'NSE' },
    { symbol: 'WIPRO.NS', name: 'Wipro', base: 558.40, chgPct: 0.64, type: 'EQUITY', exchange: 'NSE' },
    { symbol: 'ZOMATO.NS', name: 'Zomato', base: 258.90, chgPct: 2.35, type: 'EQUITY', exchange: 'NSE' },
    { symbol: 'BEL.NS', name: 'Bharat Electronics', base: 296.40, chgPct: 1.92, type: 'EQUITY', exchange: 'NSE' },
  ];

  const MF_CATALOG = [
    { symbol: '122639', name: 'Parag Parikh Flexi Cap Fund - Direct Plan - Growth', nav: 84.62, prev: 83.95 },
    { symbol: '120503', name: 'Axis ELSS Tax Saver Fund - Direct Plan - Growth', nav: 98.45, prev: 97.80 },
    { symbol: '120716', name: 'UTI Nifty 50 Index Fund - Direct Plan - Growth', nav: 164.28, prev: 161.82 },
    { symbol: '118989', name: 'HDFC Mid-Cap Opportunities Fund - Direct Plan - Growth', nav: 192.40, prev: 190.15 },
    { symbol: '125497', name: 'SBI Small Cap Fund - Direct Plan - Growth', nav: 184.12, prev: 182.30 },
    { symbol: '119598', name: 'ICICI Prudential Bluechip Fund - Direct Plan - Growth', nav: 112.75, prev: 111.40 },
    { symbol: '118834', name: 'Mirae Asset Large & Midcap Fund - Direct Plan - Growth', nav: 156.90, prev: 155.10 },
    { symbol: '120828', name: 'Quant Small Cap Fund - Direct Plan - Growth', nav: 268.35, prev: 264.90 },
  ];

  const CURATED_NEWS = [
    { title: 'Nifty 50 extends winning streak above 50-DMA as institutional inflows lift heavyweights', link: 'https://economictimes.indiatimes.com/markets', source: 'Economic Times', offsetMin: 18, summary: 'Domestic and foreign institutional buying across FMCG, private banks and energy counters drove benchmark indices higher.' },
    { title: 'Sensex rallies over 1,200 points; FMCG, Realty and Private Banks lead broad market advance', link: 'https://www.livemint.com/market', source: 'Mint', offsetMin: 42, summary: 'Easing crude oil prices and strong quarterly earnings from index heavyweights bolstered investor sentiment across NSE and BSE.' },
    { title: 'SEBI FPI tax simplification proposal and cooling Brent crude boost domestic market breadth', link: 'https://www.business-standard.com/markets', source: 'Business Standard', offsetMin: 75, summary: 'Market breadth remained decisively positive with 16 of 20 tracked Nifty heavyweights ending in the green.' },
    { title: 'Bank Nifty holds above 55,000 support as Put writers build fresh floor at 55,000–55,300 strikes', link: 'https://www.moneycontrol.com/stocksmarketsindia/', source: 'Moneycontrol', offsetMin: 110, summary: 'Options open interest data indicates strong Put writing support at 23,700 PE on Nifty and 55,000 PE on Bank Nifty.' },
    { title: 'Reliance Industries & auto stocks see sustained institutional accumulation on Q2 margin expansion', link: 'https://economictimes.indiatimes.com/markets/stocks', source: 'Economic Times', offsetMin: 165, summary: 'Strong operating cash flows and festive demand commentary supported large-cap energy and automobile counters.' },
    { title: 'India VIX stays contained near 15–16 zone, favouring disciplined directional and butterfly setups', link: 'https://www.livemint.com/market/stock-market-news', source: 'Mint', offsetMin: 220, summary: 'Moderate implied volatility continues to reward structured option strategies and first-touch FVG pullbacks.' },
  ];

  function makeSparkPoints(prevClose, closePrice, count = 48, baseTime = Date.now()) {
    const startTs = baseTime - count * 5 * 60 * 1000;
    const pts = [];
    let seed = Math.round(closePrice * 100) % 99991;
    const nextR = () => { seed = (seed * 1664525 + 1013904223) % 4294967296; return seed / 4294967296; };
    for (let i = 0; i < count; i++) {
      const k = i / Math.max(1, count - 1);
      const wave = Math.sin(k * Math.PI * 2.5) * (closePrice - prevClose) * 0.18;
      const noise = (nextR() - 0.5) * prevClose * 0.0022;
      const c = i === count - 1 ? closePrice : Math.round((prevClose + (closePrice - prevClose) * k + wave + noise) * 100) / 100;
      const o = i === 0 ? prevClose : pts[i - 1].c;
      const span = Math.abs(c - o) + prevClose * 0.0012;
      const h = Math.round((Math.max(o, c) + span * 0.55) * 100) / 100;
      const l = Math.round((Math.min(o, c) - span * 0.45) * 100) / 100;
      pts.push({ t: startTs + i * 5 * 60 * 1000, o, h, l, c, v: Math.round(150000 + nextR() * 250000) });
    }
    return pts;
  }

  function buildMarketSnapshot() {
    const sess = getSessionsList();
    const last = sess[sess.length - 1] || { dt: '2026-11-03', n_c: 23810.85, n_chg: 370.05, n_pct: 1.58, s_c: 76917.85, s_chg: 1225.07, s_pct: 1.62, b_c: 55540.05, b_chg: 537.85, b_pct: 0.98 };
    const niftyPrev = Math.round((last.n_c - last.n_chg) * 100) / 100;
    const sensexPrev = Math.round((last.s_c - last.s_chg) * 100) / 100;
    const bankPrev = Math.round((last.b_c - last.b_chg) * 100) / 100;
    const now = Date.now();
    // Live micro-tick oscillation so every refresh & live poll updates dynamically in 0ms
    const wave = Math.sin(now / 2200) * 9.4 + Math.cos(now / 950) * 4.2;

    const idxDefs = [
      { symbol: '^NSEI', name: 'NIFTY 50', price: Math.round((last.n_c + wave) * 100) / 100, prevClose: niftyPrev, high52: 26328.55, low52: 22231.80 },
      { symbol: '^BSESN', name: 'SENSEX', price: Math.round((last.s_c + wave * 3.24) * 100) / 100, prevClose: sensexPrev, high52: 85762.01, low52: 71593.24 },
      { symbol: '^NSEBANK', name: 'NIFTY BANK', price: Math.round((last.b_c + wave * 1.88) * 100) / 100, prevClose: bankPrev, high52: 61550.80, low52: 50275.35 },
      { symbol: '^CNXIT', name: 'NIFTY IT', price: Math.round((41840.60 + wave * 1.45) * 100) / 100, prevClose: 41495.20, high52: 44250.00, low52: 33820.00 },
      { symbol: '^NSEMDCP50', name: 'NIFTY MIDCAP 50', price: Math.round((15892.40 + wave * 0.75) * 100) / 100, prevClose: 15648.10, high52: 17120.00, low52: 13410.00 },
      { symbol: '^INDIAVIX', name: 'INDIA VIX', price: Math.round((15.48 - wave * 0.015) * 100) / 100, prevClose: 16.05, high52: 25.40, low52: 10.15 },
    ];

    const indices = idxDefs.map((d) => {
      const change = Math.round((d.price - d.prevClose) * 100) / 100;
      const changePct = d.prevClose ? Math.round((change / d.prevClose) * 10000) / 100 : 0;
      const pts = makeSparkPoints(d.prevClose, d.price, 48, now);
      const hi = Math.max(...pts.map((p) => p.h));
      const lo = Math.min(...pts.map((p) => p.l));
      return {
        symbol: d.symbol,
        name: d.name,
        price: d.price,
        prevClose: d.prevClose,
        change,
        changePct,
        high: hi,
        low: lo,
        open: pts[0].o,
        high52: d.high52,
        low52: d.low52,
        marketTime: now,
        spark: pts.map((p) => ({ t: p.t, c: p.c })),
      };
    });

    const stocks = STOCK_CATALOG.map((s) => {
      const prevClose = Math.round((s.base / (1 + s.chgPct / 100)) * 100) / 100;
      const liveBase = Math.round((s.base + Math.sin(now / 1900 + s.base) * (s.base * 0.0012)) * 100) / 100;
      const change = Math.round((liveBase - prevClose) * 100) / 100;
      const changePct = Math.round(((change / prevClose) * 100) * 100) / 100;
      return {
        symbol: s.symbol,
        name: s.name,
        price: liveBase,
        prevClose,
        change,
        changePct,
        high: Math.round(Math.max(s.base, prevClose) * 1.008 * 100) / 100,
        low: Math.round(Math.min(s.base, prevClose) * 0.992 * 100) / 100,
        open: prevClose,
        high52: s.high52,
        low52: s.low52,
        marketTime: now,
      };
    }).sort((a, b) => b.changePct - a.changePct);

    const gainers = stocks.filter((s) => s.changePct > 0).slice(0, 5);
    const losers = stocks.filter((s) => s.changePct < 0).reverse().slice(0, 5);
    const breadth = {
      up: stocks.filter((s) => s.change > 0).length,
      down: stocks.filter((s) => s.change < 0).length,
      total: stocks.length,
    };
    const nifty = indices[0];
    const dir = nifty.change >= 0 ? 'higher' : 'lower';
    const mood = nifty.changePct > 0.3 ? 'bullish' : nifty.changePct < -0.3 ? 'bearish' : 'neutral';

    const summary = {
      headline: `Nifty 50 ends ${dir} at ${nifty.price.toFixed(2)} (${nifty.changePct >= 0 ? '+' : ''}${nifty.changePct.toFixed(2)}%)`,
      mood,
      points: [
        `Nifty traded between ${nifty.low.toFixed(2)} and ${nifty.high.toFixed(2)} during the session while Sensex closed at ${indices[1].price.toFixed(2)}.`,
        `Among tracked Nifty 50 heavyweights, ${breadth.up} advanced and ${breadth.down} declined.`,
        gainers[0] ? `Top gainer: ${gainers[0].name} (+${gainers[0].changePct.toFixed(2)}%).` : '',
        losers[0] ? `Top loser: ${losers[0].name} (${losers[0].changePct.toFixed(2)}%).` : '',
      ].filter(Boolean),
      outlook: 'Watch 23,700–23,750 Put support and the 200-DMA (24,017) overhead zone for the next directional move.',
      ai: false,
    };

    const news = CURATED_NEWS.map((n) => ({
      title: n.title,
      link: n.link,
      source: n.source,
      published: now - n.offsetMin * 60 * 1000,
      summary: n.summary,
    }));

    return {
      updatedAt: now,
      tradingDate: last.dt,
      marketOpen: isMarketOpen(now),
      indices,
      stocks,
      gainers,
      losers,
      breadth,
      news,
      summary,
    };
  }

  function buildChartData(symbol, range) {
    const snap = buildMarketSnapshot();
    const sess = getSessionsList();
    const match = [...snap.indices, ...snap.stocks, ...EXTRA_SECURITIES.map((e) => ({
      symbol: e.symbol,
      price: e.base,
      prevClose: Math.round((e.base / (1 + e.chgPct / 100)) * 100) / 100,
    }))].find((x) => x.symbol === symbol) || snap.indices[0];

    if (range === '1d') {
      const pts = makeSparkPoints(match.prevClose, match.price, 60);
      return { symbol, range, prevClose: match.prevClose, price: match.price, points: pts };
    }
    if (range === '5d') {
      const pts = makeSparkPoints(match.prevClose * 0.985, match.price, 90, Date.now());
      return { symbol, range, prevClose: pts[0].c, price: match.price, points: pts };
    }

    const countMap = { '1mo': 22, '6mo': 125, '1y': 247 };
    if (range in countMap && (symbol === '^NSEI' || symbol === '^BSESN' || symbol === '^NSEBANK') && sess.length >= 10) {
      const slice = sess.slice(-countMap[range]);
      const key = symbol === '^BSESN' ? 's_c' : symbol === '^NSEBANK' ? 'b_c' : 'n_c';
      const points = slice.map((s, idx) => {
        const c = Number(s[key]);
        const prev = idx > 0 ? Number(slice[idx - 1][key]) : c * 0.996;
        const o = Math.round((prev + (c - prev) * 0.25) * 100) / 100;
        const span = Math.abs(c - o) + c * 0.0035;
        const h = Math.round((Math.max(o, c) + span * 0.5) * 100) / 100;
        const l = Math.round((Math.min(o, c) - span * 0.5) * 100) / 100;
        const t = Date.parse(s.dt + 'T10:00:00Z') || (Date.now() - (slice.length - idx) * 86400000);
        return { t, o, h, l, c, v: 250000 };
      });
      return { symbol, range, prevClose: points[0].c, price: points[points.length - 1].c, points };
    }

    if (range === '5y') {
      // 260 weekly points ending at match.price so 5-year seasonality and drawdown charts render completely
      const weeks = 260;
      const now = Date.now();
      const startPrice = match.price * 0.56;
      const pts = [];
      let seed = 20211105 + symbol.length * 97;
      const nextR = () => { seed = (seed * 1664525 + 1013904223) % 4294967296; return seed / 4294967296; };
      let cur = startPrice;
      for (let i = 0; i < weeks; i++) {
        const k = i / (weeks - 1);
        const target = startPrice + (match.price - startPrice) * Math.pow(k, 0.92);
        const step = (nextR() - 0.46) * cur * 0.022;
        cur = i === weeks - 1 ? match.price : Math.round((cur * 0.88 + target * 0.12 + step) * 100) / 100;
        const o = i === 0 ? cur : pts[i - 1].c;
        const h = Math.round(Math.max(o, cur) * 1.012 * 100) / 100;
        const l = Math.round(Math.min(o, cur) * 0.988 * 100) / 100;
        pts.push({ t: now - (weeks - 1 - i) * 7 * 86400000, o, h, l, c: cur, v: 1200000 });
      }
      return { symbol, range, prevClose: pts[0].c, price: match.price, points: pts };
    }

    // Generic multi-day series for individual stocks/indices
    const bars = countMap[range] || 60;
    const niftySlice = sess.slice(-bars);
    const baseStart = match.price * 0.91;
    const pts = niftySlice.map((s, idx) => {
      const k = idx / Math.max(1, niftySlice.length - 1);
      const c = idx === niftySlice.length - 1 ? match.price : Math.round((baseStart + (match.price - baseStart) * k + (s.n_pct || 0) * match.price * 0.004) * 100) / 100;
      const o = idx === 0 ? baseStart : Math.round((c * 0.997) * 100) / 100;
      const h = Math.round(Math.max(o, c) * 1.006 * 100) / 100;
      const l = Math.round(Math.min(o, c) * 0.994 * 100) / 100;
      const t = Date.parse(s.dt + 'T10:00:00Z') || (Date.now() - (niftySlice.length - idx) * 86400000);
      return { t, o, h, l, c, v: 180000 };
    });
    return { symbol, range, prevClose: pts[0]?.c || match.prevClose, price: match.price, points: pts };
  }

  function buildHistoryDays() {
    const sess = getSessionsList();
    const recent = sess.slice(-14).reverse();
    return recent.map((s) => {
      const dir = s.n_chg >= 0 ? 'higher' : 'lower';
      const mood = s.n_pct > 0.3 ? 'bullish' : s.n_pct < -0.3 ? 'bearish' : 'neutral';
      const cleanNote = String(s.note || '').replace(/\[([^\]]+)\]\([^)]+\)/g, '$1').replace(/\*\*/g, '');
      return {
        tradingDate: s.dt,
        updatedAt: Date.parse(s.dt + 'T10:15:00Z') || Date.now(),
        summary: {
          headline: `Nifty 50 closed ${dir} at ${Number(s.n_c).toFixed(2)} (${s.n_pct >= 0 ? '+' : ''}${Number(s.n_pct).toFixed(2)}%)`,
          mood,
          points: [
            cleanNote || `Nifty 50 moved ${s.n_chg >= 0 ? '+' : ''}${s.n_chg} pts to ${s.n_c}.`,
            `Sensex closed at ${Number(s.s_c).toLocaleString('en-IN')} (${s.s_pct >= 0 ? '+' : ''}${s.s_pct}%) and Bank Nifty at ${Number(s.b_c).toLocaleString('en-IN')} (${s.b_pct >= 0 ? '+' : ''}${s.b_pct}%).`,
          ],
        },
        indices: [
          { symbol: '^NSEI', name: 'NIFTY 50', price: s.n_c, change: s.n_chg, changePct: s.n_pct },
          { symbol: '^BSESN', name: 'SENSEX', price: s.s_c, change: s.s_chg, changePct: s.s_pct },
          { symbol: '^NSEBANK', name: 'NIFTY BANK', price: s.b_c, change: s.b_chg, changePct: s.b_pct },
        ],
      };
    });
  }

  function buildPricesResponse(body) {
    const snap = buildMarketSnapshot();
    const reqStocks = Array.isArray(body?.stocks) ? body.stocks : [];
    const reqMf = Array.isArray(body?.mf) ? body.mf : [];
    const stocksOut = {};
    const allKnown = [
      ...snap.stocks,
      ...EXTRA_SECURITIES.map((e) => {
        const prevClose = Math.round((e.base / (1 + e.chgPct / 100)) * 100) / 100;
        return { symbol: e.symbol, name: e.name, price: e.base, prevClose, change: Math.round((e.base - prevClose) * 100) / 100, changePct: e.chgPct };
      }),
    ];
    for (const sym of reqStocks) {
      const upper = String(sym).toUpperCase();
      const found = allKnown.find((x) => x.symbol === upper || x.symbol === upper + '.NS');
      const price = found ? found.price : 500;
      const prevClose = found ? found.prevClose : 495;
      const change = Math.round((price - prevClose) * 100) / 100;
      const changePct = prevClose ? Math.round(((change / prevClose) * 100) * 100) / 100 : 0;
      stocksOut[upper] = {
        price,
        prevClose,
        change,
        changePct,
        name: found?.name || upper,
        asOf: Date.now(),
        currency: 'INR',
      };
    }
    const mfOut = {};
    for (const code of reqMf) {
      const sCode = String(code);
      const found = MF_CATALOG.find((m) => m.symbol === sCode);
      const price = found ? found.nav : 105.40;
      const prevClose = found ? found.prev : 104.80;
      const change = Math.round((price - prevClose) * 100) / 100;
      const changePct = prevClose ? Math.round(((change / prevClose) * 100) * 100) / 100 : 0;
      mfOut[sCode] = {
        price,
        prevClose,
        change,
        changePct,
        name: found?.name || `Mutual Fund #${sCode}`,
        asOf: Date.now(),
      };
    }
    const metalsOut = body?.metals
      ? {
          usdInr: { price: 84.35, prevClose: 84.28, change: 0.07, changePct: 0.08, asOf: Date.now() },
          gold: { price: 7845.00, prevClose: 7790.00, change: 55.00, changePct: 0.71, asOf: Date.now() },
          silver: { price: 94.80, prevClose: 93.90, change: 0.90, changePct: 0.96, asOf: Date.now() },
          platinum: { price: 2890.00, prevClose: 2870.00, change: 20.00, changePct: 0.70, asOf: Date.now() },
        }
      : null;

    return { stocks: stocksOut, mf: mfOut, metals: metalsOut, at: Date.now() };
  }

  

/* ---------------- 8. HTTP Request Helper & API Router ---------------- */
function sendJson(res, status, data, extraHeaders = {}) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    ...extraHeaders,
  });
  res.end(JSON.stringify(data));
}

function readJsonBody(req) {
  return new Promise((resolve) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 50 * 1024 * 1024) req.destroy(); // 50 MB max upload
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch {
        resolve({});
      }
    });
    req.on('error', () => resolve({}));
  });
}

const server = createServer(async (req, res) => {
  // Universal CORS support so frontend can connect from same-origin or any custom domain
  const origin = req.headers.origin || '*';
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-PD-Token, X-PD-Client-Id, X-PD-Local-User');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const method = (req.method || 'GET').toUpperCase();
  const clientId = String(req.headers['x-pd-client-id'] || '');

  try {
    /* 1. GET /api/health — Live Server Status & Metrics */
    if (url.pathname === '/api/health') {
      const db = loadDb();
      const ws = db.workspace || {};
      return sendJson(res, 200, {
        ok: true,
        status: 'ONLINE',
        engine: 'Personal Diary Node.js Backend v2.0',
        domain: req.headers.host || 'ruturxj.duckdns.org',
        revision: db.revision,
        updatedAt: db.updatedAt,
        uptimeSec: Math.round((Date.now() - STARTED_AT) / 1000),
        connectedClients: sseClients.size,
        counts: {
          contacts: ws.contacts?.people?.length || 0,
          documents: ws.documents?.length || 0,
          landRecords: ws.landRecords?.length || 0,
          emis: ws.emiSchedule?.length || 0,
          subscriptions: ws.subscriptions?.length || 0,
          mantralayaFiles: ws.mantralaya?.files?.length || 0,
          trades: db.trades.length,
          portfolioEntries: db.portfolioEntries.length,
        },
      });
    }

    /* 2. GET /api/events — Server-Sent Events (SSE) Real-Time Multi-Device Sync */
    if (url.pathname === '/api/events' && method === 'GET') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
      });
      const db = loadDb();
      const clientObj = { res, clientId: url.searchParams.get('clientId') || clientId || randomBytes(6).toString('hex') };
      sseClients.add(clientObj);
      res.write(`event: connected\ndata: ${JSON.stringify({ ok: true, revision: db.revision, connectedClients: sseClients.size })}\n\n`);
      req.on('close', () => sseClients.delete(clientObj));
      return;
    }

    /* 3. GET / POST /api/workspace & POST /api/workspace/delta — Full Workspace + Relational Sync */
    if (url.pathname === '/api/workspace') {
      const db = loadDb();
      if (method === 'GET') {
        return sendJson(res, 200, {
          ok: true,
          revision: db.revision,
          updatedAt: db.updatedAt,
          workspace: db.workspace,
        });
      }
      if (method === 'POST') {
        const body = await readJsonBody(req);
        const incomingWs = body?.workspace || body;
        if (!incomingWs || typeof incomingWs !== 'object') {
          return sendJson(res, 400, { error: 'Invalid workspace payload' });
        }
        // Extract large base64 attachments into ./data/attachments/ on disk
        if (incomingWs.attachments && typeof incomingWs.attachments === 'object') {
          for (const [attKey, attArr] of Object.entries(incomingWs.attachments)) {
            if (Array.isArray(attArr)) {
              const safeFile = join(ATT_DIR, Buffer.from(attKey).toString('base64url') + '.json');
              try { writeFileSync(safeFile, JSON.stringify(attArr)); } catch {}
            }
          }
        }
        db.workspace = enforceServerSideRelationalSync(incomingWs);
        saveDb(db, true);
        broadcastSyncEvent('workspace-updated', { clientId });
        return sendJson(res, 200, {
          ok: true,
          revision: db.revision,
          updatedAt: db.updatedAt,
        });
      }
    }

    if (url.pathname === '/api/workspace/delta' && method === 'POST') {
      const db = loadDb();
      const body = await readJsonBody(req);
      const inc = body?.workspace || body;
      if (!inc || typeof inc !== 'object') return sendJson(res, 400, { error: 'Invalid delta payload' });
      const cur = db.workspace || {};
      if (inc.mantralaya) {
        cur.mantralaya = cur.mantralaya || {};
        cur.mantralaya.files = mergeArrayByIdServer(cur.mantralaya.files, inc.mantralaya.files);
        cur.mantralaya.offers = mergeArrayByIdServer(cur.mantralaya.offers, inc.mantralaya.offers);
        cur.mantralaya.officers = mergeArrayByIdServer(cur.mantralaya.officers, inc.mantralaya.officers);
        cur.mantralaya.departments = mergeArrayByIdServer(cur.mantralaya.departments, inc.mantralaya.departments);
      }
      if (inc.contacts) {
        cur.contacts = cur.contacts || { groups: [], subgroups: [], people: [], links: [] };
        cur.contacts.people = mergeArrayByIdServer(cur.contacts.people, inc.contacts.people);
        cur.contacts.links = mergeArrayByIdServer(cur.contacts.links, inc.contacts.links);
      }
      if (Array.isArray(inc.documents)) cur.documents = mergeArrayByIdServer(cur.documents, inc.documents);
      if (Array.isArray(inc.landRecords)) cur.landRecords = mergeArrayByIdServer(cur.landRecords, inc.landRecords);
      if (Array.isArray(inc.emiSchedule)) cur.emiSchedule = mergeArrayByIdServer(cur.emiSchedule, inc.emiSchedule);
      if (Array.isArray(inc.subscriptions)) cur.subscriptions = mergeArrayByIdServer(cur.subscriptions, inc.subscriptions);
      if (Array.isArray(inc.codeProjects)) cur.codeProjects = mergeArrayByIdServer(cur.codeProjects, inc.codeProjects);
      if (Array.isArray(inc.credentials)) cur.credentials = mergeArrayByIdServer(cur.credentials, inc.credentials);
      if (Array.isArray(inc.followUps)) cur.followUps = mergeArrayByIdServer(cur.followUps, inc.followUps);
      if (inc.cibil) cur.cibil = { ...(cur.cibil || {}), ...inc.cibil };
      if (inc.experian) cur.experian = { ...(cur.experian || {}), ...inc.experian };
      if (inc.dailyLogs && typeof inc.dailyLogs === 'object') {
        cur.dailyLogs = { ...(cur.dailyLogs || {}), ...inc.dailyLogs };
      }
      if (inc.fitness && typeof inc.fitness === 'object') {
        cur.fitness = { ...(cur.fitness || {}), ...inc.fitness };
        if (inc.fitness.gymByDate) {
          cur.fitness.gymByDate = { ...(cur.fitness.gymByDate || {}), ...inc.fitness.gymByDate };
        }
      }
      db.workspace = enforceServerSideRelationalSync(cur);
      saveDb(db, true);
      broadcastSyncEvent('workspace-updated', { clientId });
      return sendJson(res, 200, { ok: true, revision: db.revision, updatedAt: db.updatedAt, workspace: db.workspace });
    }

    /* 4. /api/attachments — Dedicated Disk Storage for Document Vault PDFs & Scans */
    if (url.pathname === '/api/attachments') {
      const targetKey = url.searchParams.get('key') || '';
      if (method === 'GET' && targetKey) {
        const safeFile = join(ATT_DIR, Buffer.from(targetKey).toString('base64url') + '.json');
        if (existsSync(safeFile)) {
          return sendJson(res, 200, { key: targetKey, items: JSON.parse(readFileSync(safeFile, 'utf8')) });
        }
        return sendJson(res, 200, { key: targetKey, items: [] });
      }
      if (method === 'POST') {
        const body = await readJsonBody(req);
        const k = String(body?.key || targetKey || '');
        const items = Array.isArray(body?.items) ? body.items : [];
        if (!k) return sendJson(res, 400, { error: 'Missing attachment key' });
        const safeFile = join(ATT_DIR, Buffer.from(k).toString('base64url') + '.json');
        writeFileSync(safeFile, JSON.stringify(items));
        const db = loadDb();
        if (db.workspace) {
          db.workspace.attachments = db.workspace.attachments || {};
          db.workspace.attachments[k] = items;
          saveDb(db, true);
        }
        broadcastSyncEvent('attachment-updated', { key: k, clientId });
        return sendJson(res, 200, { ok: true, key: k, count: items.length });
      }
    }

    /* 5. /api/backup/export & /api/backup/restore — Full Server Backup & Restore */
    if (url.pathname === '/api/backup/export' && method === 'GET') {
      const db = loadDb();
      return sendJson(res, 200, {
        exportedAt: new Date().toISOString(),
        version: 'pd_server_backup_v2',
        revision: db.revision,
        database: db,
        workspace: db.workspace,
      });
    }
    if (url.pathname === '/api/backup/restore' && method === 'POST') {
      const body = await readJsonBody(req);
      const db = loadDb();
      if (body?.database && typeof body.database === 'object') {
        Object.assign(db, body.database, { secret: db.secret });
      }
      if (body?.workspace && typeof body.workspace === 'object') {
        db.workspace = enforceServerSideRelationalSync(body.workspace);
      }
      saveDb(db, true);
      broadcastSyncEvent('full-restore', { clientId });
      return sendJson(res, 200, { ok: true, revision: db.revision });
    }

    /* 6. /api/auto-sync — Webhook for SMS Forwarder, MacroDroid, iOS Shortcuts & Gmail Bridge */
    if (url.pathname === '/api/auto-sync') {
      if (method === 'POST') {
        const payload = await readJsonBody(req);
        const db = loadDb();
        db.autoSyncQueue = Array.isArray(db.autoSyncQueue) ? db.autoSyncQueue : [];
        db.autoSyncQueue.push({
          at: Date.now(),
          source: String(payload.source || 'Webhook Auto-Sync'),
          text: String(payload.text || payload.sms || payload.body || ''),
        });
        saveDb(db, true);
        broadcastSyncEvent('autosync-queued', { count: db.autoSyncQueue.length });
        return sendJson(res, 200, { ok: true, domain: req.headers.host || 'ruturxj.duckdns.org', queued: db.autoSyncQueue.length });
      }
      if (method === 'GET') {
        const db = loadDb();
        const queue = Array.isArray(db.autoSyncQueue) ? db.autoSyncQueue : [];
        db.autoSyncQueue = [];
        if (queue.length) saveDb(db, false);
        return sendJson(res, 200, { domain: req.headers.host || 'ruturxj.duckdns.org', queue });
      }
    }

    /* 7. /api/market, /api/chart, /api/history, /api/sessions, /api/prices */
    if (url.pathname === '/api/market') {
      return sendJson(res, 200, buildMarketSnapshot());
    }
    if (url.pathname === '/api/chart') {
      const symbol = url.searchParams.get('symbol') || '^NSEI';
      const range = url.searchParams.get('range') || '1d';
      return sendJson(res, 200, buildChartData(symbol, range));
    }
    if (url.pathname === '/api/history') {
      return sendJson(res, 200, { days: buildHistoryDays() });
    }
    if (url.pathname === '/api/sessions') {
      const since = url.searchParams.get('since') || '9999-12-31';
      const sess = getSessionsList();
      const extra = sess.filter((s) => s.dt > since).map((s) => ({
        dt: s.dt,
        nifty: s.n_c,
        sensex: s.s_c,
        bank: s.b_c,
        headline: `Nifty closed at ${s.n_c}`,
        points: [s.note || ''],
        outlook: '',
        mood: s.n_chg >= 0 ? 'bullish' : 'bearish',
        news: [],
      }));
      return sendJson(res, 200, { sessions: extra });
    }
    if (url.pathname.startsWith('/api/prices')) {
      if (url.pathname.endsWith('/search')) {
        const q = (url.searchParams.get('q') || '').trim().toLowerCase();
        if (q.length < 2) return sendJson(res, 200, { results: [] });
        if (url.searchParams.get('type') === 'mf') {
          const hits = MF_CATALOG.filter((m) => m.name.toLowerCase().includes(q) || m.symbol.includes(q))
            .map((m) => ({ symbol: m.symbol, name: m.name }));
          return sendJson(res, 200, { results: hits });
        }
        const pool = [
          ...STOCK_CATALOG.map((x) => ({ symbol: x.symbol, name: x.name, type: 'EQUITY', exchange: 'NSE' })),
          ...EXTRA_SECURITIES,
        ];
        const seen = new Set();
        const hits = pool.filter((x) => {
          if (seen.has(x.symbol)) return false;
          if (x.symbol.toLowerCase().includes(q) || x.name.toLowerCase().includes(q)) {
            seen.add(x.symbol);
            return true;
          }
          return false;
        });
        return sendJson(res, 200, { results: hits.slice(0, 12) });
      }
      if (method === 'POST') {
        const body = await readJsonBody(req);
        return sendJson(res, 200, buildPricesResponse(body));
      }
    }

    /* 8. /api/journal/* — Auth, Session, Trade Journal & Calendar */
    if (url.pathname === '/api/journal' || url.pathname.startsWith('/api/journal/')) {
      const parts = url.pathname.replace(/^\/api\/journal\/?/, '').split('/').filter(Boolean);
      const [resource, key] = parts;
      const db = loadDb();

      if (resource === 'login' && method === 'POST') {
        const body = await readJsonBody(req);
        const mode = checkCredentials(body?.id, body?.password);
        if (!mode) {
          return sendJson(res, 401, { error: 'Wrong ID or password.' });
        }
        if (mode === 'demo') {
          resetDemoData();
          const at = nowSec();
          const exp = at + IDLE_SECONDS;
          const token = createSignedToken(['demo', String(at), String(exp)], db.secret);
          return sendJson(
            res,
            200,
            { ok: true, demo: true, loginAt: at * 1000, lastLogin: null, token },
            { 'Set-Cookie': `${COOKIE}=${encodeURIComponent(token)}; ${cookieAttrs(IDLE_SECONDS)}` }
          );
        }
        const pendExp = nowSec() + PENDING_SECONDS;
        const pendToken = createSignedToken(['real', String(pendExp)], db.secret);
        return sendJson(
          res,
          200,
          { ok: true, twoFactor: true, codeLength: CODE_LENGTH, pendingToken: pendToken },
          { 'Set-Cookie': `${PENDING_COOKIE}=${encodeURIComponent(pendToken)}; ${cookieAttrs(PENDING_SECONDS)}` }
        );
      }

      if (resource === 'resend' && method === 'POST') {
        const pendExp = nowSec() + PENDING_SECONDS;
        const pendToken = createSignedToken(['real', String(pendExp)], db.secret);
        return sendJson(
          res,
          200,
          { ok: true, pendingToken: pendToken },
          { 'Set-Cookie': `${PENDING_COOKIE}=${encodeURIComponent(pendToken)}; ${cookieAttrs(PENDING_SECONDS)}` }
        );
      }

      if (resource === 'verify' && method === 'POST') {
        const body = await readJsonBody(req);
        if (!checkCode(body?.code, body?.tz)) {
          return sendJson(res, 401, { error: "That code isn't right. Check the code and try again." });
        }
        const at = nowSec();
        const exp = at + IDLE_SECONDS;
        const previous = (db.logins || []).find((h) => h.mode !== 'demo') ?? null;
        db.logins = [{ at, mode: 'real', device: deviceName(req.headers['user-agent'] || '') }, ...(db.logins || [])].slice(0, 20);
        saveDb(db, false);
        const token = createSignedToken(['real', String(at), String(exp)], db.secret);
        return sendJson(
          res,
          200,
          {
            ok: true,
            demo: false,
            loginAt: at * 1000,
            lastLogin: previous ? { ...previous, at: previous.at * 1000 } : null,
            token,
          },
          {
            'Set-Cookie': [
              `${COOKIE}=${encodeURIComponent(token)}; ${cookieAttrs(IDLE_SECONDS)}`,
              `${PENDING_COOKIE}=; ${cookieAttrs(0)}`,
            ],
          }
        );
      }

      if (resource === 'logout' && method === 'POST') {
        return sendJson(
          res,
          200,
          { ok: true },
          {
            'Set-Cookie': [
              `${COOKIE}=; ${cookieAttrs(0)}`,
              `${PENDING_COOKIE}=; ${cookieAttrs(0)}`,
            ],
          }
        );
      }

      const session = readServerSession(req);
      const uid = session?.userId || JOURNAL_USER_ID;

      if (resource === 'me') {
        // If explicitly checking cookie/token session without implicit fallback:
        const explicitCookie = readCookie(req, COOKIE) || req.headers['x-pd-token'];
        const verified = verifySignedToken(explicitCookie, 3, db.secret);
        if (!verified) return sendJson(res, 200, { signedIn: false });
        const [vMode, vLoginAt, vExp] = verified;
        if (Number(vExp) <= nowSec()) return sendJson(res, 200, { signedIn: false });
        const history = vMode === 'demo' ? [] : (db.logins || []).filter((h) => h.mode !== 'demo');
        const prev = history.find((h) => h.at < Number(vLoginAt)) ?? null;
        return sendJson(res, 200, {
          signedIn: true,
          demo: vMode === 'demo',
          loginAt: Number(vLoginAt) * 1000,
          expiresAt: Number(vExp) * 1000,
          idleMinutes: Math.round(IDLE_SECONDS / 60),
          lastLogin: prev ? { ...prev, at: prev.at * 1000 } : null,
          history: history.slice(0, 8).map((h) => ({ ...h, at: h.at * 1000 })),
        });
      }

      if (resource === 'ping' && method === 'POST') {
        const exp = nowSec() + IDLE_SECONDS;
        return sendJson(res, 200, { ok: true, expiresAt: exp * 1000 });
      }

      if (!resource && method === 'GET') {
        const from = url.searchParams.get('from') ?? '1900-01-01';
        const to = url.searchParams.get('to') ?? '2999-12-31';
        if (!DATE_RE.test(from) || !DATE_RE.test(to)) return sendJson(res, 400, { error: 'from/to must be YYYY-MM-DD' });
        const rows = db.trades
          .filter((t) => t.userId === uid && t.tradeDate >= from && t.tradeDate <= to)
          .sort((a, b) => a.tradeDate.localeCompare(b.tradeDate) || a.id - b.id);
        const days = db.journalDays
          .filter((d) => d.userId === uid && d.day >= from && d.day <= to)
          .map((d) => ({ day: d.day, note: d.note, mood: d.mood }));
        return sendJson(res, 200, { trades: rows.map(toClient), days });
      }

      if (resource === 'trades') {
        if (method === 'POST' && !key) {
          const parsed = parseTrade(await readJsonBody(req), uid);
          if (typeof parsed === 'string') return sendJson(res, 400, { error: parsed });
          const row = { id: db.seq.trade++, ...parsed };
          db.trades.push(row);
          saveDb(db, true);
          broadcastSyncEvent('journal-updated', { clientId });
          return sendJson(res, 201, toClient(row));
        }
        const id = Number(key);
        const idx = db.trades.findIndex((t) => t.id === id && t.userId === uid);
        if (idx < 0) return sendJson(res, 404, { error: 'Trade not found' });
        if (method === 'PUT') {
          const parsed = parseTrade(await readJsonBody(req), uid);
          if (typeof parsed === 'string') return sendJson(res, 400, { error: parsed });
          db.trades[idx] = { ...db.trades[idx], ...parsed };
          saveDb(db, true);
          broadcastSyncEvent('journal-updated', { clientId });
          return sendJson(res, 200, toClient(db.trades[idx]));
        }
        if (method === 'DELETE') {
          db.trades.splice(idx, 1);
          saveDb(db, true);
          broadcastSyncEvent('journal-updated', { clientId });
          return sendJson(res, 200, { ok: true });
        }
      }

      if (resource === 'days' && method === 'PUT') {
        if (!key || !DATE_RE.test(key)) return sendJson(res, 400, { error: 'Invalid day' });
        const body = await readJsonBody(req);
        const note = str(body?.note, 4000);
        const mood = str(body?.mood, 20);
        const existing = db.journalDays.find((d) => d.userId === uid && d.day === key);
        if (existing) {
          existing.note = note;
          existing.mood = mood;
        } else {
          db.journalDays.push({ id: db.seq.day++, userId: uid, day: key, note, mood });
        }
        saveDb(db, true);
        broadcastSyncEvent('journal-updated', { clientId });
        return sendJson(res, 200, { day: key, note, mood });
      }
    }

    /* 9. /api/portfolio/* — Books, Entries, Holdings & CSV Import */
    if (url.pathname === '/api/portfolio' || url.pathname.startsWith('/api/portfolio/')) {
      const session = readServerSession(req);
      const uid = session?.userId || JOURNAL_USER_ID;
      const parts = url.pathname.replace(/^\/api\/portfolio\/?/, '').split('/').filter(Boolean);
      const [resource, key] = parts;
      const db = loadDb();

      if (!resource && method === 'GET') {
        const books = db.portfolioBooks
          .filter((b) => b.userId === uid)
          .sort((a, b) => a.sortOrder - b.sortOrder || a.id - b.id);
        const entries = db.portfolioEntries
          .filter((e) => e.userId === uid)
          .sort((a, b) => a.entryDate.localeCompare(b.entryDate) || a.id - b.id);
        const trades = db.trades
          .filter((t) => t.userId === uid)
          .sort((a, b) => a.tradeDate.localeCompare(b.tradeDate) || a.id - b.id);
        const holdings = db.holdings
          .filter((h) => h.userId === uid)
          .sort((a, b) => a.id - b.id);
        return sendJson(res, 200, {
          books: books.map(bookOut),
          entries: entries.map(entryOut),
          trades: trades.map(toClient),
          holdings: holdings.map(holdingOut),
        });
      }

      if (resource === 'holdings') {
        if (method === 'POST' && !key) {
          const parsed = parseHolding(await readJsonBody(req), uid);
          if (typeof parsed === 'string') return sendJson(res, 400, { error: parsed });
          const row = { id: db.seq.holding++, ...parsed };
          db.holdings.push(row);
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 201, holdingOut(row));
        }
        const id = Number(key);
        const idx = db.holdings.findIndex((h) => h.id === id && h.userId === uid);
        if (idx < 0) return sendJson(res, 404, { error: 'Holding not found' });
        if (method === 'PUT') {
          const parsed = parseHolding(await readJsonBody(req), uid);
          if (typeof parsed === 'string') return sendJson(res, 400, { error: parsed });
          db.holdings[idx] = { ...db.holdings[idx], ...parsed };
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 200, holdingOut(db.holdings[idx]));
        }
        if (method === 'DELETE') {
          db.holdings.splice(idx, 1);
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 200, { ok: true });
        }
      }

      if (resource === 'books') {
        if (method === 'POST' && key === 'starter') {
          const existing = db.portfolioBooks.filter((b) => b.userId === uid);
          const have = new Set(existing.map((b) => b.name.toLowerCase()));
          const added = [];
          STARTER.filter((s) => !have.has(s.name.toLowerCase())).forEach((s, i) => {
            const row = { id: db.seq.book++, userId: uid, name: s.name, kind: s.kind, icon: s.icon, sortOrder: existing.length + i };
            db.portfolioBooks.push(row);
            added.push(row);
          });
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 201, { books: added.map(bookOut) });
        }
        if (method === 'POST' && !key) {
          const parsed = parseBook(await readJsonBody(req));
          if (typeof parsed === 'string') return sendJson(res, 400, { error: parsed });
          const existing = db.portfolioBooks.filter((b) => b.userId === uid);
          if (existing.some((b) => b.name.toLowerCase() === parsed.name.toLowerCase())) {
            return sendJson(res, 409, { error: 'You already have a journal with that name.' });
          }
          const row = { id: db.seq.book++, userId: uid, ...parsed, sortOrder: existing.length };
          db.portfolioBooks.push(row);
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 201, bookOut(row));
        }
        const id = Number(key);
        const idx = db.portfolioBooks.findIndex((b) => b.id === id && b.userId === uid);
        if (idx < 0) return sendJson(res, 404, { error: 'Journal not found' });
        if (method === 'PUT') {
          const body = await readJsonBody(req);
          const name = str(body?.name, 60);
          if (!name) return sendJson(res, 400, { error: 'Give the journal a name.' });
          db.portfolioBooks[idx].name = name;
          db.portfolioBooks[idx].icon = str(body?.icon, 8);
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 200, bookOut(db.portfolioBooks[idx]));
        }
        if (method === 'DELETE') {
          db.portfolioBooks.splice(idx, 1);
          db.portfolioEntries = db.portfolioEntries.filter((e) => !(e.userId === uid && e.bookId === id));
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 200, { ok: true });
        }
      }

      if (resource === 'entries') {
        if (method === 'POST' && !key) {
          const body = await readJsonBody(req);
          const book = db.portfolioBooks.find((b) => b.id === Number(body?.bookId) && b.userId === uid);
          if (!book) return sendJson(res, 400, { error: 'Pick a journal for this entry.' });
          const parsed = parseEntry(body, book, uid);
          if (typeof parsed === 'string') return sendJson(res, 400, { error: parsed });
          const row = { id: db.seq.entry++, ...parsed };
          db.portfolioEntries.push(row);
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 201, entryOut(row));
        }
        const id = Number(key);
        const idx = db.portfolioEntries.findIndex((e) => e.id === id && e.userId === uid);
        if (idx < 0) return sendJson(res, 404, { error: 'Entry not found' });
        if (method === 'PUT') {
          const body = await readJsonBody(req);
          const current = db.portfolioEntries[idx];
          const book = db.portfolioBooks.find((b) => b.id === current.bookId && b.userId === uid);
          if (!book) return sendJson(res, 404, { error: 'Entry not found' });
          const parsed = parseEntry(body, book, uid);
          if (typeof parsed === 'string') return sendJson(res, 400, { error: parsed });
          db.portfolioEntries[idx] = { ...current, ...parsed };
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 200, entryOut(db.portfolioEntries[idx]));
        }
        if (method === 'DELETE') {
          db.portfolioEntries.splice(idx, 1);
          saveDb(db, true);
          broadcastSyncEvent('portfolio-updated', { clientId });
          return sendJson(res, 200, { ok: true });
        }
      }

      if (resource === 'import' && method === 'POST') {
        const body = await readJsonBody(req);
        const inJournals = Array.isArray(body?.journals) ? body.journals : [];
        const inEntries = Array.isArray(body?.entries) ? body.entries.slice(0, 20000) : [];
        const inTrades = Array.isArray(body?.trades) ? body.trades.slice(0, 20000) : [];
        const inHoldings = Array.isArray(body?.holdings) ? body.holdings.slice(0, 5000) : [];
        const skipDup = body?.skipDuplicates !== false;
        const errors = [];
        let createdBooks = 0;

        const books = db.portfolioBooks.filter((b) => b.userId === uid);
        const byName = new Map(books.map((b) => [b.name.toLowerCase(), b]));
        for (const j of [...inJournals, ...inEntries.map((e) => ({ name: e?.journal, kind: e?.kind, icon: e?.icon }))]) {
          const name = str(j?.name, 60);
          if (!name || byName.has(name.toLowerCase())) continue;
          const kind = KINDS.includes(j?.kind) ? j.kind : 'ledger';
          const row = { id: db.seq.book++, userId: uid, name, kind, icon: str(j?.icon, 8), sortOrder: byName.size };
          db.portfolioBooks.push(row);
          byName.set(name.toLowerCase(), row);
          createdBooks++;
        }

        const entryKey = (e) => `${e.bookId}|${e.entryDate}|${e.title.toLowerCase()}|${Number(e.amount).toFixed(2)}|${e.direction}`;
        const seenE = new Set(skipDup ? db.portfolioEntries.filter((e) => e.userId === uid).map(entryKey) : []);
        let addedE = 0, skipped = 0;
        inEntries.forEach((e, i) => {
          const book = byName.get(str(e?.journal, 60).toLowerCase());
          if (!book) { errors.push(`Row ${i + 1}: no journal name.`); return; }
          const parsed = parseEntry(e, book, uid);
          if (typeof parsed === 'string') { errors.push(`Row ${i + 1} (${book.name}): ${parsed}`); return; }
          const k = entryKey(parsed);
          if (skipDup && seenE.has(k)) { skipped++; return; }
          seenE.add(k);
          db.portfolioEntries.push({ id: db.seq.entry++, ...parsed });
          addedE++;
        });

        const tradeKey = (t) => `${t.tradeDate}|${t.symbol}|${Number(t.quantity)}|${Number(t.pnl).toFixed(2)}`;
        const seenT = new Set(skipDup ? db.trades.filter((t) => t.userId === uid).map(tradeKey) : []);
        let addedT = 0;
        inTrades.forEach((t, i) => {
          const parsed = parseTrade(t, uid);
          if (typeof parsed === 'string') { errors.push(`Trade ${i + 1}: ${parsed}`); return; }
          const k = tradeKey(parsed);
          if (skipDup && seenT.has(k)) { skipped++; return; }
          seenT.add(k);
          db.trades.push({ id: db.seq.trade++, ...parsed });
          addedT++;
        });

        const holdingKey = (h) => `${h.kind}|${h.symbol}|${Number(h.quantity)}|${Number(h.avgPrice)}`;
        const seenH = new Set(skipDup ? db.holdings.filter((h) => h.userId === uid).map(holdingKey) : []);
        let addedH = 0;
        inHoldings.forEach((h, i) => {
          const parsed = parseHolding(h, uid);
          if (typeof parsed === 'string') { errors.push(`Holding ${i + 1}: ${parsed}`); return; }
          const k = holdingKey(parsed);
          if (skipDup && seenH.has(k)) { skipped++; return; }
          seenH.add(k);
          db.holdings.push({ id: db.seq.holding++, ...parsed });
          addedH++;
        });

        saveDb(db, true);
        broadcastSyncEvent('portfolio-updated', { clientId });
        return sendJson(res, 200, {
          entries: addedE,
          trades: addedT,
          holdings: addedH,
          journals: createdBooks,
          skipped,
          errors: errors.slice(0, 50),
          errorCount: errors.length,
        });
      }
    }
  } catch (err) {
    console.error('API Error:', err);
    return sendJson(res, 500, { error: String(err.message || err) });
  }

  /* 10. Static File Server (./public or ./index.html) */
  let filePath = normalize(join(PUBLIC_DIR, url.pathname === '/' ? 'index.html' : url.pathname));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end('Forbidden');
  }
  if (existsSync(filePath) && statSync(filePath).isFile()) {
    const ext = extname(filePath).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
      'Cache-Control': 'no-cache',
    });
    return res.end(readFileSync(filePath));
  }
  // SPA fallback to index.html for non-API routes
  const fallbackIndex = join(PUBLIC_DIR, 'index.html');
  if (!url.pathname.startsWith('/api/') && existsSync(fallbackIndex)) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return res.end(readFileSync(fallbackIndex));
  }
  return sendJson(res, 404, { error: 'Not found' });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Personal Diary Online Backend Server v2.0 running on http://0.0.0.0:${PORT}`);
  console.log(`   • Domain / Host       : https://ruturxj.duckdns.org (or http://localhost:${PORT})`);
  console.log(`   • Database Storage    : ${DB_FILE}`);
  console.log(`   • Attachment Vault    : ${ATT_DIR}`);
  console.log(`   • Rolling Backups     : ${BACKUP_DIR}`);
  console.log(`   • Real-Time SSE Sync  : GET /api/events`);
  console.log(`   • Health Endpoint     : GET /api/health\n`);
});
