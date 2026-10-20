/**
 * Yathra product chat server (Express + MongoDB).
 *
 * Mount in your existing Render app:
 *     const chat = require('./chat-server');
 *     app.use('/api/chat', chat.router);      // after: await chat.init(process.env.MONGODB_URI)
 *
 * Or run standalone:  MONGODB_URI=... ADMIN_KEY=... node chat-server.js
 *
 * npm i express cors mongodb          (firebase-admin optional, see verifyUser)
 *
 * Env:
 *   MONGODB_URI        MongoDB Atlas URI. If missing, an in-memory store is used (dev only, data is lost on restart).
 *   ADMIN_KEY          secret for the seller/support endpoints (/admin/*). Required to reply.
 *   ALLOWED_ORIGINS    comma separated list, e.g. https://jsnvym.github.io  (default: allow all)
 *   DELETE_WINDOW_MS   "delete for everyone" window, default 48h
 *   FIREBASE_SERVICE_ACCOUNT   JSON string; if set, "Authorization: Bearer <idToken>" is verified and uid is used as owner
 *
 * Customer API (all under the mount path):
 *   POST /conversations                        {productKey, productName, price, image}      -> {id}
 *   GET  /conversations/:id/messages?since=&seen=1                                          -> {messages, cursor, presence}
 *   POST /conversations/:id/messages           {id, type:text|sticker|voice, text, media, dur, at} -> {message}
 *   POST /conversations/:id/media              raw body (image/* or audio/*)                -> {url}
 *   POST /conversations/:id/messages/:mid/delete {scope:'me'|'all'}                         -> {ok}
 *   GET  /media/:id                            serves uploaded media
 * Seller API (header x-admin-key):
 *   GET  /admin/conversations                                                              -> list with last message + unread
 *   GET  /admin/conversations/:id/messages?since=
 *   POST /admin/conversations/:id/messages     {type, text, media, dur}
 *   POST /admin/conversations/:id/messages/:mid/delete {scope}
 */
const express = require('express');
const cors = require('cors');
const crypto = require('crypto');

const DELETE_WINDOW_MS = +process.env.DELETE_WINDOW_MS || 48 * 3600 * 1000;
const MAX_TEXT = 2000;
const MAX_MEDIA = 2 * 1024 * 1024; // 2 MB (60 s voice at 24 kbps is ~200 KB, a sticker ~50 KB)
const uid = () => crypto.randomUUID();

/* ---------------- stores ---------------- */
function memoryStore() {
  const convs = new Map(), msgs = new Map(), media = new Map();
  let last = 0; const now = () => (last = Math.max(last + 1, Date.now() * 1000)); // strictly increasing
  return {
    kind: 'memory', now,
    async findConv(owner, productKey) { return [...convs.values()].find(c => c.owner === owner && c.productKey === productKey) || null; },
    async getConv(id) { return convs.get(id) || null; },
    async insertConv(c) { convs.set(c.id, c); return c; },
    async listConvs() { return [...convs.values()].sort((a, b) => b.updatedAt - a.updatedAt); },
    async touchConv(id, patch) { Object.assign(convs.get(id), patch); },
    async getMsg(cid, id) { return msgs.get(cid + ':' + id) || null; },
    async insertMsg(m) { msgs.set(m.cid + ':' + m.id, m); return m; },
    async updateMsg(m) { msgs.set(m.cid + ':' + m.id, m); },
    async msgsSince(cid, since) { return [...msgs.values()].filter(m => m.cid === cid && m.updatedAt > since).sort((a, b) => a.at - b.at); },
    async lastMsg(cid) { return [...msgs.values()].filter(m => m.cid === cid).sort((a, b) => b.at - a.at)[0] || null; },
    async unseen(cid, sender) { return [...msgs.values()].filter(m => m.cid === cid && m.sender === sender && m.status !== 'seen' && !m.deleted).length; },
    async markStatus(cid, otherSender, status, order) {
      const out = [];
      for (const m of msgs.values()) {
        if (m.cid === cid && m.sender === otherSender && order.indexOf(m.status) < order.indexOf(status)) { m.status = status; m.updatedAt = now(); out.push(m); }
      }
      return out;
    },
    async putMedia(d) { media.set(d.id, d); },
    async getMedia(id) { return media.get(id) || null; },
  };
}
async function mongoStore(uri) {
  const { MongoClient } = require('mongodb');
  const client = new MongoClient(uri); await client.connect();
  const db = client.db(process.env.MONGODB_DB || undefined);
  const C = db.collection('chat_conversations'), M = db.collection('chat_messages'), F = db.collection('chat_media');
  await C.createIndex({ owner: 1, productKey: 1 }, { unique: true });
  await M.createIndex({ cid: 1, id: 1 }, { unique: true });
  await M.createIndex({ cid: 1, updatedAt: 1 });
  let last = 0; const now = () => (last = Math.max(last + 1, Date.now() * 1000));
  const strip = d => d && (delete d._id, d);
  return {
    kind: 'mongo', now,
    async findConv(owner, productKey) { return strip(await C.findOne({ owner, productKey })); },
    async getConv(id) { return strip(await C.findOne({ id })); },
    async insertConv(c) { await C.insertOne({ ...c }); return c; },
    async listConvs() { return (await C.find({}).sort({ updatedAt: -1 }).limit(200).toArray()).map(strip); },
    async touchConv(id, patch) { await C.updateOne({ id }, { $set: patch }); },
    async getMsg(cid, id) { return strip(await M.findOne({ cid, id })); },
    async insertMsg(m) { await M.insertOne({ ...m }); return m; },
    async updateMsg(m) { await M.replaceOne({ cid: m.cid, id: m.id }, m); },
    async msgsSince(cid, since) { return (await M.find({ cid, updatedAt: { $gt: since } }).sort({ at: 1 }).limit(500).toArray()).map(strip); },
    async lastMsg(cid) { return strip(await M.find({ cid }).sort({ at: -1 }).limit(1).next()); },
    async unseen(cid, sender) { return M.countDocuments({ cid, sender, status: { $ne: 'seen' }, deleted: { $ne: true } }); },
    async markStatus(cid, otherSender, status, order) {
      const below = order.slice(0, order.indexOf(status));
      const docs = await M.find({ cid, sender: otherSender, status: { $in: below } }).toArray();
      if (!docs.length) return [];
      const t = now();
      await M.updateMany({ cid, sender: otherSender, status: { $in: below } }, { $set: { status, updatedAt: t } });
      return docs.map(d => ({ ...strip(d), status, updatedAt: t }));
    },
    async putMedia(d) { await F.insertOne({ ...d }); },
    async getMedia(id) { const d = await F.findOne({ id }); return d && { ...d, data: d.data.buffer ? Buffer.from(d.data.buffer) : d.data }; },
  };
}

/* ---------------- helpers ---------------- */
const ORDER = ['sent', 'delivered', 'seen'];
let store = memoryStore();
let verifyUser = async () => null;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  const admin = require('firebase-admin');
  admin.initializeApp({ credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
  verifyUser = async token => { try { return (await admin.auth().verifyIdToken(token)).uid; } catch { return null; } };
}
const wrap = fn => (req, res) => fn(req, res).catch(e => { console.error('[chat]', e); res.status(500).json({ error: 'server_error' }); });
const bad = (res, code, error) => res.status(code).json({ error });

/** The viewer is identified by Firebase uid if a valid token is sent, otherwise by the anonymous X-Client-Id. */
async function owner(req) {
  const h = req.get('authorization') || '';
  if (h.startsWith('Bearer ') && process.env.FIREBASE_SERVICE_ACCOUNT) { const u = await verifyUser(h.slice(7)); if (u) return 'u:' + u; }
  const c = req.get('x-client-id') || '';
  return /^[\w-]{8,64}$/.test(c) ? 'c:' + c : null;
}
const isAdmin = req => { const k = process.env.ADMIN_KEY; if (!k) return false; const a = Buffer.from(req.get('x-admin-key') || ''), b = Buffer.from(k); return a.length === b.length && crypto.timingSafeEqual(a, b); };

/** Server message -> what a viewer sees. viewer is 'user' or 'seller'. */
function view(m, viewer) {
  const hidden = (m.hiddenFor || []).includes(viewer);
  return {
    id: m.id, sender: m.sender, type: m.type, at: m.at, status: m.status,
    text: m.deleted ? '' : m.text, media: m.deleted ? '' : m.media, dur: m.dur || 0,
    deleted: !!m.deleted, hidden,
  };
}
function cleanMsg(b, cid, sender, store_) {
  const type = ['text', 'sticker', 'voice'].includes(b.type) ? b.type : null;
  if (!type) return { err: 'bad_type' };
  const text = String(b.text || '').slice(0, MAX_TEXT), media = String(b.media || '').slice(0, 500);
  if (type === 'text' && !text.trim()) return { err: 'empty' };
  if (type !== 'text' && !/^https?:\/\/[^\s]+$/.test(media)) return { err: 'bad_media' };
  const id = String(b.id || ''); if (!/^[\w-]{6,64}$/.test(id)) return { err: 'bad_id' };
  let at = +b.at || Date.now(); const n = Date.now(); if (at > n + 60000 || at < n - 7 * 864e5) at = n;
  const t = store_.now();
  return { m: { cid, id, sender, type, text: type === 'text' ? text : '', media: type === 'text' ? '' : media, dur: Math.min(+b.dur || 0, 120), at, status: 'sent', deleted: false, hiddenFor: [], createdAt: t, updatedAt: t } };
}

/* ---------------- router ---------------- */
const router = express.Router();
router.use(cors({ origin: process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',').map(s => s.trim()) : true, allowedHeaders: ['Content-Type', 'Authorization', 'X-Client-Id', 'X-Admin-Key'] }));
router.use(express.json({ limit: '50kb' }));

async function ownConv(req, res) {
  const o = await owner(req); if (!o) { bad(res, 401, 'no_identity'); return null; }
  const c = await store.getConv(req.params.id);
  if (!c || c.owner !== o) { bad(res, 404, 'not_found'); return null; }
  return c;
}

router.post('/conversations', wrap(async (req, res) => {
  const o = await owner(req); if (!o) return bad(res, 401, 'no_identity');
  const productKey = String(req.body.productKey || '').slice(0, 500); if (!productKey) return bad(res, 400, 'productKey');
  let c = await store.findConv(o, productKey);
  if (!c) {
    const t = store.now();
    try { c = await store.insertConv({ id: uid(), owner: o, productKey, productName: String(req.body.productName || '').slice(0, 200), price: String(req.body.price || '').slice(0, 20), image: String(req.body.image || '').slice(0, 500), createdAt: t, updatedAt: t }); }
    catch (e) { c = await store.findConv(o, productKey); if (!c) throw e; } // unique-index race
  }
  res.json({ id: c.id });
}));

router.get('/conversations/:id/messages', wrap(async (req, res) => {
  const c = await ownConv(req, res); if (!c) return;
  const since = +req.query.since || 0;
  // the seller's messages are now delivered (and seen, if the customer has the chat open)
  await store.markStatus(c.id, 'seller', 'delivered', ORDER);
  if (req.query.seen === '1') await store.markStatus(c.id, 'seller', 'seen', ORDER);
  const rows = await store.msgsSince(c.id, since);
  const cursor = rows.reduce((a, m) => Math.max(a, m.updatedAt), since);
  res.json({ messages: rows.map(m => view(m, 'user')), cursor, presence: 'Support' });
}));

router.post('/conversations/:id/messages', wrap(async (req, res) => {
  const c = await ownConv(req, res); if (!c) return;
  const { m, err } = cleanMsg(req.body || {}, c.id, 'user', store); if (err) return bad(res, 400, err);
  const old = await store.getMsg(c.id, m.id);
  if (old) return res.json({ message: view(old, 'user') }); // idempotent retry
  await store.insertMsg(m); await store.touchConv(c.id, { updatedAt: m.updatedAt });
  res.json({ message: view(m, 'user') });
}));

router.post('/conversations/:id/media', express.raw({ type: ['image/*', 'audio/*'], limit: MAX_MEDIA }), wrap(async (req, res) => {
  const c = await ownConv(req, res); if (!c) return;
  if (!Buffer.isBuffer(req.body) || !req.body.length) return bad(res, 400, 'empty');
  const type = (req.get('content-type') || '').split(';')[0];
  if (!/^(image\/(webp|png|jpeg|gif)|audio\/(webm|mp4|ogg|mpeg|aac|x-m4a))$/.test(type)) return bad(res, 400, 'bad_type');
  const id = uid(); await store.putMedia({ id, cid: c.id, type, data: req.body, at: Date.now() });
  res.json({ url: `${req.protocol}://${req.get('host')}${req.baseUrl}/media/${id}` });
}));
router.use((err, req, res, next) => (err && err.type === 'entity.too.large') ? bad(res, 413, 'too_large') : next(err));

// Media ids are unguessable UUIDs, same model as WhatsApp/Telegram media links.
router.get('/media/:id', wrap(async (req, res) => {
  if (!/^[\w-]{36}$/.test(req.params.id)) return bad(res, 404, 'not_found');
  const d = await store.getMedia(req.params.id); if (!d) return bad(res, 404, 'not_found');
  res.set({ 'Content-Type': d.type, 'Cache-Control': 'public, max-age=31536000, immutable', 'X-Content-Type-Options': 'nosniff', 'Cross-Origin-Resource-Policy': 'cross-origin' });
  res.send(d.data);
}));

async function doDelete(req, res, c, viewer) {
  const m = await store.getMsg(c.id, req.params.mid); if (!m) return bad(res, 404, 'not_found');
  const scope = req.body && req.body.scope;
  if (scope === 'me') {
    if (!(m.hiddenFor || []).includes(viewer)) { m.hiddenFor = [...(m.hiddenFor || []), viewer]; m.updatedAt = store.now(); await store.updateMsg(m); }
    return res.json({ ok: true });
  }
  if (scope === 'all') {
    if (m.sender !== viewer) return bad(res, 403, 'not_sender');
    if (Date.now() - m.at > DELETE_WINDOW_MS) return bad(res, 403, 'too_late');
    if (!m.deleted) { m.deleted = true; m.text = ''; m.media = ''; m.updatedAt = store.now(); await store.updateMsg(m); }
    return res.json({ ok: true });
  }
  bad(res, 400, 'bad_scope');
}
router.post('/conversations/:id/messages/:mid/delete', wrap(async (req, res) => { const c = await ownConv(req, res); if (c) await doDelete(req, res, c, 'user'); }));

/* ---- seller / support side ---- */
const admin = express.Router();
admin.use((req, res, next) => isAdmin(req) ? next() : bad(res, 401, 'admin_only'));
admin.get('/conversations', wrap(async (req, res) => {
  const out = [];
  for (const c of await store.listConvs()) {
    const last = await store.lastMsg(c.id);
    out.push({ id: c.id, productName: c.productName, productKey: c.productKey, image: c.image, price: c.price, updatedAt: c.updatedAt, unread: await store.unseen(c.id, 'user'), last: last && view(last, 'seller') });
  }
  res.json({ conversations: out });
}));
async function adminConv(req, res) { const c = await store.getConv(req.params.id); if (!c) { bad(res, 404, 'not_found'); return null; } return c; }
admin.get('/conversations/:id/messages', wrap(async (req, res) => {
  const c = await adminConv(req, res); if (!c) return;
  await store.markStatus(c.id, 'user', 'delivered', ORDER);
  await store.markStatus(c.id, 'user', 'seen', ORDER); // opening a thread in the dashboard marks it read
  const since = +req.query.since || 0, rows = await store.msgsSince(c.id, since);
  res.json({ messages: rows.map(m => view(m, 'seller')), cursor: rows.reduce((a, m) => Math.max(a, m.updatedAt), since) });
}));
admin.post('/conversations/:id/messages', wrap(async (req, res) => {
  const c = await adminConv(req, res); if (!c) return;
  const body = { ...req.body, id: (req.body && req.body.id) || uid() };
  const { m, err } = cleanMsg(body, c.id, 'seller', store); if (err) return bad(res, 400, err);
  await store.insertMsg(m); await store.touchConv(c.id, { updatedAt: m.updatedAt });
  res.json({ message: view(m, 'seller') });
}));
admin.post('/conversations/:id/messages/:mid/delete', wrap(async (req, res) => { const c = await adminConv(req, res); if (c) await doDelete(req, res, c, 'seller'); }));
router.use('/admin', admin);

async function init(uri) { store = uri ? await mongoStore(uri) : memoryStore(); console.log('[chat] store:', store.kind); return store; }

module.exports = { router, init };

if (require.main === module) {
  const app = express();
  app.set('trust proxy', 1); // Render terminates TLS; makes req.protocol = https for media URLs
  init(process.env.MONGODB_URI).then(() => {
    app.use('/api/chat', router);
    app.get('/health', (q, r) => r.json({ ok: true }));
    app.listen(process.env.PORT || 3000, () => console.log('chat server on', process.env.PORT || 3000));
  });
}
