"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TC_TOKEN_INDEX_KEY = void 0;
exports.isRegularUser = isRegularUser;
exports.resolveTcTokenStorageJid = resolveTcTokenStorageJid;
exports.readTcTokenIndex = readTcTokenIndex;
exports.readLastTcTokenPruneTs = readLastTcTokenPruneTs;
exports.buildMergedTcTokenIndexWrite = buildMergedTcTokenIndexWrite;
exports.buildTcTokenIndexEntry = buildTcTokenIndexEntry;
exports.isTcTokenExpired = isTcTokenExpired;
exports.shouldSendNewTcToken = shouldSendNewTcToken;
exports.buildTcTokenFromJid = buildTcTokenFromJid;
exports.storeTcTokensFromIqResult = storeTcTokensFromIqResult;
exports.storeTcTokensFromHistorySync = storeTcTokensFromHistorySync;
const WABinary_1 = require("../WABinary");
const generics_1 = require("./generics");
const BOT_PHONE_REGEX = /^1313555\d{4}$|^131655500\d{2}$/;
const TC_TOKEN_BUCKET_DURATION = 604800;
const TC_TOKEN_NUM_BUCKETS = 4;
const TC_TOKEN_HISTORY_BATCH_SIZE = 20;
/** Chave sentinela com o índice dos JIDs rastreados e o timestamp do último prune. */
exports.TC_TOKEN_INDEX_KEY = '__index';
function isRegularUser(jid) {
    var _a;
    if (!jid)
        return false;
    const user = (_a = jid.split('@')[0]) !== null && _a !== void 0 ? _a : '';
    if (user === '0' || BOT_PHONE_REGEX.test(user) || (0, WABinary_1.isJidMetaIa)(jid))
        return false;
    return !!((0, WABinary_1.isJidUser)(jid) || (0, WABinary_1.isLidUser)(jid) || jid.endsWith('@c.us'));
}
/** Usa LID como chave quando conhecido, igual ao cliente oficial; PN é apenas fallback. */
function resolveTcTokenStorageJid(jid, resolveLid) {
    const base = (0, WABinary_1.jidNormalizedUser)(jid) || jid;
    if ((0, WABinary_1.isLidUser)(base))
        return base;
    const lid = resolveLid === null || resolveLid === void 0 ? void 0 : resolveLid(base);
    return lid ? (0, WABinary_1.jidNormalizedUser)(lid) || base : base;
}
async function readTcTokenIndexEntry(keys) {
    const data = await keys.get('tctoken', [exports.TC_TOKEN_INDEX_KEY]);
    return data[exports.TC_TOKEN_INDEX_KEY];
}
function parseTcTokenIndex(entry) {
    var _a;
    if (!((_a = entry === null || entry === void 0 ? void 0 : entry.token) === null || _a === void 0 ? void 0 : _a.length))
        return [];
    try {
        const parsed = JSON.parse(Buffer.from(entry.token).toString());
        if (!Array.isArray(parsed))
            return [];
        return parsed.filter((jid) => typeof jid === 'string' && jid.length > 0 && jid !== exports.TC_TOKEN_INDEX_KEY);
    }
    catch (_b) {
        return [];
    }
}
async function readTcTokenIndex(keys) {
    return parseTcTokenIndex(await readTcTokenIndexEntry(keys));
}
async function readLastTcTokenPruneTs(keys) {
    var _a;
    const timestamp = Number((_a = (await readTcTokenIndexEntry(keys))) === null || _a === void 0 ? void 0 : _a.timestamp);
    return Number.isFinite(timestamp) && timestamp > 0 ? timestamp : 0;
}
async function buildMergedTcTokenIndexWrite(keys, addedJids) {
    const entry = await readTcTokenIndexEntry(keys);
    const merged = new Set(parseTcTokenIndex(entry));
    for (const jid of addedJids) {
        if (jid && jid !== exports.TC_TOKEN_INDEX_KEY)
            merged.add(jid);
    }
    return {
        [exports.TC_TOKEN_INDEX_KEY]: buildTcTokenIndexEntry(merged, entry === null || entry === void 0 ? void 0 : entry.timestamp)
    };
}
function buildTcTokenIndexEntry(jids, pruneTs) {
    return {
        token: Buffer.from(JSON.stringify([...jids])),
        ...(pruneTs !== undefined ? { timestamp: String(pruneTs) } : {})
    };
}
function isTcTokenExpired(timestamp) {
    if (timestamp === null || timestamp === undefined)
        return true;
    const ts = typeof timestamp === 'string' ? parseInt(timestamp) : timestamp;
    if (isNaN(ts))
        return true;
    const currentBucket = Math.floor(Date.now() / 1000 / TC_TOKEN_BUCKET_DURATION);
    const cutoffTimestamp = (currentBucket - (TC_TOKEN_NUM_BUCKETS - 1)) * TC_TOKEN_BUCKET_DURATION;
    return ts < cutoffTimestamp;
}
function shouldSendNewTcToken(senderTimestamp) {
    if (senderTimestamp === undefined)
        return true;
    const currentBucket = Math.floor(Date.now() / 1000 / TC_TOKEN_BUCKET_DURATION);
    const senderBucket = Math.floor(senderTimestamp / TC_TOKEN_BUCKET_DURATION);
    return currentBucket > senderBucket;
}
async function buildTcTokenFromJid({ keys, jid, baseContent = [], resolveLid }) {
    try {
        const storageJid = resolveTcTokenStorageJid(jid, resolveLid);
        const entry = (await keys.get('tctoken', [storageJid]))[storageJid];
        const token = entry === null || entry === void 0 ? void 0 : entry.token;
        if (!(token === null || token === void 0 ? void 0 : token.length) || isTcTokenExpired(entry === null || entry === void 0 ? void 0 : entry.timestamp)) {
            if (token === null || token === void 0 ? void 0 : token.length) {
                const cleared = entry.senderTimestamp !== undefined
                    ? { token: Buffer.alloc(0), senderTimestamp: entry.senderTimestamp }
                    : null;
                await keys.set({ tctoken: { [storageJid]: cleared } });
            }
            return baseContent.length ? baseContent : undefined;
        }
        baseContent.push({ tag: 'tctoken', attrs: {}, content: token });
        return baseContent;
    }
    catch (_a) {
        return baseContent.length ? baseContent : undefined;
    }
}
/** Persiste tctokens recebidos por IQ/notificação usando o remetente, nunca o JID do próprio device. */
async function storeTcTokensFromIqResult({ result, fallbackJid, keys, resolveLid, onNewJidStored }) {
    const tokensNode = (0, WABinary_1.getBinaryNodeChild)(result, 'tokens');
    if (!tokensNode)
        return [];
    const storedJids = [];
    for (const tokenNode of (0, WABinary_1.getBinaryNodeChildren)(tokensNode, 'token')) {
        if (tokenNode.attrs.type !== 'trusted_contact' || !(tokenNode.content instanceof Uint8Array))
            continue;
        const storageJid = resolveTcTokenStorageJid(fallbackJid || tokenNode.attrs.jid, resolveLid);
        if (!isRegularUser(storageJid))
            continue;
        const incomingTs = tokenNode.attrs.t ? Number(tokenNode.attrs.t) : 0;
        if (!incomingTs)
            continue;
        const existingEntry = (await keys.get('tctoken', [storageJid]))[storageJid];
        const existingTs = (existingEntry === null || existingEntry === void 0 ? void 0 : existingEntry.timestamp) ? Number(existingEntry.timestamp) : 0;
        if (existingTs > incomingTs)
            continue;
        await keys.set({
            tctoken: {
                [storageJid]: {
                    ...existingEntry,
                    token: Buffer.from(tokenNode.content),
                    timestamp: tokenNode.attrs.t
                }
            }
        });
        onNewJidStored === null || onNewJidStored === void 0 ? void 0 : onNewJidStored(storageJid);
        storedJids.push(storageJid);
    }
    return storedJids;
}
function toSecondsTimestamp(value) {
    if (value === undefined || value === null)
        return 0;
    const timestamp = (0, generics_1.toNumber)(value);
    if (!Number.isFinite(timestamp) || timestamp <= 0)
        return 0;
    return timestamp > 1e12 ? Math.floor(timestamp / 1000) : timestamp;
}
/** Importa somente os tctokens das conversas do history sync, em lotes limitados. */
async function storeTcTokensFromHistorySync(chats, keyStore, logger, resolveLid) {
    var _a;
    const candidates = [];
    for (const chat of chats) {
        const timestamp = toSecondsTimestamp(chat.tcTokenTimestamp);
        if (!((_a = chat.tcToken) === null || _a === void 0 ? void 0 : _a.length) || timestamp <= 0 || !chat.id)
            continue;
        candidates.push({
            storageJid: resolveTcTokenStorageJid(chat.lidJid || chat.id, resolveLid),
            token: Buffer.from(chat.tcToken),
            ts: timestamp,
            senderTs: chat.tcTokenSenderTimestamp
                ? toSecondsTimestamp(chat.tcTokenSenderTimestamp)
                : undefined
        });
    }
    if (!candidates.length)
        return 0;
    const storedJids = new Set();
    try {
        for (let offset = 0; offset < candidates.length; offset += TC_TOKEN_HISTORY_BATCH_SIZE) {
            const batch = candidates.slice(offset, offset + TC_TOKEN_HISTORY_BATCH_SIZE);
            const existing = await keyStore.get('tctoken', batch.map(candidate => candidate.storageJid));
            const entries = {};
            for (const candidate of batch) {
                const current = existing[candidate.storageJid];
                const currentTs = (current === null || current === void 0 ? void 0 : current.timestamp) ? Number(current.timestamp) : 0;
                if (currentTs >= candidate.ts)
                    continue;
                entries[candidate.storageJid] = {
                    ...current,
                    token: candidate.token,
                    timestamp: String(candidate.ts),
                    ...(candidate.senderTs !== undefined ? { senderTimestamp: candidate.senderTs } : {})
                };
            }
            if (Object.keys(entries).length) {
                await keyStore.set({ tctoken: entries });
                for (const jid of Object.keys(entries))
                    storedJids.add(jid);
            }
        }
        if (storedJids.size) {
            const index = await buildMergedTcTokenIndexWrite(keyStore, storedJids);
            await keyStore.set({ tctoken: index });
            logger === null || logger === void 0 ? void 0 : logger.info({ event: 'tc_tokens_history_sync_imported', count: storedJids.size }, 'tctokens importados do history sync');
        }
    }
    catch (err) {
        logger === null || logger === void 0 ? void 0 : logger.warn({ err }, 'falha ao importar tctokens do history sync');
    }
    return storedJids.size;
}
