"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeTcTokenManager = void 0;
const Utils_1 = require("../Utils");
const bounded_ttl_map_1 = require("../Utils/bounded-ttl-map");
const cache_utils_1 = __importDefault(require("../Utils/cache-utils"));
const cs_token_utils_1 = require("../Utils/cs-token-utils");
const make_mutex_1 = require("../Utils/make-mutex");
const tc_token_utils_1 = require("../Utils/tc-token-utils");
const WABinary_1 = require("../WABinary");
/**
 * Privacy token das mensagens 1:1 (tctoken, com cstoken de fallback): índice dos jids com token, mapa PN→LID,
 * emissão depois do envio, reemissão quando o contato troca de identidade e prune diário dos vencidos.
 * Código só do fork, separado do envio para não conflitar com o upstream.
 */
const makeTcTokenManager = ({ authState, logger, query }) => {
    const inFlightTcTokenIssuance = new Set();
    const TC_TOKEN_MAX_CONCURRENT_ISSUANCE = 2;
    // teto único pros dois caminhos: emissão pós-envio desiste quando não há vaga (a próxima
    // mensagem tenta de novo), reemissão por troca de identidade entra na fila e espera
    const tcTokenIssuanceSemaphore = (0, make_mutex_1.makeSemaphore)(TC_TOKEN_MAX_CONCURRENT_ISSUANCE);
    const TC_TOKEN_INDEX_FLUSH_MAX_PENDING = 100;
    const TC_TOKEN_INDEX_MAX_PENDING = 5000;
    const TC_TOKEN_INDEX_FLUSH_INTERVAL_MS = 30000;
    const pendingTcTokenIndexJids = new Set();
    const recentlyTrackedTcTokenJids = new bounded_ttl_map_1.BoundedTtlMap(5000, 24 * 60 * 60 * 1000);
    let tcTokenIndexFlushTimer;
    let tcTokenIndexFlushInFlight;
    let lastTcTokenIndexFullWarnMs = 0;
    const tcTokenIndexMutex = (0, make_mutex_1.makeMutex)();
    function armTcTokenIndexFlush() {
        if (tcTokenIndexFlushTimer || tcTokenIndexFlushInFlight)
            return;
        tcTokenIndexFlushTimer = setTimeout(() => {
            tcTokenIndexFlushTimer = undefined;
            void flushTcTokenIndex().catch(err => logger.warn({ err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao salvar índice de tctokens'));
        }, TC_TOKEN_INDEX_FLUSH_INTERVAL_MS);
    }
    function trackTcTokenJid(jid) {
        if (!jid || jid === tc_token_utils_1.TC_TOKEN_INDEX_KEY || recentlyTrackedTcTokenJids.has(jid))
            return;
        if (pendingTcTokenIndexJids.size >= TC_TOKEN_INDEX_MAX_PENDING) {
            if (Date.now() - lastTcTokenIndexFullWarnMs >= 60000) {
                lastTcTokenIndexFullWarnMs = Date.now();
                logger.warn({ pending: pendingTcTokenIndexJids.size }, 'fila do índice de tctokens cheia');
            }
            return;
        }
        recentlyTrackedTcTokenJids.set(jid, true);
        pendingTcTokenIndexJids.add(jid);
        if (pendingTcTokenIndexJids.size >= TC_TOKEN_INDEX_FLUSH_MAX_PENDING) {
            void flushTcTokenIndex().catch(err => logger.warn({ err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao salvar lote do índice de tctokens'));
        }
        else {
            armTcTokenIndexFlush();
        }
    }
    async function writePendingTcTokenIndex() {
        while (pendingTcTokenIndexJids.size) {
            const batch = [...pendingTcTokenIndexJids];
            pendingTcTokenIndexJids.clear();
            try {
                const write = await (0, tc_token_utils_1.buildMergedTcTokenIndexWrite)(authState.keys, batch);
                await authState.keys.set({ tctoken: write });
            }
            catch (err) {
                for (const jid of batch)
                    pendingTcTokenIndexJids.add(jid);
                throw err;
            }
        }
    }
    function flushTcTokenIndex() {
        if (tcTokenIndexFlushInFlight)
            return tcTokenIndexFlushInFlight;
        if (tcTokenIndexFlushTimer) {
            clearTimeout(tcTokenIndexFlushTimer);
            tcTokenIndexFlushTimer = undefined;
        }
        tcTokenIndexFlushInFlight = tcTokenIndexMutex.mutex(writePendingTcTokenIndex).finally(() => {
            tcTokenIndexFlushInFlight = undefined;
            if (pendingTcTokenIndexJids.size)
                armTcTokenIndexFlush();
        });
        return tcTokenIndexFlushInFlight;
    }
    function withFlushedTcTokenIndex(task) {
        return tcTokenIndexMutex.mutex(async () => {
            await writePendingTcTokenIndex();
            return task();
        });
    }
    const pnToLid = new bounded_ttl_map_1.BoundedTtlMap(5000, 10 * 60 * 1000);
    const getLidForPn = pnJid => pnToLid.get((0, WABinary_1.jidNormalizedUser)(pnJid)) || cache_utils_1.default.lidCache.get((0, WABinary_1.jidNormalizedUser)(pnJid));
    function cacheLidMapping(pnJid, lidJid) {
        if (!pnJid || !lidJid)
            return;
        const pn = (0, WABinary_1.jidNormalizedUser)(pnJid);
        const lid = (0, WABinary_1.jidNormalizedUser)(lidJid);
        if (!(0, WABinary_1.isJidUser)(pn) || !(0, WABinary_1.isLidUser)(lid))
            return;
        pnToLid.set(pn, lid);
        cache_utils_1.default.lidCache.set(pn, lid);
    }
    const tcTokenStorageJid = (jid) => (0, tc_token_utils_1.resolveTcTokenStorageJid)(jid, getLidForPn);
    async function buildCsTokenForJid(jid) {
        try {
            const recipientLid = tcTokenStorageJid(jid);
            if (!(0, WABinary_1.isLidUser)(recipientLid))
                return { reason: 'missing_lid' };
            const salt = await (0, cs_token_utils_1.readNctSalt)(authState.keys);
            if (!(salt === null || salt === void 0 ? void 0 : salt.length))
                return { reason: 'missing_nct_salt' };
            return { token: (0, cs_token_utils_1.generateCsToken)(salt, recipientLid) };
        }
        catch (err) {
            logger.debug({ jid, err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao gerar cstoken');
            return { reason: 'keystore_error' };
        }
    }
    /**
     * Anexa tctoken (ou cstoken como fallback) ao stanza de mensagem 1:1. Devolve se é envio 1:1 e o jid onde o
     * tctoken é guardado, para a emissão de tctoken depois do envio.
     */
    const appendPrivacyToken = async (stanza, { destinationJid, isGroup, isStatus, isNewsletter, isPeer, isRetry, participantJid, meIds }) => {
        var _a;
        const privacyTokenIntent = (0, tc_token_utils_1.resolvePrivacyTokenIntent)({
            isUserDestination: !!((0, WABinary_1.isJidUser)(destinationJid) || (0, WABinary_1.isLidUser)(destinationJid)),
            isGroup,
            isStatus,
            isNewsletter,
            isPeer,
            isRetry,
            hasParticipant: !!participantJid,
            isSelfParticipant: !!participantJid && meIds.some(meJid => (0, WABinary_1.areJidsSameUser)(participantJid, meJid))
        });
        const is1on1Send = privacyTokenIntent === 'send';
        const tcTokenJid = privacyTokenIntent !== 'none' ? tcTokenStorageJid(destinationJid) : undefined;
        if (!tcTokenJid) {
            return { is1on1Send, tcTokenJid };
        }
        let tcTokenEntry;
        let tcTokenReadFailed = false;
        try {
            tcTokenEntry = (await authState.keys.get('tctoken', [tcTokenJid]))[tcTokenJid];
        }
        catch (err) {
            tcTokenReadFailed = true;
            logger.debug({ jid: destinationJid, err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao ler tctoken');
        }
        let tcTokenBuffer = tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.token;
        let tcTokenState = tcTokenEntry
            ? (tcTokenBuffer === null || tcTokenBuffer === void 0 ? void 0 : tcTokenBuffer.length)
                ? 'ready'
                : tcTokenEntry.senderTimestamp !== undefined
                    ? 'awaiting_recipient'
                    : 'missing'
            : 'missing';
        if ((tcTokenBuffer === null || tcTokenBuffer === void 0 ? void 0 : tcTokenBuffer.length) && (0, tc_token_utils_1.isTcTokenExpired)(tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.timestamp)) {
            logger.debug({ jid: destinationJid, timestamp: tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.timestamp }, 'tctoken expired, clearing');
            tcTokenBuffer = undefined;
            tcTokenState = 'expired';
            const cleared = (tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.senderTimestamp) !== undefined
                ? { token: Buffer.alloc(0), senderTimestamp: tcTokenEntry.senderTimestamp }
                : null;
            try {
                await authState.keys.set({ tctoken: { [tcTokenJid]: cleared } });
            }
            catch (err) {
                logger.debug({ jid: destinationJid, err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao limpar tctoken vencido');
            }
        }
        const logPrivacyToken = (level, privacyTokenType, msg, extra = {}) => logger[level]({
            event: 'privacy_token_outgoing_message',
            msgId: stanza.attrs.id,
            recipient: (0, WABinary_1.jidNormalizedUser)(destinationJid),
            storageJid: tcTokenJid,
            privacyTokenType,
            tcTokenState,
            isretry: isRetry,
            ...extra
        }, msg);
        if (tcTokenBuffer === null || tcTokenBuffer === void 0 ? void 0 : tcTokenBuffer.length) {
            ;
            stanza.content.push({ tag: 'tctoken', attrs: {}, content: tcTokenBuffer });
            logPrivacyToken('info', 'tctoken', 'mensagem 1:1 protegida por tctoken');
            return { is1on1Send, tcTokenJid };
        }
        const csTokenResult = await buildCsTokenForJid(destinationJid);
        if ((_a = csTokenResult.token) === null || _a === void 0 ? void 0 : _a.length) {
            ;
            stanza.content.push({ tag: 'cstoken', attrs: {}, content: csTokenResult.token });
            logPrivacyToken('info', 'cstoken', 'mensagem 1:1 protegida por cstoken', { tcTokenReadFailed });
        }
        else {
            logPrivacyToken('warn', 'none', 'mensagem 1:1 sem privacy token', {
                reason: csTokenResult.reason,
                tcTokenReadFailed
            });
        }
        return { is1on1Send, tcTokenJid };
    };
    /** emite nosso token para o contato e guarda o dele, se o servidor devolver */
    async function requestAndStoreTcTokens(jid, storageJid, timestamp) {
        const result = await getPrivacyTokens([jid], timestamp);
        return (0, tc_token_utils_1.storeTcTokensFromIqResult)({
            result,
            fallbackJid: storageJid,
            keys: authState.keys,
            resolveLid: getLidForPn,
            onNewJidStored: trackTcTokenJid
        });
    }
    async function maybeIssueTcToken(jid, message, options) {
        var _a, _b, _c, _d, _e;
        try {
            if (options.participant || ((_a = options.additionalAttributes) === null || _a === void 0 ? void 0 : _a['category']) === 'peer')
                return;
            if ((_b = (0, Utils_1.normalizeMessageContent)(message)) === null || _b === void 0 ? void 0 : _b.protocolMessage)
                return;
            const current = await authState.keys.get('tctoken', [options.storageJid]);
            if (!(0, tc_token_utils_1.shouldSendNewTcToken)((_c = current[options.storageJid]) === null || _c === void 0 ? void 0 : _c.senderTimestamp))
                return;
            if (inFlightTcTokenIssuance.has(options.storageJid))
                return;
            if (!tcTokenIssuanceSemaphore.tryAcquire())
                return;
            inFlightTcTokenIssuance.add(options.storageJid);
            const issueTimestamp = (0, Utils_1.unixTimestampSeconds)();
            try {
                const storedJids = await requestAndStoreTcTokens(jid, options.storageJid, issueTimestamp);
                const afterEntry = (await authState.keys.get('tctoken', [options.storageJid]))[options.storageJid];
                const recipientTokenStored = storedJids.includes(options.storageJid);
                const recipientTokenPresent = !!((_d = afterEntry === null || afterEntry === void 0 ? void 0 : afterEntry.token) === null || _d === void 0 ? void 0 : _d.length);
                await authState.keys.set({
                    tctoken: {
                        [options.storageJid]: {
                            ...afterEntry,
                            token: (_e = afterEntry === null || afterEntry === void 0 ? void 0 : afterEntry.token) !== null && _e !== void 0 ? _e : Buffer.alloc(0),
                            senderTimestamp: issueTimestamp
                        }
                    }
                });
                trackTcTokenJid(options.storageJid);
                logger.info({
                    event: 'tc_token_issued',
                    msgId: options.msgId,
                    recipient: (0, WABinary_1.jidNormalizedUser)(jid),
                    storageJid: options.storageJid,
                    recipientTokenStored,
                    recipientTokenPresent
                }, recipientTokenStored
                    ? 'tc token emitido e token do destinatário persistido'
                    : recipientTokenPresent
                        ? 'tc token emitido; token existente preservado'
                        : 'tc token emitido; aguardando token do destinatário');
            }
            finally {
                inFlightTcTokenIssuance.delete(options.storageJid);
                tcTokenIssuanceSemaphore.release();
            }
        }
        catch (err) {
            logger.debug({ jid, err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao emitir tctoken');
        }
    }
    /**
     * Quando o contato troca de identidade Signal, o token que emitimos pra ele deixa de valer.
     * Reemite reusando o senderTimestamp armazenado, pra não avançar o bucket de emissão.
     */
    async function reissueTcTokenAfterIdentityChange(jid) {
        var _a, _b, _c;
        try {
            // só a identidade do device primário conta; companion trocando de chave não invalida o token
            if ((_a = (0, WABinary_1.jidDecode)(jid)) === null || _a === void 0 ? void 0 : _a.device)
                return;
            // troca da nossa própria identidade não é reach-out: não há token nosso pra reemitir
            if ((0, WABinary_1.areJidsSameUser)(jid, (_b = authState.creds.me) === null || _b === void 0 ? void 0 : _b.id) ||
                (0, WABinary_1.areJidsSameUser)(jid, (_c = authState.creds.me) === null || _c === void 0 ? void 0 : _c.lid)) {
                return;
            }
            if (!(0, tc_token_utils_1.isRegularUser)((0, WABinary_1.jidNormalizedUser)(jid)))
                return;
            const storageJid = tcTokenStorageJid(jid);
            const entry = (await authState.keys.get('tctoken', [storageJid]))[storageJid];
            const senderTimestamp = entry === null || entry === void 0 ? void 0 : entry.senderTimestamp;
            // nunca emitimos pra esse contato, ou a janela do emissor já expirou: nada a reemitir
            if (senderTimestamp === undefined || (0, tc_token_utils_1.isTcTokenExpired)(senderTimestamp))
                return;
            if (inFlightTcTokenIssuance.has(storageJid))
                return;
            inFlightTcTokenIssuance.add(storageJid);
            try {
                // espera vaga em vez de desistir: reemissão não tem segunda chance, ao contrário
                // da emissão pós-envio, que a próxima mensagem repete
                await tcTokenIssuanceSemaphore.acquire();
                try {
                    const storedJids = await requestAndStoreTcTokens(jid, storageJid, senderTimestamp);
                    logger.info({
                        event: 'tc_token_reissued_identity_change',
                        recipient: (0, WABinary_1.jidNormalizedUser)(jid),
                        storageJid,
                        senderTimestamp,
                        recipientTokenStored: storedJids.includes(storageJid)
                    }, 'tc token reemitido após troca de identidade');
                }
                finally {
                    tcTokenIssuanceSemaphore.release();
                }
            }
            finally {
                inFlightTcTokenIssuance.delete(storageJid);
            }
        }
        catch (err) {
            logger.debug({ jid, err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao reemitir tctoken após troca de identidade');
        }
    }
    const getPrivacyTokens = async (jids, timestamp) => {
        const t = (timestamp !== null && timestamp !== void 0 ? timestamp : (0, Utils_1.unixTimestampSeconds)()).toString();
        const result = await query({
            tag: 'iq',
            attrs: {
                to: WABinary_1.S_WHATSAPP_NET,
                type: 'set',
                xmlns: 'privacy'
            },
            content: [
                {
                    tag: 'tokens',
                    attrs: {},
                    content: jids.map(jid => ({
                        tag: 'token',
                        attrs: {
                            jid: (0, WABinary_1.jidNormalizedUser)(jid),
                            t,
                            type: 'trusted_contact'
                        }
                    }))
                }
            ]
        });
        return result;
    };
    const TC_TOKEN_PRUNE_BATCH = 20;
    const TC_TOKEN_PRUNE_INTERVAL = 24 * 60 * 60;
    const TC_TOKEN_PRUNE_MAX_JITTER_MS = 15 * 60 * 1000;
    let tcTokenPruneInFlight = false;
    let tcTokenPruneTimer;
    function scheduleTcTokenPrune() {
        if (tcTokenPruneTimer || tcTokenPruneInFlight)
            return;
        tcTokenPruneTimer = setTimeout(() => {
            tcTokenPruneTimer = undefined;
            void maybePruneExpiredTcTokens();
        }, Math.floor(Math.random() * TC_TOKEN_PRUNE_MAX_JITTER_MS));
    }
    async function maybePruneExpiredTcTokens() {
        if (tcTokenPruneInFlight)
            return;
        tcTokenPruneInFlight = true;
        try {
            const lastPrune = await (0, tc_token_utils_1.readLastTcTokenPruneTs)(authState.keys);
            if ((0, Utils_1.unixTimestampSeconds)() - lastPrune >= TC_TOKEN_PRUNE_INTERVAL) {
                await withFlushedTcTokenIndex(runPruneExpiredTcTokens);
            }
        }
        catch (err) {
            logger.warn({ err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao executar prune de tctokens');
        }
        finally {
            tcTokenPruneInFlight = false;
        }
    }
    async function runPruneExpiredTcTokens() {
        var _a, _b;
        const persisted = await (0, tc_token_utils_1.readTcTokenIndex)(authState.keys);
        if (!persisted.length) {
            await authState.keys.set({
                tctoken: { [tc_token_utils_1.TC_TOKEN_INDEX_KEY]: (0, tc_token_utils_1.buildTcTokenIndexEntry)([], (0, Utils_1.unixTimestampSeconds)()) }
            });
            return;
        }
        const survivors = new Set();
        let mutated = 0;
        for (let offset = 0; offset < persisted.length; offset += TC_TOKEN_PRUNE_BATCH) {
            const batch = persisted.slice(offset, offset + TC_TOKEN_PRUNE_BATCH);
            const tokens = await authState.keys.get('tctoken', batch);
            const writes = {};
            for (const jid of batch) {
                const entry = tokens[jid];
                if (!entry) {
                    mutated += 1;
                    continue;
                }
                const keepPeerToken = !!((_a = entry.token) === null || _a === void 0 ? void 0 : _a.length) && !(0, tc_token_utils_1.isTcTokenExpired)(entry.timestamp);
                const keepSenderTs = entry.senderTimestamp !== undefined && !(0, tc_token_utils_1.isTcTokenExpired)(entry.senderTimestamp);
                if (!keepPeerToken && !keepSenderTs) {
                    writes[jid] = null;
                    mutated += 1;
                }
                else if (!keepPeerToken && keepSenderTs && ((_b = entry.token) === null || _b === void 0 ? void 0 : _b.length)) {
                    writes[jid] = { token: Buffer.alloc(0), senderTimestamp: entry.senderTimestamp };
                    survivors.add(jid);
                    mutated += 1;
                }
                else {
                    survivors.add(jid);
                }
            }
            if (Object.keys(writes).length)
                await authState.keys.set({ tctoken: writes });
        }
        await authState.keys.set({
            tctoken: {
                [tc_token_utils_1.TC_TOKEN_INDEX_KEY]: (0, tc_token_utils_1.buildTcTokenIndexEntry)(survivors, (0, Utils_1.unixTimestampSeconds)())
            }
        });
        logger.debug({ mutated, remaining: survivors.size }, 'tctokens expirados removidos');
    }
    function cancelTcTokenPrune() {
        if (tcTokenPruneTimer) {
            clearTimeout(tcTokenPruneTimer);
            tcTokenPruneTimer = undefined;
        }
    }
    return {
        trackTcTokenJid,
        flushTcTokenIndex,
        withFlushedTcTokenIndex,
        getLidForPn,
        cacheLidMapping,
        tcTokenStorageJid,
        appendPrivacyToken,
        maybeIssueTcToken,
        reissueTcTokenAfterIdentityChange,
        getPrivacyTokens,
        scheduleTcTokenPrune,
        cancelTcTokenPrune
    };
};
exports.makeTcTokenManager = makeTcTokenManager;
