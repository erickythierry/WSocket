"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.initAuthCreds = exports.addTransactionCapability = void 0;
exports.makeCacheableSignalKeyStore = makeCacheableSignalKeyStore;
const async_hooks_1 = require("async_hooks");
const crypto_1 = require("crypto");
const Defaults_1 = require("../Defaults");
const crypto_2 = require("./crypto");
const generics_1 = require("./generics");
const make_mutex_1 = require("./make-mutex");
/**
 * Map com TTL a partir do set e teto de entradas, sem timer e sem estatística.
 * O @cacheable/node-cache media o tamanho do valor em todo set (65 ms para 400 KB) e deixava um timer vivo por socket.
 * A ordem de inserção do Map é a ordem de vencimento (TTL fixo, get não renova), então a limpeza olha só a frente.
 */
class SignalStoreCache {
    constructor(maxEntries, ttlMs) {
        this.maxEntries = maxEntries;
        this.ttlMs = ttlMs;
        this.entries = new Map();
    }
    get(key) {
        const entry = this.entries.get(key);
        if (!entry) {
            return undefined;
        }
        if (entry.expiresAt <= Date.now()) {
            this.entries.delete(key);
            return undefined;
        }
        return entry.value;
    }
    set(key, value) {
        this.entries.delete(key);
        this.entries.set(key, { value, expiresAt: Date.now() + this.ttlMs });
        const now = Date.now();
        for (const [oldKey, entry] of this.entries) {
            if (this.entries.size <= this.maxEntries && entry.expiresAt > now) {
                break;
            }
            this.entries.delete(oldKey);
        }
    }
    del(key) {
        this.entries.delete(key);
    }
    flushAll() {
        this.entries.clear();
    }
}
const SIGNAL_STORE_MAX_KEYS = 10000;
/**
 * Adds caching capability to a SignalKeyStore
 * @param store the store to add caching to
 * @param logger to log trace events
 * @param _cache cache store to use
 */
function makeCacheableSignalKeyStore(store, logger, _cache) {
    const cache = _cache || new SignalStoreCache(SIGNAL_STORE_MAX_KEYS, Defaults_1.DEFAULT_CACHE_TTLS.SIGNAL_STORE * 1000);
    function getUniqueId(type, id) {
        return `${type}.${id}`;
    }
    return {
        async get(type, ids) {
            const data = {};
            const idsToFetch = [];
            for (const id of ids) {
                const item = cache.get(getUniqueId(type, id));
                if (typeof item !== 'undefined') {
                    data[id] = item;
                }
                else {
                    idsToFetch.push(id);
                }
            }
            if (idsToFetch.length) {
                logger === null || logger === void 0 ? void 0 : logger.trace({ items: idsToFetch.length }, 'loading from store');
                const fetched = await store.get(type, idsToFetch);
                for (const id of idsToFetch) {
                    // um set durante o await já pôs o valor novo no cache: o lido do store é velho
                    const cached = cache.get(getUniqueId(type, id));
                    if (typeof cached !== 'undefined') {
                        data[id] = cached;
                        continue;
                    }
                    const item = fetched[id];
                    if (item) {
                        data[id] = item;
                        cache.set(getUniqueId(type, id), item);
                    }
                }
            }
            return data;
        },
        async set(data) {
            await store.set(data);
            let keys = 0;
            for (const type in data) {
                for (const id in data[type]) {
                    cache.set(getUniqueId(type, id), data[type][id]);
                    keys += 1;
                }
            }
            logger === null || logger === void 0 ? void 0 : logger.trace({ keys }, 'updated cache');
        },
        async clear() {
            var _a;
            cache.flushAll();
            await ((_a = store.clear) === null || _a === void 0 ? void 0 : _a.call(store));
        }
    };
}
/** um ALS por processo: um por socket marca todo recurso assíncrono do processo e vazava heap no Node 22 */
const transactionStorage = new async_hooks_1.AsyncLocalStorage();
const LONG_TRANSACTION_MS = 30000;
/** tipos cujo estado só avança: gravar um avanço não usado é inofensivo, perder um usado quebra a sessão */
const FORWARD_SAFE_TYPES = new Set(['session', 'sender-key', 'pre-key']);
/**
 * Adds DB like transaction capability (https://en.wikipedia.org/wiki/Database_transaction) to the SignalKeyStore,
 * this allows batch read & write operations & improves the performance of the lib
 *
 * Cada transação tem as próprias mutações e commita quando termina, sem esperar as outras. Antes era um
 * contador por socket: nada ia ao banco até a última transação sobreposta terminar, e se ela falhasse as
 * mutações das que deram certo sumiam (ratchet já usado na rede e não salvo).
 *
 * A leitura enxerga as escritas ainda não commitadas de todas as transações abertas (a mais nova vence): é o
 * que mantém encrypt e decrypt coerentes em memória, como o cache compartilhado fazia. A fila do libsignal por
 * endereço e por sender key é o lock por registro. Cada escrita leva uma sequência, e o commit não grava uma
 * escrita mais velha que a última gravada para a mesma chave.
 *
 * @param state the key store to apply this capability to
 * @param logger logger to log events
 * @returns SignalKeyStore with transaction capability
 */
const addTransactionCapability = (state, logger, { maxCommitRetries, delayBetweenTriesMs }) => {
    const owner = {};
    let seq = 0;
    /** escritas ainda não gravadas, por chave, da mais velha para a mais nova */
    const pending = new Map();
    /** maior seq já gravado das chaves que ainda têm escrita pendente */
    const committedSeq = new Map();
    /** gravações que disputam chave com outra transação vão em série, para o banco ver a ordem das seqs */
    const commitMutex = (0, make_mutex_1.makeMutex)();
    /** escrita direta sem conflito em voo, por chave: o commit de transação espera ela antes de gravar */
    const directInFlight = new Map();
    const keyOf = (type, id) => `${type}\u0000${id}`;
    const currentTx = () => {
        const ctx = transactionStorage.getStore();
        // callback que escapou da transação (void, nextTick) depois do commit grava direto
        return (ctx === null || ctx === void 0 ? void 0 : ctx.owner) === owner && !ctx.done ? ctx : undefined;
    };
    /** escrita pendente mais nova que o último valor gravado; undefined manda ler do store */
    const latestPending = (key) => {
        var _a;
        const writes = pending.get(key);
        const write = (writes === null || writes === void 0 ? void 0 : writes.length) ? writes[writes.length - 1] : undefined;
        return write && write.seq >= ((_a = committedSeq.get(key)) !== null && _a !== void 0 ? _a : 0) ? write : undefined;
    };
    const addPending = (key, write) => {
        const writes = pending.get(key);
        if (writes) {
            writes.push(write);
        }
        else {
            pending.set(key, [write]);
        }
    };
    const removePending = (key, predicate) => {
        const writes = pending.get(key);
        if (!writes) {
            return;
        }
        const left = writes.filter(write => !predicate(write));
        if (left.length) {
            pending.set(key, left);
        }
        else {
            pending.delete(key);
            committedSeq.delete(key);
        }
    };
    const writeWithRetry = async (data) => {
        let tries = maxCommitRetries;
        for (;;) {
            try {
                await state.set(data);
                return;
            }
            catch (error) {
                tries -= 1;
                logger.warn(`failed to commit ${Object.keys(data).length} mutation types, tries left=${tries}`);
                if (tries <= 0) {
                    throw error;
                }
                await (0, generics_1.delay)(delayBetweenTriesMs);
            }
        }
    };
    /** grava as escritas da transação que ainda são as mais novas gravadas para cada chave */
    const commit = (ctx, types) => commitMutex.mutex(async () => {
        var _a, _b;
        const batch = {};
        const written = [];
        for (const type in ctx.mutations) {
            if (types && !types.has(type)) {
                continue;
            }
            for (const id in ctx.mutations[type]) {
                const key = keyOf(type, id);
                const writeSeq = ctx.seqs.get(key);
                if (writeSeq > ((_a = committedSeq.get(key)) !== null && _a !== void 0 ? _a : 0)) {
                    batch[type] || (batch[type] = {});
                    batch[type][id] = ctx.mutations[type][id];
                    written.push([key, writeSeq]);
                }
            }
        }
        if (!written.length) {
            return;
        }
        // escrita direta que começou sem conflito pode estar indo ao banco agora: grava depois dela
        const inFlight = written.map(([key]) => directInFlight.get(key)).filter(Boolean);
        if (inFlight.length) {
            await Promise.allSettled(inFlight);
        }
        await writeWithRetry(batch);
        for (const [key, writeSeq] of written) {
            if (pending.has(key)) {
                committedSeq.set(key, Math.max((_b = committedSeq.get(key)) !== null && _b !== void 0 ? _b : 0, writeSeq));
            }
        }
    });
    const finish = (ctx) => {
        ctx.done = true;
        for (const key of ctx.seqs.keys()) {
            removePending(key, write => write.ctx === ctx);
        }
    };
    return {
        get: async (type, ids) => {
            const result = {};
            const missing = [];
            for (const id of ids) {
                const write = latestPending(keyOf(type, id));
                if (write) {
                    if (write.value) {
                        result[id] = write.value;
                    }
                }
                else {
                    missing.push(id);
                }
            }
            if (missing.length) {
                const fetched = await state.get(type, missing);
                for (const id of missing) {
                    // uma escrita pode ter entrado durante o await: ela é mais nova que o lido
                    const write = latestPending(keyOf(type, id));
                    const value = write ? write.value : fetched[id];
                    if (value) {
                        result[id] = value;
                    }
                }
            }
            return result;
        },
        set: data => {
            var _a;
            const ctx = currentTx();
            if (ctx) {
                logger.trace({ types: Object.keys(data) }, 'caching in transaction');
                for (const type in data) {
                    (_a = ctx.mutations)[type] || (_a[type] = {});
                    for (const id in data[type]) {
                        const key = keyOf(type, id);
                        const writeSeq = ++seq;
                        const value = data[type][id];
                        ctx.mutations[type][id] = value;
                        ctx.seqs.set(key, writeSeq);
                        addPending(key, { seq: writeSeq, ctx, value });
                    }
                }
                return;
            }
            // fora de transação: grava já, e a escrita vence o que estiver pendente para a chave
            const writeSeq = ++seq;
            const keys = [];
            let conflicts = false;
            for (const type in data) {
                for (const id in data[type]) {
                    const key = keyOf(type, id);
                    keys.push(key);
                    conflicts || (conflicts = pending.has(key));
                    addPending(key, { seq: writeSeq, ctx: undefined, value: data[type][id] });
                }
            }
            const write = async () => {
                var _a;
                if (conflicts) {
                    // outra escrita direta da mesma chave pode estar indo ao banco: grava depois dela
                    const inFlight = keys.map(key => directInFlight.get(key)).filter(Boolean);
                    if (inFlight.length) {
                        await Promise.allSettled(inFlight);
                    }
                }
                await state.set(data);
                for (const key of keys) {
                    if (pending.has(key)) {
                        committedSeq.set(key, Math.max((_a = committedSeq.get(key)) !== null && _a !== void 0 ? _a : 0, writeSeq));
                    }
                }
            };
            const promise = (conflicts ? commitMutex.mutex(write) : write()).finally(() => {
                for (const key of keys) {
                    if (directInFlight.get(key) === promise) {
                        directInFlight.delete(key);
                    }
                    // só a própria entrada: a de transação sai no finish dela, depois do commit, senão o
                    // committedSeq seria apagado com um commit velho ainda na fila
                    removePending(key, w => w.seq === writeSeq && !w.ctx);
                }
            });
            if (!conflicts) {
                for (const key of keys) {
                    directInFlight.set(key, promise);
                }
            }
            return promise;
        },
        isInTransaction: () => !!currentTx(),
        async transaction(work) {
            var _a;
            if (currentTx()) {
                // aninhada: entra na transação de fora
                return work();
            }
            const ctx = { owner, mutations: {}, seqs: new Map(), done: false };
            const longTransactionTimer = setTimeout(() => {
                logger.warn({ types: Object.keys(ctx.mutations) }, `transação de chaves aberta há ${LONG_TRANSACTION_MS}ms`);
            }, LONG_TRANSACTION_MS);
            (_a = longTransactionTimer.unref) === null || _a === void 0 ? void 0 : _a.call(longTransactionTimer);
            let committing = false;
            try {
                const result = await transactionStorage.run(ctx, work);
                // fechada antes do commit: o que escapar daqui em diante (void, nextTick) grava direto
                ctx.done = true;
                committing = true;
                await commit(ctx);
                return result;
            }
            catch (error) {
                ctx.done = true;
                // falhou: grava só o que só anda para frente (sessão e sender key já usadas na rede).
                // sender-key-memory e app-state ficam de fora porque podem marcar o que não foi enviado.
                if (!committing && ctx.seqs.size) {
                    logger.warn({ types: Object.keys(ctx.mutations), err: error === null || error === void 0 ? void 0 : error.message }, 'transação de chaves falhou com mutações pendentes');
                    try {
                        await commit(ctx, FORWARD_SAFE_TYPES);
                    }
                    catch (err) {
                        logger.error({ err }, 'falha ao gravar o estado de sessão da transação que falhou');
                    }
                }
                throw error;
            }
            finally {
                clearTimeout(longTransactionTimer);
                finish(ctx);
            }
        }
    };
};
exports.addTransactionCapability = addTransactionCapability;
const initAuthCreds = () => {
    const identityKey = crypto_2.Curve.generateKeyPair();
    return {
        noiseKey: crypto_2.Curve.generateKeyPair(),
        pairingEphemeralKeyPair: crypto_2.Curve.generateKeyPair(),
        signedIdentityKey: identityKey,
        signedPreKey: (0, crypto_2.signedKeyPair)(identityKey, 1),
        registrationId: (0, generics_1.generateRegistrationId)(),
        advSecretKey: (0, crypto_1.randomBytes)(32).toString('base64'),
        processedHistoryMessages: [],
        nextPreKeyId: 1,
        firstUnuploadedPreKeyId: 1,
        accountSyncCounter: 0,
        accountSettings: {
            unarchiveChats: false
        },
        registered: false,
        pairingCode: undefined,
        lastPropHash: undefined,
        routingInfo: undefined
    };
};
exports.initAuthCreds = initAuthCreds;
