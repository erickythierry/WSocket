"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NCT_SALT_KEY = exports.NCT_SALT_SYNC_INDEX = void 0;
exports.generateCsToken = generateCsToken;
exports.readNctSalt = readNctSalt;
exports.storeNctSaltFromHistorySync = storeNctSaltFromHistorySync;
exports.handleNctSaltMutation = handleNctSaltMutation;
const crypto_1 = require("crypto");
const WAProto_1 = require("../../WAProto");
const WABinary_1 = require("../WABinary");
const tc_token_utils_1 = require("./tc-token-utils");
exports.NCT_SALT_SYNC_INDEX = 'nct_salt_sync';
exports.NCT_SALT_KEY = 'salt';
/** Deriva HMAC-SHA256(nctSalt, recipientLID), igual ao cliente de referência. */
function generateCsToken(salt, recipientLid) {
    if (!(salt === null || salt === void 0 ? void 0 : salt.length) || !(0, WABinary_1.isLidUser)(recipientLid) || !(0, tc_token_utils_1.isRegularUser)(recipientLid))
        return undefined;
    return (0, crypto_1.createHmac)('sha256', salt).update(recipientLid).digest();
}
async function readNctSalt(keys) {
    var _a;
    const salt = (_a = (await keys.get('nct-salt', [exports.NCT_SALT_KEY]))[exports.NCT_SALT_KEY]) === null || _a === void 0 ? void 0 : _a.salt;
    return (salt === null || salt === void 0 ? void 0 : salt.length) ? Buffer.from(salt) : undefined;
}
async function persistNctSalt(keys, salt) {
    const value = Buffer.from(salt);
    await keys.set({ 'nct-salt': { [exports.NCT_SALT_KEY]: { salt: value } } });
    const persisted = await readNctSalt(keys);
    if (!(persisted === null || persisted === void 0 ? void 0 : persisted.equals(value)))
        throw new Error('nct salt não sobreviveu ao keystore');
}
async function storeNctSaltFromHistorySync(historySync, keys, logger) {
    const salt = historySync.nctSalt;
    logger === null || logger === void 0 ? void 0 : logger.debug({ event: 'nct_salt_history_sync_received', syncType: historySync.syncType, saltPresent: !!(salt === null || salt === void 0 ? void 0 : salt.length) }, 'nct salt inspecionado no history sync');
    if (!(salt === null || salt === void 0 ? void 0 : salt.length))
        return false;
    await persistNctSalt(keys, salt);
    logger === null || logger === void 0 ? void 0 : logger.info({ event: 'nct_salt_stored', source: 'history_sync' }, 'nct salt armazenado');
    return true;
}
async function handleNctSaltMutation({ mutation, keys, logger }) {
    var _a, _b, _c, _d;
    if (((_a = mutation.index) === null || _a === void 0 ? void 0 : _a[0]) !== exports.NCT_SALT_SYNC_INDEX)
        return false;
    const isRemove = mutation.operation === WAProto_1.proto.SyncdMutation.SyncdOperation.REMOVE;
    const salt = (_d = (_c = (_b = mutation.syncAction) === null || _b === void 0 ? void 0 : _b.value) === null || _c === void 0 ? void 0 : _c.nctSaltSyncAction) === null || _d === void 0 ? void 0 : _d.salt;
    logger === null || logger === void 0 ? void 0 : logger.debug({ event: 'nct_salt_mutation_received', operation: isRemove ? 'remove' : 'set', saltPresent: !!(salt === null || salt === void 0 ? void 0 : salt.length) }, 'nct salt mutation recebida');
    if (isRemove) {
        await keys.set({ 'nct-salt': { [exports.NCT_SALT_KEY]: null } });
        logger === null || logger === void 0 ? void 0 : logger.info({ event: 'nct_salt_removed' }, 'nct salt removido');
        return true;
    }
    if (!(salt === null || salt === void 0 ? void 0 : salt.length)) {
        logger === null || logger === void 0 ? void 0 : logger.warn({ event: 'nct_salt_mutation_empty' }, 'mutation nct_salt_sync recebida sem salt');
        return true;
    }
    await persistNctSalt(keys, salt);
    logger === null || logger === void 0 ? void 0 : logger.info({ event: 'nct_salt_stored', source: 'app_state' }, 'nct salt armazenado');
    return true;
}
