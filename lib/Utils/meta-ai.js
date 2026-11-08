"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseBotChunkNode = exports.buildMetaAiPrompt = exports.decryptBotChunk = exports.deriveBotChunkKey = exports.genBotMsgSecret = exports.META_AI_BOT_JID = void 0;
const crypto_1 = require("crypto");
// Protocolo portado do zapo-js 1.10 (WaBotCoordinator e message/kinds/bot)
exports.META_AI_BOT_JID = '867051314767696@bot';
const GCM_TAG_LENGTH = 16;
const BOT_SECRET_BYTES = 32;
/** sem um conjunto de capacidades a Meta AI pode recusar o prompt em silêncio */
const DEFAULT_CAPABILITIES = [2, 3, 6, 7, 8, 21, 24, 26, 31, 34, 37, 39, 41, 42, 43];
const AI_THREAD = 2;
const AI_THREAD_DEFAULT = 1;
const hkdf = (ikm, info) => Buffer.from((0, crypto_1.hkdfSync)('sha256', ikm, new Uint8Array(0), info, BOT_SECRET_BYTES));
const genBotMsgSecret = (messageSecret) => hkdf(messageSecret, 'Bot Message');
exports.genBotMsgSecret = genBotMsgSecret;
const deriveBotChunkKey = (botMsgSecret, saltId, targetSenderJid, authorJid) => hkdf(botMsgSecret, saltId + targetSenderJid + authorJid);
exports.deriveBotChunkKey = deriveBotChunkKey;
/** o chunk não tem padding PKCS7: o texto decifrado já é o proto Message */
const decryptBotChunk = (input) => {
    const key = (0, exports.deriveBotChunkKey)((0, exports.genBotMsgSecret)(input.parentMessageSecret), input.saltId, input.targetSenderJid, input.authorJid);
    const decipher = (0, crypto_1.createDecipheriv)('aes-256-gcm', key, input.encIv);
    decipher.setAAD(Buffer.from(`${input.saltId}\u0000${input.authorJid}`));
    decipher.setAuthTag(input.encPayload.subarray(input.encPayload.length - GCM_TAG_LENGTH));
    return Buffer.concat([decipher.update(input.encPayload.subarray(0, input.encPayload.length - GCM_TAG_LENGTH)), decipher.final()]);
};
exports.decryptBotChunk = decryptBotChunk;
/** metadados e thread que a Meta AI exige no prompt direto para `@bot` */
const buildMetaAiPrompt = (message, { aiThreadId = (0, crypto_1.randomUUID)(), messageSecret }) => {
    var _a;
    return ({
        aiThreadId,
        message: {
            ...message,
            messageContextInfo: {
                ...message.messageContextInfo,
                messageSecret,
                threadId: [
                    { threadType: AI_THREAD, threadKey: { remoteJid: exports.META_AI_BOT_JID, fromMe: true, id: aiThreadId } }
                ],
                botMetadata: {
                    ...(_a = message.messageContextInfo) === null || _a === void 0 ? void 0 : _a.botMetadata,
                    capabilityMetadata: { capabilities: DEFAULT_CAPABILITIES },
                    botThreadInfo: { clientInfo: { type: AI_THREAD_DEFAULT } }
                }
            }
        }
    });
};
exports.buildMetaAiPrompt = buildMetaAiPrompt;
const EDIT_TYPES = ['first', 'inner', 'last', 'full'];
/**
 * Lê da stanza do chunk (`<enc type="msmsg">` + `<bot>` + `<meta>`) o que a decifração precisa.
 * Devolve undefined se não for chunk de bot.
 */
const parseBotChunkNode = (node, meLid, meId) => {
    var _a, _b;
    const children = Array.isArray(node.content) ? node.content : [];
    const enc = children.find(child => child.tag === 'enc' && child.attrs.type === 'msmsg');
    const bot = children.find(child => child.tag === 'bot');
    if (!enc || !bot || !(enc.content instanceof Uint8Array)) {
        return;
    }
    const meta = children.find(child => child.tag === 'meta');
    const editType = (_a = EDIT_TYPES.find(type => type === bot.attrs.edit)) !== null && _a !== void 0 ? _a : 'full';
    const editTargetId = bot.attrs.edit_target_id || undefined;
    const saltId = editType === 'inner' || editType === 'last' ? editTargetId : node.attrs.id;
    const authorJid = toUserJid(node.attrs.participant || node.attrs.from);
    // bot FBID (`@bot`) deriva a chave do meu LID; o bot legado por PN, do meu PN
    const fallbackMe = ((_b = node.attrs.from) === null || _b === void 0 ? void 0 : _b.endsWith('@bot')) ? meLid || meId : meId;
    const targetSenderJid = toUserJid((meta === null || meta === void 0 ? void 0 : meta.attrs.target_sender_jid) || fallbackMe);
    if (!saltId || !authorJid || !targetSenderJid) {
        return;
    }
    return {
        payload: enc.content,
        targetMessageId: meta === null || meta === void 0 ? void 0 : meta.attrs.target_id,
        editType,
        editTargetId,
        saltId,
        authorJid,
        targetSenderJid
    };
};
exports.parseBotChunkNode = parseBotChunkNode;
const toUserJid = (jid) => {
    if (!(jid === null || jid === void 0 ? void 0 : jid.includes('@'))) {
        return undefined;
    }
    const [user, server] = jid.split('@');
    return `${user.split(':')[0]}@${server}`;
};
