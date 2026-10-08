"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getChatId = exports.shouldIncrementChatUnread = exports.isRealMessage = exports.cleanMessage = void 0;
exports.decryptPollVote = decryptPollVote;
const WAProto_1 = require("../../WAProto");
const Types_1 = require("../Types");
const messages_1 = require("../Utils/messages");
const WABinary_1 = require("../WABinary");
const crypto_1 = require("./crypto");
const generics_1 = require("./generics");
const history_1 = require("./history");
const cs_token_utils_1 = require("./cs-token-utils");
const tc_token_utils_1 = require("./tc-token-utils");
const REAL_MSG_STUB_TYPES = new Set([
    Types_1.WAMessageStubType.CALL_MISSED_GROUP_VIDEO,
    Types_1.WAMessageStubType.CALL_MISSED_GROUP_VOICE,
    Types_1.WAMessageStubType.CALL_MISSED_VIDEO,
    Types_1.WAMessageStubType.CALL_MISSED_VOICE
]);
const REAL_MSG_REQ_ME_STUB_TYPES = new Set([Types_1.WAMessageStubType.GROUP_PARTICIPANT_ADD]);
/** Cleans a received message to further processing */
const cleanMessage = (message, meId) => {
    // ensure remoteJid and participant doesn't have device or agent in it
    message.key.remoteJid = (0, WABinary_1.jidNormalizedUser)(message.key.remoteJid);
    message.key.participant = message.key.participant ? (0, WABinary_1.jidNormalizedUser)(message.key.participant) : undefined;
    const content = (0, messages_1.normalizeMessageContent)(message.message);
    // if the message has a reaction, ensure fromMe & remoteJid are from our perspective
    if (content === null || content === void 0 ? void 0 : content.reactionMessage) {
        normaliseKey(content.reactionMessage.key);
    }
    if (content === null || content === void 0 ? void 0 : content.pollUpdateMessage) {
        normaliseKey(content.pollUpdateMessage.pollCreationMessageKey);
    }
    function normaliseKey(msgKey) {
        // if the reaction is from another user
        // we've to correctly map the key to this user's perspective
        if (!message.key.fromMe) {
            // if the sender believed the message being reacted to is not from them
            // we've to correct the key to be from them, or some other participant
            msgKey.fromMe = !msgKey.fromMe
                ? (0, WABinary_1.areJidsSameUser)(msgKey.participant || msgKey.remoteJid, meId)
                : // if the message being reacted to, was from them
                    // fromMe automatically becomes false
                    false;
            // set the remoteJid to being the same as the chat the message came from
            msgKey.remoteJid = message.key.remoteJid;
            // set participant of the message
            msgKey.participant = msgKey.participant || message.key.participant;
        }
    }
};
exports.cleanMessage = cleanMessage;
const isRealMessage = (message, meId) => {
    var _a;
    const normalizedContent = (0, messages_1.normalizeMessageContent)(message.message);
    const hasSomeContent = !!(0, messages_1.getContentType)(normalizedContent);
    return ((!!normalizedContent ||
        REAL_MSG_STUB_TYPES.has(message.messageStubType) ||
        (REAL_MSG_REQ_ME_STUB_TYPES.has(message.messageStubType) &&
            ((_a = message.messageStubParameters) === null || _a === void 0 ? void 0 : _a.some(p => (0, WABinary_1.areJidsSameUser)(meId, p))))) &&
        hasSomeContent &&
        !(normalizedContent === null || normalizedContent === void 0 ? void 0 : normalizedContent.protocolMessage) &&
        !(normalizedContent === null || normalizedContent === void 0 ? void 0 : normalizedContent.reactionMessage) &&
        !(normalizedContent === null || normalizedContent === void 0 ? void 0 : normalizedContent.pollUpdateMessage));
};
exports.isRealMessage = isRealMessage;
const shouldIncrementChatUnread = (message) => !message.key.fromMe && !message.messageStubType;
exports.shouldIncrementChatUnread = shouldIncrementChatUnread;
/**
 * Get the ID of the chat from the given key.
 * Typically -- that'll be the remoteJid, but for broadcasts, it'll be the participant
 */
const getChatId = ({ remoteJid, participant, fromMe }) => {
    if ((0, WABinary_1.isJidBroadcast)(remoteJid) && !(0, WABinary_1.isJidStatusBroadcast)(remoteJid) && !fromMe) {
        return participant;
    }
    return remoteJid;
};
exports.getChatId = getChatId;
/**
 * Decrypt a poll vote
 * @param vote encrypted vote
 * @param ctx additional info about the poll required for decryption
 * @returns list of SHA256 options
 */
function decryptPollVote({ encPayload, encIv }, { pollCreatorJid, pollMsgId, pollEncKey, voterJid }) {
    const sign = Buffer.concat([
        toBinary(pollMsgId),
        toBinary(pollCreatorJid),
        toBinary(voterJid),
        toBinary('Poll Vote'),
        new Uint8Array([1])
    ]);
    const key0 = (0, crypto_1.hmacSign)(pollEncKey, new Uint8Array(32), 'sha256');
    const decKey = (0, crypto_1.hmacSign)(sign, key0, 'sha256');
    const aad = toBinary(`${pollMsgId}\u0000${voterJid}`);
    const decrypted = (0, crypto_1.aesDecryptGCM)(encPayload, decKey, encIv, aad);
    return WAProto_1.proto.Message.PollVoteMessage.decode(decrypted);
    function toBinary(txt) {
        return Buffer.from(txt);
    }
}
const forgetSenderKeyMemory = async (keyStore, groupJid, participants) => {
    var _a;
    const users = new Set(participants.map(p => { var _a; return (_a = (0, WABinary_1.jidDecode)(p)) === null || _a === void 0 ? void 0 : _a.user; }).filter(Boolean));
    const memory = (await keyStore.get('sender-key-memory', [groupJid]))[groupJid];
    if (!users.size || !memory) {
        return;
    }
    const next = { ...memory };
    let changed = false;
    for (const deviceJid of Object.keys(next)) {
        if (users.has((_a = (0, WABinary_1.jidDecode)(deviceJid)) === null || _a === void 0 ? void 0 : _a.user)) {
            delete next[deviceJid];
            changed = true;
        }
    }
    if (changed) {
        await keyStore.set({ 'sender-key-memory': { [groupJid]: next } });
    }
};
const forgetGroupSenderKeys = async (keyStore, groupJid, meJid) => {
    const me = (0, WABinary_1.jidDecode)(meJid);
    await keyStore.set({
        'sender-key-memory': { [groupJid]: null },
        ...(me ? { 'sender-key': { [`${groupJid}::${me.user}::${me.device || 0}`]: null } } : {})
    });
};
const processMessage = async (message, { shouldProcessHistoryMsg, placeholderResendCache, ev, creds, keyStore, logger, options }) => {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u;
    const meId = creds.me.id;
    const { accountSettings } = creds;
    const chat = { id: (0, WABinary_1.jidNormalizedUser)((0, exports.getChatId)(message.key)) };
    const isRealMsg = (0, exports.isRealMessage)(message, meId);
    if (isRealMsg) {
        chat.messages = [{ message }];
        chat.conversationTimestamp = (0, generics_1.toNumber)(message.messageTimestamp);
        // only increment unread count if not CIPHERTEXT and from another person
        if ((0, exports.shouldIncrementChatUnread)(message)) {
            chat.unreadCount = (chat.unreadCount || 0) + 1;
        }
    }
    const content = (0, messages_1.normalizeMessageContent)(message.message);
    // unarchive chat if it's a real message, or someone reacted to our message
    // and we've the unarchive chats setting on
    if ((isRealMsg || ((_b = (_a = content === null || content === void 0 ? void 0 : content.reactionMessage) === null || _a === void 0 ? void 0 : _a.key) === null || _b === void 0 ? void 0 : _b.fromMe)) && (accountSettings === null || accountSettings === void 0 ? void 0 : accountSettings.unarchiveChats)) {
        chat.archived = false;
        chat.readOnly = false;
    }
    const protocolMsg = content === null || content === void 0 ? void 0 : content.protocolMessage;
    if (protocolMsg) {
        switch (protocolMsg.type) {
            case WAProto_1.proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION:
                const histNotification = protocolMsg.historySyncNotification;
                const process = shouldProcessHistoryMsg;
                const isLatest = !((_c = creds.processedHistoryMessages) === null || _c === void 0 ? void 0 : _c.length);
                const inlinePayloadLength = ((_d = histNotification.initialHistBootstrapInlinePayload) === null || _d === void 0 ? void 0 : _d.length) || 0;
                logger === null || logger === void 0 ? void 0 : logger.debug({
                    syncType: histNotification.syncType,
                    chunkOrder: histNotification.chunkOrder,
                    progress: histNotification.progress,
                    fileLength: (_e = histNotification.fileLength) === null || _e === void 0 ? void 0 : _e.toString(),
                    hasInlinePayload: inlinePayloadLength > 0,
                    inlinePayloadLength,
                    inspectionSource: inlinePayloadLength > 0 ? 'inline' : 'external',
                    process,
                    id: message.key.id,
                    isLatest
                }, 'got history notification');
                if (process) {
                    if (histNotification.syncType !== WAProto_1.proto.HistorySync.HistorySyncType.ON_DEMAND) {
                        ev.emit('creds.update', {
                            processedHistoryMessages: [
                                ...(creds.processedHistoryMessages || []),
                                { key: message.key, messageTimestamp: message.messageTimestamp }
                            ]
                        });
                    }
                }
                let historySync;
                try {
                    historySync = await (0, history_1.downloadHistory)(histNotification, options);
                }
                catch (err) {
                    if (process)
                        throw err;
                    logger === null || logger === void 0 ? void 0 : logger.warn({
                        event: 'privacy_history_sync_inspection_failed',
                        msgId: message.key.id,
                        syncType: histNotification.syncType,
                        source: inlinePayloadLength > 0 ? 'inline' : 'external',
                        err: err === null || err === void 0 ? void 0 : err.message
                    }, 'falha ao inspecionar material de privacidade no history sync');
                    break;
                }
                try {
                    await (0, cs_token_utils_1.storeNctSaltFromHistorySync)(historySync, keyStore, logger);
                }
                catch (err) {
                    logger === null || logger === void 0 ? void 0 : logger.warn({ err }, 'falha ao persistir nct salt do history sync');
                }
                await (0, tc_token_utils_1.storeTcTokensFromHistorySync)((historySync.conversations || []), keyStore, logger);
                if (process) {
                    const data = (0, history_1.processHistoryMessage)(historySync);
                    ev.emit('messaging-history.set', {
                        ...data,
                        isLatest: histNotification.syncType !== WAProto_1.proto.HistorySync.HistorySyncType.ON_DEMAND ? isLatest : undefined,
                        peerDataRequestSessionId: histNotification.peerDataRequestSessionId
                    });
                }
                break;
            case WAProto_1.proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE:
                const keys = protocolMsg.appStateSyncKeyShare.keys;
                if (keys === null || keys === void 0 ? void 0 : keys.length) {
                    let newAppStateSyncKeyId = '';
                    await keyStore.transaction(async () => {
                        const newKeys = [];
                        for (const { keyData, keyId } of keys) {
                            const strKeyId = Buffer.from(keyId.keyId).toString('base64');
                            newKeys.push(strKeyId);
                            await keyStore.set({ 'app-state-sync-key': { [strKeyId]: keyData } });
                            newAppStateSyncKeyId = strKeyId;
                        }
                        logger === null || logger === void 0 ? void 0 : logger.info({ newAppStateSyncKeyId, newKeys }, 'injecting new app state sync keys');
                    });
                    ev.emit('creds.update', { myAppStateKeyId: newAppStateSyncKeyId });
                }
                else {
                    logger === null || logger === void 0 ? void 0 : logger.info({ protocolMsg }, 'recv app state sync with 0 keys');
                }
                break;
            case WAProto_1.proto.Message.ProtocolMessage.Type.REVOKE:
                ev.emit('messages.update', [
                    {
                        key: {
                            ...message.key,
                            id: protocolMsg.key.id
                        },
                        update: { message: null, messageStubType: Types_1.WAMessageStubType.REVOKE, key: message.key }
                    }
                ]);
                break;
            case WAProto_1.proto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING:
                Object.assign(chat, {
                    ephemeralSettingTimestamp: (0, generics_1.toNumber)(message.messageTimestamp),
                    ephemeralExpiration: protocolMsg.ephemeralExpiration || null
                });
                break;
            case WAProto_1.proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE:
                const response = protocolMsg.peerDataOperationRequestResponseMessage;
                if (response) {
                    placeholderResendCache === null || placeholderResendCache === void 0 ? void 0 : placeholderResendCache.del(response.stanzaId);
                    // TODO: IMPLEMENT HISTORY SYNC ETC (sticker uploads etc.).
                    const { peerDataOperationResult } = response;
                    for (const result of peerDataOperationResult) {
                        const { placeholderMessageResendResponse: retryResponse } = result;
                        //eslint-disable-next-line max-depth
                        if (retryResponse) {
                            const webMessageInfo = WAProto_1.proto.WebMessageInfo.decode(retryResponse.webMessageInfoBytes);
                            // wait till another upsert event is available, don't want it to be part of the PDO response message
                            setTimeout(() => {
                                ev.emit('messages.upsert', {
                                    messages: [webMessageInfo],
                                    type: 'notify',
                                    requestId: response.stanzaId
                                });
                            }, 500);
                        }
                    }
                }
            case WAProto_1.proto.Message.ProtocolMessage.Type.MESSAGE_EDIT:
                ev.emit('messages.update', [
                    {
                        // flip the sender / fromMe properties because they're in the perspective of the sender
                        key: { ...message.key, id: (_f = protocolMsg.key) === null || _f === void 0 ? void 0 : _f.id },
                        update: {
                            message: {
                                editedMessage: {
                                    message: protocolMsg.editedMessage
                                }
                            },
                            messageTimestamp: protocolMsg.timestampMs
                                ? Math.floor((0, generics_1.toNumber)(protocolMsg.timestampMs) / 1000)
                                : message.messageTimestamp
                        }
                    }
                ]);
                break;
        }
    }
    else if (content === null || content === void 0 ? void 0 : content.reactionMessage) {
        const reaction = {
            ...content.reactionMessage,
            key: message.key
        };
        ev.emit('messages.reaction', [
            {
                reaction,
                key: (_g = content.reactionMessage) === null || _g === void 0 ? void 0 : _g.key
            }
        ]);
    }
    else if (message.messageStubType) {
        const jid = (_h = message.key) === null || _h === void 0 ? void 0 : _h.remoteJid;
        //let actor = whatsappID (message.participant)
        let participants;
        const emitParticipantsUpdate = (action) => ev.emit('group-participants.update', { id: jid, author: message.participant, participants, action });
        const emitGroupUpdate = (update) => {
            var _a;
            ev.emit('groups.update', [{ id: jid, ...update, author: (_a = message.participant) !== null && _a !== void 0 ? _a : undefined }]);
        };
        const emitGroupRequestJoin = (participant, action, method) => {
            ev.emit('group.join-request', { id: jid, author: message.participant, participant, action, method: method });
        };
        const participantsIncludesMe = () => participants.find(jid => { var _a; return (0, WABinary_1.areJidsSameUser)(meId, jid) || (!!((_a = creds.me) === null || _a === void 0 ? void 0 : _a.lid) && (0, WABinary_1.areJidsSameUser)(creds.me.lid, jid)); });
        switch (message.messageStubType) {
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_CHANGE_NUMBER:
                participants = message.messageStubParameters || [];
                emitParticipantsUpdate('modify');
                break;
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_LEAVE:
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_REMOVE:
                participants = message.messageStubParameters || [];
                emitParticipantsUpdate('remove');
                // mark the chat read only if you left the group
                if (participantsIncludesMe()) {
                    chat.readOnly = true;
                    // o bot saiu: a memória de SKDM e a própria sender key do grupo só ocupam espaço (recriadas se voltar)
                    try {
                        await forgetGroupSenderKeys(keyStore, jid, ((_j = creds.me) === null || _j === void 0 ? void 0 : _j.lid) || meId);
                    }
                    catch (err) {
                        logger === null || logger === void 0 ? void 0 : logger.warn({ err, jid }, 'falha ao limpar chaves do grupo que o bot saiu');
                    }
                }
                else {
                    // quem sai perde a marca de "já recebeu a sender key"; se voltar, recebe o SKDM no próximo envio
                    try {
                        await forgetSenderKeyMemory(keyStore, jid, participants);
                    }
                    catch (err) {
                        logger === null || logger === void 0 ? void 0 : logger.warn({ err, jid }, 'falha ao limpar sender-key-memory de quem saiu');
                    }
                }
                break;
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_ADD:
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_INVITE:
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_ADD_REQUEST_JOIN:
                participants = message.messageStubParameters || [];
                if (participantsIncludesMe()) {
                    chat.readOnly = false;
                }
                emitParticipantsUpdate('add');
                break;
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_DEMOTE:
                participants = message.messageStubParameters || [];
                emitParticipantsUpdate('demote');
                break;
            case Types_1.WAMessageStubType.GROUP_PARTICIPANT_PROMOTE:
                participants = message.messageStubParameters || [];
                emitParticipantsUpdate('promote');
                break;
            case Types_1.WAMessageStubType.GROUP_CHANGE_ANNOUNCE:
                const announceValue = (_k = message.messageStubParameters) === null || _k === void 0 ? void 0 : _k[0];
                emitGroupUpdate({ announce: announceValue === 'true' || announceValue === 'on' });
                break;
            case Types_1.WAMessageStubType.GROUP_CHANGE_RESTRICT:
                const restrictValue = (_l = message.messageStubParameters) === null || _l === void 0 ? void 0 : _l[0];
                emitGroupUpdate({ restrict: restrictValue === 'true' || restrictValue === 'on' });
                break;
            case Types_1.WAMessageStubType.GROUP_CHANGE_SUBJECT:
                const name = (_m = message.messageStubParameters) === null || _m === void 0 ? void 0 : _m[0];
                chat.name = name;
                emitGroupUpdate({ subject: name });
                break;
            case Types_1.WAMessageStubType.GROUP_CHANGE_DESCRIPTION:
                const description = (_o = message.messageStubParameters) === null || _o === void 0 ? void 0 : _o[0];
                chat.description = description;
                emitGroupUpdate({ desc: description });
                break;
            case Types_1.WAMessageStubType.GROUP_CHANGE_INVITE_LINK:
                const code = (_p = message.messageStubParameters) === null || _p === void 0 ? void 0 : _p[0];
                emitGroupUpdate({ inviteCode: code });
                break;
            case Types_1.WAMessageStubType.GROUP_MEMBER_ADD_MODE:
                const memberAddValue = (_q = message.messageStubParameters) === null || _q === void 0 ? void 0 : _q[0];
                emitGroupUpdate({ memberAddMode: memberAddValue === 'all_member_add' });
                break;
            case Types_1.WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_MODE:
                const approvalMode = (_r = message.messageStubParameters) === null || _r === void 0 ? void 0 : _r[0];
                emitGroupUpdate({ joinApprovalMode: approvalMode === 'on' });
                break;
            case Types_1.WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD:
                const participant = (_s = message.messageStubParameters) === null || _s === void 0 ? void 0 : _s[0];
                const action = (_t = message.messageStubParameters) === null || _t === void 0 ? void 0 : _t[1];
                const method = (_u = message.messageStubParameters) === null || _u === void 0 ? void 0 : _u[2];
                emitGroupRequestJoin(participant, action, method);
                break;
        }
    }
    if (Object.keys(chat).length > 1) {
        ev.emit('chats.update', [chat]);
    }
};
exports.default = processMessage;
