"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeMessagesSocket = void 0;
const node_cache_1 = __importDefault(require("@cacheable/node-cache"));
const boom_1 = require("@hapi/boom");
const WAProto_1 = require("../../WAProto");
const Defaults_1 = require("../Defaults");
var ListType = WAProto_1.proto.Message.ListMessage.ListType;
const Utils_1 = require("../Utils");
const link_preview_1 = require("../Utils/link-preview");
const tc_token_utils_1 = require("../Utils/tc-token-utils");
const WABinary_1 = require("../WABinary");
const WAUSync_1 = require("../WAUSync");
const newsletter_1 = require("./newsletter");
const cache_utils_1 = __importDefault(require("../Utils/cache-utils"));
const makeMessagesSocket = (config) => {
    const { logger, linkPreviewImageThumbnailWidth, generateHighQualityLinkPreview, options: axiosOptions, patchMessageBeforeSending, cachedGroupMetadata } = config;
    const sock = (0, newsletter_1.makeNewsletterSocket)(config);
    const { ev, authState, processingMutex, signalRepository, upsertMessage, query, fetchPrivacySettings, sendNode, groupMetadata, groupToggleEphemeral } = sock;
    const userDevicesCache = config.userDevicesCache ||
        new node_cache_1.default({
            stdTTL: Defaults_1.DEFAULT_CACHE_TTLS.USER_DEVICES,
            useClones: false
        });
    let mediaConn;
    const refreshMediaConn = async (forceGet = false) => {
        const media = await mediaConn;
        if (!media || forceGet || new Date().getTime() - media.fetchDate.getTime() > media.ttl * 1000) {
            mediaConn = (async () => {
                const result = await query({
                    tag: 'iq',
                    attrs: {
                        type: 'set',
                        xmlns: 'w:m',
                        to: WABinary_1.S_WHATSAPP_NET
                    },
                    content: [{ tag: 'media_conn', attrs: {} }]
                });
                const mediaConnNode = (0, WABinary_1.getBinaryNodeChild)(result, 'media_conn');
                const node = {
                    hosts: (0, WABinary_1.getBinaryNodeChildren)(mediaConnNode, 'host').map(({ attrs }) => ({
                        hostname: attrs.hostname,
                        maxContentLengthBytes: +attrs.maxContentLengthBytes
                    })),
                    auth: mediaConnNode.attrs.auth,
                    ttl: +mediaConnNode.attrs.ttl,
                    fetchDate: new Date()
                };
                logger.debug('fetched media conn');
                return node;
            })();
        }
        return mediaConn;
    };
    /**
     * generic send receipt function
     * used for receipts of phone call, read, delivery etc.
     * */
    const sendReceipt = async (jid, participant, messageIds, type) => {
        const node = {
            tag: 'receipt',
            attrs: {
                id: messageIds[0]
            }
        };
        const isReadReceipt = type === 'read' || type === 'read-self';
        if (isReadReceipt) {
            node.attrs.t = (0, Utils_1.unixTimestampSeconds)().toString();
        }
        if (type === 'sender' && (0, WABinary_1.isJidUser)(jid)) {
            node.attrs.recipient = jid;
            node.attrs.to = participant;
        }
        else {
            node.attrs.to = jid;
            if (participant) {
                node.attrs.participant = participant;
            }
        }
        if (type) {
            node.attrs.type = type;
        }
        const remainingMessageIds = messageIds.slice(1);
        if (remainingMessageIds.length) {
            node.content = [
                {
                    tag: 'list',
                    attrs: {},
                    content: remainingMessageIds.map(id => ({
                        tag: 'item',
                        attrs: { id }
                    }))
                }
            ];
        }
        logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages');
        await sendNode(node);
    };
    /** Correctly bulk send receipts to multiple chats, participants */
    const sendReceipts = async (keys, type) => {
        const recps = (0, Utils_1.aggregateMessageKeysNotFromMe)(keys);
        for (const { jid, participant, messageIds } of recps) {
            await sendReceipt(jid, participant, messageIds, type);
        }
    };
    /** Bulk read messages. Keys can be from different chats & participants */
    const readMessages = async (keys) => {
        const privacySettings = await fetchPrivacySettings();
        // based on privacy settings, we have to change the read type
        const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self';
        await sendReceipts(keys, readType);
    };
    /** Fetch all the devices we've to send a message to */
    const getUSyncDevices = async (jids, useCache, ignoreZeroDevices) => {
        var _a;
        const deviceResults = [];
        if (!useCache) {
            logger.debug('not using cache for devices');
        }
        const toFetch = [];
        jids = Array.from(new Set(jids));
        for (let jid of jids) {
            jid = (0, WABinary_1.jidNormalizedUser)(jid);
            if (useCache) {
                const devices = userDevicesCache.get(jid);
                if (devices) {
                    deviceResults.push(...devices);
                    logger.trace({ jid }, 'using cache for devices');
                }
                else {
                    toFetch.push(jid);
                }
            }
            else {
                toFetch.push(jid);
            }
        }
        if (!toFetch.length) {
            return deviceResults;
        }
        const query = new WAUSync_1.USyncQuery().withContext('message').withDeviceProtocol();
        for (const jid of toFetch) {
            query.withUser(new WAUSync_1.USyncUser().withId(jid));
        }
        const result = await sock.executeUSyncQuery(query);
        if (result) {
            const extracted = (0, Utils_1.extractDeviceJids)(result === null || result === void 0 ? void 0 : result.list, authState.creds.me.id, ignoreZeroDevices, (_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.lid);
            const deviceMap = {};
            for (const item of extracted) {
                const cacheKey = (0, WABinary_1.jidNormalizedUser)(item.jid);
                deviceMap[cacheKey] = deviceMap[cacheKey] || [];
                deviceMap[cacheKey].push(item);
                deviceResults.push(item);
            }
            for (const key in deviceMap) {
                userDevicesCache.set(key, deviceMap[key]);
            }
        }
        return deviceResults;
    };
    const assertSessions = async (jids, force, lids) => {
        var _a, _b;
        let didFetchNewSession = false;
        const melid = (0, WABinary_1.jidNormalizedUser)((_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.lid);
        const meid = (0, WABinary_1.jidNormalizedUser)((_b = authState.creds.me) === null || _b === void 0 ? void 0 : _b.id);
        let jidsRequiringFetch = [];
        if (force) {
            jidsRequiringFetch = jids;
        }
        else {
            const pairs = jids.map(jid => ({
                jid,
                signalId: signalRepository.jidToSignalProtocolAddress((0, Utils_1.convertlidDevice)(jid, lids, meid, melid))
            }));
            const sessions = await authState.keys.get('session', pairs.map(p => p.signalId));
            for (const { jid, signalId } of pairs) {
                if (!sessions[signalId]) {
                    jidsRequiringFetch.push(jid);
                }
            }
        }
        if (jidsRequiringFetch.length) {
            logger.debug({ jidsRequiringFetch }, 'fetching sessions');
            const result = await query({
                tag: 'iq',
                attrs: {
                    xmlns: 'encrypt',
                    type: 'get',
                    to: WABinary_1.S_WHATSAPP_NET
                },
                content: [
                    {
                        tag: 'key',
                        attrs: {},
                        content: jidsRequiringFetch.map(jid => ({
                            tag: 'user',
                            attrs: { jid }
                        }))
                    }
                ]
            });
            await (0, Utils_1.parseAndInjectE2ESessions)(result, signalRepository, lids, meid, melid);
            didFetchNewSession = true;
        }
        return didFetchNewSession;
    };
    const sendPeerDataOperationMessage = async (pdoMessage) => {
        var _a;
        //TODO: for later, abstract the logic to send a Peer Message instead of just PDO - useful for App State Key Resync with phone
        if (!((_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.id)) {
            throw new boom_1.Boom('Not authenticated');
        }
        const protocolMessage = {
            protocolMessage: {
                peerDataOperationRequestMessage: pdoMessage,
                type: WAProto_1.proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
            }
        };
        const meJid = (0, WABinary_1.jidNormalizedUser)(authState.creds.me.id);
        const msgId = await relayMessage(meJid, protocolMessage, {
            additionalAttributes: {
                category: 'peer',
                // eslint-disable-next-line camelcase
                push_priority: 'high_force'
            }
        });
        return msgId;
    };
    const createParticipantNodes = async (jids, message, extraAttrs, lid, meid, melid) => {
        let patched = await patchMessageBeforeSending(message, jids);
        if (!Array.isArray(patched)) {
            patched = jids ? jids.map(jid => ({ recipientJid: jid, ...patched })) : [patched];
        }
        let shouldIncludeDeviceIdentity = false;
        const nodes = await Promise.all(patched.map(async (patchedMessageWithJid) => {
            const { recipientJid: jid, ...patchedMessage } = patchedMessageWithJid;
            if (!jid) {
                return {};
            }
            const bytes = (0, Utils_1.encodeWAMessage)(patchedMessage);
            const { type, ciphertext } = await signalRepository.encryptMessage({
                jid: (0, Utils_1.convertlidDevice)(jid, lid, meid, melid),
                data: bytes
            });
            if (type === 'pkmsg') {
                shouldIncludeDeviceIdentity = true;
            }
            const node = {
                tag: 'to',
                attrs: { jid },
                content: [
                    {
                        tag: 'enc',
                        attrs: {
                            v: '2',
                            type,
                            ...(extraAttrs || {})
                        },
                        content: ciphertext
                    }
                ]
            };
            return node;
        }));
        return { nodes, shouldIncludeDeviceIdentity };
    };
    const relayMessage = async (jid, message, { messageId: msgId, participant, additionalAttributes, additionalNodes, useUserDevicesCache, useCachedGroupMetadata, statusJidList, isretry, excludeJids, includeJids }) => {
        var _a, _b, _c;
        if (additionalAttributes) {
            additionalAttributes = { ...additionalAttributes };
        }
        const meId = authState.creds.me.id;
        const meLid = authState.creds.me.lid || authState.creds.me.id;
        const lidattrs = (0, WABinary_1.jidDecode)((_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.lid);
        const jlidUser = lidattrs === null || lidattrs === void 0 ? void 0 : lidattrs.user;
        let lids;
        if ((0, WABinary_1.isJidUser)(jid) || (0, WABinary_1.isJidUser)(participant === null || participant === void 0 ? void 0 : participant.jid)) {
            const userQuery = (0, WABinary_1.jidNormalizedUser)((participant === null || participant === void 0 ? void 0 : participant.jid) || jid);
            if (!(0, WABinary_1.isLidUser)(userQuery)) {
                const verify = cache_utils_1.default.lidCache.get(userQuery);
                if (verify) {
                    lids = verify;
                }
                else {
                    const usyncQuery = new WAUSync_1.USyncQuery().withContactProtocol().withLIDProtocol();
                    usyncQuery.withUser(new WAUSync_1.USyncUser().withPhone(userQuery.split('@')[0]));
                    const results = await sock.executeUSyncQuery(usyncQuery);
                    if (results === null || results === void 0 ? void 0 : results.list) {
                        const maybeLid = (_b = results.list[0]) === null || _b === void 0 ? void 0 : _b.lid;
                        if (typeof maybeLid === 'string') {
                            cache_utils_1.default.lidCache.set(userQuery, maybeLid);
                            lids = maybeLid;
                        }
                    }
                }
            }
        }
        const { user, server } = (0, WABinary_1.jidDecode)(jid);
        const statusJid = 'status@broadcast';
        const isGroup = server === 'g.us';
        const isStatus = jid === statusJid;
        const isLid = server === 'lid';
        const isNewsletter = server === 'newsletter';
        let shouldIncludeDeviceIdentity = false;
        msgId = msgId || (0, Utils_1.generateMessageIDV2)((_c = sock.user) === null || _c === void 0 ? void 0 : _c.id);
        useUserDevicesCache = useUserDevicesCache !== false;
        useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus;
        const participants = [];
        const destinationJid = !isStatus ? (0, WABinary_1.jidEncode)(user, isLid ? 'lid' : isGroup ? 'g.us' : 's.whatsapp.net') : statusJid;
        const binaryNodeContent = [];
        const devices = [];
        const extraAttrs = {};
        if (participant) {
            if (!isGroup && !isStatus) {
                additionalAttributes = { ...additionalAttributes, device_fanout: 'false' };
            }
            const { user, device } = (0, WABinary_1.jidDecode)(participant.jid);
            devices.push({ user, device, jid: (0, WABinary_1.jidNormalizedUser)(participant.jid) });
        }
        await authState.keys.transaction(async () => {
            var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y, _z;
            const mediaType = getMediaType(message);
            if (mediaType) {
                extraAttrs['mediatype'] = mediaType;
            }
            if (isNewsletter) {
                const patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message;
                const bytes = (0, Utils_1.encodeNewsletterMessage)(patched);
                binaryNodeContent.push({
                    tag: 'plaintext',
                    attrs: {},
                    content: bytes
                });
                const stanza = {
                    tag: 'message',
                    attrs: {
                        to: jid,
                        id: msgId,
                        type: getMessageType(message),
                        ...(additionalAttributes || {})
                    },
                    content: binaryNodeContent
                };
                logger.debug({ msgId }, `sending newsletter message to ${jid}`);
                await sendNode(stanza);
                return;
            }
            if ((_a = (0, Utils_1.normalizeMessageContent)(message)) === null || _a === void 0 ? void 0 : _a.pinInChatMessage) {
                extraAttrs['decrypt-fail'] = 'hide';
            }
            if (isGroup || isStatus) {
                const [groupData, senderKeyMap] = await Promise.all([
                    (async () => {
                        let groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined;
                        if (groupData && Array.isArray(groupData === null || groupData === void 0 ? void 0 : groupData.participants)) {
                            logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata');
                        }
                        else if (!isStatus) {
                            groupData = await groupMetadata(jid);
                        }
                        return groupData;
                    })(),
                    (async () => {
                        if (!participant && !isStatus) {
                            const result = await authState.keys.get('sender-key-memory', [jid]);
                            return result[jid] || {};
                        }
                        return {};
                    })()
                ]);
                if (!participant) {
                    const participantsList = groupData && !isStatus ? groupData.participants.map(p => p.lid || p.id) : [];
                    if (isStatus && statusJidList) {
                        participantsList.push(...statusJidList);
                    }
                    if (!isStatus) {
                        additionalAttributes = {
                            ...additionalAttributes,
                            addressing_mode: (groupData === null || groupData === void 0 ? void 0 : groupData.addressingMode) || 'pn'
                        };
                    }
                    const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false);
                    devices.push(...additionalDevices);
                    const Mephone = additionalDevices.some(d => d.user === jlidUser && d.device === 0);
                    if (!Mephone && jlidUser) {
                        devices.push({ user: jlidUser, device: 0, jid: (0, WABinary_1.jidNormalizedUser)(meLid) });
                    }
                    // POC exclude-relay (inverso): whitelist. Mantem SOMENTE os devices dos usuarios em
                    // includeJids (mais o proprio remetente, para seus devices sincronizarem); todos os
                    // demais sao removidos => nao recebem sender-key => veem o stub. Precede excludeJids.
                    if (includeJids === null || includeJids === void 0 ? void 0 : includeJids.length) {
                        const includeUsers = new Set();
                        // remetente sempre incluido (phone + lid), senao o proprio bot nao le a mensagem
                        for (const meJid of [meId, meLid]) {
                            const meUserDec = (_b = (0, WABinary_1.jidDecode)((0, WABinary_1.jidNormalizedUser)(meJid))) === null || _b === void 0 ? void 0 : _b.user;
                            if (meUserDec) {
                                includeUsers.add(meUserDec);
                            }
                        }
                        if (jlidUser) {
                            includeUsers.add(jlidUser);
                        }
                        for (const inc of includeJids) {
                            const incNorm = (0, WABinary_1.jidNormalizedUser)(inc);
                            const incUser = (_c = (0, WABinary_1.jidDecode)(incNorm)) === null || _c === void 0 ? void 0 : _c.user;
                            if (incUser) {
                                includeUsers.add(incUser);
                            }
                            for (const p of (groupData === null || groupData === void 0 ? void 0 : groupData.participants) || []) {
                                const pid = p.id;
                                const plid = p.lid;
                                if ((0, WABinary_1.areJidsSameUser)(pid, incNorm) || (plid && (0, WABinary_1.areJidsSameUser)(plid, incNorm))) {
                                    const uid = pid ? (_d = (0, WABinary_1.jidDecode)(pid)) === null || _d === void 0 ? void 0 : _d.user : undefined;
                                    const ulid = plid ? (_e = (0, WABinary_1.jidDecode)(plid)) === null || _e === void 0 ? void 0 : _e.user : undefined;
                                    if (uid) {
                                        includeUsers.add(uid);
                                    }
                                    if (ulid) {
                                        includeUsers.add(ulid);
                                    }
                                }
                            }
                        }
                        for (let i = devices.length - 1; i >= 0; i--) {
                            if (!devices[i].user || !includeUsers.has(devices[i].user)) {
                                devices.splice(i, 1);
                            }
                        }
                        logger.info({ includeUsers: [...includeUsers], remaining: devices.length }, 'exclude-relay: whitelist aplicada (apenas includeJids + remetente recebem sender-key)');
                    }
                    else if (excludeJids === null || excludeJids === void 0 ? void 0 : excludeJids.length) {
                        // POC exclude-relay: remove TODOS os devices dos usuarios em excludeJids (ex.: admins),
                        // resolvendo phone<->lid via participantes do grupo. Sem device no <participants> => sem
                        // sender-key => nao decifram o skmsg => a mensagem simplesmente nao aparece pra eles.
                        const excludeUsers = new Set();
                        for (const ex of excludeJids) {
                            const exNorm = (0, WABinary_1.jidNormalizedUser)(ex);
                            const exUser = (_f = (0, WABinary_1.jidDecode)(exNorm)) === null || _f === void 0 ? void 0 : _f.user;
                            if (exUser) {
                                excludeUsers.add(exUser);
                            }
                            for (const p of (groupData === null || groupData === void 0 ? void 0 : groupData.participants) || []) {
                                const pid = p.id;
                                const plid = p.lid;
                                if ((0, WABinary_1.areJidsSameUser)(pid, exNorm) || (plid && (0, WABinary_1.areJidsSameUser)(plid, exNorm))) {
                                    const uid = pid ? (_g = (0, WABinary_1.jidDecode)(pid)) === null || _g === void 0 ? void 0 : _g.user : undefined;
                                    const ulid = plid ? (_h = (0, WABinary_1.jidDecode)(plid)) === null || _h === void 0 ? void 0 : _h.user : undefined;
                                    if (uid) {
                                        excludeUsers.add(uid);
                                    }
                                    if (ulid) {
                                        excludeUsers.add(ulid);
                                    }
                                }
                            }
                        }
                        for (let i = devices.length - 1; i >= 0; i--) {
                            if (devices[i].user && excludeUsers.has(devices[i].user)) {
                                devices.splice(i, 1);
                            }
                        }
                        logger.info({ excludeUsers: [...excludeUsers], remaining: devices.length }, 'exclude-relay: devices filtrados (usuarios excluidos nao recebem sender-key)');
                    }
                }
                const patched = await patchMessageBeforeSending(message);
                if (Array.isArray(patched)) {
                    throw new boom_1.Boom('Per-jid patching is not supported in groups');
                }
                const bytes = (0, Utils_1.encodeWAMessage)(patched);
                const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
                    group: destinationJid,
                    data: bytes,
                    meId: meLid,
                    // POC exclude-relay: se ha exclusao (ou whitelist), rotaciona a sender-key para que quem
                    // ficou de fora (e ja possa ter a chave antiga) nao decifre este skmsg -> ve o stub
                    // "aguardando esta mensagem". Quem esta incluido recebe o SKDM e decifra normal.
                    forceRotate: !!(excludeJids === null || excludeJids === void 0 ? void 0 : excludeJids.length) || !!(includeJids === null || includeJids === void 0 ? void 0 : includeJids.length)
                });
                const senderKeyJids = [];
                for (const { user, device, jid } of devices) {
                    const server = ((_j = (0, WABinary_1.jidDecode)(jid)) === null || _j === void 0 ? void 0 : _j.server) || 'lid';
                    const senderId = (0, WABinary_1.jidEncode)(user, server, device);
                    senderKeyJids.push(senderId);
                    senderKeyMap[senderId] = true;
                }
                // if there are some participants with whom the session has not been established
                // if there are, we re-send the senderkey
                if (senderKeyJids.length) {
                    logger.debug({ senderKeyJids }, 'sending new sender key');
                    const senderKeyMsg = {
                        senderKeyDistributionMessage: {
                            axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
                            groupId: destinationJid
                        }
                    };
                    await assertSessions(senderKeyJids, isretry ? true : false, lids);
                    const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, extraAttrs, lids, meId, meLid);
                    shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity;
                    participants.push(...result.nodes);
                }
                binaryNodeContent.push({
                    tag: 'enc',
                    attrs: { v: '2', type: 'skmsg' },
                    content: ciphertext
                });
                await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } });
            }
            else {
                const { user: meUser, device: meDevice } = (0, WABinary_1.jidDecode)(meId);
                if (!participant) {
                    devices.push({ user, device: 0, jid });
                    if (meDevice !== undefined && meDevice !== 0) {
                        if ((0, WABinary_1.isLidUser)(jid) && jlidUser) {
                            devices.push({ user: jlidUser, device: 0, jid: (0, WABinary_1.jidNormalizedUser)(meLid) });
                            const additionalDevices = await getUSyncDevices([jid, meLid], !!useUserDevicesCache, true);
                            devices.push(...additionalDevices);
                        }
                        else {
                            devices.push({ user: meUser, device: 0, jid: (0, WABinary_1.jidNormalizedUser)(meId) });
                            const additionalDevices = await getUSyncDevices([jid, meId], !!useUserDevicesCache, true);
                            devices.push(...additionalDevices);
                        }
                    }
                }
                const allJids = [];
                const meJids = [];
                const otherJids = [];
                for (const { user, device, jid } of devices) {
                    const isMe = user === meUser;
                    const ismeLid = user === jlidUser;
                    const server = ((_k = (0, WABinary_1.jidDecode)(jid)) === null || _k === void 0 ? void 0 : _k.server) || 'lid';
                    const senderId = (0, WABinary_1.jidEncode)(user, server, device);
                    if (isMe || ismeLid) {
                        meJids.push(senderId);
                    }
                    else {
                        otherJids.push(senderId);
                    }
                    allJids.push(senderId);
                }
                await assertSessions(allJids, isretry ? true : false, lids);
                const meMsg = {
                    deviceSentMessage: {
                        destinationJid,
                        message
                    },
                    messageContextInfo: message.messageContextInfo
                };
                const [{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 }, { nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }] = await Promise.all([
                    createParticipantNodes(meJids, meMsg, extraAttrs, lids, meId, meLid),
                    createParticipantNodes(otherJids, message, extraAttrs, lids, meId, meLid)
                ]);
                participants.push(...meNodes);
                participants.push(...otherNodes);
                shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2;
            }
            if (participants.length) {
                if ((additionalAttributes === null || additionalAttributes === void 0 ? void 0 : additionalAttributes['category']) === 'peer') {
                    const peerNode = (_m = (_l = participants[0]) === null || _l === void 0 ? void 0 : _l.content) === null || _m === void 0 ? void 0 : _m[0];
                    if (peerNode) {
                        binaryNodeContent.push(peerNode); // push only enc
                    }
                }
                else {
                    binaryNodeContent.push({
                        tag: 'participants',
                        attrs: {},
                        content: participants
                    });
                }
            }
            const stanza = {
                tag: 'message',
                attrs: {
                    id: msgId,
                    type: getMessageType(message),
                    ...(additionalAttributes || {})
                },
                content: binaryNodeContent
            };
            // if the participant to send to is explicitly specified (generally retry recp)
            // ensure the message is only sent to that person
            // if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
            if (participant) {
                if ((0, WABinary_1.isJidGroup)(destinationJid)) {
                    stanza.attrs.to = destinationJid;
                    stanza.attrs.participant = participant.jid;
                }
                else if ((0, WABinary_1.areJidsSameUser)(participant.jid, meId)) {
                    stanza.attrs.to = participant.jid;
                    stanza.attrs.recipient = destinationJid;
                }
                else {
                    stanza.attrs.to = participant.jid;
                }
            }
            else {
                stanza.attrs.to = destinationJid;
            }
            if (shouldIncludeDeviceIdentity) {
                ;
                stanza.content.push({
                    tag: 'device-identity',
                    attrs: {},
                    content: (0, Utils_1.encodeSignedDeviceIdentity)(authState.creds.account, true)
                });
                logger.debug({ jid }, 'adding device identity');
            }
            const buttonType = getButtonType(message);
            if (buttonType) {
                ;
                stanza.content.push({
                    tag: 'biz',
                    attrs: {},
                    content: [
                        {
                            tag: buttonType,
                            attrs: getButtonArgs(message)
                        }
                    ]
                });
                logger.debug({ jid }, 'adding business node');
            }
            const isPeerMessage = (additionalAttributes === null || additionalAttributes === void 0 ? void 0 : additionalAttributes['category']) === 'peer';
            const is1on1Send = !isGroup && !isStatus && !isNewsletter && !isretry && !isPeerMessage;
            const tcTokenJid = is1on1Send ? (0, WABinary_1.jidNormalizedUser)(destinationJid) : undefined;
            const tcTokenEntry = tcTokenJid
                ? (await authState.keys.get('contacts-tc-token', [tcTokenJid]))[tcTokenJid]
                : undefined;
            let tcTokenBuffer = tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.token;
            if ((tcTokenBuffer === null || tcTokenBuffer === void 0 ? void 0 : tcTokenBuffer.length) && (0, tc_token_utils_1.isTcTokenExpired)(tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.timestamp)) {
                logger.debug({ jid: destinationJid, timestamp: tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.timestamp }, 'tctoken expired, clearing');
                tcTokenBuffer = undefined;
                const cleared = (tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.senderTimestamp) !== undefined
                    ? { token: Buffer.alloc(0), senderTimestamp: tcTokenEntry.senderTimestamp }
                    : null;
                try {
                    await authState.keys.set({ 'contacts-tc-token': { [tcTokenJid]: cleared } });
                }
                catch (_0) { }
            }
            if (tcTokenBuffer === null || tcTokenBuffer === void 0 ? void 0 : tcTokenBuffer.length) {
                ;
                stanza.content.push({
                    tag: 'tctoken',
                    attrs: {},
                    content: tcTokenBuffer
                });
            }
            if (additionalNodes && additionalNodes.length > 0) {
                ;
                stanza.content.push(...additionalNodes);
            }
            const content = (0, Utils_1.normalizeMessageContent)(message);
            const contentType = (0, Utils_1.getContentType)(content);
            if ((0, WABinary_1.isJidGroup)(jid) ||
                (0, WABinary_1.isJidUser)(jid) ||
                ((0, WABinary_1.isLidUser)(jid) &&
                    (contentType === 'interactiveMessage' || contentType === 'buttonsMessage' || contentType === 'listMessage'))) {
                const bizNode = { tag: 'biz', attrs: {} };
                if (((_p = (_o = message === null || message === void 0 ? void 0 : message.viewOnceMessage) === null || _o === void 0 ? void 0 : _o.message) === null || _p === void 0 ? void 0 : _p.interactiveMessage) ||
                    ((_r = (_q = message === null || message === void 0 ? void 0 : message.viewOnceMessageV2) === null || _q === void 0 ? void 0 : _q.message) === null || _r === void 0 ? void 0 : _r.interactiveMessage) ||
                    ((_t = (_s = message === null || message === void 0 ? void 0 : message.viewOnceMessageV2Extension) === null || _s === void 0 ? void 0 : _s.message) === null || _t === void 0 ? void 0 : _t.interactiveMessage) ||
                    (message === null || message === void 0 ? void 0 : message.interactiveMessage) ||
                    ((_v = (_u = message === null || message === void 0 ? void 0 : message.viewOnceMessage) === null || _u === void 0 ? void 0 : _u.message) === null || _v === void 0 ? void 0 : _v.buttonsMessage) ||
                    ((_x = (_w = message === null || message === void 0 ? void 0 : message.viewOnceMessageV2) === null || _w === void 0 ? void 0 : _w.message) === null || _x === void 0 ? void 0 : _x.buttonsMessage) ||
                    ((_z = (_y = message === null || message === void 0 ? void 0 : message.viewOnceMessageV2Extension) === null || _y === void 0 ? void 0 : _y.message) === null || _z === void 0 ? void 0 : _z.buttonsMessage) ||
                    (message === null || message === void 0 ? void 0 : message.buttonsMessage)) {
                    bizNode.content = [
                        {
                            tag: 'interactive',
                            attrs: {
                                type: 'native_flow',
                                v: '1'
                            },
                            content: [
                                {
                                    tag: 'native_flow',
                                    attrs: { v: '9', name: 'mixed' }
                                }
                            ]
                        }
                    ];
                }
                ;
                stanza.content.push(bizNode);
            }
            logger.debug({ msgId }, `sending message to ${participants.length} devices`);
            await sendNode(stanza);
            if (is1on1Send && tcTokenJid && (0, tc_token_utils_1.shouldSendNewTcToken)(tcTokenEntry === null || tcTokenEntry === void 0 ? void 0 : tcTokenEntry.senderTimestamp)) {
                const issueTimestamp = (0, Utils_1.unixTimestampSeconds)();
                getPrivacyTokens([tcTokenJid], issueTimestamp)
                    .then(async (result) => {
                    await (0, tc_token_utils_1.storeTcTokensFromIqResult)({
                        result,
                        fallbackJid: tcTokenJid,
                        keys: authState.keys
                    });
                    const currentData = await authState.keys.get('contacts-tc-token', [tcTokenJid]);
                    const currentEntry = currentData[tcTokenJid];
                    await authState.keys.set({
                        'contacts-tc-token': {
                            [tcTokenJid]: {
                                ...(currentEntry !== null && currentEntry !== void 0 ? currentEntry : { token: Buffer.alloc(0) }),
                                senderTimestamp: issueTimestamp
                            }
                        }
                    });
                })
                    .catch(err => {
                    logger.debug({ jid: destinationJid, err: err === null || err === void 0 ? void 0 : err.message }, 'fire-and-forget tctoken issuance failed');
                });
            }
        });
        return msgId;
    };
    const getMessageType = (message) => {
        if (message.pollCreationMessage || message.pollCreationMessageV2 || message.pollCreationMessageV3) {
            return 'poll';
        }
        return 'text';
    };
    const getMediaType = (message) => {
        if (message.imageMessage) {
            return 'image';
        }
        else if (message.videoMessage) {
            return message.videoMessage.gifPlayback ? 'gif' : 'video';
        }
        else if (message.audioMessage) {
            return message.audioMessage.ptt ? 'ptt' : 'audio';
        }
        else if (message.contactMessage) {
            return 'vcard';
        }
        else if (message.documentMessage) {
            return 'document';
        }
        else if (message.contactsArrayMessage) {
            return 'contact_array';
        }
        else if (message.liveLocationMessage) {
            return 'livelocation';
        }
        else if (message.stickerMessage) {
            return 'sticker';
        }
        else if (message.listMessage) {
            return 'list';
        }
        else if (message.listResponseMessage) {
            return 'list_response';
        }
        else if (message.buttonsResponseMessage) {
            return 'buttons_response';
        }
        else if (message.orderMessage) {
            return 'order';
        }
        else if (message.productMessage) {
            return 'product';
        }
        else if (message.interactiveResponseMessage) {
            return 'native_flow_response';
        }
        else if (message.groupInviteMessage) {
            return 'url';
        }
    };
    const getButtonType = (message) => {
        if (message.buttonsMessage) {
            return 'buttons';
        }
        else if (message.buttonsResponseMessage) {
            return 'buttons_response';
        }
        else if (message.interactiveResponseMessage) {
            return 'interactive_response';
        }
        else if (message.listMessage) {
            return 'list';
        }
        else if (message.listResponseMessage) {
            return 'list_response';
        }
    };
    const getButtonArgs = (message) => {
        if (message.templateMessage) {
            // TODO: Add attributes
            return {};
        }
        else if (message.listMessage) {
            const type = message.listMessage.listType;
            if (!type) {
                throw new boom_1.Boom('Expected list type inside message');
            }
            return { v: '2', type: ListType[type].toLowerCase() };
        }
        else {
            return {};
        }
    };
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
    const waUploadToServer = (0, Utils_1.getWAUploadToServer)(config, refreshMediaConn);
    const waitForMsgMediaUpdate = (0, Utils_1.bindWaitForEvent)(ev, 'messages.media-update');
    return {
        ...sock,
        getPrivacyTokens,
        assertSessions,
        relayMessage,
        sendReceipt,
        sendReceipts,
        readMessages,
        refreshMediaConn,
        waUploadToServer,
        fetchPrivacySettings,
        sendPeerDataOperationMessage,
        createParticipantNodes,
        getUSyncDevices,
        updateMediaMessage: async (message) => {
            const content = (0, Utils_1.assertMediaContent)(message.message);
            const mediaKey = content.mediaKey;
            const meId = authState.creds.me.id;
            const node = await (0, Utils_1.encryptMediaRetryRequest)(message.key, mediaKey, meId);
            let error = undefined;
            await Promise.all([
                sendNode(node),
                waitForMsgMediaUpdate(async (update) => {
                    const result = update.find(c => c.key.id === message.key.id);
                    if (result) {
                        if (result.error) {
                            error = result.error;
                        }
                        else {
                            try {
                                const media = await (0, Utils_1.decryptMediaRetryData)(result.media, mediaKey, result.key.id);
                                if (media.result !== WAProto_1.proto.MediaRetryNotification.ResultType.SUCCESS) {
                                    const resultStr = WAProto_1.proto.MediaRetryNotification.ResultType[media.result];
                                    throw new boom_1.Boom(`Media re-upload failed by device (${resultStr})`, {
                                        data: media,
                                        statusCode: (0, Utils_1.getStatusCodeForMediaRetry)(media.result) || 404
                                    });
                                }
                                content.directPath = media.directPath;
                                content.url = (0, Utils_1.getUrlFromDirectPath)(content.directPath);
                                logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful');
                            }
                            catch (err) {
                                error = err;
                            }
                        }
                        return true;
                    }
                })
            ]);
            if (error) {
                throw error;
            }
            ev.emit('messages.update', [{ key: message.key, update: { message: message.message } }]);
            return message;
        },
        sendMessage: async (jid, content, options = {}) => {
            var _a, _b, _c;
            const userJid = authState.creds.me.id;
            if (typeof content === 'object' &&
                'disappearingMessagesInChat' in content &&
                typeof content['disappearingMessagesInChat'] !== 'undefined' &&
                (0, WABinary_1.isJidGroup)(jid)) {
                const { disappearingMessagesInChat } = content;
                const value = typeof disappearingMessagesInChat === 'boolean'
                    ? disappearingMessagesInChat
                        ? Defaults_1.WA_DEFAULT_EPHEMERAL
                        : 0
                    : disappearingMessagesInChat;
                await groupToggleEphemeral(jid, value);
            }
            else {
                const fullMsg = await (0, Utils_1.generateWAMessage)(jid, content, {
                    logger,
                    userJid,
                    getUrlInfo: text => (0, link_preview_1.getUrlInfo)(text, {
                        thumbnailWidth: linkPreviewImageThumbnailWidth,
                        fetchOpts: {
                            timeout: 3000,
                            ...(axiosOptions || {})
                        },
                        logger,
                        uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
                    }),
                    //TODO: CACHE
                    getProfilePicUrl: sock.profilePictureUrl,
                    upload: waUploadToServer,
                    mediaCache: config.mediaCache,
                    options: config.options,
                    messageId: (0, Utils_1.generateMessageIDV2)((_a = sock.user) === null || _a === void 0 ? void 0 : _a.id),
                    ...options
                });
                const isDeleteMsg = 'delete' in content && !!content.delete;
                const isEditMsg = 'edit' in content && !!content.edit;
                const isPinMsg = 'pin' in content && !!content.pin;
                const isPollMessage = 'poll' in content && !!content.poll;
                const additionalAttributes = {};
                const additionalNodes = [];
                // required for delete
                if (isDeleteMsg) {
                    // if the chat is a group, and I am not the author, then delete the message as an admin
                    if ((0, WABinary_1.isJidGroup)((_b = content.delete) === null || _b === void 0 ? void 0 : _b.remoteJid) && !((_c = content.delete) === null || _c === void 0 ? void 0 : _c.fromMe)) {
                        additionalAttributes.edit = '8';
                    }
                    else {
                        additionalAttributes.edit = '7';
                    }
                }
                else if (isEditMsg) {
                    additionalAttributes.edit = '1';
                }
                else if (isPinMsg) {
                    additionalAttributes.edit = '2';
                }
                else if (isPollMessage) {
                    additionalNodes.push({
                        tag: 'meta',
                        attrs: {
                            polltype: 'creation'
                        }
                    });
                }
                if ('cachedGroupMetadata' in options) {
                    console.warn('cachedGroupMetadata in sendMessage are deprecated, now cachedGroupMetadata is part of the socket config.');
                }
                await relayMessage(jid, fullMsg.message, {
                    messageId: fullMsg.key.id,
                    useCachedGroupMetadata: options.useCachedGroupMetadata,
                    additionalAttributes,
                    statusJidList: options.statusJidList,
                    additionalNodes,
                    excludeJids: options.excludeJids,
                    includeJids: options.includeJids
                });
                if (config.emitOwnEvents) {
                    process.nextTick(() => {
                        processingMutex.mutex(() => upsertMessage(fullMsg, 'append'));
                    });
                }
                return fullMsg;
            }
        },
        /**
         * Envia uma mensagem em grupo apenas para um participante (targetJid).
         * Só o target recebe a mensagem; os demais podem ver "aguardando esta mensagem".
         * Deve ser usada apenas para grupos.
         *
         * @param jid - JID do grupo (g.us)
         * @param messageObject - Conteúdo da mensagem (só entregue ao targetJid)
         * @param options - Opções incluindo targetJid e targetOnly0Device
         */
        sendSecretGroupMessage: async (jid, messageObject, options = {}) => {
            var _a, _b, _c, _d, _e, _f;
            if (!(0, WABinary_1.isJidGroup)(jid)) {
                throw new boom_1.Boom('sendSecretGroupMessage deve ser usada apenas para grupos (g.us)', { statusCode: 400 });
            }
            const targetJid = options.targetJid;
            if (!targetJid) {
                throw new boom_1.Boom('options.targetJid é obrigatório em sendSecretGroupMessage', { statusCode: 400 });
            }
            const { targetJid: _targetJid, targetOnly0Device: _targetOnly0Device, ...messageGenOptions } = options;
            const targetOnly0Device = options.targetOnly0Device === true;
            const userJid = authState.creds.me.id;
            const meLid = authState.creds.me.lid || authState.creds.me.id;
            const jlidUser = (_b = (0, WABinary_1.jidDecode)((_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.lid)) === null || _b === void 0 ? void 0 : _b.user;
            const useUserDevicesCache = options.useUserDevicesCache !== false;
            const useCachedGroupMetadata = options.useCachedGroupMetadata !== false;
            const additionalAttributes = options.additionalAttributes || {};
            const additionalNodes = options.additionalNodes || [];
            const groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined;
            const resolvedGroupData = groupData && Array.isArray(groupData === null || groupData === void 0 ? void 0 : groupData.participants) ? groupData : await groupMetadata(jid);
            const participantsList = resolvedGroupData.participants
                .map((p) => p.lid || p.id)
                .filter((jid) => !!jid);
            // Resolver targetJid para o(s) jid(s) do participante no grupo (LID ou id), pois o grupo pode usar LID
            const normalizedTarget = (0, WABinary_1.jidNormalizedUser)(targetJid);
            const targetParticipantJids = new Set();
            for (const p of resolvedGroupData.participants) {
                const pid = p.id;
                const plid = p.lid;
                const pjid = p.jid;
                const matches = (0, WABinary_1.areJidsSameUser)(pid, normalizedTarget) ||
                    (plid ? (0, WABinary_1.areJidsSameUser)(plid, normalizedTarget) : false) ||
                    (pjid ? (0, WABinary_1.areJidsSameUser)(pjid, normalizedTarget) : false);
                if (matches) {
                    const j = plid || pid;
                    if (j)
                        targetParticipantJids.add((0, WABinary_1.jidNormalizedUser)(j));
                }
            }
            logger.debug({ targetJid: normalizedTarget, targetParticipantJids: [...targetParticipantJids] }, 'sendSecretGroupMessage: target resolved');
            const additionalDevices = await getUSyncDevices(participantsList, useUserDevicesCache, false);
            const devices = [...additionalDevices];
            const mePhone = additionalDevices.some((d) => { var _a; return d.user === jlidUser && ((_a = d.device) !== null && _a !== void 0 ? _a : 0) === 0; });
            if (!mePhone && jlidUser) {
                devices.push({ user: jlidUser, device: 0, jid: (0, WABinary_1.jidNormalizedUser)(meLid) });
            }
            const messageId = options.messageId || (0, Utils_1.generateMessageIDV2)((_c = sock.user) === null || _c === void 0 ? void 0 : _c.id);
            const messageIdReal = (0, Utils_1.generateMessageIDV2)((_d = sock.user) === null || _d === void 0 ? void 0 : _d.id);
            const targetDeviceJids = [];
            for (const d of devices) {
                const server = ((_e = (0, WABinary_1.jidDecode)(d.jid)) === null || _e === void 0 ? void 0 : _e.server) || 'lid';
                const deviceNum = (_f = d.device) !== null && _f !== void 0 ? _f : 0;
                const fullDeviceJid = (0, WABinary_1.jidEncode)(d.user, server, deviceNum);
                const deviceParticipantJid = d.jid ? (0, WABinary_1.jidNormalizedUser)(d.jid) : '';
                const isTarget = !!deviceParticipantJid &&
                    targetParticipantJids.has(deviceParticipantJid) &&
                    (!targetOnly0Device || deviceNum === 0);
                if (isTarget)
                    targetDeviceJids.push(fullDeviceJid);
            }
            // Placeholder (mensagem vazia) para o grupo com messageId; sem isso o servidor não exibe a mensagem.
            const placeholderMsg = await (0, Utils_1.generateWAMessage)(jid, { text: '\u200B' }, {
                logger,
                userJid,
                getUrlInfo: async () => undefined,
                getProfilePicUrl: sock.profilePictureUrl,
                upload: waUploadToServer,
                mediaCache: config.mediaCache,
                options: config.options,
                messageId,
                ...messageGenOptions
            });
            await relayMessage(jid, placeholderMsg.message, {
                messageId,
                useCachedGroupMetadata: true,
                useUserDevicesCache: true,
                additionalAttributes,
                additionalNodes
            });
            // Mensagem real só para o target, com outro messageId para de fato chegar (o cliente não substitui pelo mesmo id).
            const fullMsgReal = await (0, Utils_1.generateWAMessage)(jid, messageObject, {
                logger,
                userJid,
                getUrlInfo: (text) => (0, link_preview_1.getUrlInfo)(text, {
                    thumbnailWidth: linkPreviewImageThumbnailWidth,
                    fetchOpts: { timeout: 3000, ...(axiosOptions || {}) },
                    logger,
                    uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
                }),
                getProfilePicUrl: sock.profilePictureUrl,
                upload: waUploadToServer,
                mediaCache: config.mediaCache,
                options: config.options,
                messageId: messageIdReal,
                ...messageGenOptions
            });
            for (const targetDeviceJid of targetDeviceJids) {
                try {
                    await relayMessage(jid, fullMsgReal.message, {
                        messageId: messageIdReal,
                        participant: { jid: targetDeviceJid, count: 0 },
                        useCachedGroupMetadata: true,
                        useUserDevicesCache: true,
                        additionalAttributes,
                        additionalNodes
                    });
                }
                catch (err) {
                    logger.warn({ err, targetDeviceJid, messageId: messageIdReal }, 'sendSecretGroupMessage: relay to target failed');
                }
            }
            logger.debug({ msgIdPlaceholder: messageId, msgIdReal: messageIdReal, targetDevices: targetDeviceJids.length }, 'sendSecretGroupMessage: placeholder to all, real (own id) to target');
            if (config.emitOwnEvents) {
                process.nextTick(() => {
                    processingMutex.mutex(() => upsertMessage(fullMsgReal, 'append'));
                });
            }
            return fullMsgReal;
        }
    };
};
exports.makeMessagesSocket = makeMessagesSocket;
