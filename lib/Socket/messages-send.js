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
const bounded_ttl_map_1 = require("../Utils/bounded-ttl-map");
const make_mutex_1 = require("../Utils/make-mutex");
const WABinary_1 = require("../WABinary");
const WAUSync_1 = require("../WAUSync");
const newsletter_1 = require("./newsletter");
const tc_token_1 = require("./tc-token");
const cache_utils_1 = __importDefault(require("../Utils/cache-utils"));
const USYNC_DEVICES_BATCH = 400;
const SESSION_FETCH_BATCH = 400;
const ENCRYPT_BATCH = 200;
const USER_DEVICES_NEGATIVE_TTL = 10 * 60;
/** ±20% para as entradas de um grupo, criadas juntas, não vencerem todas no mesmo envio */
const jitteredTtl = (ttl) => Math.round(ttl * (0.8 + Math.random() * 0.4));
const makeMessagesSocket = (config) => {
    const { logger, linkPreviewImageThumbnailWidth, generateHighQualityLinkPreview, options: axiosOptions, patchMessageBeforeSending, cachedGroupMetadata } = config;
    const sock = (0, newsletter_1.makeNewsletterSocket)(config);
    const pocRelayTrace = process.env.WA_POC_RELAY_TRACE === '1';
    // O retry chega depois do sendMessage e não carrega includeJids. Guardamos o
    // conjunto resolvido (PN + LID) para recuperar apenas devices que receberam o relay original.
    // guardam a mensagem inteira: teto de entradas e 15 min (retry chega em segundos), sem timer por socket
    const selectiveRelayCache = new bounded_ttl_map_1.BoundedTtlMap(500, 15 * 60 * 1000);
    const selectiveMessageCache = new bounded_ttl_map_1.BoundedTtlMap(500, 15 * 60 * 1000);
    const { ev, authState, messageMutex, signalRepository, upsertMessage, query, fetchPrivacySettings, sendNode, groupMetadata, groupToggleEphemeral } = sock;
    const { trackTcTokenJid, flushTcTokenIndex, withFlushedTcTokenIndex, getLidForPn, cacheLidMapping, tcTokenStorageJid, appendPrivacyToken, maybeIssueTcToken, reissueTcTokenAfterIdentityChange, getPrivacyTokens, scheduleTcTokenPrune, cancelTcTokenPrune } = (0, tc_token_1.makeTcTokenManager)({ authState, logger, query });
    let userDevicesCache;
    if (config.userDevicesCache) {
        userDevicesCache = config.userDevicesCache;
    }
    else {
        const cache = new node_cache_1.default({
            stdTTL: Defaults_1.DEFAULT_CACHE_TTLS.USER_DEVICES,
            useClones: false
        });
        sock.onSocketEnd(() => cache.close());
        userDevicesCache = cache;
    }
    const groupRelayMutex = (0, make_mutex_1.makeKeyedMutex)();
    const emitOwnMessage = (msg) => {
        var _a, _b;
        const author = (0, WABinary_1.jidNormalizedUser)((_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.id);
        messageMutex
            .mutex((0, WABinary_1.jidNormalizedUser)(((_b = msg.key) === null || _b === void 0 ? void 0 : _b.remoteJid) || author), author, () => upsertMessage(msg, 'append'))
            .catch(err => { var _a; return logger.warn({ err, id: (_a = msg.key) === null || _a === void 0 ? void 0 : _a.id }, 'falha ao emitir mensagem própria'); });
    };
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
    /**
     * Devices dos jids. O cache guarda a lista completa (com o device 0) e o filtro sai na leitura: o envio 1:1
     * pede sem device 0 e o de grupo com, e a mesma entrada servia os dois cortada.
     * `complete` é falso quando algum lote do USync não respondeu ou veio usuário com erro.
     */
    const fetchUSyncDevices = async (jids, useCache, ignoreZeroDevices) => {
        var _a;
        const deviceResults = [];
        const keep = (item) => !ignoreZeroDevices || item.device !== 0;
        let complete = true;
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
                    deviceResults.push(...devices.filter(keep));
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
            return { devices: deviceResults, complete };
        }
        const deviceMap = {};
        const answeredWithoutError = new Set();
        // grupo grande num IQ único estoura o tamanho da resposta: consulta em lotes
        for (let i = 0; i < toFetch.length; i += USYNC_DEVICES_BATCH) {
            const query = new WAUSync_1.USyncQuery().withContext('message').withDeviceProtocol();
            for (const jid of toFetch.slice(i, i + USYNC_DEVICES_BATCH)) {
                query.withUser(new WAUSync_1.USyncUser().withId(jid));
            }
            const result = await sock.executeUSyncQuery(query);
            if (!result) {
                complete = false;
                continue;
            }
            for (const item of result.list) {
                const devices = item.devices;
                if (devices === null || devices === void 0 ? void 0 : devices.error) {
                    complete = false;
                }
                else if (item.id) {
                    answeredWithoutError.add((0, WABinary_1.jidNormalizedUser)(item.id));
                }
            }
            const extracted = (0, Utils_1.extractDeviceJids)(result.list, authState.creds.me.id, false, (_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.lid);
            for (const item of extracted) {
                const cacheKey = (0, WABinary_1.jidNormalizedUser)(item.jid);
                deviceMap[cacheKey] = deviceMap[cacheKey] || [];
                deviceMap[cacheKey].push(item);
                if (keep(item)) {
                    deviceResults.push(item);
                }
            }
        }
        for (const key in deviceMap) {
            userDevicesCache.set(key, deviceMap[key], jitteredTtl(Defaults_1.DEFAULT_CACHE_TTLS.USER_DEVICES));
        }
        // cache negativo: sem isso quem não tem device é reconsultado em todo envio. Só para quem o servidor
        // respondeu sem erro; erro passageiro não pode deixar o usuário 10 min fora do envio.
        for (const jid of toFetch) {
            if (answeredWithoutError.has(jid) && !deviceMap[jid]) {
                userDevicesCache.set(jid, [], USER_DEVICES_NEGATIVE_TTL);
            }
        }
        return { devices: deviceResults, complete };
    };
    /** Fetch all the devices we've to send a message to */
    const getUSyncDevices = async (jids, useCache, ignoreZeroDevices) => (await fetchUSyncDevices(jids, useCache, ignoreZeroDevices)).devices;
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
        // grupo grande num IQ único: resposta enorme e todas as sessões injetadas de uma vez
        for (let i = 0; i < jidsRequiringFetch.length; i += SESSION_FETCH_BATCH) {
            const batch = jidsRequiringFetch.slice(i, i + SESSION_FETCH_BATCH);
            logger.debug({ jidsRequiringFetch: batch }, 'fetching sessions');
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
                        content: batch.map(jid => ({
                            tag: 'user',
                            attrs: { jid }
                        }))
                    }
                ]
            });
            const { failed } = await (0, Utils_1.parseAndInjectE2ESessions)(result, signalRepository, lids, meid, melid);
            if (failed.length) {
                logger.warn({ failed }, 'falha ao injetar sessão; esses devices ficam para o retry');
            }
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
        const failedJids = [];
        const encryptOne = async (patchedMessageWithJid) => {
            const { recipientJid: jid, ...patchedMessage } = patchedMessageWithJid;
            if (!jid) {
                return undefined;
            }
            const bytes = (0, Utils_1.encodeWAMessage)(patchedMessage);
            let encrypted;
            try {
                encrypted = await signalRepository.encryptMessage({
                    jid: (0, Utils_1.convertlidDevice)(jid, lid, meid, melid),
                    data: bytes
                });
            }
            catch (err) {
                // um device com sessão ruim não derruba o envio dos outros; ele cai no retry
                logger.warn({ jid, err: err === null || err === void 0 ? void 0 : err.message }, 'falha ao cifrar para o device');
                failedJids.push(jid);
                return undefined;
            }
            const { type, ciphertext } = encrypted;
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
        };
        // cifrar milhares de devices num Promise.all só segura o event loop de todas as sessões do processo
        const results = [];
        for (let i = 0; i < patched.length; i += ENCRYPT_BATCH) {
            if (i) {
                await new Promise(resolve => setImmediate(resolve));
            }
            results.push(...(await Promise.all(patched.slice(i, i + ENCRYPT_BATCH).map(encryptOne))));
        }
        const nodes = results.filter((node) => !!node);
        return { nodes, shouldIncludeDeviceIdentity, failedJids };
    };
    /** usuários (PN e LID, sem server) de um participante do grupo */
    const participantUsers = (p) => [p.id, p.lid].map(jid => { var _a; return (jid ? (_a = (0, WABinary_1.jidDecode)(jid)) === null || _a === void 0 ? void 0 : _a.user : undefined); }).filter((user) => !!user);
    const participantMatches = (p, jid) => [p.id, p.lid, p.jid].some(candidate => !!candidate && (0, WABinary_1.areJidsSameUser)(candidate, jid));
    /** usuários de cada jid da lista, já com o par PN/LID que a metadata do grupo conhece */
    const resolveGroupUsers = (jids, groupData) => {
        var _a;
        const users = new Set();
        for (const jid of jids) {
            const normalized = (0, WABinary_1.jidNormalizedUser)(jid);
            const user = (_a = (0, WABinary_1.jidDecode)(normalized)) === null || _a === void 0 ? void 0 : _a.user;
            if (user) {
                users.add(user);
            }
            for (const p of (groupData === null || groupData === void 0 ? void 0 : groupData.participants) || []) {
                if (participantMatches(p, normalized)) {
                    participantUsers(p).forEach(u => users.add(u));
                }
            }
        }
        return users;
    };
    /**
     * Relay seletivo (sussurro): tira de `devices` quem não está em includeJids (o remetente sempre fica) e
     * devolve os usuários que podem pedir retry. Quem fica sem device no <participants> não recebe o SKDM e vê
     * o stub.
     */
    const applySelectiveRelayFilter = (devices, groupData, { includeJids, meJids, jlidUser }) => {
        const meUsers = meJids.map(jid => { var _a; return (_a = (0, WABinary_1.jidDecode)((0, WABinary_1.jidNormalizedUser)(jid))) === null || _a === void 0 ? void 0 : _a.user; }).filter((u) => !!u);
        // remetente sempre incluído (phone + lid), senão o próprio bot não lê a mensagem
        const includeUsers = resolveGroupUsers(includeJids, groupData);
        meUsers.forEach(u => includeUsers.add(u));
        if (jlidUser) {
            includeUsers.add(jlidUser);
        }
        for (let i = devices.length - 1; i >= 0; i--) {
            if (!devices[i].user || !includeUsers.has(devices[i].user)) {
                devices.splice(i, 1);
            }
        }
        logger.info({ includeUsers: [...includeUsers], remaining: devices.length }, 'exclude-relay: whitelist aplicada (apenas includeJids + remetente recebem sender-key)');
        return includeUsers;
    };
    const relayMessage = async (jid, message, { messageId: msgId, participant, additionalAttributes, additionalNodes, useUserDevicesCache, useCachedGroupMetadata, statusJidList, newsletterMediaId, isretry, includeJids, decryptFailHide }) => {
        var _a, _b, _c, _d;
        if (!((_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.id)) {
            throw new boom_1.Boom('Not authenticated');
        }
        if (additionalAttributes) {
            additionalAttributes = { ...additionalAttributes };
        }
        const meId = authState.creds.me.id;
        const meLid = authState.creds.me.lid || authState.creds.me.id;
        const lidattrs = (0, WABinary_1.jidDecode)((_b = authState.creds.me) === null || _b === void 0 ? void 0 : _b.lid);
        const jlidUser = lidattrs === null || lidattrs === void 0 ? void 0 : lidattrs.user;
        let lids;
        if ((0, WABinary_1.isJidUser)(jid) || (0, WABinary_1.isJidUser)(participant === null || participant === void 0 ? void 0 : participant.jid)) {
            const userQuery = (0, WABinary_1.jidNormalizedUser)((participant === null || participant === void 0 ? void 0 : participant.jid) || jid);
            if (!(0, WABinary_1.isLidUser)(userQuery)) {
                const verify = cache_utils_1.default.lidCache.get(userQuery);
                if (verify) {
                    lids = verify;
                    cacheLidMapping(userQuery, verify);
                }
                else {
                    const usyncQuery = new WAUSync_1.USyncQuery().withContactProtocol().withLIDProtocol();
                    usyncQuery.withUser(new WAUSync_1.USyncUser().withPhone(userQuery.split('@')[0]));
                    const results = await sock.executeUSyncQuery(usyncQuery);
                    if (results === null || results === void 0 ? void 0 : results.list) {
                        const maybeLid = (_c = results.list[0]) === null || _c === void 0 ? void 0 : _c.lid;
                        if (typeof maybeLid === 'string') {
                            cacheLidMapping(userQuery, maybeLid);
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
        // Relays seletivos ocultam por padrão a falha de decrypt nos devices que não receberam
        // o SKDM. O chamador ainda pode usar decryptFailHide:false para observar o placeholder.
        const shouldHideDecryptFailure = decryptFailHide !== null && decryptFailHide !== void 0 ? decryptFailHide : !!(includeJids === null || includeJids === void 0 ? void 0 : includeJids.length);
        let shouldIncludeDeviceIdentity = false;
        msgId = msgId || (0, Utils_1.generateMessageIDV2)((_d = sock.user) === null || _d === void 0 ? void 0 : _d.id);
        useUserDevicesCache = useUserDevicesCache !== false;
        useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus;
        const participants = [];
        const destinationJid = !isStatus ? (0, WABinary_1.jidEncode)(user, isLid ? 'lid' : isGroup ? 'g.us' : 's.whatsapp.net') : statusJid;
        const binaryNodeContent = [];
        const devices = [];
        let selectiveAllowedUsers;
        const extraAttrs = {};
        if (participant) {
            if (!isGroup && !isStatus) {
                additionalAttributes = { ...additionalAttributes, device_fanout: 'false' };
            }
            const { user, device } = (0, WABinary_1.jidDecode)(participant.jid);
            devices.push({ user, device, jid: (0, WABinary_1.jidNormalizedUser)(participant.jid) });
        }
        // metadata e devices não tocam chave: buscados antes da transação para não segurá-la durante a rede.
        // No retry (participant) a metadata não é usada.
        let groupData;
        let groupDevices = [];
        let groupDevicesComplete = false;
        if ((isGroup || isStatus) && !participant) {
            if (isGroup) {
                groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined;
                if (groupData && Array.isArray(groupData.participants)) {
                    logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata');
                }
                else {
                    groupData = await groupMetadata(jid);
                }
            }
            const participantsList = groupData ? groupData.participants.map(p => p.lid || p.id) : [];
            if (isStatus && statusJidList) {
                participantsList.push(...statusJidList);
            }
            const fetched = await fetchUSyncDevices(participantsList, !!useUserDevicesCache, false);
            groupDevices = fetched.devices;
            // poda só com a lista de fato completa: lote sem resposta ou metadata vazia apagaria as marcas e o
            // envio seguinte redistribuiria o SKDM para o grupo inteiro
            groupDevicesComplete = fetched.complete && participantsList.length > 0;
        }
        const relayInTransaction = () => authState.keys.transaction(async () => {
            var _a, _b, _c, _d, _e;
            const mediaType = getMediaType(message);
            if (mediaType) {
                extraAttrs['mediatype'] = mediaType;
            }
            if (isNewsletter) {
                const patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message;
                const bytes = (0, Utils_1.encodeNewsletterMessage)(patched);
                binaryNodeContent.push({
                    tag: 'plaintext',
                    attrs: mediaType ? { mediatype: mediaType } : {},
                    content: bytes
                });
                const stanza = {
                    tag: 'message',
                    attrs: {
                        to: jid,
                        id: msgId,
                        type: getMessageType(message),
                        ...(newsletterMediaId ? { media_id: newsletterMediaId } : {}),
                        ...(additionalAttributes || {})
                    },
                    content: binaryNodeContent
                };
                logger.debug({ msgId, mediaType, hasMediaId: !!newsletterMediaId }, `sending newsletter message to ${jid}`);
                await sendNode(stanza);
                return;
            }
            if ((_a = (0, Utils_1.normalizeMessageContent)(message)) === null || _a === void 0 ? void 0 : _a.pinInChatMessage) {
                extraAttrs['decrypt-fail'] = 'hide';
            }
            if (isGroup || isStatus) {
                // cópia: o objeto do cache não pode ficar marcado se o envio falhar no meio
                const senderKeyMap = {};
                if (!participant && !isStatus) {
                    const result = await authState.keys.get('sender-key-memory', [jid]);
                    Object.assign(senderKeyMap, result[jid]);
                }
                let senderKeyMapChanged = false;
                if (!participant) {
                    if (!isStatus) {
                        additionalAttributes = {
                            ...additionalAttributes,
                            addressing_mode: (groupData === null || groupData === void 0 ? void 0 : groupData.addressingMode) || 'pn'
                        };
                    }
                    devices.push(...groupDevices);
                    const Mephone = groupDevices.some(d => d.user === jlidUser && d.device === 0);
                    if (!Mephone && jlidUser) {
                        devices.push({ user: jlidUser, device: 0, jid: (0, WABinary_1.jidNormalizedUser)(meLid) });
                    }
                    if (includeJids === null || includeJids === void 0 ? void 0 : includeJids.length) {
                        selectiveAllowedUsers = applySelectiveRelayFilter(devices, groupData, {
                            includeJids,
                            meJids: [meId, meLid],
                            jlidUser
                        });
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
                    forceRotate: !!(includeJids === null || includeJids === void 0 ? void 0 : includeJids.length)
                });
                const senderKeyJids = [];
                // sender key rotacionada (selective relay) precisa ser redistribuída a todos
                const skdmToAll = !!(includeJids === null || includeJids === void 0 ? void 0 : includeJids.length);
                if (skdmToAll && !participant) {
                    // a chave antiga morreu na rotação e o SKDM da nova só vai pros devices que
                    // sobraram no relay seletivo. Sem zerar o map, os de fora continuam marcados
                    // como "já recebeu" e nunca ganham a chave nova => todas as mensagens
                    // seguintes do grupo ficam em "aguardando" pra eles. Zerado, o próximo envio
                    // normal redistribui o SKDM (já na iteração atual, então o skmsg seletivo
                    // continua indecifrável pra quem ficou de fora).
                    for (const key of Object.keys(senderKeyMap)) {
                        delete senderKeyMap[key];
                        senderKeyMapChanged = true;
                    }
                }
                const currentDeviceIds = new Set();
                for (const { user, device, jid } of devices) {
                    const server = ((_b = (0, WABinary_1.jidDecode)(jid)) === null || _b === void 0 ? void 0 : _b.server) || 'lid';
                    const senderId = (0, WABinary_1.jidEncode)(user, server, device);
                    currentDeviceIds.add(senderId);
                    // só manda SKDM pra quem ainda não recebeu a sender key;
                    // mandar pra todos em todo envio provoca retry receipt de devices
                    // quebrados a cada mensagem
                    if (!senderKeyMap[senderId] || !!participant || skdmToAll) {
                        senderKeyJids.push(senderId);
                        senderKeyMapChanged = senderKeyMapChanged || !senderKeyMap[senderId];
                        senderKeyMap[senderId] = true;
                    }
                }
                // com a lista completa de devices do grupo, quem saiu sai do mapa; se voltar, recebe o SKDM de novo
                if (isGroup && !participant && groupDevicesComplete && !(includeJids === null || includeJids === void 0 ? void 0 : includeJids.length)) {
                    for (const key of Object.keys(senderKeyMap)) {
                        if (!currentDeviceIds.has(key)) {
                            delete senderKeyMap[key];
                            senderKeyMapChanged = true;
                        }
                    }
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
                    // sem force: no retry o sendMessagesAgain já renovou a sessão; forçar de novo
                    // sobrescreve a sessão recém-injetada e gasta outra prekey do destinatário
                    await assertSessions(senderKeyJids, false, lids);
                    const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, extraAttrs, lids, meId, meLid);
                    shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity;
                    for (const failedJid of result.failedJids) {
                        delete senderKeyMap[failedJid];
                    }
                    participants.push(...result.nodes);
                }
                binaryNodeContent.push({
                    tag: 'enc',
                    attrs: {
                        v: '2',
                        type: 'skmsg',
                        ...extraAttrs,
                        ...(shouldHideDecryptFailure ? { 'decrypt-fail': 'hide' } : {})
                    },
                    content: ciphertext
                });
                // só persiste no envio normal e quando mudou; no retry (participant) o map começa vazio
                // e persistir aqui clobberaria o map completo do grupo
                if (!participant && !isStatus && senderKeyMapChanged) {
                    await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } });
                }
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
                    const server = ((_c = (0, WABinary_1.jidDecode)(jid)) === null || _c === void 0 ? void 0 : _c.server) || 'lid';
                    const senderId = (0, WABinary_1.jidEncode)(user, server, device);
                    if (isMe || ismeLid) {
                        meJids.push(senderId);
                    }
                    else {
                        otherJids.push(senderId);
                    }
                    allJids.push(senderId);
                }
                await assertSessions(allJids, false, lids);
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
                // os devices do próprio bot quase sempre cifram: o que importa é o destinatário ter recebido
                if (otherJids.length && !otherNodes.length) {
                    throw new boom_1.Boom('All encryptions failed', { statusCode: 500 });
                }
                participants.push(...meNodes);
                participants.push(...otherNodes);
                shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2;
            }
            if (participants.length) {
                if ((additionalAttributes === null || additionalAttributes === void 0 ? void 0 : additionalAttributes['category']) === 'peer') {
                    const peerNode = (_e = (_d = participants[0]) === null || _d === void 0 ? void 0 : _d.content) === null || _e === void 0 ? void 0 : _e[0];
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
            const { is1on1Send, tcTokenJid } = await appendPrivacyToken(stanza, {
                destinationJid,
                isGroup,
                isStatus,
                isNewsletter,
                isPeer: (additionalAttributes === null || additionalAttributes === void 0 ? void 0 : additionalAttributes['category']) === 'peer',
                isRetry: !!isretry,
                participantJid: participant === null || participant === void 0 ? void 0 : participant.jid,
                meIds: [meId, meLid]
            });
            if (additionalNodes && additionalNodes.length > 0) {
                ;
                stanza.content.push(...additionalNodes);
            }
            const hasCustomBizNode = additionalNodes === null || additionalNodes === void 0 ? void 0 : additionalNodes.some(node => node.tag === 'biz');
            const bizNode = hasCustomBizNode ? undefined : getBusinessNode(message);
            if (bizNode) {
                ;
                stanza.content.push(bizNode);
                logger.debug({ jid }, 'adding business node');
            }
            logger.debug({ msgId }, `sending message to ${participants.length} devices`);
            if (pocRelayTrace && (isGroup || participant)) {
                logger.info({
                    msgId,
                    to: stanza.attrs.to,
                    participant: stanza.attrs.participant,
                    isretry: !!isretry,
                    participantNodes: participants.length,
                    deviceCandidates: devices.length,
                    contentTags: Array.isArray(stanza.content) ? stanza.content.map(node => node.tag) : [],
                    hasSkmsg: binaryNodeContent.some(node => node.tag === 'enc' && node.attrs.type === 'skmsg'),
                    includeCount: (includeJids === null || includeJids === void 0 ? void 0 : includeJids.length) || 0,
                    decryptFailHide: shouldHideDecryptFailure
                }, '[POC relay trace] outbound stanza');
            }
            await sendNode(stanza);
            if (isGroup && !participant && selectiveAllowedUsers) {
                const selectiveCacheKey = `${destinationJid}:${msgId}`;
                selectiveRelayCache.set(selectiveCacheKey, {
                    groupJid: destinationJid,
                    allowedUsers: [...selectiveAllowedUsers],
                    decryptFailHide: shouldHideDecryptFailure
                });
                selectiveMessageCache.set(selectiveCacheKey, message);
            }
            if (is1on1Send && tcTokenJid) {
                void maybeIssueTcToken(destinationJid, message, {
                    participant,
                    additionalAttributes,
                    storageJid: tcTokenJid,
                    msgId: stanza.attrs.id
                });
            }
        });
        // envios ao mesmo grupo em série: o segundo via os devices novos como "já receberam" o SKDM do
        // primeiro, que ainda estava cifrando, e chegava antes dele sem a chave (retry no destinatário)
        if (isGroup && !participant) {
            await groupRelayMutex.mutex(jid, relayInTransaction);
        }
        else {
            await relayInTransaction();
        }
        return msgId;
    };
    const getMessageType = (message) => {
        var _a;
        if (message.pollCreationMessage || message.pollCreationMessageV2 || message.pollCreationMessageV3) {
            return 'poll';
        }
        if (getMediaType(message)) {
            return 'media';
        }
        if ((_a = (0, Utils_1.normalizeMessageContent)(message)) === null || _a === void 0 ? void 0 : _a.listMessage) {
            return 'media';
        }
        return 'text';
    };
    const getMediaType = (message) => {
        message = (0, Utils_1.normalizeMessageContent)(message) || message;
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
            return { v: '2', type: type === ListType.SINGLE_SELECT ? 'product_list' : ListType[type].toLowerCase() };
        }
        else {
            return {};
        }
    };
    const getBusinessNode = (message) => {
        var _a, _b, _c;
        const content = (0, Utils_1.normalizeMessageContent)(message);
        if (!content) {
            return;
        }
        const attrs = {
            actual_actors: '2',
            host_storage: '2',
            privacy_mode_ts: (0, Utils_1.unixTimestampSeconds)().toString()
        };
        const nativeFlow = (_a = content.interactiveMessage) === null || _a === void 0 ? void 0 : _a.nativeFlowMessage;
        const paymentFlowName = ((_b = nativeFlow === null || nativeFlow === void 0 ? void 0 : nativeFlow.buttons) === null || _b === void 0 ? void 0 : _b.some(button => button.name === 'payment_info'))
            ? 'payment_info'
            : ((_c = nativeFlow === null || nativeFlow === void 0 ? void 0 : nativeFlow.buttons) === null || _c === void 0 ? void 0 : _c.some(button => button.name === 'review_and_pay'))
                ? 'order_details'
                : undefined;
        if (paymentFlowName) {
            return {
                tag: 'biz',
                attrs: {
                    ...attrs,
                    native_flow_name: paymentFlowName
                }
            };
        }
        if (nativeFlow || content.buttonsMessage) {
            return {
                tag: 'biz',
                attrs,
                content: [
                    {
                        tag: 'interactive',
                        attrs: { type: 'native_flow', v: '1' },
                        content: [
                            {
                                tag: 'native_flow',
                                attrs: { v: '9', name: 'mixed' }
                            }
                        ]
                    },
                    {
                        tag: 'quality_control',
                        attrs: { source_type: 'third_party' }
                    }
                ]
            };
        }
        const buttonType = getButtonType(content);
        if (!buttonType) {
            return;
        }
        return {
            tag: 'biz',
            attrs: {},
            content: [
                {
                    tag: buttonType,
                    attrs: getButtonArgs(content)
                }
            ]
        };
    };
    const waUploadToServer = (0, Utils_1.getWAUploadToServer)(config, refreshMediaConn);
    const waitForMsgMediaUpdate = (0, Utils_1.bindWaitForEvent)(ev, 'messages.media-update');
    return {
        ...sock,
        getPrivacyTokens,
        reissueTcTokenAfterIdentityChange,
        scheduleTcTokenPrune,
        cancelTcTokenPrune,
        getLidForPn,
        cacheLidMapping,
        tcTokenStorageJid,
        trackTcTokenJid,
        flushTcTokenIndex,
        withFlushedTcTokenIndex,
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
        userDevicesCache,
        getSelectiveRelayContext: (groupJid, messageId) => selectiveRelayCache.get(`${groupJid}:${messageId}`),
        getSelectiveSentMessage: (groupJid, messageId) => selectiveMessageCache.get(`${groupJid}:${messageId}`),
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
            var _a, _b, _c, _d, _e, _f;
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
                const newsletterMediaId = (_b = fullMsg.message) === null || _b === void 0 ? void 0 : _b.__newsletterMediaId;
                const additionalAttributes = {};
                const additionalNodes = [];
                // required for delete
                if (isDeleteMsg) {
                    // if the chat is a group, and I am not the author, then delete the message as an admin
                    if ((0, WABinary_1.isJidGroup)((_c = content.delete) === null || _c === void 0 ? void 0 : _c.remoteJid) && !((_d = content.delete) === null || _d === void 0 ? void 0 : _d.fromMe)) {
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
                    logger.warn('cachedGroupMetadata in sendMessage are deprecated, now cachedGroupMetadata is part of the socket config.');
                }
                await relayMessage(jid, fullMsg.message, {
                    messageId: fullMsg.key.id,
                    // Relays seletivos não podem depender de metadata/devices antigos: um device
                    // recém-vinculado receberia o skmsg sem o respectivo SKDM.
                    useCachedGroupMetadata: ((_e = options.includeJids) === null || _e === void 0 ? void 0 : _e.length) ? false : options.useCachedGroupMetadata,
                    useUserDevicesCache: ((_f = options.includeJids) === null || _f === void 0 ? void 0 : _f.length) ? false : options.useUserDevicesCache,
                    additionalAttributes,
                    statusJidList: options.statusJidList,
                    newsletterMediaId,
                    additionalNodes,
                    includeJids: options.includeJids,
                    decryptFailHide: options.decryptFailHide
                });
                if (config.emitOwnEvents) {
                    process.nextTick(() => {
                        emitOwnMessage(fullMsg);
                    });
                }
                return fullMsg;
            }
        }
    };
};
exports.makeMessagesSocket = makeMessagesSocket;
