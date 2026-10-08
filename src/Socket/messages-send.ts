import NodeCache from '@cacheable/node-cache'
import { Boom } from '@hapi/boom'
import { proto } from '../../WAProto'
import { DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL } from '../Defaults'
import ListType = proto.Message.ListMessage.ListType
import {
	AnyMessageContent,
	CacheStore,
	GroupMetadata,
	MediaConnInfo,
	MessageReceiptType,
	MessageRelayOptions,
	MiscMessageGenerationOptions,
	SocketConfig,
	WAMessageKey
} from '../Types'
import {
	aggregateMessageKeysNotFromMe,
	assertMediaContent,
	bindWaitForEvent,
	decryptMediaRetryData,
	encodeSignedDeviceIdentity,
	encodeWAMessage,
	encryptMediaRetryRequest,
	extractDeviceJids,
	generateMessageIDV2,
	generateWAMessage,
	getStatusCodeForMediaRetry,
	getUrlFromDirectPath,
	getWAUploadToServer,
	normalizeMessageContent,
	parseAndInjectE2ESessions,
	unixTimestampSeconds,
	convertlidDevice,
	encodeNewsletterMessage
} from '../Utils'
import { getUrlInfo } from '../Utils/link-preview'
import { BoundedTtlMap } from '../Utils/bounded-ttl-map'
import { makeKeyedMutex } from '../Utils/make-mutex'
import {
	areJidsSameUser,
	BinaryNode,
	BinaryNodeAttributes,
	getBinaryNodeChild,
	getBinaryNodeChildren,
	isJidGroup,
	isJidUser,
	isLidUser,
	jidDecode,
	jidEncode,
	jidNormalizedUser,
	JidWithDevice,
	S_WHATSAPP_NET
} from '../WABinary'
import { ParsedDeviceInfo, USyncQuery, USyncUser } from '../WAUSync'
import { makeNewsletterSocket } from './newsletter'
import { makeTcTokenManager } from './tc-token'
import caches from '../Utils/cache-utils'

const USYNC_DEVICES_BATCH = 400
const SESSION_FETCH_BATCH = 400
const ENCRYPT_BATCH = 200
const USER_DEVICES_NEGATIVE_TTL = 10 * 60
/** ±20% para as entradas de um grupo, criadas juntas, não vencerem todas no mesmo envio */
const jitteredTtl = (ttl: number) => Math.round(ttl * (0.8 + Math.random() * 0.4))

export const makeMessagesSocket = (config: SocketConfig) => {
	const {
		logger,
		linkPreviewImageThumbnailWidth,
		generateHighQualityLinkPreview,
		options: axiosOptions,
		patchMessageBeforeSending,
		cachedGroupMetadata
	} = config
	const sock = makeNewsletterSocket(config)
	const pocRelayTrace = process.env.WA_POC_RELAY_TRACE === '1'
	type SelectiveRelayContext = {
		groupJid: string
		allowedUsers: string[]
		decryptFailHide: boolean
	}
	// O retry chega depois do sendMessage e não carrega includeJids. Guardamos o
	// conjunto resolvido (PN + LID) para recuperar apenas devices que receberam o relay original.
	// guardam a mensagem inteira: teto de entradas e 15 min (retry chega em segundos), sem timer por socket
	const selectiveRelayCache = new BoundedTtlMap<string, SelectiveRelayContext>(500, 15 * 60 * 1000)
	const selectiveMessageCache = new BoundedTtlMap<string, proto.IMessage>(500, 15 * 60 * 1000)

	const {
		ev,
		authState,
		messageMutex,
		signalRepository,
		upsertMessage,
		query,
		fetchPrivacySettings,
		sendNode,
		groupMetadata,
		groupToggleEphemeral
	} = sock

	const {
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
	} = makeTcTokenManager({ authState, logger, query })

	let userDevicesCache: CacheStore
	if (config.userDevicesCache) {
		userDevicesCache = config.userDevicesCache
	} else {
		const cache = new NodeCache<any>({
			stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES,
			useClones: false
		})
		sock.onSocketEnd(() => cache.close())
		userDevicesCache = cache
	}

	const groupRelayMutex = makeKeyedMutex()

	const emitOwnMessage = (msg: proto.IWebMessageInfo) => {
		const author = jidNormalizedUser(authState.creds.me?.id)
		messageMutex
			.mutex(jidNormalizedUser(msg.key?.remoteJid || author), author, () => upsertMessage(msg, 'append'))
			.catch(err => logger.warn({ err, id: msg.key?.id }, 'falha ao emitir mensagem própria'))
	}

	let mediaConn: Promise<MediaConnInfo>
	const refreshMediaConn = async (forceGet = false) => {
		const media = await mediaConn
		if (!media || forceGet || new Date().getTime() - media.fetchDate.getTime() > media.ttl * 1000) {
			mediaConn = (async () => {
				const result = await query({
					tag: 'iq',
					attrs: {
						type: 'set',
						xmlns: 'w:m',
						to: S_WHATSAPP_NET
					},
					content: [{ tag: 'media_conn', attrs: {} }]
				})
				const mediaConnNode = getBinaryNodeChild(result, 'media_conn')
				const node: MediaConnInfo = {
					hosts: getBinaryNodeChildren(mediaConnNode, 'host').map(({ attrs }) => ({
						hostname: attrs.hostname,
						maxContentLengthBytes: +attrs.maxContentLengthBytes
					})),
					auth: mediaConnNode!.attrs.auth,
					ttl: +mediaConnNode!.attrs.ttl,
					fetchDate: new Date()
				}
				logger.debug('fetched media conn')
				return node
			})()
		}

		return mediaConn
	}

	/**
	 * generic send receipt function
	 * used for receipts of phone call, read, delivery etc.
	 * */
	const sendReceipt = async (
		jid: string,
		participant: string | undefined,
		messageIds: string[],
		type: MessageReceiptType
	) => {
		const node: BinaryNode = {
			tag: 'receipt',
			attrs: {
				id: messageIds[0]
			}
		}
		const isReadReceipt = type === 'read' || type === 'read-self'
		if (isReadReceipt) {
			node.attrs.t = unixTimestampSeconds().toString()
		}

		if (type === 'sender' && isJidUser(jid)) {
			node.attrs.recipient = jid
			node.attrs.to = participant!
		} else {
			node.attrs.to = jid
			if (participant) {
				node.attrs.participant = participant
			}
		}

		if (type) {
			node.attrs.type = type
		}

		const remainingMessageIds = messageIds.slice(1)
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
			]
		}

		logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages')
		await sendNode(node)
	}

	/** Correctly bulk send receipts to multiple chats, participants */
	const sendReceipts = async (keys: WAMessageKey[], type: MessageReceiptType) => {
		const recps = aggregateMessageKeysNotFromMe(keys)
		for (const { jid, participant, messageIds } of recps) {
			await sendReceipt(jid, participant, messageIds, type)
		}
	}

	/** Bulk read messages. Keys can be from different chats & participants */
	const readMessages = async (keys: WAMessageKey[]) => {
		const privacySettings = await fetchPrivacySettings()
		// based on privacy settings, we have to change the read type
		const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self'
		await sendReceipts(keys, readType)
	}

	/** Fetch all the devices we've to send a message to */
	/**
	 * Devices dos jids. O cache guarda a lista completa (com o device 0) e o filtro sai na leitura: o envio 1:1
	 * pede sem device 0 e o de grupo com, e a mesma entrada servia os dois cortada.
	 * `complete` é falso quando algum lote do USync não respondeu ou veio usuário com erro.
	 */
	const fetchUSyncDevices = async (jids: string[], useCache: boolean, ignoreZeroDevices: boolean) => {
		const deviceResults: JidWithDevice[] = []
		const keep = (item: JidWithDevice) => !ignoreZeroDevices || item.device !== 0
		let complete = true

		if (!useCache) {
			logger.debug('not using cache for devices')
		}

		const toFetch: string[] = []
		jids = Array.from(new Set(jids))

		for (let jid of jids) {
			jid = jidNormalizedUser(jid)
			if (useCache) {
				const devices = userDevicesCache.get<JidWithDevice[]>(jid)
				if (devices) {
					deviceResults.push(...devices.filter(keep))

					logger.trace({ jid }, 'using cache for devices')
				} else {
					toFetch.push(jid)
				}
			} else {
				toFetch.push(jid)
			}
		}

		if (!toFetch.length) {
			return { devices: deviceResults, complete }
		}

		const deviceMap: { [_: string]: JidWithDevice[] } = {}
		const answeredWithoutError = new Set<string>()
		// grupo grande num IQ único estoura o tamanho da resposta: consulta em lotes
		for (let i = 0; i < toFetch.length; i += USYNC_DEVICES_BATCH) {
			const query = new USyncQuery().withContext('message').withDeviceProtocol()
			for (const jid of toFetch.slice(i, i + USYNC_DEVICES_BATCH)) {
				query.withUser(new USyncUser().withId(jid))
			}

			const result = await sock.executeUSyncQuery(query)
			if (!result) {
				complete = false
				continue
			}

			for (const item of result.list) {
				const devices = (item as { devices?: ParsedDeviceInfo }).devices
				if (devices?.error) {
					complete = false
				} else if (item.id) {
					answeredWithoutError.add(jidNormalizedUser(item.id))
				}
			}

			const extracted = extractDeviceJids(result.list, authState.creds.me!.id, false, authState.creds.me?.lid)
			for (const item of extracted) {
				const cacheKey = jidNormalizedUser(item.jid)
				deviceMap[cacheKey] = deviceMap[cacheKey] || []
				deviceMap[cacheKey].push(item)

				if (keep(item)) {
					deviceResults.push(item)
				}
			}
		}

		for (const key in deviceMap) {
			userDevicesCache.set(key, deviceMap[key], jitteredTtl(DEFAULT_CACHE_TTLS.USER_DEVICES))
		}

		// cache negativo: sem isso quem não tem device é reconsultado em todo envio. Só para quem o servidor
		// respondeu sem erro; erro passageiro não pode deixar o usuário 10 min fora do envio.
		for (const jid of toFetch) {
			if (answeredWithoutError.has(jid) && !deviceMap[jid]) {
				userDevicesCache.set(jid, [], USER_DEVICES_NEGATIVE_TTL)
			}
		}

		return { devices: deviceResults, complete }
	}

	/** Fetch all the devices we've to send a message to */
	const getUSyncDevices = async (jids: string[], useCache: boolean, ignoreZeroDevices: boolean) =>
		(await fetchUSyncDevices(jids, useCache, ignoreZeroDevices)).devices

	const assertSessions = async (jids: string[], force: boolean, lids?: string) => {
		let didFetchNewSession = false
		const melid = jidNormalizedUser(authState.creds.me?.lid)
		const meid = jidNormalizedUser(authState.creds.me?.id)
		let jidsRequiringFetch: string[] = []
		if (force) {
			jidsRequiringFetch = jids
		} else {
			const pairs = jids.map(jid => ({
				jid,
				signalId: signalRepository.jidToSignalProtocolAddress(convertlidDevice(jid, lids, meid, melid))
			}))
			const sessions = await authState.keys.get('session', pairs.map(p => p.signalId))
			for (const { jid, signalId } of pairs) {
				if (!sessions[signalId]) {
					jidsRequiringFetch.push(jid)
				}
			}
		}

		// grupo grande num IQ único: resposta enorme e todas as sessões injetadas de uma vez
		for (let i = 0; i < jidsRequiringFetch.length; i += SESSION_FETCH_BATCH) {
			const batch = jidsRequiringFetch.slice(i, i + SESSION_FETCH_BATCH)
			logger.debug({ jidsRequiringFetch: batch }, 'fetching sessions')
			const result = await query({
				tag: 'iq',
				attrs: {
					xmlns: 'encrypt',
					type: 'get',
					to: S_WHATSAPP_NET
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
			})
			const { failed } = await parseAndInjectE2ESessions(result, signalRepository, lids, meid, melid)
			if (failed.length) {
				logger.warn({ failed }, 'falha ao injetar sessão; esses devices ficam para o retry')
			}

			didFetchNewSession = true
		}

		return didFetchNewSession
	}

	const sendPeerDataOperationMessage = async (
		pdoMessage: proto.Message.IPeerDataOperationRequestMessage
	): Promise<string> => {
		//TODO: for later, abstract the logic to send a Peer Message instead of just PDO - useful for App State Key Resync with phone
		if (!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		const protocolMessage: proto.IMessage = {
			protocolMessage: {
				peerDataOperationRequestMessage: pdoMessage,
				type: proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
			}
		}

		const meJid = jidNormalizedUser(authState.creds.me.id)

		const msgId = await relayMessage(meJid, protocolMessage, {
			additionalAttributes: {
				category: 'peer',
				// eslint-disable-next-line camelcase
				push_priority: 'high_force'
			}
		})

		return msgId
	}

	const createParticipantNodes = async (
		jids: string[],
		message: proto.IMessage,
		extraAttrs?: BinaryNode['attrs'],
		lid?,
		meid?,
		melid?
	) => {
		let patched = await patchMessageBeforeSending(message, jids)
		if (!Array.isArray(patched)) {
			patched = jids ? jids.map(jid => ({ recipientJid: jid, ...patched })) : [patched]
		}

		let shouldIncludeDeviceIdentity = false
		const failedJids: string[] = []

		const encryptOne = async (patchedMessageWithJid: (typeof patched)[number]) => {
			const { recipientJid: jid, ...patchedMessage } = patchedMessageWithJid
			if (!jid) {
				return undefined
			}

			const bytes = encodeWAMessage(patchedMessage)
			let encrypted: { type: 'pkmsg' | 'msg'; ciphertext: Uint8Array }
			try {
				encrypted = await signalRepository.encryptMessage({
					jid: convertlidDevice(jid, lid, meid, melid),
					data: bytes
				})
			} catch (err) {
				// um device com sessão ruim não derruba o envio dos outros; ele cai no retry
				logger.warn({ jid, err: (err as Error)?.message }, 'falha ao cifrar para o device')
				failedJids.push(jid)
				return undefined
			}

			const { type, ciphertext } = encrypted
			if (type === 'pkmsg') {
				shouldIncludeDeviceIdentity = true
			}

			const node: BinaryNode = {
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
			}
			return node
		}

		// cifrar milhares de devices num Promise.all só segura o event loop de todas as sessões do processo
		const results: (BinaryNode | undefined)[] = []
		for (let i = 0; i < patched.length; i += ENCRYPT_BATCH) {
			if (i) {
				await new Promise(resolve => setImmediate(resolve))
			}

			results.push(...(await Promise.all(patched.slice(i, i + ENCRYPT_BATCH).map(encryptOne))))
		}

		const nodes = results.filter((node): node is BinaryNode => !!node)
		return { nodes, shouldIncludeDeviceIdentity, failedJids }
	}

	/** usuários (PN e LID, sem server) de um participante do grupo */
	const participantUsers = (p: { id?: string; lid?: string }) =>
		[p.id, p.lid].map(jid => (jid ? jidDecode(jid)?.user : undefined)).filter((user): user is string => !!user)

	const participantMatches = (p: { id?: string; lid?: string; jid?: string }, jid: string) =>
		[p.id, p.lid, p.jid].some(candidate => !!candidate && areJidsSameUser(candidate, jid))

	/** usuários de cada jid da lista, já com o par PN/LID que a metadata do grupo conhece */
	const resolveGroupUsers = (jids: string[], groupData: GroupMetadata | undefined) => {
		const users = new Set<string>()
		for (const jid of jids) {
			const normalized = jidNormalizedUser(jid)
			const user = jidDecode(normalized)?.user
			if (user) {
				users.add(user)
			}

			for (const p of groupData?.participants || []) {
				if (participantMatches(p, normalized)) {
					participantUsers(p).forEach(u => users.add(u))
				}
			}
		}

		return users
	}

	/**
	 * Relay seletivo (sussurro): tira de `devices` quem não está em includeJids (o remetente sempre fica) e
	 * devolve os usuários que podem pedir retry. Quem fica sem device no <participants> não recebe o SKDM e vê
	 * o stub.
	 */
	const applySelectiveRelayFilter = (
		devices: JidWithDevice[],
		groupData: GroupMetadata | undefined,
		{ includeJids, meJids, jlidUser }: { includeJids: string[]; meJids: string[]; jlidUser?: string }
	) => {
		const meUsers = meJids.map(jid => jidDecode(jidNormalizedUser(jid))?.user).filter((u): u is string => !!u)
		// remetente sempre incluído (phone + lid), senão o próprio bot não lê a mensagem
		const includeUsers = resolveGroupUsers(includeJids, groupData)
		meUsers.forEach(u => includeUsers.add(u))
		if (jlidUser) {
			includeUsers.add(jlidUser)
		}

		for (let i = devices.length - 1; i >= 0; i--) {
			if (!devices[i].user || !includeUsers.has(devices[i].user)) {
				devices.splice(i, 1)
			}
		}

		logger.info(
			{ includeUsers: [...includeUsers], remaining: devices.length },
			'exclude-relay: whitelist aplicada (apenas includeJids + remetente recebem sender-key)'
		)
		return includeUsers
	}


	const relayMessage = async (
		jid: string,
		message: proto.IMessage,
		{
			messageId: msgId,
			participant,
			additionalAttributes,
			additionalNodes,
			useUserDevicesCache,
			useCachedGroupMetadata,
			statusJidList,
			newsletterMediaId,
			isretry,
			includeJids,
			decryptFailHide
		}: MessageRelayOptions
	) => {
		if (!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		if (additionalAttributes) {
			additionalAttributes = { ...additionalAttributes }
		}

		const meId = authState.creds.me.id
		const meLid = authState.creds.me.lid || authState.creds.me.id
		const lidattrs = jidDecode(authState.creds.me?.lid)
		const jlidUser = lidattrs?.user
		let lids: string
		if (isJidUser(jid) || isJidUser(participant?.jid)) {
			const userQuery = jidNormalizedUser(participant?.jid || jid)

			if (!isLidUser(userQuery)) {
				const verify = caches.lidCache.get(userQuery)
				if (verify) {
					lids = verify
					cacheLidMapping(userQuery, verify)
				} else {
					const usyncQuery = new USyncQuery().withContactProtocol().withLIDProtocol()
					usyncQuery.withUser(new USyncUser().withPhone(userQuery.split('@')[0]))
					const results = await sock.executeUSyncQuery(usyncQuery)
					if (results?.list) {
						const maybeLid = results.list[0]?.lid
						if (typeof maybeLid === 'string') {
							cacheLidMapping(userQuery, maybeLid)
							lids = maybeLid
						}
					}
				}
			}
		}
		const { user, server } = jidDecode(jid)!
		const statusJid = 'status@broadcast'
		const isGroup = server === 'g.us'
		const isStatus = jid === statusJid
		const isLid = server === 'lid'
		const isNewsletter = server === 'newsletter'
		// Relays seletivos ocultam por padrão a falha de decrypt nos devices que não receberam
		// o SKDM. O chamador ainda pode usar decryptFailHide:false para observar o placeholder.
		const shouldHideDecryptFailure =
			decryptFailHide ?? !!includeJids?.length

		let shouldIncludeDeviceIdentity = false

		msgId = msgId || generateMessageIDV2(sock.user?.id)
		useUserDevicesCache = useUserDevicesCache !== false
		useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus

		const participants: BinaryNode[] = []
		const destinationJid = !isStatus ? jidEncode(user, isLid ? 'lid' : isGroup ? 'g.us' : 's.whatsapp.net') : statusJid
		const binaryNodeContent: BinaryNode[] = []
		const devices: JidWithDevice[] = []
		let selectiveAllowedUsers: Set<string> | undefined

		const extraAttrs = {}

		if (participant) {
			if (!isGroup && !isStatus) {
				additionalAttributes = { ...additionalAttributes, device_fanout: 'false' }
			}
			const { user, device } = jidDecode(participant.jid)!
			devices.push({ user, device, jid: jidNormalizedUser(participant.jid) })
		}

		// metadata e devices não tocam chave: buscados antes da transação para não segurá-la durante a rede.
		// No retry (participant) a metadata não é usada.
		let groupData: GroupMetadata | undefined
		let groupDevices: JidWithDevice[] = []
		let groupDevicesComplete = false
		if ((isGroup || isStatus) && !participant) {
			if (isGroup) {
				groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined
				if (groupData && Array.isArray(groupData.participants)) {
					logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata')
				} else {
					groupData = await groupMetadata(jid)
				}
			}

			const participantsList = groupData ? groupData.participants.map(p => p.lid || p.id) : []
			if (isStatus && statusJidList) {
				participantsList.push(...statusJidList)
			}

			const fetched = await fetchUSyncDevices(participantsList, !!useUserDevicesCache, false)
			groupDevices = fetched.devices
			// poda só com a lista de fato completa: lote sem resposta ou metadata vazia apagaria as marcas e o
			// envio seguinte redistribuiria o SKDM para o grupo inteiro
			groupDevicesComplete = fetched.complete && participantsList.length > 0
		}

		const relayInTransaction = () =>
			authState.keys.transaction(async () => {
				const mediaType = getMediaType(message)
				if (mediaType) {
					extraAttrs['mediatype'] = mediaType
				}

				if (isNewsletter) {
					const patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message
					const bytes = encodeNewsletterMessage(patched as proto.IMessage)
					binaryNodeContent.push({
						tag: 'plaintext',
						attrs: mediaType ? { mediatype: mediaType } : {},
						content: bytes
					})
					const stanza: BinaryNode = {
						tag: 'message',
						attrs: {
							to: jid,
							id: msgId,
							type: getMessageType(message),
							...(newsletterMediaId ? { media_id: newsletterMediaId } : {}),
							...(additionalAttributes || {})
						},
						content: binaryNodeContent
					}
					logger.debug(
						{ msgId, mediaType, hasMediaId: !!newsletterMediaId },
						`sending newsletter message to ${jid}`
					)
					await sendNode(stanza)
					return
				}

				if (normalizeMessageContent(message)?.pinInChatMessage) {
					extraAttrs['decrypt-fail'] = 'hide'
				}

				if (isGroup || isStatus) {
					// cópia: o objeto do cache não pode ficar marcado se o envio falhar no meio
					const senderKeyMap: { [jid: string]: boolean } = {}
					if (!participant && !isStatus) {
						const result = await authState.keys.get('sender-key-memory', [jid])
						Object.assign(senderKeyMap, result[jid])
					}

					let senderKeyMapChanged = false

					if (!participant) {
						if (!isStatus) {
							additionalAttributes = {
								...additionalAttributes,
								addressing_mode: groupData?.addressingMode || 'pn'
							}
						}

						devices.push(...groupDevices)
						const Mephone = groupDevices.some(d => d.user === jlidUser && d.device === 0)
						if (!Mephone && jlidUser) {
							devices.push({ user: jlidUser, device: 0, jid: jidNormalizedUser(meLid) })
						}

						if (includeJids?.length) {
							selectiveAllowedUsers = applySelectiveRelayFilter(devices, groupData, {
								includeJids,
								meJids: [meId, meLid],
								jlidUser
							})
						}
					}

					const patched = await patchMessageBeforeSending(message)

					if (Array.isArray(patched)) {
						throw new Boom('Per-jid patching is not supported in groups')
					}

					const bytes = encodeWAMessage(patched)

					const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
						group: destinationJid,
						data: bytes,
						meId: meLid,
						// POC exclude-relay: se ha exclusao (ou whitelist), rotaciona a sender-key para que quem
						// ficou de fora (e ja possa ter a chave antiga) nao decifre este skmsg -> ve o stub
						// "aguardando esta mensagem". Quem esta incluido recebe o SKDM e decifra normal.
						forceRotate: !!includeJids?.length
					})

					const senderKeyJids: string[] = []
					// sender key rotacionada (selective relay) precisa ser redistribuída a todos
					const skdmToAll = !!includeJids?.length
					if (skdmToAll && !participant) {
						// a chave antiga morreu na rotação e o SKDM da nova só vai pros devices que
						// sobraram no relay seletivo. Sem zerar o map, os de fora continuam marcados
						// como "já recebeu" e nunca ganham a chave nova => todas as mensagens
						// seguintes do grupo ficam em "aguardando" pra eles. Zerado, o próximo envio
						// normal redistribui o SKDM (já na iteração atual, então o skmsg seletivo
						// continua indecifrável pra quem ficou de fora).
						for (const key of Object.keys(senderKeyMap)) {
							delete senderKeyMap[key]
							senderKeyMapChanged = true
						}
					}

					const currentDeviceIds = new Set<string>()
					for (const { user, device, jid } of devices) {
						const server = jidDecode(jid)?.server || 'lid'
						const senderId = jidEncode(user, server, device)
						currentDeviceIds.add(senderId)
						// só manda SKDM pra quem ainda não recebeu a sender key;
						// mandar pra todos em todo envio provoca retry receipt de devices
						// quebrados a cada mensagem
						if (!senderKeyMap[senderId] || !!participant || skdmToAll) {
							senderKeyJids.push(senderId)
							senderKeyMapChanged = senderKeyMapChanged || !senderKeyMap[senderId]
							senderKeyMap[senderId] = true
						}
					}

					// com a lista completa de devices do grupo, quem saiu sai do mapa; se voltar, recebe o SKDM de novo
					if (isGroup && !participant && groupDevicesComplete && !includeJids?.length) {
						for (const key of Object.keys(senderKeyMap)) {
							if (!currentDeviceIds.has(key)) {
								delete senderKeyMap[key]
								senderKeyMapChanged = true
							}
						}
					}

					// if there are some participants with whom the session has not been established
					// if there are, we re-send the senderkey
					if (senderKeyJids.length) {
						logger.debug({ senderKeyJids }, 'sending new sender key')

						const senderKeyMsg: proto.IMessage = {
							senderKeyDistributionMessage: {
								axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
								groupId: destinationJid
							}
						}

						// sem force: no retry o sendMessagesAgain já renovou a sessão; forçar de novo
						// sobrescreve a sessão recém-injetada e gasta outra prekey do destinatário
						await assertSessions(senderKeyJids, false, lids)

						const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, extraAttrs, lids, meId, meLid)
						shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity
						for (const failedJid of result.failedJids) {
							delete senderKeyMap[failedJid]
						}

						participants.push(...result.nodes)
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
					})

					// só persiste no envio normal e quando mudou; no retry (participant) o map começa vazio
					// e persistir aqui clobberaria o map completo do grupo
					if (!participant && !isStatus && senderKeyMapChanged) {
						await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
					}
				} else {
					const { user: meUser, device: meDevice } = jidDecode(meId)!

					if (!participant) {
						devices.push({ user, device: 0, jid })
						if (meDevice !== undefined && meDevice !== 0) {
							if (isLidUser(jid) && jlidUser) {
								devices.push({ user: jlidUser, device: 0, jid: jidNormalizedUser(meLid) })
								const additionalDevices = await getUSyncDevices([jid, meLid], !!useUserDevicesCache, true)
								devices.push(...additionalDevices)
							} else {
								devices.push({ user: meUser, device: 0, jid: jidNormalizedUser(meId) })
								const additionalDevices = await getUSyncDevices([jid, meId], !!useUserDevicesCache, true)
								devices.push(...additionalDevices)
							}
						}
					}

					const allJids: string[] = []
					const meJids: string[] = []
					const otherJids: string[] = []
					for (const { user, device, jid } of devices) {
						const isMe = user === meUser
						const ismeLid = user === jlidUser
						const server = jidDecode(jid)?.server || 'lid'
						const senderId = jidEncode(user, server, device)
						if (isMe || ismeLid) {
							meJids.push(senderId)
						} else {
							otherJids.push(senderId)
						}
						allJids.push(senderId)
					}

					await assertSessions(allJids, false, lids)

					const meMsg: proto.IMessage = {
						deviceSentMessage: {
							destinationJid,
							message
						},
						messageContextInfo: message.messageContextInfo
					}

					const [
						{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 },
						{ nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }
					] = await Promise.all([
						createParticipantNodes(meJids, meMsg, extraAttrs, lids, meId, meLid),
						createParticipantNodes(otherJids, message, extraAttrs, lids, meId, meLid)
					])
					// os devices do próprio bot quase sempre cifram: o que importa é o destinatário ter recebido
					if (otherJids.length && !otherNodes.length) {
						throw new Boom('All encryptions failed', { statusCode: 500 })
					}

					participants.push(...meNodes)
					participants.push(...otherNodes)

					shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2
				}

				if (participants.length) {
					if (additionalAttributes?.['category'] === 'peer') {
						const peerNode = participants[0]?.content?.[0] as BinaryNode
						if (peerNode) {
							binaryNodeContent.push(peerNode) // push only enc
						}
					} else {
						binaryNodeContent.push({
							tag: 'participants',
							attrs: {},
							content: participants
						})
					}
				}

				const stanza: BinaryNode = {
					tag: 'message',
					attrs: {
						id: msgId,
						type: getMessageType(message),
						...(additionalAttributes || {})
					},
					content: binaryNodeContent
				}
				// if the participant to send to is explicitly specified (generally retry recp)
				// ensure the message is only sent to that person
				// if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
				if (participant) {
					if (isJidGroup(destinationJid)) {
						stanza.attrs.to = destinationJid
						stanza.attrs.participant = participant.jid
					} else if (areJidsSameUser(participant.jid, meId)) {
						stanza.attrs.to = participant.jid
						stanza.attrs.recipient = destinationJid
					} else {
						stanza.attrs.to = participant.jid
					}
				} else {
					stanza.attrs.to = destinationJid
				}

				if (shouldIncludeDeviceIdentity) {
					;(stanza.content as BinaryNode[]).push({
						tag: 'device-identity',
						attrs: {},
						content: encodeSignedDeviceIdentity(authState.creds.account!, true)
					})

					logger.debug({ jid }, 'adding device identity')
				}

				const { is1on1Send, tcTokenJid } = await appendPrivacyToken(stanza, {
					destinationJid,
					isGroup,
					isStatus,
					isNewsletter,
					isPeer: additionalAttributes?.['category'] === 'peer',
					isRetry: !!isretry,
					participantJid: participant?.jid,
					meIds: [meId, meLid]
				})

				if (additionalNodes && additionalNodes.length > 0) {
					;(stanza.content as BinaryNode[]).push(...additionalNodes)
				}

				const hasCustomBizNode = additionalNodes?.some(node => node.tag === 'biz')
				const bizNode = hasCustomBizNode ? undefined : getBusinessNode(message)
				if (bizNode) {
					;(stanza.content as BinaryNode[]).push(bizNode)
					logger.debug({ jid }, 'adding business node')
				}

				logger.debug({ msgId }, `sending message to ${participants.length} devices`)
				if (pocRelayTrace && (isGroup || participant)) {
					logger.info(
						{
							msgId,
							to: stanza.attrs.to,
							participant: stanza.attrs.participant,
							isretry: !!isretry,
							participantNodes: participants.length,
							deviceCandidates: devices.length,
							contentTags: Array.isArray(stanza.content) ? stanza.content.map(node => node.tag) : [],
							hasSkmsg: binaryNodeContent.some(
								node => node.tag === 'enc' && node.attrs.type === 'skmsg'
							),
							includeCount: includeJids?.length || 0,
							decryptFailHide: shouldHideDecryptFailure
						},
						'[POC relay trace] outbound stanza'
					)
				}

				await sendNode(stanza)
				if (isGroup && !participant && selectiveAllowedUsers) {
					const selectiveCacheKey = `${destinationJid}:${msgId}`
					selectiveRelayCache.set(selectiveCacheKey, {
						groupJid: destinationJid,
						allowedUsers: [...selectiveAllowedUsers],
						decryptFailHide: shouldHideDecryptFailure
					})
					selectiveMessageCache.set(selectiveCacheKey, message)
				}

				if (is1on1Send && tcTokenJid) {
					void maybeIssueTcToken(destinationJid, message, {
						participant,
						additionalAttributes,
						storageJid: tcTokenJid,
						msgId: stanza.attrs.id
					})
				}
			})

		// envios ao mesmo grupo em série: o segundo via os devices novos como "já receberam" o SKDM do
		// primeiro, que ainda estava cifrando, e chegava antes dele sem a chave (retry no destinatário)
		if (isGroup && !participant) {
			await groupRelayMutex.mutex(jid, relayInTransaction)
		} else {
			await relayInTransaction()
		}

		return msgId
	}

	const getMessageType = (message: proto.IMessage) => {
		if (message.pollCreationMessage || message.pollCreationMessageV2 || message.pollCreationMessageV3) {
			return 'poll'
		}
		if (getMediaType(message)) {
			return 'media'
		}
		if (normalizeMessageContent(message)?.listMessage) {
			return 'media'
		}

		return 'text'
	}

	const getMediaType = (message: proto.IMessage) => {
		message = normalizeMessageContent(message) || message

		if (message.imageMessage) {
			return 'image'
		} else if (message.videoMessage) {
			return message.videoMessage.gifPlayback ? 'gif' : 'video'
		} else if (message.audioMessage) {
			return message.audioMessage.ptt ? 'ptt' : 'audio'
		} else if (message.contactMessage) {
			return 'vcard'
		} else if (message.documentMessage) {
			return 'document'
		} else if (message.contactsArrayMessage) {
			return 'contact_array'
		} else if (message.liveLocationMessage) {
			return 'livelocation'
		} else if (message.stickerMessage) {
			return 'sticker'
		} else if (message.listMessage) {
			return 'list'
		} else if (message.listResponseMessage) {
			return 'list_response'
		} else if (message.buttonsResponseMessage) {
			return 'buttons_response'
		} else if (message.orderMessage) {
			return 'order'
		} else if (message.productMessage) {
			return 'product'
		} else if (message.interactiveResponseMessage) {
			return 'native_flow_response'
		} else if (message.groupInviteMessage) {
			return 'url'
		}
	}

	const getButtonType = (message: proto.IMessage) => {
		if (message.buttonsMessage) {
			return 'buttons'
		} else if (message.buttonsResponseMessage) {
			return 'buttons_response'
		} else if (message.interactiveResponseMessage) {
			return 'interactive_response'
		} else if (message.listMessage) {
			return 'list'
		} else if (message.listResponseMessage) {
			return 'list_response'
		}
	}

	const getButtonArgs = (message: proto.IMessage): BinaryNode['attrs'] => {
		if (message.templateMessage) {
			// TODO: Add attributes
			return {}
		} else if (message.listMessage) {
			const type = message.listMessage.listType
			if (!type) {
				throw new Boom('Expected list type inside message')
			}

			return { v: '2', type: type === ListType.SINGLE_SELECT ? 'product_list' : ListType[type].toLowerCase() }
		} else {
			return {}
		}
	}

	const getBusinessNode = (message: proto.IMessage): BinaryNode | undefined => {
		const content = normalizeMessageContent(message)
		if (!content) {
			return
		}

		const attrs: BinaryNodeAttributes = {
			actual_actors: '2',
			host_storage: '2',
			privacy_mode_ts: unixTimestampSeconds().toString()
		}
		const nativeFlow = content.interactiveMessage?.nativeFlowMessage
		const paymentFlowName = nativeFlow?.buttons?.some(button => button.name === 'payment_info')
			? 'payment_info'
			: nativeFlow?.buttons?.some(button => button.name === 'review_and_pay')
				? 'order_details'
				: undefined

		if (paymentFlowName) {
			return {
				tag: 'biz',
				attrs: {
					...attrs,
					native_flow_name: paymentFlowName
				}
			}
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
			}
		}

		const buttonType = getButtonType(content)
		if (!buttonType) {
			return
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
		}
	}

	const waUploadToServer = getWAUploadToServer(config, refreshMediaConn)

	const waitForMsgMediaUpdate = bindWaitForEvent(ev, 'messages.media-update')

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
		getSelectiveRelayContext: (groupJid: string, messageId: string) =>
			selectiveRelayCache.get(`${groupJid}:${messageId}`),
		getSelectiveSentMessage: (groupJid: string, messageId: string) =>
			selectiveMessageCache.get(`${groupJid}:${messageId}`),
		updateMediaMessage: async (message: proto.IWebMessageInfo) => {
			const content = assertMediaContent(message.message)
			const mediaKey = content.mediaKey!
			const meId = authState.creds.me!.id
			const node = await encryptMediaRetryRequest(message.key, mediaKey, meId)

			let error: Error | undefined = undefined
			await Promise.all([
				sendNode(node),
				waitForMsgMediaUpdate(async update => {
					const result = update.find(c => c.key.id === message.key.id)
					if (result) {
						if (result.error) {
							error = result.error
						} else {
							try {
								const media = await decryptMediaRetryData(result.media!, mediaKey, result.key.id!)
								if (media.result !== proto.MediaRetryNotification.ResultType.SUCCESS) {
									const resultStr = proto.MediaRetryNotification.ResultType[media.result!]
									throw new Boom(`Media re-upload failed by device (${resultStr})`, {
										data: media,
										statusCode: getStatusCodeForMediaRetry(media.result!) || 404
									})
								}

								content.directPath = media.directPath
								content.url = getUrlFromDirectPath(content.directPath!)

								logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful')
							} catch (err) {
								error = err
							}
						}

						return true
					}
				})
			])

			if (error) {
				throw error
			}

			ev.emit('messages.update', [{ key: message.key, update: { message: message.message } }])

			return message
		},
		sendMessage: async (jid: string, content: AnyMessageContent, options: MiscMessageGenerationOptions = {}) => {
			const userJid = authState.creds.me!.id
			if (
				typeof content === 'object' &&
				'disappearingMessagesInChat' in content &&
				typeof content['disappearingMessagesInChat'] !== 'undefined' &&
				isJidGroup(jid)
			) {
				const { disappearingMessagesInChat } = content
				const value =
					typeof disappearingMessagesInChat === 'boolean'
						? disappearingMessagesInChat
							? WA_DEFAULT_EPHEMERAL
							: 0
						: disappearingMessagesInChat
				await groupToggleEphemeral(jid, value)
			} else {
				const fullMsg = await generateWAMessage(jid, content, {
					logger,
					userJid,
					getUrlInfo: text =>
						getUrlInfo(text, {
							thumbnailWidth: linkPreviewImageThumbnailWidth,
							fetchOpts: {
								timeout: 3_000,
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
					messageId: generateMessageIDV2(sock.user?.id),
					...options
				})
				const isDeleteMsg = 'delete' in content && !!content.delete
				const isEditMsg = 'edit' in content && !!content.edit
				const isPinMsg = 'pin' in content && !!content.pin
				const isPollMessage = 'poll' in content && !!content.poll
				const newsletterMediaId = (
					fullMsg.message as (proto.IMessage & { __newsletterMediaId?: string }) | undefined
				)?.__newsletterMediaId
				const additionalAttributes: BinaryNodeAttributes = {}
				const additionalNodes: BinaryNode[] = []
				// required for delete
				if (isDeleteMsg) {
					// if the chat is a group, and I am not the author, then delete the message as an admin
					if (isJidGroup(content.delete?.remoteJid as string) && !content.delete?.fromMe) {
						additionalAttributes.edit = '8'
					} else {
						additionalAttributes.edit = '7'
					}
				} else if (isEditMsg) {
					additionalAttributes.edit = '1'
				} else if (isPinMsg) {
					additionalAttributes.edit = '2'
				} else if (isPollMessage) {
					additionalNodes.push({
						tag: 'meta',
						attrs: {
							polltype: 'creation'
						}
					} as BinaryNode)
				}

				if ('cachedGroupMetadata' in options) {
					logger.warn(
						'cachedGroupMetadata in sendMessage are deprecated, now cachedGroupMetadata is part of the socket config.'
					)
				}

				await relayMessage(jid, fullMsg.message!, {
					messageId: fullMsg.key.id!,
					// Relays seletivos não podem depender de metadata/devices antigos: um device
					// recém-vinculado receberia o skmsg sem o respectivo SKDM.
					useCachedGroupMetadata:
						options.includeJids?.length ? false : options.useCachedGroupMetadata,
					useUserDevicesCache:
						options.includeJids?.length ? false : options.useUserDevicesCache,
					additionalAttributes,
					statusJidList: options.statusJidList,
					newsletterMediaId,
					additionalNodes,
					includeJids: options.includeJids,
					decryptFailHide: options.decryptFailHide
				})
				if (config.emitOwnEvents) {
					process.nextTick(() => {
						emitOwnMessage(fullMsg)
					})
				}

				return fullMsg
			}
		}
	}
}
