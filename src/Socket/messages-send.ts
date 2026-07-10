import NodeCache from '@cacheable/node-cache'
import { Boom } from '@hapi/boom'
import { proto } from '../../WAProto'
import { DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL } from '../Defaults'
import ListType = proto.Message.ListMessage.ListType
import {
	AnyMessageContent,
	CacheStore,
	MediaConnInfo,
	MessageReceiptType,
	MessageRelayOptions,
	MiscMessageGenerationOptions,
	SecretGroupMessageOptions,
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
import { isTcTokenExpired, shouldSendNewTcToken, storeTcTokensFromIqResult } from '../Utils/tc-token-utils'
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
import { USyncQuery, USyncUser } from '../WAUSync'
import { makeNewsletterSocket } from './newsletter'
import caches from '../Utils/cache-utils'

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
	const {
		ev,
		authState,
		processingMutex,
		signalRepository,
		upsertMessage,
		query,
		fetchPrivacySettings,
		sendNode,
		groupMetadata,
		groupToggleEphemeral
	} = sock

	const userDevicesCache: CacheStore =
		config.userDevicesCache ||
		new NodeCache<any>({
			stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES,
			useClones: false
		})

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
	const getUSyncDevices = async (jids: string[], useCache: boolean, ignoreZeroDevices: boolean) => {
		const deviceResults: JidWithDevice[] = []

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
					deviceResults.push(...devices)

					logger.trace({ jid }, 'using cache for devices')
				} else {
					toFetch.push(jid)
				}
			} else {
				toFetch.push(jid)
			}
		}

		if (!toFetch.length) {
			return deviceResults
		}

		const query = new USyncQuery().withContext('message').withDeviceProtocol()

		for (const jid of toFetch) {
			query.withUser(new USyncUser().withId(jid))
		}

		const result = await sock.executeUSyncQuery(query)

		if (result) {
			const extracted = extractDeviceJids(
				result?.list,
				authState.creds.me!.id,
				ignoreZeroDevices,
				authState.creds.me?.lid
			)
			const deviceMap: { [_: string]: JidWithDevice[] } = {}

			for (const item of extracted) {
				const cacheKey = jidNormalizedUser(item.jid!)
				deviceMap[cacheKey] = deviceMap[cacheKey] || []
				deviceMap[cacheKey].push(item)

				deviceResults.push(item)
			}

			for (const key in deviceMap) {
				userDevicesCache.set(key, deviceMap[key])
			}
		}

		return deviceResults
	}

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

		if (jidsRequiringFetch.length) {
			logger.debug({ jidsRequiringFetch }, 'fetching sessions')
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
						content: jidsRequiringFetch.map(jid => ({
							tag: 'user',
							attrs: { jid }
						}))
					}
				]
			})
			await parseAndInjectE2ESessions(result, signalRepository, lids, meid, melid)

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

		const nodes = await Promise.all(
			patched.map(async patchedMessageWithJid => {
				const { recipientJid: jid, ...patchedMessage } = patchedMessageWithJid
				if (!jid) {
					return {} as BinaryNode
				}

				const bytes = encodeWAMessage(patchedMessage)
				const { type, ciphertext } = await signalRepository.encryptMessage({
					jid: convertlidDevice(jid, lid, meid, melid),
					data: bytes
				})
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
			})
		)
		return { nodes, shouldIncludeDeviceIdentity }
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
			isretry,
			excludeJids,
			includeJids
		}: MessageRelayOptions
	) => {
		if (additionalAttributes) {
			additionalAttributes = { ...additionalAttributes }
		}

		const meId = authState.creds.me!.id
		const meLid = authState.creds.me!.lid || authState.creds.me!.id
		const lidattrs = jidDecode(authState.creds.me?.lid)
		const jlidUser = lidattrs?.user
		let lids: string
		if (isJidUser(jid) || isJidUser(participant?.jid)) {
			const userQuery = jidNormalizedUser(participant?.jid || jid)

			if (!isLidUser(userQuery)) {
				const verify = caches.lidCache.get(userQuery)
				if (verify) {
					lids = verify
				} else {
					const usyncQuery = new USyncQuery().withContactProtocol().withLIDProtocol()
					usyncQuery.withUser(new USyncUser().withPhone(userQuery.split('@')[0]))
					const results = await sock.executeUSyncQuery(usyncQuery)
					if (results?.list) {
						const maybeLid = results.list[0]?.lid
						if (typeof maybeLid === 'string') {
							caches.lidCache.set(userQuery, maybeLid)
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

		let shouldIncludeDeviceIdentity = false

		msgId = msgId || generateMessageIDV2(sock.user?.id)
		useUserDevicesCache = useUserDevicesCache !== false
		useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus

		const participants: BinaryNode[] = []
		const destinationJid = !isStatus ? jidEncode(user, isLid ? 'lid' : isGroup ? 'g.us' : 's.whatsapp.net') : statusJid
		const binaryNodeContent: BinaryNode[] = []
		const devices: JidWithDevice[] = []

		const extraAttrs = {}

		if (participant) {
			if (!isGroup && !isStatus) {
				additionalAttributes = { ...additionalAttributes, device_fanout: 'false' }
			}
			const { user, device } = jidDecode(participant.jid)!
			devices.push({ user, device, jid: jidNormalizedUser(participant.jid) })
		}

		await authState.keys.transaction(async () => {
			const mediaType = getMediaType(message)
			if (mediaType) {
				extraAttrs['mediatype'] = mediaType
			}

			if (isNewsletter) {
				const patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message
				const bytes = encodeNewsletterMessage(patched as proto.IMessage)
				binaryNodeContent.push({
					tag: 'plaintext',
					attrs: {},
					content: bytes
				})
				const stanza: BinaryNode = {
					tag: 'message',
					attrs: {
						to: jid,
						id: msgId,
						type: getMessageType(message),
						...(additionalAttributes || {})
					},
					content: binaryNodeContent
				}
				logger.debug({ msgId }, `sending newsletter message to ${jid}`)
				await sendNode(stanza)
				return
			}

			if (normalizeMessageContent(message)?.pinInChatMessage) {
				extraAttrs['decrypt-fail'] = 'hide'
			}

			if (isGroup || isStatus) {
				const [groupData, senderKeyMap] = await Promise.all([
					(async () => {
						let groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined
						if (groupData && Array.isArray(groupData?.participants)) {
							logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata')
						} else if (!isStatus) {
							groupData = await groupMetadata(jid)
						}

						return groupData
					})(),
					(async () => {
						if (!participant && !isStatus) {
							const result = await authState.keys.get('sender-key-memory', [jid])
							return result[jid] || {}
						}

						return {}
					})()
				])

				if (!participant) {
					const participantsList = groupData && !isStatus ? groupData.participants.map(p => p.lid || p.id) : []
					if (isStatus && statusJidList) {
						participantsList.push(...statusJidList)
					}

					if (!isStatus) {
						additionalAttributes = {
							...additionalAttributes,
							addressing_mode: groupData?.addressingMode || 'pn'
						}
					}

					const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false)
					devices.push(...additionalDevices)
					const Mephone = additionalDevices.some(d => d.user === jlidUser && d.device === 0)
					if (!Mephone && jlidUser) {
						devices.push({ user: jlidUser, device: 0, jid: jidNormalizedUser(meLid) })
					}

					// POC exclude-relay (inverso): whitelist. Mantem SOMENTE os devices dos usuarios em
					// includeJids (mais o proprio remetente, para seus devices sincronizarem); todos os
					// demais sao removidos => nao recebem sender-key => veem o stub. Precede excludeJids.
					if (includeJids?.length) {
						const includeUsers = new Set<string>()
						// remetente sempre incluido (phone + lid), senao o proprio bot nao le a mensagem
						for (const meJid of [meId, meLid]) {
							const meUserDec = jidDecode(jidNormalizedUser(meJid))?.user
							if (meUserDec) {
								includeUsers.add(meUserDec)
							}
						}
						if (jlidUser) {
							includeUsers.add(jlidUser)
						}
						for (const inc of includeJids) {
							const incNorm = jidNormalizedUser(inc)
							const incUser = jidDecode(incNorm)?.user
							if (incUser) {
								includeUsers.add(incUser)
							}
							for (const p of groupData?.participants || []) {
								const pid = (p as { id?: string; lid?: string }).id
								const plid = (p as { id?: string; lid?: string }).lid
								if (areJidsSameUser(pid, incNorm) || (plid && areJidsSameUser(plid, incNorm))) {
									const uid = pid ? jidDecode(pid)?.user : undefined
									const ulid = plid ? jidDecode(plid)?.user : undefined
									if (uid) {
										includeUsers.add(uid)
									}
									if (ulid) {
										includeUsers.add(ulid)
									}
								}
							}
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
					} else if (excludeJids?.length) {
						// POC exclude-relay: remove TODOS os devices dos usuarios em excludeJids (ex.: admins),
						// resolvendo phone<->lid via participantes do grupo. Sem device no <participants> => sem
						// sender-key => nao decifram o skmsg => a mensagem simplesmente nao aparece pra eles.
						const excludeUsers = new Set<string>()
						for (const ex of excludeJids) {
							const exNorm = jidNormalizedUser(ex)
							const exUser = jidDecode(exNorm)?.user
							if (exUser) {
								excludeUsers.add(exUser)
							}
							for (const p of groupData?.participants || []) {
								const pid = (p as { id?: string; lid?: string }).id
								const plid = (p as { id?: string; lid?: string }).lid
								if (areJidsSameUser(pid, exNorm) || (plid && areJidsSameUser(plid, exNorm))) {
									const uid = pid ? jidDecode(pid)?.user : undefined
									const ulid = plid ? jidDecode(plid)?.user : undefined
									if (uid) {
										excludeUsers.add(uid)
									}
									if (ulid) {
										excludeUsers.add(ulid)
									}
								}
							}
						}
						for (let i = devices.length - 1; i >= 0; i--) {
							if (devices[i].user && excludeUsers.has(devices[i].user)) {
								devices.splice(i, 1)
							}
						}
						logger.info(
							{ excludeUsers: [...excludeUsers], remaining: devices.length },
							'exclude-relay: devices filtrados (usuarios excluidos nao recebem sender-key)'
						)
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
					forceRotate: !!excludeJids?.length || !!includeJids?.length
				})

				const senderKeyJids: string[] = []
				for (const { user, device, jid } of devices) {
					const server = jidDecode(jid)?.server || 'lid'
					const senderId = jidEncode(user, server, device)
					senderKeyJids.push(senderId)
					senderKeyMap[senderId] = true
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

					await assertSessions(senderKeyJids, isretry ? true : false, lids)

					const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, extraAttrs, lids, meId, meLid)
					shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity

					participants.push(...result.nodes)
				}

				binaryNodeContent.push({
					tag: 'enc',
					attrs: { v: '2', type: 'skmsg' },
					content: ciphertext
				})

				await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
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

				await assertSessions(allJids, isretry ? true : false, lids)

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

			const isPeerMessage = additionalAttributes?.['category'] === 'peer'
			const is1on1Send = !isGroup && !isStatus && !isNewsletter && !isretry && !isPeerMessage
			const tcTokenJid = is1on1Send ? jidNormalizedUser(destinationJid) : undefined
			const tcTokenEntry = tcTokenJid
				? (await authState.keys.get('contacts-tc-token', [tcTokenJid]))[tcTokenJid]
				: undefined
			let tcTokenBuffer: Buffer | undefined = tcTokenEntry?.token
			if (tcTokenBuffer?.length && isTcTokenExpired(tcTokenEntry?.timestamp)) {
				logger.debug({ jid: destinationJid, timestamp: tcTokenEntry?.timestamp }, 'tctoken expired, clearing')
				tcTokenBuffer = undefined
				const cleared =
					tcTokenEntry?.senderTimestamp !== undefined
						? { token: Buffer.alloc(0), senderTimestamp: tcTokenEntry.senderTimestamp }
						: null
				try {
					await authState.keys.set({ 'contacts-tc-token': { [tcTokenJid!]: cleared } })
				} catch {}
			}
			if (tcTokenBuffer?.length) {
				;(stanza.content as BinaryNode[]).push({
					tag: 'tctoken',
					attrs: {},
					content: tcTokenBuffer
				})
			}

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

			await sendNode(stanza)

			if (is1on1Send && tcTokenJid && shouldSendNewTcToken(tcTokenEntry?.senderTimestamp)) {
				const issueTimestamp = unixTimestampSeconds()
				getPrivacyTokens([tcTokenJid], issueTimestamp)
					.then(async result => {
						await storeTcTokensFromIqResult({
							result,
							fallbackJid: tcTokenJid,
							keys: authState.keys
						})
						const currentData = await authState.keys.get('contacts-tc-token', [tcTokenJid])
						const currentEntry = currentData[tcTokenJid]
						await authState.keys.set({
							'contacts-tc-token': {
								[tcTokenJid]: {
									...(currentEntry ?? { token: Buffer.alloc(0) }),
									senderTimestamp: issueTimestamp
								}
							}
						})
					})
					.catch(err => {
						logger.debug({ jid: destinationJid, err: err?.message }, 'fire-and-forget tctoken issuance failed')
					})
			}
		})

		return msgId
	}

	const getMessageType = (message: proto.IMessage) => {
		if (message.pollCreationMessage || message.pollCreationMessageV2 || message.pollCreationMessageV3) {
			return 'poll'
		}

		return 'text'
	}

	const getMediaType = (message: proto.IMessage) => {
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

			return { v: '2', type: ListType[type].toLowerCase() }
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
		if (!content.listMessage) {
			return {
				tag: 'biz',
				attrs: {},
				content: [{ tag: buttonType, attrs: getButtonArgs(content) }]
			}
		}

		return {
			tag: 'biz',
			attrs,
			content: [
				{
					tag: buttonType,
					attrs: getButtonArgs(content)
				},
				{
					tag: 'quality_control',
					attrs: { source_type: 'third_party' }
				}
			]
		}
	}

	const getPrivacyTokens = async (jids: string[], timestamp?: number) => {
		const t = (timestamp ?? unixTimestampSeconds()).toString()
		const result = await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
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
							jid: jidNormalizedUser(jid),
							t,
							type: 'trusted_contact'
						}
					}))
				}
			]
		})

		return result
	}

	const waUploadToServer = getWAUploadToServer(config, refreshMediaConn)

	const waitForMsgMediaUpdate = bindWaitForEvent(ev, 'messages.media-update')

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
					console.warn(
						'cachedGroupMetadata in sendMessage are deprecated, now cachedGroupMetadata is part of the socket config.'
					)
				}

				await relayMessage(jid, fullMsg.message!, {
					messageId: fullMsg.key.id!,
					useCachedGroupMetadata: options.useCachedGroupMetadata,
					additionalAttributes,
					statusJidList: options.statusJidList,
					additionalNodes,
					excludeJids: options.excludeJids,
					includeJids: options.includeJids
				})
				if (config.emitOwnEvents) {
					process.nextTick(() => {
						processingMutex.mutex(() => upsertMessage(fullMsg, 'append'))
					})
				}

				return fullMsg
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
		sendSecretGroupMessage: async (
			jid: string,
			messageObject: AnyMessageContent,
			options: SecretGroupMessageOptions = {} as SecretGroupMessageOptions
		) => {
			if (!isJidGroup(jid)) {
				throw new Boom('sendSecretGroupMessage deve ser usada apenas para grupos (g.us)', { statusCode: 400 })
			}
			const targetJid = options.targetJid
			if (!targetJid) {
				throw new Boom('options.targetJid é obrigatório em sendSecretGroupMessage', { statusCode: 400 })
			}

			const { targetJid: _targetJid, targetOnly0Device: _targetOnly0Device, ...messageGenOptions } = options
			const targetOnly0Device = options.targetOnly0Device === true
			const userJid = authState.creds.me!.id
			const meLid = authState.creds.me!.lid || authState.creds.me!.id
			const jlidUser = jidDecode(authState.creds.me?.lid)?.user
			const useUserDevicesCache = options.useUserDevicesCache !== false
			const useCachedGroupMetadata = options.useCachedGroupMetadata !== false
			const additionalAttributes: BinaryNodeAttributes = options.additionalAttributes || {}
			const additionalNodes: BinaryNode[] = options.additionalNodes || []

			const groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined
			const resolvedGroupData =
				groupData && Array.isArray(groupData?.participants) ? groupData : await groupMetadata(jid)

			const participantsList = resolvedGroupData.participants
				.map((p: { lid?: string; id?: string; jid?: string }) => p.lid || p.id)
				.filter((jid): jid is string => !!jid)

			// Resolver targetJid para o(s) jid(s) do participante no grupo (LID ou id), pois o grupo pode usar LID
			const normalizedTarget = jidNormalizedUser(targetJid)
			const targetParticipantJids = new Set<string>()
			for (const p of resolvedGroupData.participants) {
				const pid = (p as { id?: string; lid?: string; jid?: string }).id
				const plid = (p as { id?: string; lid?: string; jid?: string }).lid
				const pjid = (p as { id?: string; lid?: string; jid?: string }).jid
				const matches =
					areJidsSameUser(pid, normalizedTarget) ||
					(plid ? areJidsSameUser(plid, normalizedTarget) : false) ||
					(pjid ? areJidsSameUser(pjid, normalizedTarget) : false)
				if (matches) {
					const j = plid || pid
					if (j) targetParticipantJids.add(jidNormalizedUser(j))
				}
			}
			logger.debug(
				{ targetJid: normalizedTarget, targetParticipantJids: [...targetParticipantJids] },
				'sendSecretGroupMessage: target resolved'
			)

			const additionalDevices = await getUSyncDevices(participantsList, useUserDevicesCache, false)
			const devices: JidWithDevice[] = [...additionalDevices]
			const mePhone = additionalDevices.some((d: JidWithDevice) => d.user === jlidUser && (d.device ?? 0) === 0)
			if (!mePhone && jlidUser) {
				devices.push({ user: jlidUser, device: 0, jid: jidNormalizedUser(meLid) })
			}

			const messageId = options.messageId || generateMessageIDV2(sock.user?.id)
			const messageIdReal = generateMessageIDV2(sock.user?.id)

			const targetDeviceJids: string[] = []
			for (const d of devices) {
				const server = jidDecode(d.jid)?.server || 'lid'
				const deviceNum = d.device ?? 0
				const fullDeviceJid = jidEncode(d.user, server as 'lid' | 's.whatsapp.net', deviceNum)
				const deviceParticipantJid = d.jid ? jidNormalizedUser(d.jid) : ''
				const isTarget =
					!!deviceParticipantJid &&
					targetParticipantJids.has(deviceParticipantJid) &&
					(!targetOnly0Device || deviceNum === 0)
				if (isTarget) targetDeviceJids.push(fullDeviceJid)
			}

			// Placeholder (mensagem vazia) para o grupo com messageId; sem isso o servidor não exibe a mensagem.
			const placeholderMsg = await generateWAMessage(
				jid,
				{ text: '\u200B' },
				{
					logger,
					userJid,
					getUrlInfo: async () => undefined,
					getProfilePicUrl: sock.profilePictureUrl,
					upload: waUploadToServer,
					mediaCache: config.mediaCache,
					options: config.options,
					messageId,
					...messageGenOptions
				}
			)
			await relayMessage(jid, placeholderMsg.message!, {
				messageId,
				useCachedGroupMetadata: true,
				useUserDevicesCache: true,
				additionalAttributes,
				additionalNodes
			})

			// Mensagem real só para o target, com outro messageId para de fato chegar (o cliente não substitui pelo mesmo id).
			const fullMsgReal = await generateWAMessage(jid, messageObject, {
				logger,
				userJid,
				getUrlInfo: (text: string) =>
					getUrlInfo(text, {
						thumbnailWidth: linkPreviewImageThumbnailWidth,
						fetchOpts: { timeout: 3_000, ...(axiosOptions || {}) },
						logger,
						uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
					}),
				getProfilePicUrl: sock.profilePictureUrl,
				upload: waUploadToServer,
				mediaCache: config.mediaCache,
				options: config.options,
				messageId: messageIdReal,
				...messageGenOptions
			})
			for (const targetDeviceJid of targetDeviceJids) {
				try {
					await relayMessage(jid, fullMsgReal.message!, {
						messageId: messageIdReal,
						participant: { jid: targetDeviceJid, count: 0 },
						useCachedGroupMetadata: true,
						useUserDevicesCache: true,
						additionalAttributes,
						additionalNodes
					})
				} catch (err) {
					logger.warn(
						{ err, targetDeviceJid, messageId: messageIdReal },
						'sendSecretGroupMessage: relay to target failed'
					)
				}
			}
			logger.debug(
				{ msgIdPlaceholder: messageId, msgIdReal: messageIdReal, targetDevices: targetDeviceJids.length },
				'sendSecretGroupMessage: placeholder to all, real (own id) to target'
			)

			if (config.emitOwnEvents) {
				process.nextTick(() => {
					processingMutex.mutex(() => upsertMessage(fullMsgReal, 'append'))
				})
			}
			return fullMsgReal
		}
	}
}
