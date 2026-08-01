import NodeCache from '@cacheable/node-cache'
import { Boom } from '@hapi/boom'
import { randomBytes } from 'crypto'
import Long = require('long')
import { proto } from '../../WAProto'
import { DEFAULT_CACHE_TTLS, KEY_BUNDLE_TYPE, MIN_PREKEY_COUNT, SERVER_ERROR_CODES } from '../Defaults'
import {
	CacheStore,
	MessageReceiptType,
	MessageRelayOptions,
	MessageUserReceipt,
	SocketConfig,
	WACallEvent,
	WAMessageKey,
	WAMessageStatus,
	WAMessageStubType,
	WAPatchName,
	WAPresence,
	PresenceData
} from '../Types'
import {
	aesDecryptCTR,
	aesEncryptGCM,
	cleanMessage,
	Curve,
	decodeMediaRetryNode,
	decodeMessageNode,
	decryptMessageNode,
	delay,
	derivePairingCodeKey,
	encodeBigEndian,
	encodeSignedDeviceIdentity,
	extractE2ESessionFromRetryReceipt,
	getCallStatusFromNode,
	getHistoryMsg,
	getNextPreKeys,
	getStatusFromReceiptType,
	hkdf,
	MISSING_KEYS_ERROR_TEXT,
	NACK_REASONS,
	retryReasonFor,
	NO_MESSAGE_FOUND_ERROR_TEXT,
	buildAckStanza,
	unixTimestampSeconds,
	xmppPreKey,
	xmppSignedPreKey
} from '../Utils'
import { makeMutex } from '../Utils/make-mutex'
import {
	buildTcTokenIndexEntry,
	isTcTokenExpired,
	readLastTcTokenPruneTs,
	readTcTokenIndex,
	storeTcTokensFromIqResult,
	TC_TOKEN_INDEX_KEY
} from '../Utils/tc-token-utils'
import {
	areJidsSameUser,
	BinaryNode,
	getAllBinaryNodeChildren,
	getBinaryNodeChild,
	getBinaryNodeChildBuffer,
	getBinaryNodeChildren,
	getBinaryNodeChildString,
	isJidGroup,
	isJidStatusBroadcast,
	isJidUser,
	isLidUser,
	jidDecode,
	jidEncode,
	jidNormalizedUser,
	S_WHATSAPP_NET
} from '../WABinary'
import { extractGroupMetadata } from './groups'
import { makeMessagesSocket } from './messages-send'


export const makeMessagesRecvSocket = (config: SocketConfig) => {
	const { logger, retryRequestDelayMs, maxMsgRetryCount, getMessage, shouldIgnoreJid } = config
	const sock = makeMessagesSocket(config)
	const {
		ev,
		authState,
		ws,
		processingMutex,
		signalRepository,
		query,
		upsertMessage,
		resyncAppState,
		onUnexpectedError,
		assertSessions,
		sendNode,
		relayMessage,
		getUSyncDevices,
		getSelectiveRelayContext,
		getSelectiveSentMessage,
		sendReceipt,
		uploadPreKeys,
		sendPeerDataOperationMessage,
		getLidForPn,
		cacheLidMapping,
		tcTokenStorageJid,
		trackTcTokenJid,
		flushTcTokenIndex,
		withFlushedTcTokenIndex
	} = sock

	/** this mutex ensures that each retryRequest will wait for the previous one to finish */
	const retryMutex = makeMutex()

	const RETRY_PER_PARTICIPANT_MAX = 3
	const RETRY_PER_PARTICIPANT_WINDOW_MS = 10 * 60 * 1000
	const participantRetryCache = new Map<string, { count: number; expiresAt: number; warned: boolean }>()
	const shouldSkipParticipantRetry = (participant: string) => {
		const now = Date.now()
		const entry = participantRetryCache.get(participant)
		if (!entry || now >= entry.expiresAt) {
			const expiresAt = now + RETRY_PER_PARTICIPANT_WINDOW_MS
			participantRetryCache.set(participant, { count: 1, expiresAt, warned: false })
			const cleanupTimer = setTimeout(() => {
				if (participantRetryCache.get(participant)?.expiresAt === expiresAt) {
					participantRetryCache.delete(participant)
				}
			}, RETRY_PER_PARTICIPANT_WINDOW_MS)
			cleanupTimer.unref()
			return { skip: false, warn: false }
		}

		entry.count++
		const skip = entry.count > RETRY_PER_PARTICIPANT_MAX
		const warn = skip && !entry.warned
		if (warn) entry.warned = true
		return { skip, warn }
	}

	const TC_TOKEN_PRUNE_BATCH = 20
	const TC_TOKEN_PRUNE_INTERVAL = 24 * 60 * 60
	const TC_TOKEN_PRUNE_MAX_JITTER_MS = 15 * 60 * 1000
	let tcTokenPruneInFlight = false
	let tcTokenPruneTimer: ReturnType<typeof setTimeout> | undefined

	function scheduleTcTokenPrune() {
		if (tcTokenPruneTimer || tcTokenPruneInFlight) return
		tcTokenPruneTimer = setTimeout(() => {
			tcTokenPruneTimer = undefined
			void maybePruneExpiredTcTokens()
		}, Math.floor(Math.random() * TC_TOKEN_PRUNE_MAX_JITTER_MS))
	}

	async function maybePruneExpiredTcTokens() {
		if (tcTokenPruneInFlight) return
		tcTokenPruneInFlight = true
		try {
			const lastPrune = await readLastTcTokenPruneTs(authState.keys)
			if (unixTimestampSeconds() - lastPrune >= TC_TOKEN_PRUNE_INTERVAL) {
				await withFlushedTcTokenIndex(runPruneExpiredTcTokens)
			}
		} catch (err) {
			logger.warn({ err: (err as Error)?.message }, 'falha ao executar prune de tctokens')
		} finally {
			tcTokenPruneInFlight = false
		}
	}

	async function runPruneExpiredTcTokens() {
		const persisted = await readTcTokenIndex(authState.keys)
		if (!persisted.length) {
			await authState.keys.set({
				tctoken: { [TC_TOKEN_INDEX_KEY]: buildTcTokenIndexEntry([], unixTimestampSeconds()) }
			})
			return
		}

		type TcTokenWrite = null | { token: Buffer; timestamp?: string; senderTimestamp?: number }
		const survivors = new Set<string>()
		let mutated = 0
		for (let offset = 0; offset < persisted.length; offset += TC_TOKEN_PRUNE_BATCH) {
			const batch = persisted.slice(offset, offset + TC_TOKEN_PRUNE_BATCH)
			const tokens = await authState.keys.get('tctoken', batch)
			const writes: Record<string, TcTokenWrite> = {}

			for (const jid of batch) {
				const entry = tokens[jid]
				if (!entry) {
					mutated += 1
					continue
				}

				const keepPeerToken = !!entry.token?.length && !isTcTokenExpired(entry.timestamp)
				const keepSenderTs = entry.senderTimestamp !== undefined && !isTcTokenExpired(entry.senderTimestamp)
				if (!keepPeerToken && !keepSenderTs) {
					writes[jid] = null
					mutated += 1
				} else if (!keepPeerToken && keepSenderTs && entry.token?.length) {
					writes[jid] = { token: Buffer.alloc(0), senderTimestamp: entry.senderTimestamp }
					survivors.add(jid)
					mutated += 1
				} else {
					survivors.add(jid)
				}
			}

			if (Object.keys(writes).length) await authState.keys.set({ tctoken: writes })
		}

		await authState.keys.set({
			tctoken: {
				[TC_TOKEN_INDEX_KEY]: buildTcTokenIndexEntry(survivors, unixTimestampSeconds())
			}
		})
		logger.debug({ mutated, remaining: survivors.size }, 'tctokens expirados removidos')
	}

	const msgRetryCache: CacheStore =
		config.msgRetryCounterCache ||
		new NodeCache<any>({
			stdTTL: DEFAULT_CACHE_TTLS.MSG_RETRY, // 1 hour
			useClones: false
		})
	const callOfferCache: CacheStore =
		config.callOfferCache ||
		new NodeCache<any>({
			stdTTL: DEFAULT_CACHE_TTLS.CALL_OFFER, // 5 mins
			useClones: false
		})

	const placeholderResendCache: CacheStore =
		config.placeholderResendCache ||
		new NodeCache<any>({
			stdTTL: DEFAULT_CACHE_TTLS.MSG_RETRY, // 1 hour
			useClones: false
		})

	let sendActiveReceipts = false

	const sendMessageAck = async (node: BinaryNode, errorCode?: number) => {
		const stanza = buildAckStanza(node, errorCode, authState.creds.me?.id)
		if (!stanza) {
			logger.warn({ tag: node.tag, attrs: node.attrs }, 'skipping ack: missing id/from')
			return
		}

		logger.debug({ recv: { tag: node.tag, attrs: node.attrs }, sent: stanza.attrs }, 'sent ack')
		await sendNode(stanza)
	}

	const rejectCall = async (callId: string, callFrom: string) => {
		const stanza: BinaryNode = {
			tag: 'call',
			attrs: {
				from: authState.creds.me!.id,
				to: callFrom
			},
			content: [
				{
					tag: 'reject',
					attrs: {
						'call-id': callId,
						'call-creator': callFrom,
						count: '0'
					},
					content: undefined
				}
			]
		}
		await query(stanza)
	}

	const sendRetryRequest = async (node: BinaryNode, forceIncludeKeys = false, retryReason?: string) => {
		const { fullMessage, author } = decodeMessageNode(node, authState.creds.me!.id, authState.creds.me!.lid || '')
		const { key: msgKey } = fullMessage
		const msgId = msgKey.id!

		const key = `${msgId}:${msgKey?.participant}`
		let retryCount = msgRetryCache.get<number>(key) || 0
		if (retryCount >= maxMsgRetryCount) {
			logger.debug({ retryCount, msgId }, 'reached retry limit, clearing')
			msgRetryCache.del(key)
			return
		}

		retryCount += 1
		msgRetryCache.set(key, retryCount)

		const { account, signedPreKey, signedIdentityKey: identityKey } = authState.creds

		if (retryCount === 1) {
			//request a resend via phone
			//const msgId = await requestPlaceholderResend(msgKey)
			//logger.debug(`sendRetryRequest: requested placeholder resend for message ${msgId}`)
			//desabilitado por mau funcionamento
		}

		const deviceIdentity = encodeSignedDeviceIdentity(account!, true)
		await authState.keys.transaction(async () => {
			const receipt: BinaryNode = {
				tag: 'receipt',
				attrs: {
					id: msgId,
					type: 'retry',
					to: node.attrs.from
				},
				content: [
					{
						tag: 'retry',
						attrs: {
							count: retryCount.toString(),
							id: node.attrs.id,
							t: node.attrs.t,
							v: '1',
							// sem o motivo o remetente reenvia o mesmo skmsg e o decrypt falha de novo:
							// error=1 pede redistribuição da sender key, error=3 pede novo bundle de pre-key
							...(retryReason ? { error: retryReason } : {})
						}
					},
					{
						tag: 'registration',
						attrs: {},
						content: encodeBigEndian(authState.creds.registrationId)
					}
				]
			}

			if (node.attrs.recipient) {
				receipt.attrs.recipient = node.attrs.recipient
			}

			if (node.attrs.participant) {
				receipt.attrs.participant = node.attrs.participant
			}

			if (retryCount <=2 && forceIncludeKeys) {
				await assertSessions([jidNormalizedUser(author)], true);
				const { update, preKeys } = await getNextPreKeys(authState, 1)

				const [keyId] = Object.keys(preKeys)
				const key = preKeys[+keyId]

				const content = receipt.content! as BinaryNode[]
				content.push({
					tag: 'keys',
					attrs: {},
					content: [
						{ tag: 'type', attrs: {}, content: Buffer.from(KEY_BUNDLE_TYPE) },
						{ tag: 'identity', attrs: {}, content: identityKey.public },
						xmppPreKey(key, +keyId),
						xmppSignedPreKey(signedPreKey),
						{ tag: 'device-identity', attrs: {}, content: deviceIdentity }
					]
				})

				ev.emit('creds.update', update)
			}

			await sendNode(receipt)

			logger.info({ msgAttrs: node.attrs, retryCount }, 'sent retry receipt')
		})
	}

	const handleEncryptNotification = async (node: BinaryNode) => {
		const from = node.attrs.from
		if (from === S_WHATSAPP_NET) {
			const countChild = getBinaryNodeChild(node, 'count')
			const count = +countChild!.attrs.value
			const shouldUploadMorePreKeys = count < MIN_PREKEY_COUNT

			logger.debug({ count, shouldUploadMorePreKeys }, 'recv pre-key count')
			if (shouldUploadMorePreKeys) {
				await uploadPreKeys()
			}
		} else {
			const identityNode = getBinaryNodeChild(node, 'identity')
			if (identityNode) {
				logger.info({ jid: from }, 'identity changed')
				// not handling right now
				// signal will override new identity anyway
			} else {
				logger.info({ node }, 'unknown encrypt notification')
			}
		}
	}

	const handleGroupNotification = (participant: string, child: BinaryNode, msg: Partial<proto.IWebMessageInfo>) => {
		const participantJid = getBinaryNodeChild(child, 'participant')?.attrs?.jid || participant
		switch (child?.tag) {
			case 'create':
				const metadata = extractGroupMetadata(child)

				msg.messageStubType = WAMessageStubType.GROUP_CREATE
				msg.messageStubParameters = [metadata.subject]
				msg.key = { participant: metadata.owner }

				ev.emit('chats.upsert', [
					{
						id: metadata.id,
						name: metadata.subject,
						conversationTimestamp: metadata.creation
					}
				])
				ev.emit('groups.upsert', [
					{
						...metadata,
						author: participant
					}
				])
				break
			case 'ephemeral':
			case 'not_ephemeral':
				msg.message = {
					protocolMessage: {
						type: proto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
						ephemeralExpiration: +(child.attrs.expiration || 0)
					}
				}
				break
			case 'modify':
				const oldNumber = getBinaryNodeChildren(child, 'participant').map(p => p.attrs.jid)
				msg.messageStubParameters = oldNumber || []
				msg.messageStubType = WAMessageStubType.GROUP_PARTICIPANT_CHANGE_NUMBER
				break
			case 'promote':
			case 'demote':
			case 'remove':
			case 'add':
			case 'leave':
				const stubType = `GROUP_PARTICIPANT_${child.tag.toUpperCase()}`
				msg.messageStubType = WAMessageStubType[stubType]

				const participants = getBinaryNodeChildren(child, 'participant').map(p => p.attrs.jid)
				if (
					participants.length === 1 &&
					// if recv. "remove" message and sender removed themselves
					// mark as left
					areJidsSameUser(participants[0], participant) &&
					child.tag === 'remove'
				) {
					msg.messageStubType = WAMessageStubType.GROUP_PARTICIPANT_LEAVE
				}

				msg.messageStubParameters = participants
				break
			case 'subject':
				msg.messageStubType = WAMessageStubType.GROUP_CHANGE_SUBJECT
				msg.messageStubParameters = [child.attrs.subject]
				break
			case 'description':
				const description = getBinaryNodeChild(child, 'body')?.content?.toString()
				msg.messageStubType = WAMessageStubType.GROUP_CHANGE_DESCRIPTION
				msg.messageStubParameters = description ? [description] : undefined
				break
			case 'announcement':
			case 'not_announcement':
				msg.messageStubType = WAMessageStubType.GROUP_CHANGE_ANNOUNCE
				msg.messageStubParameters = [child.tag === 'announcement' ? 'on' : 'off']
				break
			case 'locked':
			case 'unlocked':
				msg.messageStubType = WAMessageStubType.GROUP_CHANGE_RESTRICT
				msg.messageStubParameters = [child.tag === 'locked' ? 'on' : 'off']
				break
			case 'invite':
				msg.messageStubType = WAMessageStubType.GROUP_CHANGE_INVITE_LINK
				msg.messageStubParameters = [child.attrs.code]
				break
			case 'member_add_mode':
				const addMode = child.content
				if (addMode) {
					msg.messageStubType = WAMessageStubType.GROUP_MEMBER_ADD_MODE
					msg.messageStubParameters = [addMode.toString()]
				}

				break
			case 'membership_approval_mode':
				const approvalMode = getBinaryNodeChild(child, 'group_join')
				if (approvalMode) {
					msg.messageStubType = WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_MODE
					msg.messageStubParameters = [approvalMode.attrs.state]
				}

				break
			case 'created_membership_requests':
				msg.messageStubType = WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD
				msg.messageStubParameters = [participantJid, 'created', child.attrs.request_method]
				break
			case 'revoked_membership_requests':
				const isDenied = areJidsSameUser(participantJid, participant)
				msg.messageStubType = WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD
				msg.messageStubParameters = [participantJid, isDenied ? 'revoked' : 'rejected']
				break
		}
	}

	const processNotification = async (node: BinaryNode) => {
		const result: Partial<proto.IWebMessageInfo> = {}
		const [child] = getAllBinaryNodeChildren(node)
		const nodeType = node.attrs.type
		const from = jidNormalizedUser(node.attrs.from)

		switch (nodeType) {
			case 'privacy_token': {
				const senderPn = node.attrs.sender_pn || (isJidUser(node.attrs.from) ? node.attrs.from : undefined)
				const senderLid = node.attrs.sender_lid || (isLidUser(node.attrs.from) ? node.attrs.from : undefined)
				cacheLidMapping(senderPn, senderLid)

				const fallbackJid = senderLid || tcTokenStorageJid(from)
				const storedJids = await storeTcTokensFromIqResult({
					result: node,
					fallbackJid,
					keys: authState.keys,
					resolveLid: getLidForPn,
					onNewJidStored: trackTcTokenJid
				})

				for (const { attrs, content } of getBinaryNodeChildren(child, 'token')) {
					ev.emit('chats.update', [{ id: attrs.jid, tcToken: content as Buffer }])
				}

				for (const storageJid of storedJids) {
					logger.info(
						{ event: 'tc_token_received', from, storageJid },
						'tc token recebido e persistido'
					)
				}
				break
			}
			case 'newsletter':
				await handleNewsletterNotification(node)
				break
			case 'mex':
				await handleMexNewsletterNotification(node)
				break
			case 'w:gp2':
				handleGroupNotification(node.attrs.participant, child, result)
				break
			case 'mediaretry':
				const event = decodeMediaRetryNode(node)
				ev.emit('messages.media-update', [event])
				break
			case 'encrypt':
				await handleEncryptNotification(node)
				break
			case 'devices':
				const devices = getBinaryNodeChildren(child, 'device')
				if (areJidsSameUser(child.attrs.jid, authState.creds.me!.id)) {
					const deviceJids = devices.map(d => d.attrs.jid)
					logger.info({ deviceJids }, 'got my own devices')
				}

				break
		case 'server_sync':
			const updates = getBinaryNodeChildren(node, 'collection')
			if (updates.length > 0) {
				// Bot multi-tenant não precisa de blocklist local; ignorar para evitar
				// loops de "rate-overlimit" em contas com muitos bloqueios acumulados.
				const SKIP_COLLECTIONS: ReadonlySet<WAPatchName> = new Set(['critical_unblock_low'])

				const collectionNames = updates
					.map(item => item.attrs.name as WAPatchName)
					.filter(name => !SKIP_COLLECTIONS.has(name))

				if (collectionNames.length === 0) {
					logger.debug(
						{ skipped: updates.map(u => u.attrs.name) },
						'server_sync ignored (all collections skipped)'
					)
					break
				}

				try {
					await resyncAppState(collectionNames, false)
				} catch (error: any) {
					logger.info(
						{ error: error?.message, collections: collectionNames },
						'failed to server_sync state'
					)
				}
			}

			break
			case 'picture':
				const setPicture = getBinaryNodeChild(node, 'set')
				const delPicture = getBinaryNodeChild(node, 'delete')

				ev.emit('contacts.update', [
					{
						id: jidNormalizedUser(node?.attrs?.from) || (setPicture || delPicture)?.attrs?.hash || '',
						imgUrl: setPicture ? 'changed' : 'removed'
					}
				])

				if (isJidGroup(from)) {
					const node = setPicture || delPicture
					result.messageStubType = WAMessageStubType.GROUP_CHANGE_ICON

					if (setPicture) {
						result.messageStubParameters = [setPicture.attrs.id]
					}

					result.participant = node?.attrs.author
					result.key = {
						...(result.key || {}),
						participant: setPicture?.attrs.author
					}
				}

				break
			case 'account_sync':
				if (child.tag === 'disappearing_mode') {
					const newDuration = +child.attrs.duration
					const timestamp = +child.attrs.t

					logger.info({ newDuration }, 'updated account disappearing mode')

					ev.emit('creds.update', {
						accountSettings: {
							...authState.creds.accountSettings,
							defaultDisappearingMode: {
								ephemeralExpiration: newDuration,
								ephemeralSettingTimestamp: timestamp
							}
						}
					})
				} else if (child.tag === 'blocklist') {
					const blocklists = getBinaryNodeChildren(child, 'item')

					for (const { attrs } of blocklists) {
						const blocklist = [attrs.jid]
						const type = attrs.action === 'block' ? 'add' : 'remove'
						ev.emit('blocklist.update', { blocklist, type })
					}
				}

				break
			case 'link_code_companion_reg':
				const linkCodeCompanionReg = getBinaryNodeChild(node, 'link_code_companion_reg')
				const ref = toRequiredBuffer(getBinaryNodeChildBuffer(linkCodeCompanionReg, 'link_code_pairing_ref'))
				const primaryIdentityPublicKey = toRequiredBuffer(
					getBinaryNodeChildBuffer(linkCodeCompanionReg, 'primary_identity_pub')
				)
				const primaryEphemeralPublicKeyWrapped = toRequiredBuffer(
					getBinaryNodeChildBuffer(linkCodeCompanionReg, 'link_code_pairing_wrapped_primary_ephemeral_pub')
				)
				const codePairingPublicKey = await decipherLinkPublicKey(primaryEphemeralPublicKeyWrapped)
				const companionSharedKey = Curve.sharedKey(
					authState.creds.pairingEphemeralKeyPair.private,
					codePairingPublicKey
				)
				const random = randomBytes(32)
				const linkCodeSalt = randomBytes(32)
				const linkCodePairingExpanded = await hkdf(companionSharedKey, 32, {
					salt: linkCodeSalt,
					info: 'link_code_pairing_key_bundle_encryption_key'
				})
				const encryptPayload = Buffer.concat([
					Buffer.from(authState.creds.signedIdentityKey.public),
					primaryIdentityPublicKey,
					random
				])
				const encryptIv = randomBytes(12)
				const encrypted = aesEncryptGCM(encryptPayload, linkCodePairingExpanded, encryptIv, Buffer.alloc(0))
				const encryptedPayload = Buffer.concat([linkCodeSalt, encryptIv, encrypted])
				const identitySharedKey = Curve.sharedKey(authState.creds.signedIdentityKey.private, primaryIdentityPublicKey)
				const identityPayload = Buffer.concat([companionSharedKey, identitySharedKey, random])
				authState.creds.advSecretKey = (await hkdf(identityPayload, 32, { info: 'adv_secret' })).toString('base64')
				await query({
					tag: 'iq',
					attrs: {
						to: S_WHATSAPP_NET,
						type: 'set',
						id: sock.generateMessageTag(),
						xmlns: 'md'
					},
					content: [
						{
							tag: 'link_code_companion_reg',
							attrs: {
								jid: authState.creds.me!.id,
								stage: 'companion_finish'
							},
							content: [
								{
									tag: 'link_code_pairing_wrapped_key_bundle',
									attrs: {},
									content: encryptedPayload
								},
								{
									tag: 'companion_identity_public',
									attrs: {},
									content: authState.creds.signedIdentityKey.public
								},
								{
									tag: 'link_code_pairing_ref',
									attrs: {},
									content: ref
								}
							]
						}
					]
				})
				authState.creds.registered = true
				ev.emit('creds.update', authState.creds)
		}

		if (Object.keys(result).length) {
			return result
		}
	}

	async function decipherLinkPublicKey(data: Uint8Array | Buffer) {
		const buffer = toRequiredBuffer(data)
		const salt = buffer.slice(0, 32)
		const secretKey = await derivePairingCodeKey(authState.creds.pairingCode!, salt)
		const iv = buffer.slice(32, 48)
		const payload = buffer.slice(48, 80)
		return aesDecryptCTR(payload, secretKey, iv)
	}

	function toRequiredBuffer(data: Uint8Array | Buffer | undefined) {
		if (data === undefined) {
			throw new Boom('Invalid buffer', { statusCode: 400 })
		}

		return data instanceof Buffer ? data : Buffer.from(data)
	}

	const willSendMessageAgain = (id: string, participant: string) => {
		const key = `${id}:${participant}`
		const retryCount = msgRetryCache.get<number>(key) || 0
		return retryCount < maxMsgRetryCount
	}

	const updateSendMessageAgainCount = (id: string, participant: string) => {
		const key = `${id}:${participant}`
		const newValue = (msgRetryCache.get<number>(key) || 0) + 1
		msgRetryCache.set(key, newValue)
	}

	const sendMessagesAgain = async (
		key: proto.IMessageKey,
		ids: string[],
		retryNode: BinaryNode,
		receiptNode: BinaryNode
	) => {
		// todo: implement a cache to store the last 256 sent messages (copy whatsmeow)
		const remoteJid = key.remoteJid!
		const msgs = await Promise.all(
			ids.map(async id => (await getMessage({ ...key, id })) || getSelectiveSentMessage(remoteJid, id))
		)
		const participant = key.participant || remoteJid
		// device zumbi pede retry em TODA mensagem (2 IQs + processingMutex bloqueado
		// por retry) e nunca converge; após 3 retries em 10min, ignora os próximos
		const participantRetry = shouldSkipParticipantRetry(participant)
		if (participantRetry.skip) {
			if (participantRetry.warn) {
				logger.warn({ participant, ids }, 'retry ignorado: participante excedeu limite de retries (device zumbi?)')
			}
			return
		}

		// if it's the primary jid sending the request
		// just re-send the message to everyone
		// prevents the first message decryption failure
		const sendToAll = !jidDecode(participant)?.device
		const participantUser = jidDecode(jidNormalizedUser(participant))?.user
		const selectiveContexts = ids.map(id =>
			isJidGroup(remoteJid) ? getSelectiveRelayContext(remoteJid, id) : undefined
		)
		const hasRetryableMessage = selectiveContexts.some(
			context => !context || (!!participantUser && context.allowedUsers.includes(participantUser))
		)
		if (!hasRetryableMessage) {
			logger.info(
				{ remoteJid, participant, ids },
				'ignorando retry sem mensagens acessíveis ao participante'
			)
			return
		}

		// Reestabelece a sessão com quem pediu o retry, senão reenviamos com a MESMA
		// sessão que o outro lado não conseguiu decriptar e o loop nunca fecha.
		// Preferência: bundle de chaves que veio dentro do próprio retry receipt
		// (portado do upstream v7); fallback: força fetch de prekey bundle via usync.
		let injectedFromBundle = false
		const bundle = extractE2ESessionFromRetryReceipt(receiptNode)
		if (bundle) {
			try {
				await signalRepository.injectE2ESession({ jid: participant, session: bundle as any })
				injectedFromBundle = true
				logger.debug({ participant }, 'injected session from retry receipt key bundle')
			} catch (error) {
				logger.warn({ error, participant }, 'failed to inject session from retry receipt')
			}
		}

		if (!injectedFromBundle) {
			try {
				await assertSessions([participant], true)
			} catch (error) {
				logger.warn({ error, participant }, 'failed to assert session for retry')
			}
		}

		if (isJidGroup(remoteJid)) {
			// remove só quem pediu o retry; zerar o grupo inteiro redistribui o SKDM
			// pra todos a cada retry (loop infinito com device zumbi)
			const mem = (await authState.keys.get('sender-key-memory', [remoteJid]))[remoteJid]
			if (mem && mem[participant]) {
				delete mem[participant]
				await authState.keys.set({ 'sender-key-memory': { [remoteJid]: mem } })
			}
		}

		logger.debug({ participant, sendToAll }, 'forced new session for retry recp')

		for (const [i, msg] of msgs.entries()) {
			if (msg) {
				const selectiveContext = selectiveContexts[i]
				if (selectiveContext) {
					if (!participantUser || !selectiveContext.allowedUsers.includes(participantUser)) {
						logger.info(
							{ remoteJid, participant, messageId: ids[i] },
							'ignorando retry de participante sem acesso ao relay seletivo'
						)
						continue
					}
				}
				updateSendMessageAgainCount(ids[i], participant)
				let retryParticipants = [participant]
				if (selectiveContext && sendToAll) {
					// Receipts do device primário às vezes chegam sem o sufixo do device. Nesse caso,
					// atualiza a lista e recupera todos os devices daquela conta autorizada.
					const freshDevices = await getUSyncDevices([jidNormalizedUser(participant)], false, false)
					retryParticipants = freshDevices.map(device => {
						const server = jidDecode(device.jid)?.server || jidDecode(participant)?.server || 'lid'
						return jidEncode(device.user, server, device.device)
					})
					if (!retryParticipants.length) retryParticipants = [participant]
				}

				for (const retryParticipant of [...new Set(retryParticipants)]) {
					const msgRelayOpts: MessageRelayOptions = {
						messageId: ids[i],
						isretry: true,
						decryptFailHide: selectiveContext?.decryptFailHide,
						useUserDevicesCache: false,
						participant: {
							jid: retryParticipant,
							count: +retryNode.attrs.count
						}
					}
					await relayMessage(key.remoteJid!, msg, msgRelayOpts)
				}
			} else {
				logger.debug({ jid: key.remoteJid, id: ids[i] }, 'recv retry request, but message not available')
			}
		}
	}

	const handleReceipt = async (node: BinaryNode) => {
		const { attrs, content } = node
		if (process.env.WA_POC_RELAY_TRACE === '1') {
			logger.info(
				{
					id: attrs.id,
					from: attrs.from,
					participant: attrs.participant,
					recipient: attrs.recipient,
					type: attrs.type,
					childTags: Array.isArray(content) ? content.map(child => child.tag) : []
				},
				'[POC relay trace] inbound receipt'
			)
		}
		if (!attrs.from || !attrs.id) {
			logger.warn({ tag: node.tag, attrs }, 'ignoring receipt with missing id/from')
			await sendMessageAck(node)
			return
		}

		const isLid = attrs.from.includes('lid')
		const isNodeFromMe = areJidsSameUser(
			attrs.participant || attrs.from,
			isLid ? authState.creds.me?.lid : authState.creds.me?.id
		)
		const remoteJid = !isNodeFromMe || isJidGroup(attrs.from) ? attrs.from : attrs.recipient
		const fromMe = !attrs.recipient || ((attrs.type === 'retry' || attrs.type === 'sender') && isNodeFromMe)

		const key: proto.IMessageKey = {
			remoteJid,
			id: '',
			fromMe,
			participant: attrs.participant
		}

		if (remoteJid && shouldIgnoreJid(remoteJid) && remoteJid !== '@s.whatsapp.net') {
			logger.debug({ remoteJid }, 'ignoring receipt from jid')
			await sendMessageAck(node)
			return
		}

		const ids = [attrs.id]
		if (Array.isArray(content)) {
			const items = getBinaryNodeChildren(content[0], 'item')
			ids.push(...items.map(i => i.attrs.id))
		}

		try {
			await Promise.all([
				processingMutex.mutex(async () => {
					const status = getStatusFromReceiptType(attrs.type)
					if (
						typeof status !== 'undefined' &&
						// basically, we only want to know when a message from us has been delivered to/read by the other person
						// or another device of ours has read some messages
						(status >= proto.WebMessageInfo.Status.SERVER_ACK || !isNodeFromMe)
					) {
						if (isJidGroup(remoteJid) || isJidStatusBroadcast(remoteJid)) {
							if (attrs.participant) {
								const updateKey: keyof MessageUserReceipt =
									status === proto.WebMessageInfo.Status.DELIVERY_ACK ? 'receiptTimestamp' : 'readTimestamp'
								const receiptTs = attrs.t ? +attrs.t : unixTimestampSeconds()
								ev.emit(
									'message-receipt.update',
									ids.map(id => ({
										key: { ...key, id },
										receipt: {
											userJid: jidNormalizedUser(attrs.participant),
											[updateKey]: receiptTs
										}
									}))
								)
							}
						} else {
							ev.emit(
								'messages.update',
								ids.map(id => ({
									key: { ...key, id },
									update: { status }
								}))
							)
						}
					}

					if (attrs.type === 'retry') {
						// correctly set who is asking for the retry
						key.participant = key.participant || attrs.from
						const retryNode = getBinaryNodeChild(node, 'retry')
						if (!retryNode) {
							logger.warn({ attrs, key }, 'retry receipt without <retry> child, skipping resend')
						} else if (willSendMessageAgain(ids[0], key.participant)) {
							if (key.fromMe) {
								try {
									logger.debug({ attrs, key }, 'recv retry request')
									await sendMessagesAgain(key, ids, retryNode, node)
								} catch (error) {
									logger.error({ key, ids, trace: error.stack }, 'error in sending message again')
								}
							} else {
								logger.info({ attrs, key }, 'recv retry for not fromMe message')
							}
						} else {
							logger.info({ attrs, key }, 'will not send message again, as sent too many times')
						}
					}
				})
			])
		} finally {
			await sendMessageAck(node)
		}
	}

	const handleNotification = async (node: BinaryNode) => {
		const remoteJid = node.attrs.from
		if (shouldIgnoreJid(remoteJid) && remoteJid !== '@s.whatsapp.net') {
			logger.debug({ remoteJid, id: node.attrs.id }, 'ignored notification')
			await sendMessageAck(node)
			return
		}

		try {
			await Promise.all([
				processingMutex.mutex(async () => {
					const msg = await processNotification(node)
					if (msg) {
						const fromMe = areJidsSameUser(node.attrs.participant || remoteJid, authState.creds.me!.id)
						msg.key = {
							remoteJid,
							fromMe,
							participant: node.attrs.participant,
							id: node.attrs.id,
							...(msg.key || {})
						}
						msg.participant ??= node.attrs.participant
						msg.messageTimestamp = +node.attrs.t

						const fullMsg = proto.WebMessageInfo.fromObject(msg)
						await upsertMessage(fullMsg, 'append')
					}
				})
			])
		} finally {
			await sendMessageAck(node)
		}
	}

	const handleMessage = async (node: BinaryNode) => {
		if (shouldIgnoreJid(node.attrs.from) && node.attrs.from !== '@s.whatsapp.net') {
			logger.debug({ key: node.attrs.key }, 'ignored message')
			await sendMessageAck(node)
			return
		}

		const encNode = getBinaryNodeChild(node, 'enc')

		// TODO: temporary fix for crashes and issues resulting of failed msmsg decryption
		if (encNode && encNode.attrs.type === 'msmsg') {
			logger.debug({ key: node.attrs.key }, 'ignored msmsg')
			await sendMessageAck(node)
			return
		}

		let response: string | undefined
		/*

		if (getBinaryNodeChild(node, 'unavailable') && !encNode) {
			await sendMessageAck(node)
			const { key } = decodeMessageNode(node, authState.creds.me!.id, authState.creds.me!.lid || '').fullMessage
			response = await requestPlaceholderResend(key)
			if (response === 'RESOLVED') {
				return
			}

			logger.debug('received unavailable message, acked and requested resend from phone')
		} else {
			if (placeholderResendCache.get(node.attrs.id)) {
				placeholderResendCache.del(node.attrs.id)
			}
		}

		//desabilitado por mau funcionamento.
			*/

		const {
			fullMessage: msg,
			category,
			author,
			decrypt
		} = decryptMessageNode(node, authState.creds.me!.id, authState.creds.me!.lid || '', signalRepository, logger)

		if (response && msg?.messageStubParameters?.[0] === NO_MESSAGE_FOUND_ERROR_TEXT) {
			msg.messageStubParameters = [NO_MESSAGE_FOUND_ERROR_TEXT, response]
		}

		if (
			msg.message?.protocolMessage?.type === proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER &&
			node.attrs.sender_pn
		) {
			ev.emit('chats.phoneNumberShare', { lid: node.attrs.from, jid: node.attrs.sender_pn })
		}

		if (msg.messageStubType === proto.WebMessageInfo.StubType.CIPHERTEXT) {
			if (
				msg?.messageStubParameters?.[0] === MISSING_KEYS_ERROR_TEXT ||
				msg.messageStubParameters?.[0] === NO_MESSAGE_FOUND_ERROR_TEXT
			) {
				return sendMessageAck(node)
			}
		}

		try {
			await Promise.all([
				processingMutex.mutex(async () => {
					await decrypt()
					// message failed to decrypt
					if (msg.messageStubType === proto.WebMessageInfo.StubType.CIPHERTEXT) {
						if (msg?.messageStubParameters?.[0] === MISSING_KEYS_ERROR_TEXT) {
							return sendMessageAck(node, NACK_REASONS.ParsingError)
						}

						retryMutex.mutex(async () => {
							if (ws.isOpen) {
								if (getBinaryNodeChild(node, 'unavailable')) {
									return
								}

								await sendRetryRequest(node, true, retryReasonFor(msg.messageStubParameters?.[0]))
								if (retryRequestDelayMs) {
									await delay(retryRequestDelayMs)
								}
							} else {
								logger.debug({ node }, 'connection closed, ignoring retry req')
							}
						})
					} else {
						// no type in the receipt => message delivered
						let type: MessageReceiptType = undefined
						let participant = msg.key.participant
						if (category === 'peer') {
							// special peer message
							type = 'peer_msg'
						} else if (msg.key.fromMe) {
							// message was sent by us from a different device
							type = 'sender'
							// need to specially handle this case
							if (isJidUser(msg.key.remoteJid!)) {
								participant = author
							}
						} else if (!sendActiveReceipts) {
							type = 'inactive'
						}

						await sendReceipt(msg.key.remoteJid!, participant!, [msg.key.id!], type)

						// send ack for history message
						const isAnyHistoryMsg = getHistoryMsg(msg.message!)
						if (isAnyHistoryMsg) {
							const jid = jidNormalizedUser(msg.key.remoteJid!)
							await sendReceipt(jid, undefined, [msg.key.id!], 'hist_sync')
						}
					}

					cleanMessage(msg, authState.creds.me!.id)

					await sendMessageAck(node)

					await upsertMessage(msg, node.attrs.offline ? 'append' : 'notify')
				})
			])
		} catch (error) {
			sendMessageAck(node)
		logger.error({ err: error, node }, 'error in handling message')
		}
	}

	const fetchMessageHistory = async (
		count: number,
		oldestMsgKey: WAMessageKey,
		oldestMsgTimestamp: number | Long
	): Promise<string> => {
		if (!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		const pdoMessage: proto.Message.IPeerDataOperationRequestMessage = {
			historySyncOnDemandRequest: {
				chatJid: oldestMsgKey.remoteJid,
				oldestMsgFromMe: oldestMsgKey.fromMe,
				oldestMsgId: oldestMsgKey.id,
				oldestMsgTimestampMs: oldestMsgTimestamp,
				onDemandMsgCount: count
			},
			peerDataOperationRequestType: proto.Message.PeerDataOperationRequestType.HISTORY_SYNC_ON_DEMAND
		}

		return sendPeerDataOperationMessage(pdoMessage)
	}

	const requestPlaceholderResend = async (messageKey: WAMessageKey): Promise<string | undefined> => {
		if (!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		if (placeholderResendCache.get(messageKey?.id!)) {
			logger.debug({ messageKey }, 'already requested resend')
			return
		} else {
			placeholderResendCache.set(messageKey?.id!, true)
		}

		await delay(5000)

		if (!placeholderResendCache.get(messageKey?.id!)) {
			logger.debug({ messageKey }, 'message received while resend requested')
			return 'RESOLVED'
		}

		const pdoMessage = {
			placeholderMessageResendRequest: [
				{
					messageKey
				}
			],
			peerDataOperationRequestType: proto.Message.PeerDataOperationRequestType.PLACEHOLDER_MESSAGE_RESEND
		}

		setTimeout(() => {
			if (placeholderResendCache.get(messageKey?.id!)) {
				logger.debug({ messageKey }, 'PDO message without response after 15 seconds. Phone possibly offline')
				placeholderResendCache.del(messageKey?.id!)
			}
		}, 15_000)

		return sendPeerDataOperationMessage(pdoMessage)
	}

	const handleCall = async (node: BinaryNode) => {
		let status
		const { attrs } = node
		const [infoChild] = getAllBinaryNodeChildren(node)
		const callId = infoChild.attrs['call-id']
		const from = infoChild.attrs.from || infoChild.attrs['call-creator']
		status = getCallStatusFromNode(infoChild)
		if(isLidUser(from) && infoChild.tag==='relaylatency')
		{
			const verify = callOfferCache.get(callId);
			if(!verify)
			{
				status = 'offer';
				callOfferCache.set(callId,true);
			}

		}
		const call: WACallEvent = {
			chatId: attrs.from,
			from,
			id: callId,
			date: new Date(+attrs.t * 1000),
			offline: !!attrs.offline,
			status
		}

		if (status === 'offer') {
			call.isVideo = !!getBinaryNodeChild(infoChild, 'video')
			call.isGroup = infoChild.attrs.type === 'group' || !!infoChild.attrs['group-jid']
			call.groupJid = infoChild.attrs['group-jid']
			callOfferCache.set(call.id, call)
		}

		const existingCall = callOfferCache.get<WACallEvent>(call.id)

		// use existing call info to populate this event
		if (existingCall) {
			call.isVideo = existingCall.isVideo
			call.isGroup = existingCall.isGroup
		}

		// delete data once call has ended
		if (status === 'reject' || status === 'accept' || status === 'timeout' || status === 'terminate') {
			callOfferCache.del(call.id)
			if(isLidUser(from))
			{
			 callOfferCache.del(from)	
			}
		}

		ev.emit('call', [call])

		await sendMessageAck(node)
	}

	const handleBadAck = async ({ attrs }: BinaryNode) => {
		if (process.env.WA_POC_RELAY_TRACE === '1') {
			logger.info(
				{
					id: attrs.id,
					from: attrs.from,
					to: attrs.to,
					participant: attrs.participant,
					error: attrs.error,
					phash: attrs.phash
				},
				'[POC relay trace] inbound ack'
			)
		}
		if (!attrs.from || !attrs.id) {
			logger.warn({ attrs }, 'ignoring bad ack with missing id/from')
			return
		}

		const key: WAMessageKey = { remoteJid: attrs.from, fromMe: true, id: attrs.id }

		// WARNING: REFRAIN FROM ENABLING THIS FOR NOW. IT WILL CAUSE A LOOP
		// // current hypothesis is that if pash is sent in the ack
		// // it means -- the message hasn't reached all devices yet
		// // we'll retry sending the message here
		// if(attrs.phash) {
		// 	logger.info({ attrs }, 'received phash in ack, resending message...')
		// 	const msg = await getMessage(key)
		// 	if(msg) {
		// 		await relayMessage(key.remoteJid!, msg, { messageId: key.id!, useUserDevicesCache: false })
		// 	} else {
		// 		logger.warn({ attrs }, 'could not send message again, as it was not found')
		// 	}
		// }

		// error in acknowledgement,
		// device could not display the message
		if (attrs.error) {
			if (attrs.error === SERVER_ERROR_CODES.MissingTcToken) {
				// Não reenviar: cada retry sem token conta como um novo reach-out e piora a restrição.
				logger.warn(
					{
						event: 'message_ack_error',
						errorCode: SERVER_ERROR_CODES.MissingTcToken,
						msgId: attrs.id,
						from: attrs.from,
						reachoutTimelocked: true
					},
					'erro 463: conta restrita ou contato sem tctoken'
				)
			} else {
				logger.warn({ attrs }, 'received error in ack')
			}
			ev.emit('messages.update', [
				{
					key,
					update: {
						status: WAMessageStatus.ERROR,
						messageStubParameters: [attrs.error]
					}
				}
			])
		}
	}

	/// processes a node with the given function
	/// and adds the task to the existing buffer if we're buffering events
	const processNodeWithBuffer = async <T>(
		node: BinaryNode,
		identifier: string,
		exec: (node: BinaryNode, offline: boolean) => Promise<T>
	) => {
		ev.buffer()
		await execTask()
		ev.flush()

		function execTask() {
			return exec(node, false).catch(err => onUnexpectedError(err, identifier))
		}
	}

	type MessageType = 'message' | 'call' | 'receipt' | 'notification'

	type OfflineNode = {
		type: MessageType
		node: BinaryNode
	}

	const makeOfflineNodeProcessor = () => {
		const nodeProcessorMap: Map<MessageType, (node: BinaryNode) => Promise<void>> = new Map([
			['message', handleMessage],
			['call', handleCall],
			['receipt', handleReceipt],
			['notification', handleNotification]
		])
		const nodes: OfflineNode[] = []
		let isProcessing = false
		const BATCH_SIZE = 10

		const yieldToEventLoop = () => new Promise<void>(resolve => setImmediate(resolve))

		const enqueue = (type: MessageType, node: BinaryNode) => {
			nodes.push({ type, node })

			if (isProcessing) {
				return
			}

			isProcessing = true

			const promise = async () => {
				let processedInBatch = 0
				while (nodes.length && ws.isOpen) {
					const { type, node } = nodes.shift()!

					const nodeProcessor = nodeProcessorMap.get(type)

					if (!nodeProcessor) {
						onUnexpectedError(new Error(`unknown offline node type: ${type}`), 'processing offline node')
						continue
					}

					// isolate per-node failures so one bad stanza doesn't stop the queue
					await nodeProcessor(node).catch(err => onUnexpectedError(err, `processing offline ${type}`))

					processedInBatch++
					if (processedInBatch >= BATCH_SIZE) {
						processedInBatch = 0
						// yield so pings/timers/other I/O can run between batches
						await yieldToEventLoop()
					}
				}

				isProcessing = false
			}

			promise().catch(error => {
				onUnexpectedError(error, 'processing offline nodes')
				sendMessageAck(node)
			})
		}

		return { enqueue }
	}

	const offlineNodeProcessor = makeOfflineNodeProcessor()

	const processNode = (
		type: MessageType,
		node: BinaryNode,
		identifier: string,
		exec: (node: BinaryNode) => Promise<void>
	) => {
		const isOffline = !!node.attrs.offline

		if (isOffline) {
			offlineNodeProcessor.enqueue(type, node)
		} else {
			processNodeWithBuffer(node, identifier, exec)
		}
	}

	// Handles newsletter notifications
	async function handleNewsletterNotification(node: BinaryNode) {
		const from = node.attrs.from
		const [child] = getAllBinaryNodeChildren(node)
		const author = node.attrs.participant

		logger.info({ from, child }, 'got newsletter notification')

		switch (child.tag) {
			case 'reaction':
				const reactionUpdate = {
					id: from,
					server_id: child.attrs.message_id,
					reaction: {
						code: getBinaryNodeChildString(child, 'reaction'),
						count: 1
					}
				}
				ev.emit('newsletter.reaction', reactionUpdate)
				break

			case 'view':
				const viewUpdate = {
					id: from,
					server_id: child.attrs.message_id,
					count: parseInt(child.content?.toString() || '0', 10)
				}
				ev.emit('newsletter.view', viewUpdate)
				break

			case 'participant':
				const participantUpdate = {
					id: from,
					author,
					user: child.attrs.jid,
					action: child.attrs.action,
					new_role: child.attrs.role
				}
				ev.emit('newsletter-participants.update', participantUpdate)
				break

			case 'update':
				const settingsNode = getBinaryNodeChild(child, 'settings')
				if (settingsNode) {
					const update: Record<string, any> = {}
					const nameNode = getBinaryNodeChild(settingsNode, 'name')
					if (nameNode?.content) update.name = nameNode.content.toString()

					const descriptionNode = getBinaryNodeChild(settingsNode, 'description')
					if (descriptionNode?.content) update.description = descriptionNode.content.toString()

					ev.emit('newsletter-settings.update', {
						id: from,
						update
					})
				}

				break

			case 'message':
				const plaintextNode = getBinaryNodeChild(child, 'plaintext')
				if (plaintextNode?.content) {
					try {
						const contentBuf =
							typeof plaintextNode.content === 'string'
								? Buffer.from(plaintextNode.content, 'binary')
								: Buffer.from(plaintextNode.content as Uint8Array)
						const messageProto = proto.Message.decode(contentBuf)
						const fullMessage = proto.WebMessageInfo.fromObject({
							key: {
								remoteJid: from,
								id: child.attrs.message_id || child.attrs.server_id,
								fromMe: false
							},
							message: messageProto,
							messageTimestamp: +child.attrs.t
						})
						await upsertMessage(fullMessage, 'append')
						logger.info('Processed plaintext newsletter message')
					} catch (error) {
						logger.error({ error }, 'Failed to decode plaintext newsletter message')
					}
				}

				break

			default:
				logger.warn({ node }, 'Unknown newsletter notification')
				break
		}
	}

	// Handles mex newsletter notifications
	async function handleMexNewsletterNotification(node: BinaryNode) {
		const mexNode = getBinaryNodeChild(node, 'mex')
		if (!mexNode?.content) {
			logger.warn({ node }, 'Invalid mex newsletter notification')
			return
		}

		let data: any
		try {
			data = JSON.parse(mexNode.content.toString())
		} catch (error) {
			logger.error({ err: error, node }, 'Failed to parse mex newsletter notification')
			return
		}

		const operation = data?.operation
		const updates = data?.updates

		if (!updates || !operation) {
			logger.warn({ data }, 'Invalid mex newsletter notification content')
			return
		}

		logger.info({ operation, updates }, 'got mex newsletter notification')

		switch (operation) {
			case 'NotificationNewsletterUpdate':
				for (const update of updates) {
					if (update.jid && update.settings && Object.keys(update.settings).length > 0) {
						ev.emit('newsletter-settings.update', {
							id: update.jid,
							update: update.settings
						})
					}
				}

				break

			case 'NotificationNewsletterAdminPromote':
				for (const update of updates) {
					if (update.jid && update.user) {
						ev.emit('newsletter-participants.update', {
							id: update.jid,
							author: node.attrs.from,
							user: update.user,
							new_role: 'ADMIN',
							action: 'promote'
						})
					}
				}

				break

			default:
				logger.info({ operation, data }, 'Unhandled mex newsletter notification')
				break
		}
	}
		const handlePresenceUpdate = ({ tag, attrs, content }: BinaryNode) => {
			let presence: PresenceData | undefined
			const jid = attrs.from
			const participant = attrs.participant || attrs.from
	
			if (shouldIgnoreJid(jid)) {
				return
			}
	
			if (tag === 'presence') {
				presence = {
					lastKnownPresence: attrs.type === 'unavailable' ? 'unavailable' : 'available',
					lastSeen: attrs.last && attrs.last !== 'deny' ? +attrs.last : undefined
				}
			} else if (Array.isArray(content)) {
				const [firstChild] = content
				let type = firstChild.tag as WAPresence
				if (type === 'paused') {
					type = 'available'
				}
	
				if (firstChild.attrs?.media === 'audio') {
					type = 'recording'
				}
	
				presence = { lastKnownPresence: type }
			} else {
				logger.error({ tag, attrs, content }, 'recv invalid presence node')
			}
	
			if (presence) {
				ev.emit('presence.update', { id: jid, presences: { [participant]: presence } })
			}
		}

	// recv a message
	ws.on('CB:message', (node: BinaryNode) => {
		processNode('message', node, 'processing message', handleMessage)
	})

	ws.on('CB:call', async (node: BinaryNode) => {
		processNode('call', node, 'handling call', handleCall)
	})

	ws.on('CB:receipt', node => {
		processNode('receipt', node, 'handling receipt', handleReceipt)
	})

	ws.on('CB:notification', async (node: BinaryNode) => {
		processNode('notification', node, 'handling notification', handleNotification)
	})
	ws.on('CB:ack,class:message', (node: BinaryNode) => {
		handleBadAck(node).catch(error => onUnexpectedError(error, 'handling bad ack'))
	})
	ws.on('CB:presence', handlePresenceUpdate)
	ws.on('CB:chatstate', handlePresenceUpdate)

	ev.on('call', ([call]) => {
		// missed call + group call notification message generation
		if (call.status === 'timeout' || (call.status === 'offer' && call.isGroup)) {
			const msg: proto.IWebMessageInfo = {
				key: {
					remoteJid: call.chatId,
					id: call.id,
					fromMe: false
				},
				messageTimestamp: unixTimestampSeconds(call.date)
			}
			if (call.status === 'timeout') {
				if (call.isGroup) {
					msg.messageStubType = call.isVideo
						? WAMessageStubType.CALL_MISSED_GROUP_VIDEO
						: WAMessageStubType.CALL_MISSED_GROUP_VOICE
				} else {
					msg.messageStubType = call.isVideo ? WAMessageStubType.CALL_MISSED_VIDEO : WAMessageStubType.CALL_MISSED_VOICE
				}
			} else {
				msg.message = { call: { callKey: Buffer.from(call.id) } }
			}

			const protoMsg = proto.WebMessageInfo.fromObject(msg)
			upsertMessage(protoMsg, call.offline ? 'append' : 'notify')
		}
	})

	ev.on('connection.update', ({ isOnline, connection }) => {
		if (typeof isOnline !== 'undefined') {
			sendActiveReceipts = isOnline
			logger.trace(`sendActiveReceipts set to "${sendActiveReceipts}"`)
		}

		if (connection === 'close') {
			if (tcTokenPruneTimer) {
				clearTimeout(tcTokenPruneTimer)
				tcTokenPruneTimer = undefined
			}
			void flushTcTokenIndex().catch(() => {})
		}

		if (isOnline) scheduleTcTokenPrune()
	})

	return {
		...sock,
		sendMessageAck,
		sendRetryRequest,
		rejectCall,
		fetchMessageHistory,
		requestPlaceholderResend
	}
}
