import { proto } from '../../WAProto'
import type { MessageRelayOptions, SignalDataTypeMap, SignalKeyStoreWithTransaction } from '../Types'
import type { AuthenticationCreds } from '../Types/Auth'
import { normalizeMessageContent, unixTimestampSeconds } from '../Utils'
import { BoundedTtlMap } from '../Utils/bounded-ttl-map'
import caches from '../Utils/cache-utils'
import { generateCsToken, readNctSalt } from '../Utils/cs-token-utils'
import { ILogger } from '../Utils/logger'
import { makeMutex, makeSemaphore } from '../Utils/make-mutex'
import {
	buildMergedTcTokenIndexWrite,
	buildTcTokenIndexEntry,
	isRegularUser,
	isTcTokenExpired,
	readLastTcTokenPruneTs,
	readTcTokenIndex,
	resolvePrivacyTokenIntent,
	resolveTcTokenStorageJid,
	shouldSendNewTcToken,
	storeTcTokensFromIqResult,
	TC_TOKEN_INDEX_KEY,
	type LidResolver
} from '../Utils/tc-token-utils'
import {
	areJidsSameUser,
	BinaryNode,
	BinaryNodeAttributes,
	isJidUser,
	isLidUser,
	jidDecode,
	jidNormalizedUser,
	S_WHATSAPP_NET
} from '../WABinary'

/**
 * Privacy token das mensagens 1:1 (tctoken, com cstoken de fallback): índice dos jids com token, mapa PN→LID,
 * emissão depois do envio, reemissão quando o contato troca de identidade e prune diário dos vencidos.
 * Código só do fork, separado do envio para não conflitar com o upstream.
 */
export const makeTcTokenManager = ({
	authState,
	logger,
	query
}: {
	authState: { creds: AuthenticationCreds; keys: SignalKeyStoreWithTransaction }
	logger: ILogger
	query: (node: BinaryNode) => Promise<BinaryNode>
}) => {
	const inFlightTcTokenIssuance = new Set<string>()
	const TC_TOKEN_MAX_CONCURRENT_ISSUANCE = 2
	// teto único pros dois caminhos: emissão pós-envio desiste quando não há vaga (a próxima
	// mensagem tenta de novo), reemissão por troca de identidade entra na fila e espera
	const tcTokenIssuanceSemaphore = makeSemaphore(TC_TOKEN_MAX_CONCURRENT_ISSUANCE)

	const TC_TOKEN_INDEX_FLUSH_MAX_PENDING = 100
	const TC_TOKEN_INDEX_MAX_PENDING = 5_000
	const TC_TOKEN_INDEX_FLUSH_INTERVAL_MS = 30_000
	const pendingTcTokenIndexJids = new Set<string>()
	const recentlyTrackedTcTokenJids = new BoundedTtlMap<string, true>(5_000, 24 * 60 * 60 * 1000)
	let tcTokenIndexFlushTimer: ReturnType<typeof setTimeout> | undefined
	let tcTokenIndexFlushInFlight: Promise<void> | undefined
	let lastTcTokenIndexFullWarnMs = 0
	const tcTokenIndexMutex = makeMutex()

	function armTcTokenIndexFlush() {
		if (tcTokenIndexFlushTimer || tcTokenIndexFlushInFlight) return
		tcTokenIndexFlushTimer = setTimeout(() => {
			tcTokenIndexFlushTimer = undefined
			void flushTcTokenIndex().catch(err => logger.warn({ err: err?.message }, 'falha ao salvar índice de tctokens'))
		}, TC_TOKEN_INDEX_FLUSH_INTERVAL_MS)
	}

	function trackTcTokenJid(jid: string) {
		if (!jid || jid === TC_TOKEN_INDEX_KEY || recentlyTrackedTcTokenJids.has(jid)) return
		if (pendingTcTokenIndexJids.size >= TC_TOKEN_INDEX_MAX_PENDING) {
			if (Date.now() - lastTcTokenIndexFullWarnMs >= 60_000) {
				lastTcTokenIndexFullWarnMs = Date.now()
				logger.warn({ pending: pendingTcTokenIndexJids.size }, 'fila do índice de tctokens cheia')
			}
			return
		}

		recentlyTrackedTcTokenJids.set(jid, true)
		pendingTcTokenIndexJids.add(jid)
		if (pendingTcTokenIndexJids.size >= TC_TOKEN_INDEX_FLUSH_MAX_PENDING) {
			void flushTcTokenIndex().catch(err => logger.warn({ err: err?.message }, 'falha ao salvar lote do índice de tctokens'))
		} else {
			armTcTokenIndexFlush()
		}
	}

	async function writePendingTcTokenIndex() {
		while (pendingTcTokenIndexJids.size) {
			const batch = [...pendingTcTokenIndexJids]
			pendingTcTokenIndexJids.clear()
			try {
				const write = await buildMergedTcTokenIndexWrite(authState.keys, batch)
				await authState.keys.set({ tctoken: write })
			} catch (err) {
				for (const jid of batch) pendingTcTokenIndexJids.add(jid)
				throw err
			}
		}
	}

	function flushTcTokenIndex(): Promise<void> {
		if (tcTokenIndexFlushInFlight) return tcTokenIndexFlushInFlight
		if (tcTokenIndexFlushTimer) {
			clearTimeout(tcTokenIndexFlushTimer)
			tcTokenIndexFlushTimer = undefined
		}

		tcTokenIndexFlushInFlight = tcTokenIndexMutex.mutex(writePendingTcTokenIndex).finally(() => {
			tcTokenIndexFlushInFlight = undefined
			if (pendingTcTokenIndexJids.size) armTcTokenIndexFlush()
		})
		return tcTokenIndexFlushInFlight
	}

	function withFlushedTcTokenIndex<T>(task: () => Promise<T>): Promise<T> {
		return tcTokenIndexMutex.mutex(async () => {
			await writePendingTcTokenIndex()
			return task()
		})
	}

	const pnToLid = new BoundedTtlMap<string, string>(5_000, 10 * 60 * 1000)
	const getLidForPn: LidResolver = pnJid =>
		pnToLid.get(jidNormalizedUser(pnJid)) || caches.lidCache.get(jidNormalizedUser(pnJid))

	function cacheLidMapping(pnJid?: string, lidJid?: string) {
		if (!pnJid || !lidJid) return
		const pn = jidNormalizedUser(pnJid)
		const lid = jidNormalizedUser(lidJid)
		if (!isJidUser(pn) || !isLidUser(lid)) return

		pnToLid.set(pn, lid)
		caches.lidCache.set(pn, lid)
	}

	const tcTokenStorageJid = (jid: string) => resolveTcTokenStorageJid(jid, getLidForPn)

	async function buildCsTokenForJid(jid: string): Promise<{
		token?: Buffer
		reason?: 'missing_lid' | 'missing_nct_salt' | 'keystore_error'
	}> {
		try {
			const recipientLid = tcTokenStorageJid(jid)
			if (!isLidUser(recipientLid)) return { reason: 'missing_lid' }
			const salt = await readNctSalt(authState.keys)
			if (!salt?.length) return { reason: 'missing_nct_salt' }
			return { token: generateCsToken(salt, recipientLid) }
		} catch (err) {
			logger.debug({ jid, err: (err as Error)?.message }, 'falha ao gerar cstoken')
			return { reason: 'keystore_error' }
		}
	}

	/**
	 * Anexa tctoken (ou cstoken como fallback) ao stanza de mensagem 1:1. Devolve se é envio 1:1 e o jid onde o
	 * tctoken é guardado, para a emissão de tctoken depois do envio.
	 */
	const appendPrivacyToken = async (
		stanza: BinaryNode,
		{
			destinationJid,
			isGroup,
			isStatus,
			isNewsletter,
			isPeer,
			isRetry,
			participantJid,
			meIds
		}: {
			destinationJid: string
			isGroup: boolean
			isStatus: boolean
			isNewsletter: boolean
			isPeer: boolean
			isRetry: boolean
			participantJid?: string
			meIds: string[]
		}
	) => {
		const privacyTokenIntent = resolvePrivacyTokenIntent({
			isUserDestination: !!(isJidUser(destinationJid) || isLidUser(destinationJid)),
			isGroup,
			isStatus,
			isNewsletter,
			isPeer,
			isRetry,
			hasParticipant: !!participantJid,
			isSelfParticipant: !!participantJid && meIds.some(meJid => areJidsSameUser(participantJid, meJid))
		})
		const is1on1Send = privacyTokenIntent === 'send'
		const tcTokenJid = privacyTokenIntent !== 'none' ? tcTokenStorageJid(destinationJid) : undefined
		if (!tcTokenJid) {
			return { is1on1Send, tcTokenJid }
		}

		let tcTokenEntry: SignalDataTypeMap['tctoken'] | undefined
		let tcTokenReadFailed = false
		try {
			tcTokenEntry = (await authState.keys.get('tctoken', [tcTokenJid]))[tcTokenJid]
		} catch (err) {
			tcTokenReadFailed = true
			logger.debug({ jid: destinationJid, err: (err as Error)?.message }, 'falha ao ler tctoken')
		}

		let tcTokenBuffer: Buffer | undefined = tcTokenEntry?.token
		let tcTokenState: 'missing' | 'awaiting_recipient' | 'ready' | 'expired' = tcTokenEntry
			? tcTokenBuffer?.length
				? 'ready'
				: tcTokenEntry.senderTimestamp !== undefined
					? 'awaiting_recipient'
					: 'missing'
			: 'missing'
		if (tcTokenBuffer?.length && isTcTokenExpired(tcTokenEntry?.timestamp)) {
			logger.debug({ jid: destinationJid, timestamp: tcTokenEntry?.timestamp }, 'tctoken expired, clearing')
			tcTokenBuffer = undefined
			tcTokenState = 'expired'
			const cleared =
				tcTokenEntry?.senderTimestamp !== undefined
					? { token: Buffer.alloc(0), senderTimestamp: tcTokenEntry.senderTimestamp }
					: null
			try {
				await authState.keys.set({ tctoken: { [tcTokenJid]: cleared } })
			} catch (err) {
				logger.debug({ jid: destinationJid, err: (err as Error)?.message }, 'falha ao limpar tctoken vencido')
			}
		}

		const logPrivacyToken = (
			level: 'info' | 'warn',
			privacyTokenType: 'tctoken' | 'cstoken' | 'none',
			msg: string,
			extra: Record<string, unknown> = {}
		) =>
			logger[level](
				{
					event: 'privacy_token_outgoing_message',
					msgId: stanza.attrs.id,
					recipient: jidNormalizedUser(destinationJid),
					storageJid: tcTokenJid,
					privacyTokenType,
					tcTokenState,
					isretry: isRetry,
					...extra
				},
				msg
			)

		if (tcTokenBuffer?.length) {
			;(stanza.content as BinaryNode[]).push({ tag: 'tctoken', attrs: {}, content: tcTokenBuffer })
			logPrivacyToken('info', 'tctoken', 'mensagem 1:1 protegida por tctoken')
			return { is1on1Send, tcTokenJid }
		}

		const csTokenResult = await buildCsTokenForJid(destinationJid)
		if (csTokenResult.token?.length) {
			;(stanza.content as BinaryNode[]).push({ tag: 'cstoken', attrs: {}, content: csTokenResult.token })
			logPrivacyToken('info', 'cstoken', 'mensagem 1:1 protegida por cstoken', { tcTokenReadFailed })
		} else {
			logPrivacyToken('warn', 'none', 'mensagem 1:1 sem privacy token', {
				reason: csTokenResult.reason,
				tcTokenReadFailed
			})
		}

		return { is1on1Send, tcTokenJid }
	}


	/** emite nosso token para o contato e guarda o dele, se o servidor devolver */
	async function requestAndStoreTcTokens(jid: string, storageJid: string, timestamp: number) {
		const result = await getPrivacyTokens([jid], timestamp)
		return storeTcTokensFromIqResult({
			result,
			fallbackJid: storageJid,
			keys: authState.keys,
			resolveLid: getLidForPn,
			onNewJidStored: trackTcTokenJid
		})
	}

	async function maybeIssueTcToken(
		jid: string,
		message: proto.IMessage,
		options: {
			participant?: MessageRelayOptions['participant']
			additionalAttributes?: BinaryNodeAttributes
			storageJid: string
			msgId: string
		}
	) {
		try {
			if (options.participant || options.additionalAttributes?.['category'] === 'peer') return
			if (normalizeMessageContent(message)?.protocolMessage) return

			const current = await authState.keys.get('tctoken', [options.storageJid])
			if (!shouldSendNewTcToken(current[options.storageJid]?.senderTimestamp)) return
			if (inFlightTcTokenIssuance.has(options.storageJid)) return
			if (!tcTokenIssuanceSemaphore.tryAcquire()) return

			inFlightTcTokenIssuance.add(options.storageJid)
			const issueTimestamp = unixTimestampSeconds()
			try {
				const storedJids = await requestAndStoreTcTokens(jid, options.storageJid, issueTimestamp)
				const afterEntry = (await authState.keys.get('tctoken', [options.storageJid]))[options.storageJid]
				const recipientTokenStored = storedJids.includes(options.storageJid)
				const recipientTokenPresent = !!afterEntry?.token?.length
				await authState.keys.set({
					tctoken: {
						[options.storageJid]: {
							...afterEntry,
							token: afterEntry?.token ?? Buffer.alloc(0),
							senderTimestamp: issueTimestamp
						}
					}
				})
				trackTcTokenJid(options.storageJid)
				logger.info(
					{
						event: 'tc_token_issued',
						msgId: options.msgId,
						recipient: jidNormalizedUser(jid),
						storageJid: options.storageJid,
						recipientTokenStored,
						recipientTokenPresent
					},
					recipientTokenStored
						? 'tc token emitido e token do destinatário persistido'
						: recipientTokenPresent
							? 'tc token emitido; token existente preservado'
							: 'tc token emitido; aguardando token do destinatário'
				)
			} finally {
				inFlightTcTokenIssuance.delete(options.storageJid)
				tcTokenIssuanceSemaphore.release()
			}
		} catch (err) {
			logger.debug({ jid, err: (err as Error)?.message }, 'falha ao emitir tctoken')
		}
	}

	/**
	 * Quando o contato troca de identidade Signal, o token que emitimos pra ele deixa de valer.
	 * Reemite reusando o senderTimestamp armazenado, pra não avançar o bucket de emissão.
	 */
	async function reissueTcTokenAfterIdentityChange(jid: string) {
		try {
			// só a identidade do device primário conta; companion trocando de chave não invalida o token
			if (jidDecode(jid)?.device) return
			// troca da nossa própria identidade não é reach-out: não há token nosso pra reemitir
			if (
				areJidsSameUser(jid, authState.creds.me?.id) ||
				areJidsSameUser(jid, authState.creds.me?.lid)
			) {
				return
			}

			if (!isRegularUser(jidNormalizedUser(jid))) return

			const storageJid = tcTokenStorageJid(jid)
			const entry = (await authState.keys.get('tctoken', [storageJid]))[storageJid]
			const senderTimestamp = entry?.senderTimestamp
			// nunca emitimos pra esse contato, ou a janela do emissor já expirou: nada a reemitir
			if (senderTimestamp === undefined || isTcTokenExpired(senderTimestamp)) return
			if (inFlightTcTokenIssuance.has(storageJid)) return

			inFlightTcTokenIssuance.add(storageJid)
			try {
				// espera vaga em vez de desistir: reemissão não tem segunda chance, ao contrário
				// da emissão pós-envio, que a próxima mensagem repete
				await tcTokenIssuanceSemaphore.acquire()
				try {
					const storedJids = await requestAndStoreTcTokens(jid, storageJid, senderTimestamp)
					logger.info(
						{
							event: 'tc_token_reissued_identity_change',
							recipient: jidNormalizedUser(jid),
							storageJid,
							senderTimestamp,
							recipientTokenStored: storedJids.includes(storageJid)
						},
						'tc token reemitido após troca de identidade'
					)
				} finally {
					tcTokenIssuanceSemaphore.release()
				}
			} finally {
				inFlightTcTokenIssuance.delete(storageJid)
			}
		} catch (err) {
			logger.debug({ jid, err: (err as Error)?.message }, 'falha ao reemitir tctoken após troca de identidade')
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

	function cancelTcTokenPrune() {
		if (tcTokenPruneTimer) {
			clearTimeout(tcTokenPruneTimer)
			tcTokenPruneTimer = undefined
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
	}
}
