import type { Chat, SignalKeyStore } from '../Types'
import {
	type BinaryNode,
	getBinaryNodeChild,
	getBinaryNodeChildren,
	isJidMetaIa,
	isJidUser,
	isLidUser,
	jidNormalizedUser
} from '../WABinary'
import { toNumber } from './generics'
import type { ILogger } from './logger'

const BOT_PHONE_REGEX = /^1313555\d{4}$|^131655500\d{2}$/
const TC_TOKEN_BUCKET_DURATION = 604800
const TC_TOKEN_NUM_BUCKETS = 4
const TC_TOKEN_HISTORY_BATCH_SIZE = 20

/** Chave sentinela com o índice dos JIDs rastreados e o timestamp do último prune. */
export const TC_TOKEN_INDEX_KEY = '__index'

export type LidResolver = (pnJid: string) => string | undefined

export function isRegularUser(jid: string | undefined): boolean {
	if (!jid) return false
	const user = jid.split('@')[0] ?? ''
	if (user === '0' || BOT_PHONE_REGEX.test(user) || isJidMetaIa(jid)) return false
	return !!(isJidUser(jid) || isLidUser(jid) || jid.endsWith('@c.us'))
}

/** Usa LID como chave quando conhecido, igual ao cliente oficial; PN é apenas fallback. */
export function resolveTcTokenStorageJid(jid: string, resolveLid?: LidResolver): string {
	const base = jidNormalizedUser(jid) || jid
	if (isLidUser(base)) return base

	const lid = resolveLid?.(base)
	return lid ? jidNormalizedUser(lid) || base : base
}

type TcTokenIndexEntry = { token: Buffer; timestamp?: string }

async function readTcTokenIndexEntry(keys: SignalKeyStore): Promise<TcTokenIndexEntry | undefined> {
	const data = await keys.get('tctoken', [TC_TOKEN_INDEX_KEY])
	return data[TC_TOKEN_INDEX_KEY]
}

function parseTcTokenIndex(entry: TcTokenIndexEntry | undefined): string[] {
	if (!entry?.token?.length) return []
	try {
		const parsed = JSON.parse(Buffer.from(entry.token).toString())
		if (!Array.isArray(parsed)) return []
		return parsed.filter(
			(jid): jid is string => typeof jid === 'string' && jid.length > 0 && jid !== TC_TOKEN_INDEX_KEY
		)
	} catch {
		return []
	}
}

export async function readTcTokenIndex(keys: SignalKeyStore): Promise<string[]> {
	return parseTcTokenIndex(await readTcTokenIndexEntry(keys))
}

export async function readLastTcTokenPruneTs(keys: SignalKeyStore): Promise<number> {
	const timestamp = Number((await readTcTokenIndexEntry(keys))?.timestamp)
	return Number.isFinite(timestamp) && timestamp > 0 ? timestamp : 0
}

export async function buildMergedTcTokenIndexWrite(
	keys: SignalKeyStore,
	addedJids: Iterable<string>
): Promise<{ [TC_TOKEN_INDEX_KEY]: TcTokenIndexEntry }> {
	const entry = await readTcTokenIndexEntry(keys)
	const merged = new Set(parseTcTokenIndex(entry))
	for (const jid of addedJids) {
		if (jid && jid !== TC_TOKEN_INDEX_KEY) merged.add(jid)
	}

	return {
		[TC_TOKEN_INDEX_KEY]: buildTcTokenIndexEntry(merged, entry?.timestamp)
	}
}

export function buildTcTokenIndexEntry(jids: Iterable<string>, pruneTs?: string | number): TcTokenIndexEntry {
	return {
		token: Buffer.from(JSON.stringify([...jids])),
		...(pruneTs !== undefined ? { timestamp: String(pruneTs) } : {})
	}
}

export function isTcTokenExpired(timestamp: number | string | null | undefined): boolean {
	if (timestamp === null || timestamp === undefined) return true
	const ts = typeof timestamp === 'string' ? parseInt(timestamp) : timestamp
	if (isNaN(ts)) return true
	const currentBucket = Math.floor(Date.now() / 1000 / TC_TOKEN_BUCKET_DURATION)
	const cutoffTimestamp = (currentBucket - (TC_TOKEN_NUM_BUCKETS - 1)) * TC_TOKEN_BUCKET_DURATION
	return ts < cutoffTimestamp
}

export function shouldSendNewTcToken(senderTimestamp: number | undefined): boolean {
	if (senderTimestamp === undefined) return true
	const currentBucket = Math.floor(Date.now() / 1000 / TC_TOKEN_BUCKET_DURATION)
	const senderBucket = Math.floor(senderTimestamp / TC_TOKEN_BUCKET_DURATION)
	return currentBucket > senderBucket
}

type BuildParams = {
	jid: string
	baseContent?: BinaryNode[]
	keys: SignalKeyStore
	resolveLid?: LidResolver
}

export async function buildTcTokenFromJid({
	keys,
	jid,
	baseContent = [],
	resolveLid
}: BuildParams): Promise<BinaryNode[] | undefined> {
	try {
		const storageJid = resolveTcTokenStorageJid(jid, resolveLid)
		const entry = (await keys.get('tctoken', [storageJid]))[storageJid]
		const token = entry?.token

		if (!token?.length || isTcTokenExpired(entry?.timestamp)) {
			if (token?.length) {
				const cleared =
					entry.senderTimestamp !== undefined
						? { token: Buffer.alloc(0), senderTimestamp: entry.senderTimestamp }
						: null
				await keys.set({ tctoken: { [storageJid]: cleared } })
			}
			return baseContent.length ? baseContent : undefined
		}

		baseContent.push({ tag: 'tctoken', attrs: {}, content: token })
		return baseContent
	} catch {
		return baseContent.length ? baseContent : undefined
	}
}

type StoreParams = {
	result: BinaryNode
	fallbackJid: string
	keys: SignalKeyStore
	resolveLid?: LidResolver
	onNewJidStored?: (jid: string) => void
}

/** Persiste tctokens recebidos por IQ/notificação usando o remetente, nunca o JID do próprio device. */
export async function storeTcTokensFromIqResult({
	result,
	fallbackJid,
	keys,
	resolveLid,
	onNewJidStored
}: StoreParams): Promise<string[]> {
	const tokensNode = getBinaryNodeChild(result, 'tokens')
	if (!tokensNode) return []

	const storedJids: string[] = []
	for (const tokenNode of getBinaryNodeChildren(tokensNode, 'token')) {
		if (tokenNode.attrs.type !== 'trusted_contact' || !(tokenNode.content instanceof Uint8Array)) continue

		const storageJid = resolveTcTokenStorageJid(fallbackJid || tokenNode.attrs.jid, resolveLid)
		if (!isRegularUser(storageJid)) continue

		const incomingTs = tokenNode.attrs.t ? Number(tokenNode.attrs.t) : 0
		if (!incomingTs) continue

		const existingEntry = (await keys.get('tctoken', [storageJid]))[storageJid]
		const existingTs = existingEntry?.timestamp ? Number(existingEntry.timestamp) : 0
		if (existingTs > incomingTs) continue

		await keys.set({
			tctoken: {
				[storageJid]: {
					...existingEntry,
					token: Buffer.from(tokenNode.content),
					timestamp: tokenNode.attrs.t
				}
			}
		})
		onNewJidStored?.(storageJid)
		storedJids.push(storageJid)
	}

	return storedJids
}

function toSecondsTimestamp(value: Chat['tcTokenTimestamp']): number {
	if (value === undefined || value === null) return 0
	const timestamp = toNumber(value)
	if (!Number.isFinite(timestamp) || timestamp <= 0) return 0
	return timestamp > 1e12 ? Math.floor(timestamp / 1000) : timestamp
}

/** Importa somente os tctokens das conversas do history sync, em lotes limitados. */
export async function storeTcTokensFromHistorySync(
	chats: Chat[],
	keyStore: SignalKeyStore,
	logger?: ILogger,
	resolveLid?: LidResolver
): Promise<number> {
	const candidates: { storageJid: string; token: Buffer; ts: number; senderTs?: number }[] = []
	for (const chat of chats) {
		const timestamp = toSecondsTimestamp(chat.tcTokenTimestamp)
		if (!chat.tcToken?.length || timestamp <= 0 || !chat.id) continue

		candidates.push({
			storageJid: resolveTcTokenStorageJid(chat.lidJid || chat.id, resolveLid),
			token: Buffer.from(chat.tcToken),
			ts: timestamp,
			senderTs: chat.tcTokenSenderTimestamp
				? toSecondsTimestamp(chat.tcTokenSenderTimestamp)
				: undefined
		})
	}

	if (!candidates.length) return 0
	const storedJids = new Set<string>()

	try {
		for (let offset = 0; offset < candidates.length; offset += TC_TOKEN_HISTORY_BATCH_SIZE) {
			const batch = candidates.slice(offset, offset + TC_TOKEN_HISTORY_BATCH_SIZE)
			const existing = await keyStore.get(
				'tctoken',
				batch.map(candidate => candidate.storageJid)
			)
			const entries: Record<string, { token: Buffer; timestamp?: string; senderTimestamp?: number }> = {}

			for (const candidate of batch) {
				const current = existing[candidate.storageJid]
				const currentTs = current?.timestamp ? Number(current.timestamp) : 0
				if (currentTs >= candidate.ts) continue

				entries[candidate.storageJid] = {
					...current,
					token: candidate.token,
					timestamp: String(candidate.ts),
					...(candidate.senderTs !== undefined ? { senderTimestamp: candidate.senderTs } : {})
				}
			}

			if (Object.keys(entries).length) {
				await keyStore.set({ tctoken: entries })
				for (const jid of Object.keys(entries)) storedJids.add(jid)
			}
		}

		if (storedJids.size) {
			const index = await buildMergedTcTokenIndexWrite(keyStore, storedJids)
			await keyStore.set({ tctoken: index })
			logger?.info({ event: 'tc_tokens_history_sync_imported', count: storedJids.size }, 'tctokens importados do history sync')
		}
	} catch (err) {
		logger?.warn({ err }, 'falha ao importar tctokens do history sync')
	}

	return storedJids.size
}
