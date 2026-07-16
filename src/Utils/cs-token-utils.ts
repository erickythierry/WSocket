import { createHmac } from 'crypto'
import { proto } from '../../WAProto'
import type { ChatMutation, SignalKeyStore } from '../Types'
import { isLidUser } from '../WABinary'
import type { ILogger } from './logger'
import { isRegularUser } from './tc-token-utils'

export const NCT_SALT_SYNC_INDEX = 'nct_salt_sync'
export const NCT_SALT_KEY = 'salt'

/** Deriva HMAC-SHA256(nctSalt, recipientLID), igual ao cliente de referência. */
export function generateCsToken(salt: Buffer, recipientLid: string): Buffer | undefined {
	if (!salt?.length || !isLidUser(recipientLid) || !isRegularUser(recipientLid)) return undefined
	return createHmac('sha256', salt).update(recipientLid).digest()
}

export async function readNctSalt(keys: SignalKeyStore): Promise<Buffer | undefined> {
	const salt = (await keys.get('nct-salt', [NCT_SALT_KEY]))[NCT_SALT_KEY]?.salt
	return salt?.length ? Buffer.from(salt) : undefined
}

async function persistNctSalt(keys: SignalKeyStore, salt: Uint8Array): Promise<void> {
	const value = Buffer.from(salt)
	await keys.set({ 'nct-salt': { [NCT_SALT_KEY]: { salt: value } } })
	const persisted = await readNctSalt(keys)
	if (!persisted?.equals(value)) throw new Error('nct salt não sobreviveu ao keystore')
}

export async function storeNctSaltFromHistorySync(
	historySync: proto.IHistorySync,
	keys: SignalKeyStore,
	logger?: ILogger
): Promise<boolean> {
	const salt = historySync.nctSalt
	logger?.debug(
		{ event: 'nct_salt_history_sync_received', syncType: historySync.syncType, saltPresent: !!salt?.length },
		'nct salt inspecionado no history sync'
	)
	if (!salt?.length) return false

	await persistNctSalt(keys, salt)
	logger?.info({ event: 'nct_salt_stored', source: 'history_sync' }, 'nct salt armazenado')
	return true
}

export async function handleNctSaltMutation({
	mutation,
	keys,
	logger
}: {
	mutation: ChatMutation
	keys: SignalKeyStore
	logger?: ILogger
}): Promise<boolean> {
	if (mutation.index?.[0] !== NCT_SALT_SYNC_INDEX) return false

	const isRemove = mutation.operation === proto.SyncdMutation.SyncdOperation.REMOVE
	const salt = mutation.syncAction?.value?.nctSaltSyncAction?.salt
	logger?.debug(
		{ event: 'nct_salt_mutation_received', operation: isRemove ? 'remove' : 'set', saltPresent: !!salt?.length },
		'nct salt mutation recebida'
	)

	if (isRemove) {
		await keys.set({ 'nct-salt': { [NCT_SALT_KEY]: null } })
		logger?.info({ event: 'nct_salt_removed' }, 'nct salt removido')
		return true
	}

	if (!salt?.length) {
		logger?.warn({ event: 'nct_salt_mutation_empty' }, 'mutation nct_salt_sync recebida sem salt')
		return true
	}

	await persistNctSalt(keys, salt)
	logger?.info({ event: 'nct_salt_stored', source: 'app_state' }, 'nct salt armazenado')
	return true
}
