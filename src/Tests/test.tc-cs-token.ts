import { createHmac } from 'crypto'
import { EventEmitter } from 'events'
import { deflateSync } from 'zlib'
import { proto } from '../../WAProto'
import type { ChatMutation, SignalDataSet, SignalDataTypeMap, SignalKeyStore } from '../Types'
import {
	generateCsToken,
	handleNctSaltMutation,
	NCT_SALT_SYNC_INDEX,
	readNctSalt,
	storeNctSaltFromHistorySync
} from '../Utils/cs-token-utils'
import { downloadHistory } from '../Utils/history'
import processMessage from '../Utils/process-message'
import {
	isTcTokenExpired,
	resolveTcTokenStorageJid,
	storeTcTokensFromHistorySync,
	storeTcTokensFromIqResult
} from '../Utils/tc-token-utils'
import type { BinaryNode } from '../WABinary'

function makeFakeKeys() {
	const stores: Record<string, Record<string, any>> = {}
	const keys: SignalKeyStore = {
		get: async (type, ids) => {
			const result: Record<string, any> = {}
			for (const id of ids) {
				if (stores[type]?.[id]) result[id] = stores[type][id]
			}
			return result
		},
		set: async (data: SignalDataSet) => {
			for (const [type, entries] of Object.entries(data)) {
				stores[type] ||= {}
				for (const [id, value] of Object.entries(entries || {})) {
					if (value === null) delete stores[type][id]
					else stores[type][id] = value
				}
			}
		}
	} as SignalKeyStore
	return { keys, stores }
}

describe('lifecycle TC/CS token', () => {
	const now = () => Math.floor(Date.now() / 1000)
	const pn = '5511999999999@s.whatsapp.net'
	const lid = '12345678901234@lid'

	it('normaliza storage por LID e timestamps do history sync em ms', async () => {
		const { keys, stores } = makeFakeKeys()
		expect(resolveTcTokenStorageJid(`${pn.split('@')[0]}:3@s.whatsapp.net`, jid => jid === pn ? lid : undefined)).toBe(lid)

		await storeTcTokensFromHistorySync([
			{
				id: pn,
				lidJid: lid,
				tcToken: Buffer.from('tc'),
				tcTokenTimestamp: now() * 1000
			} as any
		], keys)

		const entry = stores.tctoken[lid] as SignalDataTypeMap['tctoken']
		expect(entry.timestamp).toBe(String(now()))
		expect(isTcTokenExpired(entry.timestamp)).toBe(false)
	})

	it('notificação usa o remetente como chave, não attrs.jid do próprio device', async () => {
		const { keys, stores } = makeFakeKeys()
		const node: BinaryNode = {
			tag: 'notification',
			attrs: { from: lid, type: 'privacy_token' },
			content: [{
				tag: 'tokens',
				attrs: {},
				content: [{
					tag: 'token',
					attrs: { jid: '5511888888888:2@s.whatsapp.net', t: String(now()), type: 'trusted_contact' },
					content: Buffer.from('peer-token')
				}]
			}]
		}

		await storeTcTokensFromIqResult({ result: node, fallbackJid: lid, keys })
		expect(stores.tctoken[lid].token).toEqual(Buffer.from('peer-token'))
		expect(stores.tctoken['5511888888888@s.whatsapp.net']).toBeUndefined()
	})

	it('cstoken é HMAC-SHA256 do salt com o LID', () => {
		const salt = Buffer.from('nct-salt')
		expect(generateCsToken(salt, lid)).toEqual(createHmac('sha256', salt).update(lid).digest())
		expect(generateCsToken(salt, pn)).toBeUndefined()
	})

	it('mutation nct_salt_sync grava e remove o salt', async () => {
		const { keys } = makeFakeKeys()
		const salt = Buffer.from('nct-salt')
		const mutation = (operation: proto.SyncdMutation.SyncdOperation, value?: Buffer): ChatMutation => ({
			index: [NCT_SALT_SYNC_INDEX],
			operation,
			syncAction: { value: value ? { nctSaltSyncAction: { salt: value } } : {} }
		})

		await handleNctSaltMutation({ mutation: mutation(proto.SyncdMutation.SyncdOperation.SET, salt), keys })
		expect(await readNctSalt(keys)).toEqual(salt)
		await handleNctSaltMutation({ mutation: mutation(proto.SyncdMutation.SyncdOperation.REMOVE), keys })
		expect(await readNctSalt(keys)).toBeUndefined()
	})

	it('protobuf preserva NCT salt no HistorySync e na SyncActionValue', async () => {
		const salt = Buffer.from([1, 2, 3])
		const history = proto.HistorySync.decode(proto.HistorySync.encode({ syncType: 0, nctSalt: salt }).finish())
		const action = proto.SyncActionValue.decode(proto.SyncActionValue.encode({ nctSaltSyncAction: { salt } }).finish())
		expect(Buffer.from(history.nctSalt!)).toEqual(salt)
		expect(Buffer.from(action.nctSaltSyncAction!.salt!)).toEqual(salt)
	})

	it('downloadHistory aceita bootstrap inline', async () => {
		const salt = Buffer.from('inline-salt')
		const encoded = proto.HistorySync.encode({ syncType: 0, nctSalt: salt }).finish()
		const decoded = await downloadHistory({ initialHistBootstrapInlinePayload: deflateSync(encoded) }, {})
		expect(Buffer.from(decoded.nctSalt!)).toEqual(salt)
	})

	it('process=false importa tctoken e salt sem emitir o histórico', async () => {
		const { keys, stores } = makeFakeKeys()
		const salt = Buffer.from('bootstrap-salt')
		const token = Buffer.from('trusted-contact-token')
		const encoded = proto.HistorySync.encode({
			syncType: 0,
			nctSalt: salt,
			conversations: [{ id: pn, lidJid: lid, tcToken: token, tcTokenTimestamp: now() }]
		}).finish()
		const ev = new EventEmitter()
		let historyEmitted = false
		ev.on('messaging-history.set', () => { historyEmitted = true })

		await processMessage({
			key: { id: 'history-token', remoteJid: pn, fromMe: true },
			messageTimestamp: 1,
			message: {
				protocolMessage: {
					type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
					historySyncNotification: { initialHistBootstrapInlinePayload: deflateSync(encoded) }
				}
			}
		}, {
			shouldProcessHistoryMsg: false,
			creds: { me: { id: '5511777777777@s.whatsapp.net' }, processedHistoryMessages: [], accountSettings: {} },
			keyStore: keys,
			ev,
			options: {}
		} as any)

		expect(await readNctSalt(keys)).toEqual(salt)
		expect(stores.tctoken[lid].token).toEqual(token)
		expect(historyEmitted).toBe(false)
	})
})
