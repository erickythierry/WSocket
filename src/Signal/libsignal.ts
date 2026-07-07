import * as libsignal from 'libsignal'
import { SignalAuthState } from '../Types'
import { SignalRepository } from '../Types/Signal'
import { generateSignalPubKey } from '../Utils'
import { isLidUser, jidDecode } from '../WABinary'
import type { SenderKeyStore } from './Group/group_cipher'
import { SenderKeyName } from './Group/sender-key-name'
import { SenderKeyRecord } from './Group/sender-key-record'
import { GroupCipher, GroupSessionBuilder, SenderKeyDistributionMessage } from './Group'

export function makeLibSignalRepository(auth: SignalAuthState): SignalRepository {
	const storage: SenderKeyStore = signalStorage(auth)

	// Sessões antigas ficam salvas sob o endereço do número (PN), mas o WhatsApp
	// passou a endereçar mensagens por LID. Sem migrar, o decrypt procura a sessão
	// no endereço LID, não acha nada e estoura "No matching sessions found".
	// Copia o record serializado PN -> LID na primeira mensagem LID daquele device.
	const migrateSessionIfNeeded = async (lidJid: string, pnJid: string) => {
		const pnUser = jidDecode(pnJid)?.user
		if (!pnUser) {
			return
		}

		const lidAddr = jidToSignalProtocolAddress(lidJid).toString()
		const { [lidAddr]: lidSess } = await auth.keys.get('session', [lidAddr])
		if (lidSess) {
			return
		}

		// mesmo device id nos dois endereçamentos
		const device = jidDecode(lidJid)?.device || 0
		const pnAddr = new libsignal.ProtocolAddress(pnUser, device).toString()
		const { [pnAddr]: pnSess } = await auth.keys.get('session', [pnAddr])
		if (pnSess) {
			await auth.keys.set({ session: { [lidAddr]: pnSess } })
		}
	}

	return {
		decryptGroupMessage({ group, authorJid, msg }) {
			const senderName = jidToSignalSenderKeyName(group, authorJid)
			const cipher = new GroupCipher(storage, senderName)

			return cipher.decrypt(msg)
		},
		async processSenderKeyDistributionMessage({ item, authorJid }) {
			const builder = new GroupSessionBuilder(storage)
			if (!item.groupId) {
				throw new Error('Group ID is required for sender key distribution message')
			}

			const senderName = jidToSignalSenderKeyName(item.groupId, authorJid)

			const senderMsg = new SenderKeyDistributionMessage(
				null,
				null,
				null,
				null,
				item.axolotlSenderKeyDistributionMessage
			)
			const senderNameStr = senderName.toString()
			const { [senderNameStr]: senderKey } = await auth.keys.get('sender-key', [senderNameStr])
			if (!senderKey) {
				await storage.storeSenderKey(senderName, new SenderKeyRecord())
			}

			await builder.process(senderName, senderMsg)
		},
		async decryptMessage({ jid, type, ciphertext, pnJid }) {
			if (pnJid && isLidUser(jid)) {
				await migrateSessionIfNeeded(jid, pnJid)
			}

			const addr = jidToSignalProtocolAddress(jid)
			const session = new libsignal.SessionCipher(storage as any, addr)
			let result: Buffer
			const ciphertextBuf = Buffer.from(ciphertext)
			switch (type) {
				case 'pkmsg':
					result = await session.decryptPreKeyWhisperMessage(ciphertextBuf)
					break
				case 'msg':
					result = await session.decryptWhisperMessage(ciphertextBuf)
					break
				default:
					throw new Error(`Unknown message type: ${type}`)
			}

			return result
		},
		async encryptMessage({ jid, data }) {
			const addr = jidToSignalProtocolAddress(jid)
			const cipher = new libsignal.SessionCipher(storage as any, addr)

			const { type: sigType, body } = await cipher.encrypt(Buffer.from(data))
			const type = sigType === 3 ? 'pkmsg' : 'msg'
			return { type, ciphertext: Buffer.from(body) }
		},
		async encryptGroupMessage({ group, meId, data, forceRotate }) {
			const senderName = jidToSignalSenderKeyName(group, meId)
			const builder = new GroupSessionBuilder(storage)

			const senderNameStr = senderName.toString()
			const { [senderNameStr]: senderKey } = await auth.keys.get('sender-key', [senderNameStr])
			// forceRotate: sobrescreve com record vazio => create() gera keyId/seed novos => quem foi
			// excluido do <participants> nao recebe o novo SKDM e nao decifra (forward secrecy protege
			// tambem os envios ocultos anteriores).
			if (!senderKey || forceRotate) {
				await storage.storeSenderKey(senderName, new SenderKeyRecord())
			}

			const senderKeyDistributionMessage = await builder.create(senderName)
			const session = new GroupCipher(storage, senderName)
			const ciphertext = await session.encrypt(data)

			return {
				ciphertext,
				senderKeyDistributionMessage: senderKeyDistributionMessage.serialize()
			}
		},
		async injectE2ESession({ jid, session }) {
			const cipher = new libsignal.SessionBuilder(storage as any, jidToSignalProtocolAddress(jid))
			await cipher.initOutgoing({
				...session,
				identityKey: Buffer.from(session.identityKey),
				signedPreKey: {
					...session.signedPreKey,
					publicKey: Buffer.from(session.signedPreKey.publicKey),
					signature: Buffer.from(session.signedPreKey.signature)
				},
				preKey: session.preKey
					? { ...session.preKey, publicKey: Buffer.from(session.preKey.publicKey) }
					: undefined
			})
		},
		jidToSignalProtocolAddress(jid) {
			return jidToSignalProtocolAddress(jid).toString()
		}
	}
}

const jidToSignalProtocolAddress = (jid: string) => {
	const { user, device } = jidDecode(jid)!
	return new libsignal.ProtocolAddress(user, device || 0)
}

const jidToSignalSenderKeyName = (group: string, user: string): SenderKeyName => {
	return new SenderKeyName(group, jidToSignalProtocolAddress(user))
}

function signalStorage({ creds, keys }: SignalAuthState): SenderKeyStore & Record<string, any> {
	return {
		loadSession: async (id: string) => {
			const { [id]: sess } = await keys.get('session', [id])
			if (sess) {
				return libsignal.SessionRecord.deserialize(sess)
			}
		},
		storeSession: async (id: string, session: InstanceType<typeof libsignal.SessionRecord>) => {
			await keys.set({ session: { [id]: session.serialize() } })
		},
		isTrustedIdentity: () => {
			return true
		},
		loadPreKey: async (id: number | string) => {
			const keyId = id.toString()
			const { [keyId]: key } = await keys.get('pre-key', [keyId])
			if (key) {
				return {
					privKey: Buffer.from(key.private),
					pubKey: Buffer.from(key.public)
				}
			}
		},
		removePreKey: (id: number) => keys.set({ 'pre-key': { [id]: null } }),
		loadSignedPreKey: () => {
			const key = creds.signedPreKey
			return {
				privKey: Buffer.from(key.keyPair.private),
				pubKey: Buffer.from(key.keyPair.public)
			}
		},
		loadSenderKey: async (senderKeyName: SenderKeyName) => {
			const keyId = senderKeyName.toString()
			const { [keyId]: key } = await keys.get('sender-key', [keyId])
			if (key) {
				return SenderKeyRecord.deserialize(key)
			}

			return new SenderKeyRecord()
		},
		storeSenderKey: async (senderKeyName: SenderKeyName, key: SenderKeyRecord) => {
			const keyId = senderKeyName.toString()
			const serialized = JSON.stringify(key.serialize())
			await keys.set({ 'sender-key': { [keyId]: Buffer.from(serialized, 'utf-8') } })
		},
		getOurRegistrationId: () => creds.registrationId,
		getOurIdentity: () => {
			const { signedIdentityKey } = creds
			return {
				privKey: Buffer.from(signedIdentityKey.private),
				pubKey: generateSignalPubKey(signedIdentityKey.public)
			}
		}
	}
}
