import { proto } from '../../WAProto'

type DecryptGroupSignalOpts = {
	group: string
	authorJid: string
	msg: Uint8Array
}

type ProcessSenderKeyDistributionMessageOpts = {
	item: proto.Message.ISenderKeyDistributionMessage
	authorJid: string
}

type DecryptSignalProtoOpts = {
	jid: string
	type: 'pkmsg' | 'msg'
	ciphertext: Uint8Array
	/** JID do número (PN) equivalente, quando `jid` é um @lid — usado pra migrar sessão PN -> LID */
	pnJid?: string
}

type EncryptMessageOpts = {
	jid: string
	data: Uint8Array
}

type EncryptGroupMessageOpts = {
	group: string
	data: Uint8Array
	meId: string
	/**
	 * POC exclude-relay: forca rotacao da sender-key (record esvaziado antes do create) para que
	 * quem foi removido do <participants> nao consiga decifrar o skmsg com uma chave antiga que ja possua.
	 */
	forceRotate?: boolean
}

type PreKey = {
	keyId: number
	publicKey: Uint8Array
}

type SignedPreKey = PreKey & {
	signature: Uint8Array
}

type E2ESession = {
	registrationId: number
	identityKey: Uint8Array
	signedPreKey: SignedPreKey
	preKey: PreKey
}

type E2ESessionOpts = {
	jid: string
	session: E2ESession
}

export type SignalRepository = {
	decryptGroupMessage(opts: DecryptGroupSignalOpts): Promise<Uint8Array>
	processSenderKeyDistributionMessage(opts: ProcessSenderKeyDistributionMessageOpts): Promise<void>
	decryptMessage(opts: DecryptSignalProtoOpts): Promise<Uint8Array>
	encryptMessage(opts: EncryptMessageOpts): Promise<{
		type: 'pkmsg' | 'msg'
		ciphertext: Uint8Array
	}>
	encryptGroupMessage(opts: EncryptGroupMessageOpts): Promise<{
		senderKeyDistributionMessage: Uint8Array
		ciphertext: Uint8Array
	}>
	injectE2ESession(opts: E2ESessionOpts): Promise<void>
	jidToSignalProtocolAddress(jid: string): string
}
