import { createDecipheriv, hkdfSync, randomUUID } from 'crypto'
import type { proto } from '../../WAProto'
import type { BinaryNode } from '../WABinary'

// Protocolo portado do zapo-js 1.10 (WaBotCoordinator e message/kinds/bot)

export const META_AI_BOT_JID = '867051314767696@bot'

export type MetaAiChunkEditType = 'first' | 'inner' | 'last' | 'full'

const GCM_TAG_LENGTH = 16
const BOT_SECRET_BYTES = 32

/** sem um conjunto de capacidades a Meta AI pode recusar o prompt em silêncio */
const DEFAULT_CAPABILITIES = [2, 3, 6, 7, 8, 21, 24, 26, 31, 34, 37, 39, 41, 42, 43]

const AI_THREAD = 2
const AI_THREAD_DEFAULT = 1

const hkdf = (ikm: Uint8Array, info: string) => Buffer.from(hkdfSync('sha256', ikm, new Uint8Array(0), info, BOT_SECRET_BYTES))

export const genBotMsgSecret = (messageSecret: Uint8Array) => hkdf(messageSecret, 'Bot Message')

export const deriveBotChunkKey = (botMsgSecret: Uint8Array, saltId: string, targetSenderJid: string, authorJid: string) =>
	hkdf(botMsgSecret, saltId + targetSenderJid + authorJid)

/** o chunk não tem padding PKCS7: o texto decifrado já é o proto Message */
export const decryptBotChunk = (input: {
	parentMessageSecret: Uint8Array
	saltId: string
	targetSenderJid: string
	authorJid: string
	encIv: Uint8Array
	encPayload: Uint8Array
}) => {
	const key = deriveBotChunkKey(
		genBotMsgSecret(input.parentMessageSecret),
		input.saltId,
		input.targetSenderJid,
		input.authorJid
	)
	const decipher = createDecipheriv('aes-256-gcm', key, input.encIv)
	decipher.setAAD(Buffer.from(`${input.saltId}\u0000${input.authorJid}`))
	decipher.setAuthTag(input.encPayload.subarray(input.encPayload.length - GCM_TAG_LENGTH))
	return Buffer.concat([decipher.update(input.encPayload.subarray(0, input.encPayload.length - GCM_TAG_LENGTH)), decipher.final()])
}

/** metadados e thread que a Meta AI exige no prompt direto para `@bot` */
export const buildMetaAiPrompt = (
	message: proto.IMessage,
	{ aiThreadId = randomUUID(), messageSecret }: { aiThreadId?: string; messageSecret: Uint8Array }
): { message: proto.IMessage; aiThreadId: string } => ({
	aiThreadId,
	message: {
		...message,
		messageContextInfo: {
			...message.messageContextInfo,
			messageSecret,
			threadId: [
				{ threadType: AI_THREAD, threadKey: { remoteJid: META_AI_BOT_JID, fromMe: true, id: aiThreadId } }
			],
			botMetadata: {
				...message.messageContextInfo?.botMetadata,
				capabilityMetadata: { capabilities: DEFAULT_CAPABILITIES },
				botThreadInfo: { clientInfo: { type: AI_THREAD_DEFAULT } }
			}
		}
	}
})

const EDIT_TYPES: MetaAiChunkEditType[] = ['first', 'inner', 'last', 'full']

/**
 * Lê da stanza do chunk (`<enc type="msmsg">` + `<bot>` + `<meta>`) o que a decifração precisa.
 * Devolve undefined se não for chunk de bot.
 */
export const parseBotChunkNode = (node: BinaryNode, meLid: string | undefined, meId: string | undefined) => {
	const children = Array.isArray(node.content) ? (node.content as BinaryNode[]) : []
	const enc = children.find(child => child.tag === 'enc' && child.attrs.type === 'msmsg')
	const bot = children.find(child => child.tag === 'bot')
	if (!enc || !bot || !(enc.content instanceof Uint8Array)) {
		return
	}

	const meta = children.find(child => child.tag === 'meta')
	const editType = EDIT_TYPES.find(type => type === bot.attrs.edit) ?? 'full'
	const editTargetId = bot.attrs.edit_target_id || undefined
	const saltId = editType === 'inner' || editType === 'last' ? editTargetId : node.attrs.id
	const authorJid = toUserJid(node.attrs.participant || node.attrs.from)
	// bot FBID (`@bot`) deriva a chave do meu LID; o bot legado por PN, do meu PN
	const fallbackMe = node.attrs.from?.endsWith('@bot') ? meLid || meId : meId
	const targetSenderJid = toUserJid(meta?.attrs.target_sender_jid || fallbackMe)
	if (!saltId || !authorJid || !targetSenderJid) {
		return
	}

	return {
		payload: enc.content,
		targetMessageId: meta?.attrs.target_id,
		editType,
		editTargetId,
		saltId,
		authorJid,
		targetSenderJid
	}
}

const toUserJid = (jid: string | undefined) => {
	if (!jid?.includes('@')) {
		return undefined
	}

	const [user, server] = jid.split('@')
	return `${user.split(':')[0]}@${server}`
}
