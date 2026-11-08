import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
	buildMetaAiPrompt,
	decryptBotChunk,
	deriveBotChunkKey,
	genBotMsgSecret,
	META_AI_BOT_JID,
	parseBotChunkNode
} from '../src/Utils/meta-ai.ts'

// vetores gerados com o zapo-js 1.10 (message/kinds/bot.js) sobre as mesmas entradas
const secret = Buffer.alloc(32, 7)
const iv = Buffer.alloc(12, 3)
const chunk = {
	parentMessageSecret: secret,
	saltId: 'PROMPT1',
	targetSenderJid: '111@lid',
	authorJid: '867051314767696@bot',
	encIv: iv,
	encPayload: Buffer.from('cc8eebb0c06ed5e13c69eed0ff952b86fe9e0537', 'hex')
}

test('chave do chunk igual à do zapo', () => {
	const botSecret = genBotMsgSecret(secret)
	assert.equal(botSecret.toString('hex'), '8bd322ad7f055561048f1be6e769b8cdf54f8d555d9b20c8f828138694a9970b')
	assert.equal(
		deriveBotChunkKey(botSecret, 'PROMPT1', '111@lid', '867051314767696@bot').toString('hex'),
		'38f4c8726ffa624b317bb1b75684478de1186599466044b0badad7eb9c437aca'
	)
})

test('decifra o chunk cifrado pelo zapo', () => {
	assert.equal(decryptBotChunk(chunk).toString('hex'), '0a026f69')
})

test('salt ou autor errado falha na tag do GCM', () => {
	assert.throws(() => decryptBotChunk({ ...chunk, saltId: 'OUTRO' }))
	assert.throws(() => decryptBotChunk({ ...chunk, authorJid: '1@bot' }))
})

test('prompt leva secret, thread de IA e capacidades', () => {
	const { message, aiThreadId } = buildMetaAiPrompt({ conversation: 'oi' }, { messageSecret: secret, aiThreadId: 't1' })
	assert.equal(aiThreadId, 't1')
	assert.equal(message.conversation, 'oi')
	const ctx = message.messageContextInfo
	assert.equal(ctx.messageSecret, secret)
	assert.deepEqual(ctx.threadId, [{ threadType: 2, threadKey: { remoteJid: META_AI_BOT_JID, fromMe: true, id: 't1' } }])
	assert.deepEqual(ctx.botMetadata.botThreadInfo, { clientInfo: { type: 1 } })
	assert.ok(ctx.botMetadata.capabilityMetadata.capabilities.length > 0)
})

const node = (botAttrs, meta) => ({
	tag: 'message',
	attrs: { id: 'CHUNK1', from: '867051314767696@bot' },
	content: [
		{ tag: 'enc', attrs: { type: 'msmsg' }, content: new Uint8Array([1]) },
		{ tag: 'bot', attrs: botAttrs },
		...(meta ? [{ tag: 'meta', attrs: meta }] : [])
	]
})

test('first usa o id da stanza como salt e cai no meu LID', () => {
	const parsed = parseBotChunkNode(node({ edit: 'first' }, { target_id: 'P1' }), '111:5@lid', '55:5@s.whatsapp.net')
	assert.equal(parsed.saltId, 'CHUNK1')
	assert.equal(parsed.targetSenderJid, '111@lid')
	assert.equal(parsed.authorJid, '867051314767696@bot')
	assert.equal(parsed.targetMessageId, 'P1')
})

test('inner e last usam edit_target_id e respeitam target_sender_jid', () => {
	const parsed = parseBotChunkNode(
		node({ edit: 'last', edit_target_id: 'CHUNK0' }, { target_id: 'P1', target_sender_jid: '222:3@lid' }),
		'111@lid',
		undefined
	)
	assert.equal(parsed.editType, 'last')
	assert.equal(parsed.saltId, 'CHUNK0')
	assert.equal(parsed.targetSenderJid, '222@lid')
})

test('inner sem edit_target_id e stanza sem <bot> são descartados', () => {
	assert.equal(parseBotChunkNode(node({ edit: 'inner' }), '111@lid', undefined), undefined)
	const semBot = node({})
	semBot.content = semBot.content.filter(child => child.tag !== 'bot')
	assert.equal(parseBotChunkNode(semBot, '111@lid', undefined), undefined)
})

test('full usa o id da stanza; bot legado por PN cai no meu PN', () => {
	const legado = node({ edit: 'full' }, { target_id: 'P1' })
	legado.attrs.from = '13135550002@s.whatsapp.net'
	const parsed = parseBotChunkNode(legado, '111@lid', '55:5@s.whatsapp.net')
	assert.equal(parsed.editType, 'full')
	assert.equal(parsed.saltId, 'CHUNK1')
	assert.equal(parsed.targetSenderJid, '55@s.whatsapp.net')
})
