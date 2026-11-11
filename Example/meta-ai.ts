import readline from 'readline'
import P from 'pino'
// @ts-ignore sem tipos
import qrcode from 'qrcode-terminal'
import makeWASocket, { DisconnectReason, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, proto, useMultiFileAuthState } from '../src'

// Teste manual da Meta AI: digite um prompt, a resposta sai no terminal. "/reset" apaga a sessão do bot, "/nova" abre outra thread.
const logger = P({ level: 'debug' }, P.destination('./meta-ai-logs.txt'))
const rl = readline.createInterface({ input: process.stdin, output: process.stdout })

// a resposta chega aninhada (edit, forwarded, futureProof): procura o primeiro texto em qualquer nível
const extractText = (msg: unknown): string | undefined => {
	if (!msg || typeof msg !== 'object') return
	const m = msg as Record<string, any>
	const subs = m.richResponseMessage?.submessages as proto.AIRichResponseMessage.IAIRichResponseSubMessage[] | undefined
	if (subs?.length) {
		return subs
			.map(s => s.messageText ?? (s.codeMetadata ? JSON.stringify(s.codeMetadata) : s.tableMetadata ? JSON.stringify(s.tableMetadata) : ''))
			.join('\n')
	}
	if (typeof m.conversation === 'string' && m.conversation) return m.conversation
	if (typeof m.text === 'string' && m.text) return m.text
	for (const value of Object.values(m)) {
		const found = extractText(value)
		if (found) return found
	}
}

const start = async () => {
	const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info')
	const { version } = await fetchLatestBaileysVersion()
	const sock = makeWASocket({
		version,
		logger,
		auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) }
	})

	let aiThreadId: string | undefined
	let ready = false

	sock.ev.on('creds.update', saveCreds)

	sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
		if (qr) qrcode.generate(qr, { small: true })
		if (connection === 'open' && !ready) {
			ready = true
			console.log('conectado. digite o prompt (/reset, /nova, /sair):')
			rl.prompt()
		}
		if (connection === 'close') {
			const code = (lastDisconnect?.error as any)?.output?.statusCode
			if (code === DisconnectReason.loggedOut) {
				console.log('deslogado. apague baileys_auth_info e rode de novo')
				process.exit(1)
			}
			ready = false
			start()
		}
	})

	sock.ev.on('messages.bot-chunk', chunk => {
		const text = extractText(chunk.message)
		if (chunk.editType === 'last' || chunk.editType === 'full') {
			console.log(`\n[meta ai] ${text ?? JSON.stringify(chunk.message, null, 2)}\n`)
			rl.prompt()
		} else {
			process.stdout.write(`\r[${chunk.editType}] ${(text ?? '').slice(-80).replace(/\n/g, ' ')}`)
		}
	})

	// resposta que não veio como chunk msmsg (ex.: mensagem normal do @bot)
	sock.ev.on('messages.upsert', ({ messages }) => {
		for (const m of messages) {
			if (!m.key.fromMe && m.key.remoteJid?.endsWith('@bot')) {
				console.log(`\n[upsert @bot] ${extractText(m.message) ?? JSON.stringify(m.message)}\n`)
				rl.prompt()
			}
		}
	})

	rl.removeAllListeners('line')
	rl.on('line', async line => {
		const input = line.trim()
		if (!input) return rl.prompt()
		try {
			if (input === '/sair') process.exit(0)
			if (input === '/nova') {
				aiThreadId = undefined
				console.log('thread nova no próximo prompt')
				return rl.prompt()
			}
			if (input === '/reset') {
				await sock.resetMetaAiSession()
				console.log('sessão do bot apagada')
				return rl.prompt()
			}
			const sent = await sock.sendMetaAiPrompt(input, { aiThreadId })
			aiThreadId = sent.aiThreadId
			console.log(`enviado ${sent.key.id} (thread ${aiThreadId}), aguardando...`)
		} catch (err) {
			console.error('erro:', err)
			rl.prompt()
		}
	})
}

start()
