import crypto from 'crypto'
import P from 'pino'
import makeWASocket, { fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'

// Probe do caminho canonical: mex WAWebMexCachedTokenJobMutation -> POST /auth/token/ -> wa_web_access_token
const logger = P({ level: 'silent' })

const b64 = (b: Buffer) => b.toString('base64')

const main = async () => {
	const { state } = await useMultiFileAuthState('baileys_auth_info')
	const { version } = await fetchLatestBaileysVersion()
	const sock = makeWASocket({ version, logger, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })

	sock.ev.on('connection.update', async ({ connection }) => {
		if (connection !== 'open') return
		try {
			const kp = await crypto.webcrypto.subtle.generateKey({ name: 'RSA-OAEP', modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-1' }, true, ['encrypt', 'decrypt'])
			const spki = Buffer.from(await crypto.webcrypto.subtle.exportKey('spki', kp.publicKey))
			const privateKey = kp.privateKey
			const pem = `-----BEGIN PUBLIC KEY-----\n${spki.toString('base64').match(/.{1,64}/g)!.join('\n')}\n-----END PUBLIC KEY-----\n`
			const clientPubKey = Buffer.from(pem, 'utf-8').toString('base64')
			const requestId = crypto.randomUUID()

			const payloadVars = { input: { client_pub_key: clientPubKey, request_id: requestId } }
			let res: any = null
			for (let i = 0; i < 5 && !res; i++) {
				const raw: any = await (sock as any).query({ tag: 'iq', attrs: { type: 'get', to: 's.whatsapp.net', xmlns: 'w:mex' }, content: [{ tag: 'query', attrs: { query_id: '27013462064904056' }, content: Buffer.from(JSON.stringify({ variables: payloadVars })) }] })
				const body = JSON.parse(raw.content?.find?.((n: any) => n.tag === 'result')?.content?.toString() || '{}')
				res = body.data?.xwa2_ent_trade_canonical_nonce_for_access_tokens
				console.log('TRY', i, res ? 'ok' : JSON.stringify(body.errors?.[0]?.extensions))
				if (!res) await new Promise(r => setTimeout(r, 3000 * (i + 1)))
			}
			console.log('TRY', JSON.stringify(res))
			console.log('MEX resp:', JSON.stringify(res)?.slice(0, 300), 'ERRS:', JSON.stringify(JSON.parse(raw.content?.find?.((n:any)=>n.tag==='result')?.content?.toString()||'{}').errors))

			const enc = res?.encrypted_access_tokens
			if (!enc) throw new Error('sem encrypted_access_tokens')
			const aesKey = Buffer.from(await crypto.webcrypto.subtle.decrypt({ name: 'RSA-OAEP' }, privateKey, Buffer.from(enc.key, 'base64')))
			const decipher = crypto.createDecipheriv('aes-256-gcm', aesKey, Buffer.from(enc.nonce, 'base64'))
			decipher.setAuthTag(Buffer.from(enc.tag, 'base64'))
			const payload = JSON.parse(Buffer.concat([decipher.update(Buffer.from(enc.data, 'base64')), decipher.final()]).toString('utf-8'))
			console.log('payload:', JSON.stringify(payload)?.slice(0, 200))

			const qs = new URLSearchParams({
				access_token: payload.access_token,
				nonce: payload.nonce,
				user_id: String(payload.fbid),
				device_id: String((sock as any).authState?.creds?.me?.id?.split(':')[1]?.replace('@c.us', '') ?? '')
			})
			const r = await fetch(`https://web.whatsapp.com/auth/token/?${qs}`, { method: 'POST', headers: { 'sec-fetch-site': 'same-origin' } })
			console.log('auth/token:', r.status, [...r.headers].filter(([k]) => /cookie/i.test(k)))
		} catch (e: any) {
			console.log("ERRO:", e.message, JSON.stringify(e.data)?.slice(0,600))
		}
		process.exit(0)
	})
}

main()
