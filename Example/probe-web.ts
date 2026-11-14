import crypto from 'crypto'
import P from 'pino'
import qrcode from 'qrcode-terminal'
import makeWASocket, { Browsers, DisconnectReason, jidEncode, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'

// Pareia um companion web e roda a cadeia canonical (mex -> decrypt -> /auth/token/).
// Uso: apague baileys_auth_web se o device foi removido, rode e escaneie o QR.
const logger = P({ level: process.env.DBG ? 'debug' : 'silent' })

const chain = async (sock: any) => {
  const kp = await crypto.webcrypto.subtle.generateKey(
    { name: 'RSA-OAEP', modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-1' },
    true,
    ['encrypt', 'decrypt']
  )
  const spki = Buffer.from(await crypto.webcrypto.subtle.exportKey('spki', kp.publicKey))
  const pem = `-----BEGIN PUBLIC KEY-----\n${spki.toString('base64').match(/.{1,64}/g)!.join('\n')}\n-----END PUBLIC KEY-----\n`
  const vars = { input: { client_pub_key: Buffer.from(pem, 'utf-8').toString('base64'), request_id: crypto.randomUUID() } }

  let body: any = {}
  for (let i = 0; i < Number(process.env.TRIES || 4); i++) {
    const raw: any = await sock.query(
      {
        tag: 'iq',
        attrs: { type: 'get', to: jidEncode(null, 's.whatsapp.net') as any, xmlns: 'w:mex', id: `${Math.floor(Math.random()*99999)}.${Math.floor(Math.random()*99999)}-${Math.floor(Math.random()*99)}` },
        content: [{ tag: 'query', attrs: { query_id: '27013462064904056' }, content: Buffer.from(JSON.stringify({ variables: { input: { client_pub_key: vars.input.client_pub_key, request_id: crypto.randomUUID() } } })) }]
      },
      20000
    )
    body = JSON.parse(raw.content?.find?.((n: any) => n.tag === 'result')?.content?.toString() || '{}')
    console.log('try', i, JSON.stringify(body).slice(0, 120))
    if (body.data?.xwa2_ent_trade_canonical_nonce_for_access_tokens) break
    await new Promise(r => setTimeout(r, 1500))
  }
  console.log('mex:', JSON.stringify(body).slice(0, 300))

  const enc = body.data?.xwa2_ent_trade_canonical_nonce_for_access_tokens?.encrypted_access_tokens
  if (!enc) return

  const aes = Buffer.from(await crypto.webcrypto.subtle.decrypt({ name: 'RSA-OAEP' }, kp.privateKey, Buffer.from(enc.key, 'base64')))
  const d = crypto.createDecipheriv('aes-256-gcm', aes, Buffer.from(enc.nonce, 'base64'))
  d.setAuthTag(Buffer.from(enc.tag, 'base64'))
  const outer = JSON.parse(Buffer.concat([d.update(Buffer.from(enc.data, 'base64')), d.final()]).toString())
  const p = typeof outer.data === 'string' ? JSON.parse(outer.data) : outer
  console.log('payload:', JSON.stringify(p).slice(0, 300))

  const dev = String(sock.authState?.creds?.me?.id || '').split(':')[1]?.split('@')[0]
  for (const D of [dev, '10']) {
    const qs = new URLSearchParams({ access_token: p.access_token, nonce: p.nonce, user_id: String(p.fbid), device_id: D })
    const r = await fetch(`https://web.whatsapp.com/auth/token/?${qs}`, { method: 'POST', headers: { 'sec-fetch-site': 'same-origin' } })
    console.log('device', D, '->', r.status, 'set-cookie:', r.headers.get('set-cookie')?.slice(0, 60), 'ct:', r.headers.get('content-type'))
  }
}

const main = async () => {
  const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_web')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, browser: Browsers.macOS('Safari'), auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })

  sock.ev.on('creds.update', saveCreds)

  let done = false
  sock.ev.on('connection.update', async ({ connection, qr, lastDisconnect }) => {
    if (qr) qrcode.generate(qr, { small: true })

    if (connection === 'close') {
      const code = (lastDisconnect?.error as any)?.output?.statusCode
      console.log('close. code=', code)
      if (code === DisconnectReason.loggedOut) {
        console.log('device removido/deslogado. apague baileys_auth_web e pareie de novo')
        process.exit(1)
      }
      // 515 (restart required) e quedas normais: reconecta com o creds salvo
      await main()
      return
    }

    if (connection !== 'open' || done) return
    done = true
    console.log('conectado. me=', sock.authState?.creds?.me?.id, 'platform=', sock.authState?.creds?.platform)
    try {
      await chain(sock)
    } catch (e: any) {
      console.log('ERRO', e.message)
    }
    console.log('fim do teste')
    process.exit(0)
  })
}

main()
