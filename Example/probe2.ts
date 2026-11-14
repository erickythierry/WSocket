import crypto from 'crypto'
import P from 'pino'
import makeWASocket, { Browsers, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'
const logger = P({ level: 'silent' })
const b64 = (b: Buffer) => b.toString('base64')
const main = async () => {
  const { state } = await useMultiFileAuthState('baileys_auth_info')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, browser: Browsers.ubuntu('Chrome'), auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })
  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection !== 'open') return
    console.log('open. platform=', (sock as any).authState?.creds?.platform)
    try {
      const kp = await crypto.webcrypto.subtle.generateKey({ name: 'RSA-OAEP', modulusLength: 2048, publicExponent: new Uint8Array([1,0,1]), hash: 'SHA-1' }, true, ['encrypt','decrypt'])
      const spki = Buffer.from(await crypto.webcrypto.subtle.exportKey('spki', kp.publicKey))
      const pem = `-----BEGIN PUBLIC KEY-----\n${spki.toString('base64').match(/.{1,64}/g)!.join('\n')}\n-----END PUBLIC KEY-----\n`
      const vars = { input: { client_pub_key: Buffer.from(pem,'utf-8').toString('base64'), request_id: crypto.randomUUID() } }
      const raw: any = await (sock as any).query({ tag:'iq', attrs:{type:'get',to:'s.whatsapp.net',xmlns:'w:mex'}, content:[{tag:'query',attrs:{query_id:'27013462064904056'},content:Buffer.from(JSON.stringify({variables:vars}))}] }, 20000)
      const body = JSON.parse(raw.content?.find?.((n:any)=>n.tag==='result')?.content?.toString()||'{}')
      console.log('mex:', JSON.stringify(body).slice(0,220))
      const enc = body.data?.xwa2_ent_trade_canonical_nonce_for_access_tokens?.encrypted_access_tokens
      if (!enc) return process.exit(0)
      const aes = Buffer.from(await crypto.webcrypto.subtle.decrypt({name:'RSA-OAEP'}, kp.privateKey, Buffer.from(enc.key,'base64')))
      const d = crypto.createDecipheriv('aes-256-gcm', aes, Buffer.from(enc.nonce,'base64')); d.setAuthTag(Buffer.from(enc.tag,'base64'))
      const p = JSON.parse(Buffer.concat([d.update(Buffer.from(enc.data,'base64')), d.final()]).toString())
      console.log('payload ok. fbid=', p.fbid)
      const dev = String((sock as any).authState?.creds?.me?.id || '').split(':')[1]?.replace('@c.us','')
      for (const D of [dev, '10']) {
        const qs = new URLSearchParams({ access_token: p.access_token, nonce: p.nonce, user_id: String(p.fbid), device_id: D })
        const r = await fetch(`https://web.whatsapp.com/auth/token/?${qs}`, { method:'POST', headers:{'sec-fetch-site':'same-origin'} })
        console.log('device', D, '->', r.status, [...r.headers].filter(([k])=>k.toLowerCase()==='set-cookie').map(([,v])=>v.slice(0,40)))
      }
    } catch (e:any) { console.log('ERRO', e.message) }
    process.exit(0)
  })
}
main()
