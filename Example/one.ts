import P from 'pino'
import makeWASocket, { fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'
const logger = P({ level: 'debug' }, P.destination('/tmp/one.log'))
const JID = process.env.JID || '1475811245607728@bot'
const main = async () => {
  const { state } = await useMultiFileAuthState('baileys_auth_web')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })
  let out = ''
  sock.ev.on('messages.upsert', ({ messages }: any) => { for (const m of messages) { const t = m.message?.extendedTextMessage?.text; if (t) console.log('UPS>', t.slice(0, 150)) } })
  sock.ev.on('messages.bot-chunk', c => { const t = c.message?.extendedTextMessage?.text; if (c.editType === 'last' || c.editType === 'full') out = t ?? out })
  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection !== 'open') return
    await new Promise(r => setTimeout(r, 1500))
    await sock.resetMetaAiSession(JID)
    const wid = '3EB0' + Date.now()
    try {
      await sock.relayMessage(JID, { protocolMessage: { key: { remoteJid: JID, fromMe: true, id: wid }, type: 18, requestWelcomeMessageMetadata: {} } } as any, { messageId: wid })
    } catch (e: any) { console.log('ERRO relay:', e.message) }
    console.log('welcome enviado')
    await new Promise(r => setTimeout(r, 8000))
    out = ''
    await sock.sendMetaAiPrompt('Qual o codigo? Responda curto.', { jid: JID })
    setTimeout(() => { console.log('BOT NOVO>', out || 'VAZIO'); process.exit(0) }, 20000)
  })
}
main()
