import P from 'pino'
import makeWASocket, { fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'
const logger = P({ level: 'silent' })
const JID = (process.env.BOT || '1475811245607728') + '@bot'
const wait = (ms: number) => new Promise(r => setTimeout(r, ms))
const main = async () => {
  const { state } = await useMultiFileAuthState('baileys_auth_web')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })
  let buf = ''
  let done = false
  sock.ev.on('messages.bot-chunk', c => {
    const t = c.message?.extendedTextMessage?.text
    if (c.editType === 'first') buf = t ?? ''
    else if (t) buf = c.editType === 'last' ? t : buf + t
    if ((c.editType === 'last' || c.editType === 'full') && !done) { done = true; console.log('RESP>', buf.slice(0, 300)); process.exit(0) }
  })
  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection !== 'open') return
    await wait(2000)
    const orig = sock.sendMetaAiPrompt
    const sent = await orig.call(sock, 'Ola, quem e voce em uma frase?', { jid: JID })
    console.log('enviado', sent.key.id)
    setTimeout(() => { console.log('TIMEOUT'); process.exit(0) }, 30000)
  })
}
main()
