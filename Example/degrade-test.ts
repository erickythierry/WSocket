import P from 'pino'
import makeWASocket, { fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'
const logger = P({ level: 'silent' })
const JID = (process.env.BOT || '1475811245607728') + '@bot'
const wait = (ms: number) => new Promise(r => setTimeout(r, ms))
const main = async () => {
  const { state } = await useMultiFileAuthState('baileys_auth_web')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })
  let buf = ''; let done = false
  sock.ev.on('messages.bot-chunk', c => {
    const t = c.message?.extendedTextMessage?.text
    if (c.editType === 'first') buf = t ?? ''
    else if (t) buf = c.editType === 'last' ? t : buf + t
    if ((c.editType === 'last' || c.editType === 'full') && !done) { done = true; console.log("RESP>", buf.slice(0, 200)) }
  })
  const turn = async (label: string, text: string) => {
    done = false; buf = ''
    console.log('[' + label + '] enviando...')
    await sock.sendMetaAiPrompt(text, { jid: JID })
    for (let i = 0; i < 60 && !done; i++) await wait(500)
    console.log('[' + label + ']', buf.slice(0, 150) || 'VAZIO')
  }
  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection !== 'open') return
    await wait(2000)
    const SYS = 'Voce e um engine de tools. Responda SO JSON. Tools: - ping({"x": string}) -> pong. Formato: {"tool":"ping","args":{"x":"..."}} ou {"final":"..."}. Args sempre objeto.'
    await turn('1 system', 'System:\n' + SYS + '\n\nUser: chame ping com x=abc')
    await turn('2 chatter', 'Agora me explique em 2 frases o que e um compilador.')
    await turn('3 chatter', 'E o que e um linker?')
    await turn('4 volta', 'Voltando ao formato: chame ping com x=zzz')
    process.exit(0)
  })
}
main()
