import P from 'pino'
import makeWASocket, { fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'

// Manda o bloco inteiro numa mensagem e captura a resposta em JSON
const logger = P({ level: 'silent' })
const JID = (process.env.BOT || '1475811245607728') + '@bot'
const wait = (ms: number) => new Promise(r => setTimeout(r, ms))

const SYSTEM = `You are the tool-calling engine of a coding agent. Output ONLY a JSON object. No prose, no markdown fences.

Tools:
- read_file({"path": string}) -> file contents
- write_file({"path": string, "content": string}) -> ok
- list_dir({"path": string}) -> string[]
- run_bash({"cmd": string}) -> stdout
- grep({"pattern": string, "path": string}) -> matches

Reply format:
To call a tool: {"tool":"<name>","args":{...}}
To answer: {"final":"<text>"}

Args are ALWAYS an object with the named keys.
Rules: one call per turn, never guess file contents, always read before answering.`

const TASK = `No repo /srv/app, quero saber quantos arquivos .ts existem em src e qual o maior. Comece listando o diretorio.`

const main = async () => {
  const { state } = await useMultiFileAuthState('baileys_auth_web')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })

  let buf = ''
  let done = false
  const finish = (why: string) => {
    if (done) return
    done = true
    console.log('\n=== RESPOSTA FINAL ===\n' + buf)
    console.log('\nJSON valido?', (() => { try { JSON.parse(buf.trim()); return 'SIM' } catch { return 'NAO' } })())
    process.exit(0)
  }

  sock.ev.on('messages.bot-chunk', c => {
    const t = c.message?.extendedTextMessage?.text
    if (c.editType === 'first') buf = t ?? ''
    else if (t) buf = c.editType === 'last' ? t : buf + t
    if (c.editType === 'last' || c.editType === 'full') finish('chunk')
  })

  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection !== 'open') return
    await wait(2000)
    // bloco inteiro numa mensagem so
    await sock.sendMetaAiPrompt(`System:\n${SYSTEM}\n\nUser: ${TASK}`, { jid: JID })
    console.log('enviado, aguardando...')
    setTimeout(() => finish('timeout'), 40000)
  })
}

main()
