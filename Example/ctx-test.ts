import P from 'pino'
import makeWASocket, { fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'

// Isolamento de contexto por aiThreadId: planta em A, pergunta em B
const logger = P({ level: 'silent' })
const JID = (process.env.BOT || '1387351107802900') + '@bot'
const txt = (m: any): string => m?.extendedTextMessage?.text ?? m?.conversation ?? m?.richResponseMessage?.submessages?.map((s: any) => s.messageText ?? '').join('\n') ?? ''
const wait = (ms: number) => new Promise(r => setTimeout(r, ms))

const main = async () => {
  const { state } = await useMultiFileAuthState('baileys_auth_web')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })

  let buf = ''
  let seen: string[] = []
  sock.ev.on('messages.bot-chunk', c => {
    if (c.editType === 'first') { buf = '' }
    const t = txt(c.message)
    if (t) { buf = c.editType === 'last' ? t : buf + t }
    if (c.editType === 'last' || c.editType === 'full') {
      const tid = c.message?.messageContextInfo?.threadId?.[0]?.threadKey?.id
      seen.push(buf); console.log('BOT>', buf.slice(0, 150), '| thread', tid?.slice(0, 8))
    }
  })

  const ask = async (label: string, text: string, aiThreadId?: string) => {
    seen.length = 0
    console.log(`\n[${label}] enviando...`)
    const r = await sock.sendMetaAiPrompt(text, { jid: JID, aiThreadId })
    for (let i = 0; i < 40 && !seen.length; i++) await wait(500)
    console.log(`[${label}] resposta:`, seen[0]?.slice(0, 120) ?? 'VAZIO')
    return { text: seen[0] ?? '', tid: r.aiThreadId }
  }

  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection !== 'open') return
    await wait(2000)

    const a = await ask('A1 planta', 'Guarde este codigo: AZUL-77. Responda so OK.')
    const a2 = await ask('A2 recorda', 'Qual o codigo?', a.tid)
    const b = await ask('B1 thread nova', 'Qual o codigo?')

    console.log('\n===== RESULTADO =====')
    console.log('A recordou (esperado sim):', a2.text.includes('AZUL'))
    console.log('B vazou contexto (esperado nao):', b.text.includes('AZUL'))
    process.exit(0)
  })
}

main()
