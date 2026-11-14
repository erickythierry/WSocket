import P from 'pino'
import makeWASocket, { fetchLatestBaileysVersion, makeCacheableSignalKeyStore, useMultiFileAuthState } from '../src'

// Despeja a estrutura crua das respostas do @bot para descobrir por onde elas chegam
const logger = P({ level: 'silent' })
const JID = (process.env.BOT || '1387351107802900') + '@bot'

const main = async () => {
  const { state } = await useMultiFileAuthState('baileys_auth_web')
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({ version, logger, auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) } })

  sock.ev.on('messages.upsert', ({ messages, type }) => {
    for (const m of messages) {
      console.log('UPSERT type=', type, 'fromMe=', m.key.fromMe, 'remote=', m.key.remoteJid)
      console.log('  RAW:', JSON.stringify(m.message).slice(0, 900))
    }
  })
  sock.ev.on('messages.bot-chunk', c => {
    console.log('CHUNK edit=', c.editType, 'remote=', c.key?.remoteJid, 'RAW:', JSON.stringify(c.message).slice(0, 900))
  })
  sock.ev.on('messages.update', u => console.log('UPDATE:', JSON.stringify(u).slice(0, 400)))

  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection !== 'open') return
    await new Promise(r => setTimeout(r, 2000))
    console.log('enviando prompt simples...')
    await sock.sendMetaAiPrompt('Diga apenas: teste', { jid: JID })
    setTimeout(() => process.exit(0), 30000)
  })
}

main()
