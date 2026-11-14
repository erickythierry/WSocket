import { encodeBinaryNode } from '../src/WABinary/encode'
import { jidEncode } from '../src/WABinary/jid-utils'
const body = Buffer.from(JSON.stringify({ variables: { input: { client_pub_key: 'AAA', request_id: 'x' } } }))
const n: any = { tag: 'iq', attrs: { id: '1.2-3', to: jidEncode(null, 's.whatsapp.net'), type: 'get', xmlns: 'w:mex' }, content: [{ tag: 'query', attrs: { query_id: '27013462064904056' }, content: body }] }
console.log(JSON.stringify(Array.from(encodeBinaryNode(n))))
