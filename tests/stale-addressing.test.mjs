import { test } from 'node:test'
import assert from 'node:assert/strict'
import { makeStaleAddressingGuard, podeReenviarAposStaleAddressing } from '../src/Utils/stale-addressing.ts'

const JANELA = 10 * 60_000
const RAJADA = 30_000
const G1 = '120363412919842815@g.us'
const G2 = '120363317250908000@g.us'
const G3 = '120363400000000000@g.us'
const guarda = (extra = {}) =>
	makeStaleAddressingGuard({ janelaMs: JANELA, rajadaMs: RAJADA, maxPorCiclo: 3, maxGrupos: 2, ...extra })

test('primeiro 421 abre o ciclo; a rajada logo depois ainda reenvia, até o teto', () => {
	const pode = guarda()
	assert.equal(pode(G1, 0), 'novo')
	assert.equal(pode(G1, 500), 'rajada')
	assert.equal(pode(G1, 900), 'rajada')
	assert.equal(pode(G1, 1_000), false)
})

test('passada a rajada, o grupo espera a janela inteira', () => {
	const pode = guarda()
	assert.equal(pode(G1, 0), 'novo')
	assert.equal(pode(G1, RAJADA), false)
	assert.equal(pode(G1, JANELA - 1), false)
	assert.equal(pode(G1, JANELA), 'novo')
})

test('teto de grupos por socket; ciclo vencido libera a vaga', () => {
	const pode = guarda()
	assert.equal(pode(G1, 0), 'novo')
	assert.equal(pode(G2, 1), 'novo')
	assert.equal(pode(G3, 2), false)
	assert.equal(pode(G3, JANELA), 'novo')
	assert.equal(pode(G2, JANELA + 1), 'novo')
})

test('reenvia texto e mídia; não reenvia delete, edit, pin nem enquete', () => {
	assert.equal(podeReenviarAposStaleAddressing({ conversation: 'oi' }), true)
	assert.equal(podeReenviarAposStaleAddressing({ imageMessage: {} }), true)
	assert.equal(podeReenviarAposStaleAddressing(undefined), false)
	assert.equal(podeReenviarAposStaleAddressing({ protocolMessage: { type: 0 } }), false)
	assert.equal(podeReenviarAposStaleAddressing({ pinInChatMessage: {} }), false)
	assert.equal(podeReenviarAposStaleAddressing({ pollCreationMessageV3: {} }), false)
})
