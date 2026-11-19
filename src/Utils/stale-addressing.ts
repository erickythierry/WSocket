import type { proto } from '../../WAProto'

/**
 * Ack 421 (StaleGroupAddressingMode): o servidor recusou a mensagem de grupo pelo modo de endereçamento.
 * A recuperação busca metadata e devices novos fora de qualquer fila. Por grupo, um ciclo a cada `janelaMs`;
 * dentro dos primeiros `rajadaMs` do ciclo, os outros acks da mesma rajada (boas-vindas + resposta) ainda
 * reenviam, até `maxPorCiclo`. Por socket, no máximo `maxGrupos` ciclos abertos ao mesmo tempo.
 */
export const makeStaleAddressingGuard = ({
	janelaMs,
	rajadaMs,
	maxPorCiclo,
	maxGrupos
}: {
	janelaMs: number
	rajadaMs: number
	maxPorCiclo: number
	maxGrupos: number
}) => {
	const ciclos = new Map<string, { inicio: number; usados: number }>()
	return (jid: string, agora = Date.now()): 'novo' | 'rajada' | false => {
		for (const [chave, ciclo] of ciclos) {
			if (agora - ciclo.inicio >= janelaMs) {
				ciclos.delete(chave)
			}
		}

		const ciclo = ciclos.get(jid)
		if (ciclo) {
			if (agora - ciclo.inicio >= rajadaMs || ciclo.usados >= maxPorCiclo) {
				return false
			}

			ciclo.usados += 1
			return 'rajada'
		}

		if (ciclos.size >= maxGrupos) {
			return false
		}

		ciclos.set(jid, { inicio: agora, usados: 1 })
		return 'novo'
	}
}

/** delete, edit, pin e enquete dependem de atributos/nós do envio original que o reenvio não tem */
export const podeReenviarAposStaleAddressing = (message: proto.IMessage | undefined) =>
	!!message &&
	!message.protocolMessage &&
	!message.pinInChatMessage &&
	!message.pollCreationMessage &&
	!message.pollCreationMessageV2 &&
	!message.pollCreationMessageV3
