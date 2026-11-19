"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.podeReenviarAposStaleAddressing = exports.makeStaleAddressingGuard = void 0;
/**
 * Ack 421 (StaleGroupAddressingMode): o servidor recusou a mensagem de grupo pelo modo de endereçamento.
 * A recuperação busca metadata e devices novos fora de qualquer fila. Por grupo, um ciclo a cada `janelaMs`;
 * dentro dos primeiros `rajadaMs` do ciclo, os outros acks da mesma rajada (boas-vindas + resposta) ainda
 * reenviam, até `maxPorCiclo`. Por socket, no máximo `maxGrupos` ciclos abertos ao mesmo tempo.
 */
const makeStaleAddressingGuard = ({ janelaMs, rajadaMs, maxPorCiclo, maxGrupos }) => {
    const ciclos = new Map();
    return (jid, agora = Date.now()) => {
        for (const [chave, ciclo] of ciclos) {
            if (agora - ciclo.inicio >= janelaMs) {
                ciclos.delete(chave);
            }
        }
        const ciclo = ciclos.get(jid);
        if (ciclo) {
            if (agora - ciclo.inicio >= rajadaMs || ciclo.usados >= maxPorCiclo) {
                return false;
            }
            ciclo.usados += 1;
            return 'rajada';
        }
        if (ciclos.size >= maxGrupos) {
            return false;
        }
        ciclos.set(jid, { inicio: agora, usados: 1 });
        return 'novo';
    };
};
exports.makeStaleAddressingGuard = makeStaleAddressingGuard;
/** delete, edit, pin e enquete dependem de atributos/nós do envio original que o reenvio não tem */
const podeReenviarAposStaleAddressing = (message) => !!message &&
    !message.protocolMessage &&
    !message.pinInChatMessage &&
    !message.pollCreationMessage &&
    !message.pollCreationMessageV2 &&
    !message.pollCreationMessageV3;
exports.podeReenviarAposStaleAddressing = podeReenviarAposStaleAddressing;
