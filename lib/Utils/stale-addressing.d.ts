import type { proto } from '../../WAProto';
/**
 * Ack 421 (StaleGroupAddressingMode): o servidor recusou a mensagem de grupo pelo modo de endereçamento.
 * A recuperação busca metadata e devices novos fora de qualquer fila. Por grupo, um ciclo a cada `janelaMs`;
 * dentro dos primeiros `rajadaMs` do ciclo, os outros acks da mesma rajada (boas-vindas + resposta) ainda
 * reenviam, até `maxPorCiclo`. Por socket, no máximo `maxGrupos` ciclos abertos ao mesmo tempo.
 */
export declare const makeStaleAddressingGuard: ({ janelaMs, rajadaMs, maxPorCiclo, maxGrupos }: {
    janelaMs: number;
    rajadaMs: number;
    maxPorCiclo: number;
    maxGrupos: number;
}) => (jid: string, agora?: number) => "novo" | "rajada" | false;
/** delete, edit, pin e enquete dependem de atributos/nós do envio original que o reenvio não tem */
export declare const podeReenviarAposStaleAddressing: (message: proto.IMessage | undefined) => boolean;
