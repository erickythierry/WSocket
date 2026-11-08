import type { proto } from '../../WAProto';
import type { BinaryNode } from '../WABinary';
export declare const META_AI_BOT_JID = "867051314767696@bot";
export type MetaAiChunkEditType = 'first' | 'inner' | 'last' | 'full';
export declare const genBotMsgSecret: (messageSecret: Uint8Array) => Buffer<ArrayBufferLike>;
export declare const deriveBotChunkKey: (botMsgSecret: Uint8Array, saltId: string, targetSenderJid: string, authorJid: string) => Buffer<ArrayBufferLike>;
/** o chunk não tem padding PKCS7: o texto decifrado já é o proto Message */
export declare const decryptBotChunk: (input: {
    parentMessageSecret: Uint8Array;
    saltId: string;
    targetSenderJid: string;
    authorJid: string;
    encIv: Uint8Array;
    encPayload: Uint8Array;
}) => Buffer<ArrayBuffer>;
/** metadados e thread que a Meta AI exige no prompt direto para `@bot` */
export declare const buildMetaAiPrompt: (message: proto.IMessage, { aiThreadId, messageSecret }: {
    aiThreadId?: string;
    messageSecret: Uint8Array;
}) => {
    message: proto.IMessage;
    aiThreadId: string;
};
/**
 * Lê da stanza do chunk (`<enc type="msmsg">` + `<bot>` + `<meta>`) o que a decifração precisa.
 * Devolve undefined se não for chunk de bot.
 */
export declare const parseBotChunkNode: (node: BinaryNode, meLid: string | undefined, meId: string | undefined) => {
    payload: Uint8Array<ArrayBufferLike>;
    targetMessageId: string | undefined;
    editType: MetaAiChunkEditType;
    editTargetId: string | undefined;
    saltId: string;
    authorJid: string;
    targetSenderJid: string;
} | undefined;
