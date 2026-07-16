import type { Chat, SignalKeyStore } from '../Types';
import { type BinaryNode } from '../WABinary';
import type { ILogger } from './logger';
/** Chave sentinela com o índice dos JIDs rastreados e o timestamp do último prune. */
export declare const TC_TOKEN_INDEX_KEY = "__index";
export type LidResolver = (pnJid: string) => string | undefined;
export declare function isRegularUser(jid: string | undefined): boolean;
/** Usa LID como chave quando conhecido, igual ao cliente oficial; PN é apenas fallback. */
export declare function resolveTcTokenStorageJid(jid: string, resolveLid?: LidResolver): string;
type TcTokenIndexEntry = {
    token: Buffer;
    timestamp?: string;
};
export declare function readTcTokenIndex(keys: SignalKeyStore): Promise<string[]>;
export declare function readLastTcTokenPruneTs(keys: SignalKeyStore): Promise<number>;
export declare function buildMergedTcTokenIndexWrite(keys: SignalKeyStore, addedJids: Iterable<string>): Promise<{
    [TC_TOKEN_INDEX_KEY]: TcTokenIndexEntry;
}>;
export declare function buildTcTokenIndexEntry(jids: Iterable<string>, pruneTs?: string | number): TcTokenIndexEntry;
export declare function isTcTokenExpired(timestamp: number | string | null | undefined): boolean;
export declare function shouldSendNewTcToken(senderTimestamp: number | undefined): boolean;
type BuildParams = {
    jid: string;
    baseContent?: BinaryNode[];
    keys: SignalKeyStore;
    resolveLid?: LidResolver;
};
export declare function buildTcTokenFromJid({ keys, jid, baseContent, resolveLid }: BuildParams): Promise<BinaryNode[] | undefined>;
type StoreParams = {
    result: BinaryNode;
    fallbackJid: string;
    keys: SignalKeyStore;
    resolveLid?: LidResolver;
    onNewJidStored?: (jid: string) => void;
};
/** Persiste tctokens recebidos por IQ/notificação usando o remetente, nunca o JID do próprio device. */
export declare function storeTcTokensFromIqResult({ result, fallbackJid, keys, resolveLid, onNewJidStored }: StoreParams): Promise<string[]>;
/** Importa somente os tctokens das conversas do history sync, em lotes limitados. */
export declare function storeTcTokensFromHistorySync(chats: Chat[], keyStore: SignalKeyStore, logger?: ILogger, resolveLid?: LidResolver): Promise<number>;
export {};
