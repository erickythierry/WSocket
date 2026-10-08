import { proto } from '../../WAProto';
import type { MessageRelayOptions, SignalKeyStoreWithTransaction } from '../Types';
import type { AuthenticationCreds } from '../Types/Auth';
import { ILogger } from '../Utils/logger';
import { type LidResolver } from '../Utils/tc-token-utils';
import { BinaryNode, BinaryNodeAttributes } from '../WABinary';
/**
 * Privacy token das mensagens 1:1 (tctoken, com cstoken de fallback): índice dos jids com token, mapa PN→LID,
 * emissão depois do envio, reemissão quando o contato troca de identidade e prune diário dos vencidos.
 * Código só do fork, separado do envio para não conflitar com o upstream.
 */
export declare const makeTcTokenManager: ({ authState, logger, query }: {
    authState: {
        creds: AuthenticationCreds;
        keys: SignalKeyStoreWithTransaction;
    };
    logger: ILogger;
    query: (node: BinaryNode) => Promise<BinaryNode>;
}) => {
    trackTcTokenJid: (jid: string) => void;
    flushTcTokenIndex: () => Promise<void>;
    withFlushedTcTokenIndex: <T>(task: () => Promise<T>) => Promise<T>;
    getLidForPn: LidResolver;
    cacheLidMapping: (pnJid?: string, lidJid?: string) => void;
    tcTokenStorageJid: (jid: string) => string;
    appendPrivacyToken: (stanza: BinaryNode, { destinationJid, isGroup, isStatus, isNewsletter, isPeer, isRetry, participantJid, meIds }: {
        destinationJid: string;
        isGroup: boolean;
        isStatus: boolean;
        isNewsletter: boolean;
        isPeer: boolean;
        isRetry: boolean;
        participantJid?: string;
        meIds: string[];
    }) => Promise<{
        is1on1Send: boolean;
        tcTokenJid: string | undefined;
    }>;
    maybeIssueTcToken: (jid: string, message: proto.IMessage, options: {
        participant?: MessageRelayOptions["participant"];
        additionalAttributes?: BinaryNodeAttributes;
        storageJid: string;
        msgId: string;
    }) => Promise<void>;
    reissueTcTokenAfterIdentityChange: (jid: string) => Promise<void>;
    getPrivacyTokens: (jids: string[], timestamp?: number) => Promise<BinaryNode>;
    scheduleTcTokenPrune: () => void;
    cancelTcTokenPrune: () => void;
};
