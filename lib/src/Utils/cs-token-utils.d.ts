import { proto } from '../../WAProto';
import type { ChatMutation, SignalKeyStore } from '../Types';
import type { ILogger } from './logger';
export declare const NCT_SALT_SYNC_INDEX = "nct_salt_sync";
export declare const NCT_SALT_KEY = "salt";
/** Deriva HMAC-SHA256(nctSalt, recipientLID), igual ao cliente de referência. */
export declare function generateCsToken(salt: Buffer, recipientLid: string): Buffer | undefined;
export declare function readNctSalt(keys: SignalKeyStore): Promise<Buffer | undefined>;
export declare function storeNctSaltFromHistorySync(historySync: proto.IHistorySync, keys: SignalKeyStore, logger?: ILogger): Promise<boolean>;
export declare function handleNctSaltMutation({ mutation, keys, logger }: {
    mutation: ChatMutation;
    keys: SignalKeyStore;
    logger?: ILogger;
}): Promise<boolean>;
