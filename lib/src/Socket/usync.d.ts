import { Boom } from '@hapi/boom';
import { SocketConfig } from '../Types';
import { BinaryNode } from '../WABinary';
import { USyncQuery } from '../WAUSync';
export declare const makeUSyncSocket: (config: SocketConfig) => {
    type: 'md';
    ws: import("./Client").WebSocketClient;
    ev: import("..").BaileysEventEmitter & {
        process(handler: (events: Partial<import("..").BaileysEventMap>) => void | Promise<void>): () => void;
        buffer(): void;
        createBufferedFunction<A extends any[], T>(work: (...args: A) => Promise<T>): (...args: A) => Promise<T>;
        flush(force?: boolean): boolean;
        isBuffering(): boolean;
    };
    authState: {
        creds: import("..").AuthenticationCreds;
        keys: import("..").SignalKeyStoreWithTransaction;
    };
    signalRepository: import("..").SignalRepository;
    user: import("..").Contact | undefined;
    generateMessageTag: () => string;
    query: (node: BinaryNode, timeoutMs?: number) => Promise<any>;
    waitForMessage: <T>(msgId: string, timeoutMs?: number | undefined) => Promise<any>;
    waitForSocketOpen: () => Promise<void>;
    sendRawMessage: (data: Uint8Array | Buffer) => Promise<void>;
    sendNode: (frame: BinaryNode) => Promise<void>;
    logout: (msg?: string) => Promise<void>;
    end: (error: Error | undefined) => void;
    onUnexpectedError: (err: Error | Boom, msg: string) => void;
    uploadPreKeys: (count?: number) => Promise<void>;
    uploadPreKeysToServerIfRequired: () => Promise<void>;
    requestPairingCode: (phoneNumber: string, customPairingCode?: string) => Promise<string>;
    waitForConnectionUpdate: (check: (u: Partial<import("..").ConnectionState>) => Promise<boolean | undefined>, timeoutMs?: number) => Promise<void>;
    sendWAMBuffer: (wamBuffer: Buffer) => Promise<any>;
    executeUSyncQuery: (usyncQuery: USyncQuery) => Promise<import("..").USyncQueryResult | undefined>;
};
