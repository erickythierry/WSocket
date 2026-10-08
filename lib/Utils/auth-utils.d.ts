import type { AuthenticationCreds, CacheStore, SignalKeyStore, SignalKeyStoreWithTransaction, TransactionCapabilityOptions } from '../Types';
import { ILogger } from './logger';
/**
 * Adds caching capability to a SignalKeyStore
 * @param store the store to add caching to
 * @param logger to log trace events
 * @param _cache cache store to use
 */
export declare function makeCacheableSignalKeyStore(store: SignalKeyStore, logger?: ILogger, _cache?: CacheStore): SignalKeyStore;
/**
 * Adds DB like transaction capability (https://en.wikipedia.org/wiki/Database_transaction) to the SignalKeyStore,
 * this allows batch read & write operations & improves the performance of the lib
 *
 * Cada transação tem as próprias mutações e commita quando termina, sem esperar as outras. Antes era um
 * contador por socket: nada ia ao banco até a última transação sobreposta terminar, e se ela falhasse as
 * mutações das que deram certo sumiam (ratchet já usado na rede e não salvo).
 *
 * A leitura enxerga as escritas ainda não commitadas de todas as transações abertas (a mais nova vence): é o
 * que mantém encrypt e decrypt coerentes em memória, como o cache compartilhado fazia. A fila do libsignal por
 * endereço e por sender key é o lock por registro. Cada escrita leva uma sequência, e o commit não grava uma
 * escrita mais velha que a última gravada para a mesma chave.
 *
 * @param state the key store to apply this capability to
 * @param logger logger to log events
 * @returns SignalKeyStore with transaction capability
 */
export declare const addTransactionCapability: (state: SignalKeyStore, logger: ILogger, { maxCommitRetries, delayBetweenTriesMs }: TransactionCapabilityOptions) => SignalKeyStoreWithTransaction;
export declare const initAuthCreds: () => AuthenticationCreds;
