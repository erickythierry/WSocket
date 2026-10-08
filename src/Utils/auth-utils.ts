import { AsyncLocalStorage } from 'async_hooks'
import { randomBytes } from 'crypto'
import { DEFAULT_CACHE_TTLS } from '../Defaults'
import type {
	AuthenticationCreds,
	CacheStore,
	SignalDataSet,
	SignalDataTypeMap,
	SignalKeyStore,
	SignalKeyStoreWithTransaction,
	TransactionCapabilityOptions
} from '../Types'
import { Curve, signedKeyPair } from './crypto'
import { delay, generateRegistrationId } from './generics'
import { ILogger } from './logger'
import { makeMutex } from './make-mutex'

/**
 * Map com TTL a partir do set e teto de entradas, sem timer e sem estatística.
 * O @cacheable/node-cache media o tamanho do valor em todo set (65 ms para 400 KB) e deixava um timer vivo por socket.
 * A ordem de inserção do Map é a ordem de vencimento (TTL fixo, get não renova), então a limpeza olha só a frente.
 */
class SignalStoreCache implements CacheStore {
	private readonly entries = new Map<string, { value: unknown; expiresAt: number }>()

	constructor(
		private readonly maxEntries: number,
		private readonly ttlMs: number
	) {}

	get<T>(key: string): T | undefined {
		const entry = this.entries.get(key)
		if (!entry) {
			return undefined
		}

		if (entry.expiresAt <= Date.now()) {
			this.entries.delete(key)
			return undefined
		}

		return entry.value as T
	}

	set<T>(key: string, value: T) {
		this.entries.delete(key)
		this.entries.set(key, { value, expiresAt: Date.now() + this.ttlMs })

		const now = Date.now()
		for (const [oldKey, entry] of this.entries) {
			if (this.entries.size <= this.maxEntries && entry.expiresAt > now) {
				break
			}

			this.entries.delete(oldKey)
		}
	}

	del(key: string) {
		this.entries.delete(key)
	}

	flushAll() {
		this.entries.clear()
	}
}

const SIGNAL_STORE_MAX_KEYS = 10_000

/**
 * Adds caching capability to a SignalKeyStore
 * @param store the store to add caching to
 * @param logger to log trace events
 * @param _cache cache store to use
 */
export function makeCacheableSignalKeyStore(
	store: SignalKeyStore,
	logger?: ILogger,
	_cache?: CacheStore
): SignalKeyStore {
	const cache: CacheStore = _cache || new SignalStoreCache(SIGNAL_STORE_MAX_KEYS, DEFAULT_CACHE_TTLS.SIGNAL_STORE * 1000)

	function getUniqueId(type: string, id: string) {
		return `${type}.${id}`
	}

	return {
		async get(type, ids) {
			const data: { [_: string]: SignalDataTypeMap[typeof type] } = {}
			const idsToFetch: string[] = []
			for (const id of ids) {
				const item = cache.get<SignalDataTypeMap[typeof type]>(getUniqueId(type, id))
				if (typeof item !== 'undefined') {
					data[id] = item
				} else {
					idsToFetch.push(id)
				}
			}

			if (idsToFetch.length) {
				logger?.trace({ items: idsToFetch.length }, 'loading from store')
				const fetched = await store.get(type, idsToFetch)
				for (const id of idsToFetch) {
					// um set durante o await já pôs o valor novo no cache: o lido do store é velho
					const cached = cache.get<SignalDataTypeMap[typeof type]>(getUniqueId(type, id))
					if (typeof cached !== 'undefined') {
						data[id] = cached
						continue
					}

					const item = fetched[id]
					if (item) {
						data[id] = item
						cache.set(getUniqueId(type, id), item)
					}
				}
			}

			return data
		},
		async set(data) {
			await store.set(data)

			let keys = 0
			for (const type in data) {
				for (const id in data[type]) {
					cache.set(getUniqueId(type, id), data[type][id])
					keys += 1
				}
			}

			logger?.trace({ keys }, 'updated cache')
		},
		async clear() {
			cache.flushAll()
			await store.clear?.()
		}
	}
}

/** um ALS por processo: um por socket marca todo recurso assíncrono do processo e vazava heap no Node 22 */
const transactionStorage = new AsyncLocalStorage<TransactionContext>()

type TransactionContext = {
	owner: object
	mutations: SignalDataSet
	/** seq da última escrita desta transação por chave */
	seqs: Map<string, number>
	done: boolean
}

type PendingWrite = { seq: number; ctx: TransactionContext | undefined; value: unknown }

const LONG_TRANSACTION_MS = 30_000
/** tipos cujo estado só avança: gravar um avanço não usado é inofensivo, perder um usado quebra a sessão */
const FORWARD_SAFE_TYPES = new Set<string>(['session', 'sender-key', 'pre-key'])

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
export const addTransactionCapability = (
	state: SignalKeyStore,
	logger: ILogger,
	{ maxCommitRetries, delayBetweenTriesMs }: TransactionCapabilityOptions
): SignalKeyStoreWithTransaction => {
	const owner = {}
	let seq = 0
	/** escritas ainda não gravadas, por chave, da mais velha para a mais nova */
	const pending = new Map<string, PendingWrite[]>()
	/** maior seq já gravado das chaves que ainda têm escrita pendente */
	const committedSeq = new Map<string, number>()
	/** gravações que disputam chave com outra transação vão em série, para o banco ver a ordem das seqs */
	const commitMutex = makeMutex()
	/** escrita direta sem conflito em voo, por chave: o commit de transação espera ela antes de gravar */
	const directInFlight = new Map<string, Promise<unknown>>()

	const keyOf = (type: string, id: string) => `${type}\u0000${id}`
	const currentTx = () => {
		const ctx = transactionStorage.getStore()
		// callback que escapou da transação (void, nextTick) depois do commit grava direto
		return ctx?.owner === owner && !ctx.done ? ctx : undefined
	}

	/** escrita pendente mais nova que o último valor gravado; undefined manda ler do store */
	const latestPending = (key: string) => {
		const writes = pending.get(key)
		const write = writes?.length ? writes[writes.length - 1] : undefined
		return write && write.seq >= (committedSeq.get(key) ?? 0) ? write : undefined
	}

	const addPending = (key: string, write: PendingWrite) => {
		const writes = pending.get(key)
		if (writes) {
			writes.push(write)
		} else {
			pending.set(key, [write])
		}
	}

	const removePending = (key: string, predicate: (write: PendingWrite) => boolean) => {
		const writes = pending.get(key)
		if (!writes) {
			return
		}

		const left = writes.filter(write => !predicate(write))
		if (left.length) {
			pending.set(key, left)
		} else {
			pending.delete(key)
			committedSeq.delete(key)
		}
	}

	const writeWithRetry = async (data: SignalDataSet) => {
		let tries = maxCommitRetries
		for (;;) {
			try {
				await state.set(data)
				return
			} catch (error) {
				tries -= 1
				logger.warn(`failed to commit ${Object.keys(data).length} mutation types, tries left=${tries}`)
				if (tries <= 0) {
					throw error
				}

				await delay(delayBetweenTriesMs)
			}
		}
	}

	/** grava as escritas da transação que ainda são as mais novas gravadas para cada chave */
	const commit = (ctx: TransactionContext, types?: Set<string>) =>
		commitMutex.mutex(async () => {
			const batch: SignalDataSet = {}
			const written: [string, number][] = []
			for (const type in ctx.mutations) {
				if (types && !types.has(type)) {
					continue
				}

				for (const id in ctx.mutations[type]) {
					const key = keyOf(type, id)
					const writeSeq = ctx.seqs.get(key)!
					if (writeSeq > (committedSeq.get(key) ?? 0)) {
						batch[type] ||= {}
						batch[type]![id] = ctx.mutations[type]![id]
						written.push([key, writeSeq])
					}
				}
			}

			if (!written.length) {
				return
			}

			// escrita direta que começou sem conflito pode estar indo ao banco agora: grava depois dela
			const inFlight = written.map(([key]) => directInFlight.get(key)).filter(Boolean)
			if (inFlight.length) {
				await Promise.allSettled(inFlight)
			}

			await writeWithRetry(batch)
			for (const [key, writeSeq] of written) {
				if (pending.has(key)) {
					committedSeq.set(key, Math.max(committedSeq.get(key) ?? 0, writeSeq))
				}
			}
		})

	const finish = (ctx: TransactionContext) => {
		ctx.done = true
		for (const key of ctx.seqs.keys()) {
			removePending(key, write => write.ctx === ctx)
		}
	}

	return {
		get: async (type, ids) => {
			const result: { [id: string]: SignalDataTypeMap[typeof type] } = {}
			const missing: string[] = []
			for (const id of ids) {
				const write = latestPending(keyOf(type, id))
				if (write) {
					if (write.value) {
						result[id] = write.value as SignalDataTypeMap[typeof type]
					}
				} else {
					missing.push(id)
				}
			}

			if (missing.length) {
				const fetched = await state.get(type, missing)
				for (const id of missing) {
					// uma escrita pode ter entrado durante o await: ela é mais nova que o lido
					const write = latestPending(keyOf(type, id))
					const value = write ? write.value : fetched[id]
					if (value) {
						result[id] = value as SignalDataTypeMap[typeof type]
					}
				}
			}

			return result
		},
		set: data => {
			const ctx = currentTx()
			if (ctx) {
				logger.trace({ types: Object.keys(data) }, 'caching in transaction')
				for (const type in data) {
					ctx.mutations[type] ||= {}
					for (const id in data[type]) {
						const key = keyOf(type, id)
						const writeSeq = ++seq
						const value = data[type]![id]
						ctx.mutations[type]![id] = value
						ctx.seqs.set(key, writeSeq)
						addPending(key, { seq: writeSeq, ctx, value })
					}
				}

				return
			}

			// fora de transação: grava já, e a escrita vence o que estiver pendente para a chave
			const writeSeq = ++seq
			const keys: string[] = []
			let conflicts = false
			for (const type in data) {
				for (const id in data[type]) {
					const key = keyOf(type, id)
					keys.push(key)
					conflicts ||= pending.has(key)
					addPending(key, { seq: writeSeq, ctx: undefined, value: data[type]![id] })
				}
			}

			const write = async () => {
				if (conflicts) {
					// outra escrita direta da mesma chave pode estar indo ao banco: grava depois dela
					const inFlight = keys.map(key => directInFlight.get(key)).filter(Boolean)
					if (inFlight.length) {
						await Promise.allSettled(inFlight)
					}
				}

				await state.set(data)
				for (const key of keys) {
					if (pending.has(key)) {
						committedSeq.set(key, Math.max(committedSeq.get(key) ?? 0, writeSeq))
					}
				}
			}

			const promise = (conflicts ? commitMutex.mutex(write) : write()).finally(() => {
				for (const key of keys) {
					if (directInFlight.get(key) === promise) {
						directInFlight.delete(key)
					}

					// só a própria entrada: a de transação sai no finish dela, depois do commit, senão o
					// committedSeq seria apagado com um commit velho ainda na fila
					removePending(key, w => w.seq === writeSeq && !w.ctx)
				}
			})
			if (!conflicts) {
				for (const key of keys) {
					directInFlight.set(key, promise)
				}
			}

			return promise
		},
		isInTransaction: () => !!currentTx(),
		async transaction(work) {
			if (currentTx()) {
				// aninhada: entra na transação de fora
				return work()
			}

			const ctx: TransactionContext = { owner, mutations: {}, seqs: new Map(), done: false }
			const longTransactionTimer = setTimeout(() => {
				logger.warn({ types: Object.keys(ctx.mutations) }, `transação de chaves aberta há ${LONG_TRANSACTION_MS}ms`)
			}, LONG_TRANSACTION_MS)
			longTransactionTimer.unref?.()

			let committing = false
			try {
				const result = await transactionStorage.run(ctx, work)
				// fechada antes do commit: o que escapar daqui em diante (void, nextTick) grava direto
				ctx.done = true
				committing = true
				await commit(ctx)
				return result
			} catch (error) {
				ctx.done = true
				// falhou: grava só o que só anda para frente (sessão e sender key já usadas na rede).
				// sender-key-memory e app-state ficam de fora porque podem marcar o que não foi enviado.
				if (!committing && ctx.seqs.size) {
					logger.warn(
						{ types: Object.keys(ctx.mutations), err: (error as Error)?.message },
						'transação de chaves falhou com mutações pendentes'
					)
					try {
						await commit(ctx, FORWARD_SAFE_TYPES)
					} catch (err) {
						logger.error({ err }, 'falha ao gravar o estado de sessão da transação que falhou')
					}
				}

				throw error
			} finally {
				clearTimeout(longTransactionTimer)
				finish(ctx)
			}
		}
	}
}

export const initAuthCreds = (): AuthenticationCreds => {
	const identityKey = Curve.generateKeyPair()
	return {
		noiseKey: Curve.generateKeyPair(),
		pairingEphemeralKeyPair: Curve.generateKeyPair(),
		signedIdentityKey: identityKey,
		signedPreKey: signedKeyPair(identityKey, 1),
		registrationId: generateRegistrationId(),
		advSecretKey: randomBytes(32).toString('base64'),
		processedHistoryMessages: [],
		nextPreKeyId: 1,
		firstUnuploadedPreKeyId: 1,
		accountSyncCounter: 0,
		accountSettings: {
			unarchiveChats: false
		},
		registered: false,
		pairingCode: undefined,
		lastPropHash: undefined,
		routingInfo: undefined
	}
}
