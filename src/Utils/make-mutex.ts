import { Mutex as AsyncMutex } from 'async-mutex'

export const makeMutex = () => {
	const mutex = new AsyncMutex()

	return {
		mutex<T>(code: () => Promise<T> | T): Promise<T> {
			// erro de uma tarefa não trava a fila
			return mutex.runExclusive(code)
		}
	}
}

export type Mutex = ReturnType<typeof makeMutex>

/**
 * Semáforo de N permits. `tryAcquire` devolve false quando não há vaga (pra quem pode desistir),
 * `acquire` entra na fila (pra quem não pode). `release` repassa o permit direto pro próximo da
 * fila, então o total em execução nunca passa de `permits`.
 */
export const makeSemaphore = (permits: number) => {
	const waiters: (() => void)[] = []
	let active = 0

	const release = () => {
		const next = waiters.shift()
		if (next) {
			next()
		} else {
			active -= 1
		}
	}

	return {
		tryAcquire(): boolean {
			if (active >= permits) return false
			active += 1
			return true
		},
		acquire(): Promise<void> {
			if (active < permits) {
				active += 1
				return Promise.resolve()
			}

			return new Promise<void>(resolve => waiters.push(resolve))
		},
		release,
		get active() {
			return active
		}
	}
}

/** um mutex por chave; a chave sai do mapa quando a última tarefa dela termina */
export const makeKeyedMutex = () => {
	const map = new Map<string, { mutex: AsyncMutex; refCount: number }>()

	return {
		async mutex<T>(key: string, task: () => Promise<T> | T): Promise<T> {
			let entry = map.get(key)
			if (!entry) {
				entry = { mutex: new AsyncMutex(), refCount: 0 }
				map.set(key, entry)
			}

			entry.refCount += 1
			try {
				return await entry.mutex.runExclusive(task)
			} finally {
				entry.refCount -= 1
				if (!entry.refCount) {
					map.delete(key)
				}
			}
		}
	}
}
