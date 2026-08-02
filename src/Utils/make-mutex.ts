export const makeMutex = () => {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let task = Promise.resolve() as Promise<any>

	let taskTimeout: NodeJS.Timeout | undefined

	return {
		mutex<T>(code: () => Promise<T> | T): Promise<T> {
			task = (async () => {
				// wait for the previous task to complete
				// if there is an error, we swallow so as to not block the queue
				try {
					await task
				} catch {}

				try {
					// execute the current task
					const result = await code()
					return result
				} finally {
					clearTimeout(taskTimeout)
				}
			})()
			// we replace the existing task, appending the new piece of execution to it
			// so the next task will have to wait for this one to finish
			return task
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

export const makeKeyedMutex = () => {
	const map: { [id: string]: Mutex } = {}

	return {
		mutex<T>(key: string, task: () => Promise<T> | T): Promise<T> {
			if (!map[key]) {
				map[key] = makeMutex()
			}

			return map[key].mutex(task)
		}
	}
}
