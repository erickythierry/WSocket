/** Map LRU simples, com TTL e limite rígido de entradas. */
export class BoundedTtlMap<K, V> {
	private readonly entries = new Map<K, { value: V; expiresAt: number }>()

	constructor(
		private readonly maxEntries: number,
		private readonly ttlMs: number
	) {}

	get(key: K): V | undefined {
		const entry = this.entries.get(key)
		if (!entry) return undefined

		if (entry.expiresAt <= Date.now()) {
			this.entries.delete(key)
			return undefined
		}

		this.entries.delete(key)
		this.entries.set(key, entry)
		return entry.value
	}

	has(key: K): boolean {
		return this.get(key) !== undefined
	}

	set(key: K, value: V): void {
		this.entries.delete(key)
		this.entries.set(key, { value, expiresAt: Date.now() + this.ttlMs })

		while (this.entries.size > this.maxEntries) {
			const oldest = this.entries.keys().next().value as K | undefined
			if (oldest === undefined) break
			this.entries.delete(oldest)
		}
	}
}
