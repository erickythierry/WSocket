import { BoundedTtlMap } from './bounded-ttl-map'

const caches = {
	// global do processo (todas as sessões): teto de entradas e sem timer
	lidCache: new BoundedTtlMap<string, string>(100_000, 60 * 60 * 1000)
}

export default caches
