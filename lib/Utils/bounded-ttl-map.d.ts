/** Map LRU simples, com TTL e limite rígido de entradas. */
export declare class BoundedTtlMap<K, V> {
    private readonly maxEntries;
    private readonly ttlMs;
    private readonly entries;
    constructor(maxEntries: number, ttlMs: number);
    get(key: K): V | undefined;
    has(key: K): boolean;
    set(key: K, value: V): void;
}
