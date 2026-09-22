"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BoundedTtlMap = void 0;
/** Map LRU simples, com TTL e limite rígido de entradas. */
class BoundedTtlMap {
    constructor(maxEntries, ttlMs) {
        this.maxEntries = maxEntries;
        this.ttlMs = ttlMs;
        this.entries = new Map();
    }
    get(key) {
        const entry = this.entries.get(key);
        if (!entry)
            return undefined;
        if (entry.expiresAt <= Date.now()) {
            this.entries.delete(key);
            return undefined;
        }
        this.entries.delete(key);
        this.entries.set(key, entry);
        return entry.value;
    }
    has(key) {
        return this.get(key) !== undefined;
    }
    set(key, value) {
        this.entries.delete(key);
        this.entries.set(key, { value, expiresAt: Date.now() + this.ttlMs });
        while (this.entries.size > this.maxEntries) {
            const oldest = this.entries.keys().next().value;
            if (oldest === undefined)
                break;
            this.entries.delete(oldest);
        }
    }
}
exports.BoundedTtlMap = BoundedTtlMap;
