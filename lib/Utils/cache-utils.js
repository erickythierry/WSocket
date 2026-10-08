"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const bounded_ttl_map_1 = require("./bounded-ttl-map");
const caches = {
    // global do processo (todas as sessões): teto de entradas e sem timer
    lidCache: new bounded_ttl_map_1.BoundedTtlMap(100000, 60 * 60 * 1000)
};
exports.default = caches;
