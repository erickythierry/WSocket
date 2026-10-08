"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeKeyedMutex = exports.makeSemaphore = exports.makeMutex = void 0;
const async_mutex_1 = require("async-mutex");
const makeMutex = () => {
    const mutex = new async_mutex_1.Mutex();
    return {
        mutex(code) {
            // erro de uma tarefa não trava a fila
            return mutex.runExclusive(code);
        }
    };
};
exports.makeMutex = makeMutex;
/**
 * Semáforo de N permits. `tryAcquire` devolve false quando não há vaga (pra quem pode desistir),
 * `acquire` entra na fila (pra quem não pode). `release` repassa o permit direto pro próximo da
 * fila, então o total em execução nunca passa de `permits`.
 */
const makeSemaphore = (permits) => {
    const waiters = [];
    let active = 0;
    const release = () => {
        const next = waiters.shift();
        if (next) {
            next();
        }
        else {
            active -= 1;
        }
    };
    return {
        tryAcquire() {
            if (active >= permits)
                return false;
            active += 1;
            return true;
        },
        acquire() {
            if (active < permits) {
                active += 1;
                return Promise.resolve();
            }
            return new Promise(resolve => waiters.push(resolve));
        },
        release,
        get active() {
            return active;
        }
    };
};
exports.makeSemaphore = makeSemaphore;
/** um mutex por chave; a chave sai do mapa quando a última tarefa dela termina */
const makeKeyedMutex = () => {
    const map = new Map();
    return {
        async mutex(key, task) {
            let entry = map.get(key);
            if (!entry) {
                entry = { mutex: new async_mutex_1.Mutex(), refCount: 0 };
                map.set(key, entry);
            }
            entry.refCount += 1;
            try {
                return await entry.mutex.runExclusive(task);
            }
            finally {
                entry.refCount -= 1;
                if (!entry.refCount) {
                    map.delete(key);
                }
            }
        }
    };
};
exports.makeKeyedMutex = makeKeyedMutex;
