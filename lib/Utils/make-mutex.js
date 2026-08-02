"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeKeyedMutex = exports.makeSemaphore = exports.makeMutex = void 0;
const makeMutex = () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let task = Promise.resolve();
    let taskTimeout;
    return {
        mutex(code) {
            task = (async () => {
                // wait for the previous task to complete
                // if there is an error, we swallow so as to not block the queue
                try {
                    await task;
                }
                catch (_a) { }
                try {
                    // execute the current task
                    const result = await code();
                    return result;
                }
                finally {
                    clearTimeout(taskTimeout);
                }
            })();
            // we replace the existing task, appending the new piece of execution to it
            // so the next task will have to wait for this one to finish
            return task;
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
const makeKeyedMutex = () => {
    const map = {};
    return {
        mutex(key, task) {
            if (!map[key]) {
                map[key] = (0, exports.makeMutex)();
            }
            return map[key].mutex(task);
        }
    };
};
exports.makeKeyedMutex = makeKeyedMutex;
