export declare const makeMutex: () => {
    mutex<T>(code: () => Promise<T> | T): Promise<T>;
};
export type Mutex = ReturnType<typeof makeMutex>;
/**
 * Semáforo de N permits. `tryAcquire` devolve false quando não há vaga (pra quem pode desistir),
 * `acquire` entra na fila (pra quem não pode). `release` repassa o permit direto pro próximo da
 * fila, então o total em execução nunca passa de `permits`.
 */
export declare const makeSemaphore: (permits: number) => {
    tryAcquire(): boolean;
    acquire(): Promise<void>;
    release: () => void;
    readonly active: number;
};
/** um mutex por chave; a chave sai do mapa quando a última tarefa dela termina */
export declare const makeKeyedMutex: () => {
    mutex<T>(key: string, task: () => Promise<T> | T): Promise<T>;
};
