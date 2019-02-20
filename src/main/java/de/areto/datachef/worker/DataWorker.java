package de.areto.datachef.worker;

import de.areto.datachef.model.worker.WorkerCargo;

abstract class DataWorker<T extends WorkerCargo> extends Worker<T> {

    protected DataWorker(T cargo) {
        super(cargo);
    }

    protected DataWorker(T cargo, long priority) {
        super(cargo, priority);
    }

}
