package de.areto.datachef.service;

import com.google.common.util.concurrent.ListenableFuture;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.worker.Worker;

/**
 * Code get's executed at the end of {@link Worker#doWork()} in the scope of the {@link Worker}s thread!
 * If possible use {@link ListenableFuture} instead.
 */
@FunctionalInterface
public interface WorkerInThreadCallback {
    void execute(WorkerCargo cargo);
}
