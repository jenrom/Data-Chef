package de.areto.datachef.scheduler;

import com.google.common.util.concurrent.ListenableFuture;
import de.areto.datachef.model.worker.WorkerCargo;
import lombok.NonNull;

@FunctionalInterface
public interface DataWorkerTrigger {

    ListenableFuture<? extends WorkerCargo> trigger(@NonNull String name);

}
