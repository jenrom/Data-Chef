package de.areto.datachef.service;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import de.areto.common.watcher.DirWatcherEvent;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.worker.MappingDataFileWorker;
import de.areto.datachef.worker.MappingWorker;
import de.areto.datachef.worker.MartWorker;
import de.areto.datachef.worker.Worker;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
class PathProcessorCallback implements FutureCallback<SinkFile> {

    private final SinkService sinkService;
    private final WorkerService workerService;
    private final DirWatcherEvent watcherEvent;

    PathProcessorCallback(@NonNull SinkService sinkService, @NonNull WorkerService workerService, @NonNull DirWatcherEvent watcherEvent) {
        checkState(sinkService.isHealthy(), "SinkService must be healthy");
        checkState(workerService.isHealthy(), "WorkerService must be healthy");
        this.sinkService = sinkService;
        this.workerService = workerService;
        this.watcherEvent = watcherEvent;
    }

    @Override
    public void onSuccess(@Nullable SinkFile file) {
        // If file is null, path can be ignored
        if (file == null) return;

        final Worker<? extends WorkerCargo> worker;

        if (file.getType().equals(SinkFile.Type.MART)) {
            worker = new MartWorker(file);
        } else if (file.getType().equals(SinkFile.Type.MAPPING)) {
            worker = new MappingWorker(file);
        } else if (file.getType().equals(SinkFile.Type.DATA)) {
            worker = new MappingDataFileWorker(file);
        } else {
            log.error("Unknown SinkFile type at '{}'", watcherEvent.getPath());
            return;
        }

        final ListenableFuture<? extends WorkerCargo> future = workerService.execute(worker);

        final SinkHouseKeepingCallback callback = new SinkHouseKeepingCallback(file, worker.getWorkerInfo());
        Futures.addCallback(future, callback, sinkService.getIoExecutor());
    }

    @Override
    public void onFailure(Throwable t) {
        log.error("PathProcessor for path '{}' threw {}", watcherEvent.getPath(), t.getClass().getSimpleName());
    }
}
