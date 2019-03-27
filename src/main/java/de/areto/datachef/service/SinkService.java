package de.areto.datachef.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.*;
import de.areto.common.watcher.DirWatcher;
import de.areto.common.watcher.DirWatcherEvent;
import de.areto.datachef.config.SinkConfig;
import de.areto.datachef.model.sink.SinkFile;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class SinkService extends AbstractIdleService {

    private final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);

    private final WorkerService workerService;
    private final PathMatcher supportFileMatcher;

    private final Queue<DirWatcherEvent> sinkEventQueue;
    private final ExecutorService eventQueueRemovalExecutor;
    private final ExecutorService watchExecutor;
    private final ListeningExecutorService processSinkPathExecutor;
    private final ExecutorService callbackExecutor;

    @Getter
    private ExecutorService ioExecutor;

    @Getter
    private boolean healthy = false;

    public SinkService(@NonNull WorkerService workerService) {
        this.workerService = workerService;

        final List<String> supportedExtensions = new ArrayList<>(sinkConfig.dataFileExtensions());
        supportedExtensions.add(sinkConfig.mappingFileExtension());
        supportedExtensions.add(sinkConfig.martFileExtension());

        final String pattern = "glob:*.{" + Joiner.on(",").join(supportedExtensions) + "}";
        supportFileMatcher = FileSystems.getDefault().getPathMatcher(pattern);

        sinkEventQueue = Queues.newLinkedBlockingQueue();
        callbackExecutor = Executors.newSingleThreadExecutor();
        watchExecutor = Executors.newSingleThreadExecutor();
        eventQueueRemovalExecutor = Executors.newSingleThreadExecutor();
        processSinkPathExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ioExecutor = Executors.newFixedThreadPool(4);
    }

    @Override
    protected void startUp() throws Exception {
        this.checkAndSetupFolders();

        if(sinkConfig.watchSink()) {
            final DirWatcher watcher = DirWatcher.create(Paths.get(sinkConfig.path()));

            watcher.registerListener((event) -> {
                if(log.isTraceEnabled()) log.trace("DirWatcherListener caught {}", event);

                if (!event.getType().equals(DirWatcherEvent.Type.CREATE))
                    return;

                queueEvent(event, true);
            });
            watchExecutor.submit(watcher::processEvents);
        }

        healthy = true;
    }

    public void queueEvent(@NonNull DirWatcherEvent event, boolean ignoreSink) {
        if(!supportFileMatcher.matches(event.getPath().getFileName()))
            return;

        final PathProcessor processor = new PathProcessor(event.getPath(), ignoreSink);
        final ListenableFuture<SinkFile> future = processSinkPathExecutor.submit(processor);
        sinkEventQueue.offer(event);

        final PathProcessorCallback callback = new PathProcessorCallback(this, workerService, event);
        Futures.addCallback(future, callback, callbackExecutor);

        Futures.addCallback(future, new FutureCallback<SinkFile>() {
            @Override
            public void onSuccess(@Nullable SinkFile result) { remove(); }

            @Override
            public void onFailure(Throwable t) { remove(); }

            private void remove() {
                if(!sinkEventQueue.remove(event))
                    log.error("Unable to remove ({}} {}", event.getEventId(), event);
            }
        }, eventQueueRemovalExecutor);
    }

    @Override
    protected void shutDown() {
        watchExecutor.shutdown();
        callbackExecutor.shutdown();
        processSinkPathExecutor.shutdown();
        ioExecutor.shutdown();
    }

    private void checkAndSetupFolders() throws IOException {
        final Path sinkPath = Paths.get(sinkConfig.path());
        if(!Files.exists(sinkPath)) Files.createDirectory(sinkPath);

        final Path servedPath = Paths.get(sinkConfig.dirServed());
        if(!Files.exists(servedPath)) Files.createDirectory(servedPath);

        final Path rottenPath = Paths.get(sinkConfig.dirRotten());
        if(!Files.exists(rottenPath)) Files.createDirectory(rottenPath);

        final Path rollbackPath = Paths.get(sinkConfig.dirRollback());
        if(!Files.exists(rollbackPath)) Files.createDirectory(rollbackPath);
    }
}