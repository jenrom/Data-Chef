package de.areto.datachef.service;

import com.google.common.util.concurrent.FutureCallback;
import de.areto.datachef.config.SinkConfig;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Slf4j
class SinkHouseKeepingCallback implements FutureCallback<WorkerCargo> {

    private final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);
    private final SinkFile file;
    private final WorkerInfo workerInfo;

    SinkHouseKeepingCallback(SinkFile file, WorkerInfo workerInfo) {
        this.file = file;
        this.workerInfo = workerInfo;
    }

    @Override
    public void onSuccess(@Nullable WorkerCargo result) {
        if (result == null)
            return;

        if (result.getStatus().equals(WorkerCargo.Status.REJECTED))
            return;

        if (!sinkConfig.moveFiles())
            return;

        try {
            if(result.hasErrors())
                transport(Paths.get(sinkConfig.dirRotten()));
            else
                transport(Paths.get(sinkConfig.dirServed()));
        } catch (IOException e) {
            log.error("Housekeeping resulted in an IOException for file '{}', '{}'", file, workerInfo);
            if (e.getMessage() != null) log.error("IOException: {}", e.getMessage());
        }
    }

    private void transport(Path target) throws IOException {
        checkState(Files.isDirectory(target), "Target must be a directory");
        final Path sinkPath = Paths.get(sinkConfig.path());
        final Path mappingFolder = Paths.get(target.toString(), file.getMappingName());

        if (!Files.exists(mappingFolder)) Files.createDirectories(mappingFolder);

        final String fileName = file.getPath().getFileName().toString();
        final Path targetFile = Paths.get(mappingFolder.toString(), fileName);
        boolean move = file.getPath().startsWith(sinkPath);

        final Path newPath;

        if (move) {
            newPath = Files.move(file.getPath(), targetFile, REPLACE_EXISTING);
            final Path parent = file.getPath().getParent();
            if (parent.startsWith(sinkPath) && isDirEmpty(parent)) Files.deleteIfExists(parent);
        } else {
            newPath = Files.copy(file.getPath(), targetFile, REPLACE_EXISTING);
        }

        if (log.isDebugEnabled()) log.debug("{} {} --> {}", (move ? "Moving" : "Copying"), file, newPath);
    }

    private static boolean isDirEmpty(final Path directory) throws IOException {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            return !dirStream.iterator().hasNext();
        }
    }

    @Override
    public void onFailure(Throwable t) {
        log.error("{} with run id '{}' resulted in {}", workerInfo.getWorkerClass().getSimpleName(), workerInfo.getRunId(), t);
    }
}
