package de.areto.datachef.worker;

import de.areto.datachef.config.SinkConfig;
import de.areto.datachef.model.worker.WorkerCargo;
import org.aeonbits.owner.ConfigCache;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

abstract class RollbackWorker<T extends WorkerCargo> extends Worker<T> {

    RollbackWorker(T cargo) {
        super(cargo, System.nanoTime());
    }

    private static void deleteNonEmptyDirectory(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    void updateJobLog(List<Long> updatableIds) {
        try {
            final String updateSql = MaintenanceStatementFactory.renderUpdateStatement(updatableIds);
            spox.executeScript(connection, updateSql);
        } catch (Exception e) {
            final String error = String.format("Unable update job log after rollback. Reason %s%s",
                    e.getClass().getSimpleName(),
                    e.getMessage() == null ? "" : ", reason: " + e.getMessage());
            addError(error);
        }
    }

    void moveFiles(String name) {
        final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);
        final Path rollbackDir = Paths.get(sinkConfig.dirRollback());
        final Path servedDir = Paths.get(sinkConfig.dirServed());

        checkState(Files.exists(rollbackDir), "Rollback dir must exist");
        checkState(Files.isDirectory(rollbackDir), "Rollback must be a directory");

        checkState(Files.exists(servedDir), "Served dir must exist");
        checkState(Files.isDirectory(servedDir), "Served must be a directory");

        final Path sourceMartFolder = Paths.get(servedDir.toString(), name);
        final Path targetRollbackFolder = Paths.get(sinkConfig.dirRollback(), name);

        if(!Files.exists(sourceMartFolder))
            return;

        try {
            if(Files.exists(targetRollbackFolder))
                deleteNonEmptyDirectory(targetRollbackFolder);
            Files.move(sourceMartFolder, targetRollbackFolder);
            cargo.addMessage("Moved files to rollback folder");
        } catch (IOException e) {
            addError(e);
        }

    }
}
