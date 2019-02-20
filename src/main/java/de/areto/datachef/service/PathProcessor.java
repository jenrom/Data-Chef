package de.areto.datachef.service;

import com.google.common.base.Joiner;
import de.areto.datachef.config.SinkConfig;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.sink.SinkFileFactory;
import de.areto.datachef.persistence.HibernateUtility;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Slf4j
class PathProcessor implements Callable<SinkFile> {

    private final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);
    private final PathMatcher dataFileMatcher;
    private final PathMatcher mappingMatcher;
    private final PathMatcher martMatcher;
    private final Path path;
    private final boolean ignoreSink;

    private SinkFile.Type fileType;

    PathProcessor(Path path, boolean ignoreSink) {
        this.path = path;
        this.ignoreSink = ignoreSink;

        final FileSystem defaultFileSystem = FileSystems.getDefault();

        martMatcher = defaultFileSystem.getPathMatcher("glob:*." + sinkConfig.martFileExtension());

        mappingMatcher = defaultFileSystem.getPathMatcher("glob:*." + sinkConfig.mappingFileExtension());

        final String extList = Joiner.on(",").join(sinkConfig.dataFileExtensions());
        dataFileMatcher = defaultFileSystem.getPathMatcher("glob:*.{" + extList + "}");
    }

    @Override
    public SinkFile call() throws Exception {
        if(!isProcessable()) return null;
        if (log.isDebugEnabled()) log.debug("Processing path '{}'");

        final SinkFile file = SinkFileFactory.create(path, fileType);

        if (isChecksumProcessed(file)) {
            if(sinkConfig.deleteDuplicates()) Files.delete(path);
            return null;
        }

        return file;
    }

    private boolean isChecksumProcessed(@NonNull SinkFile file) {
        final Session session = HibernateUtility.getSessionFactory().openSession();
        final String csQuery = "select count(*) from WorkerCargo w where w.checkSum = :checksum";
        final Long csCount = session.createQuery(csQuery, Long.class)
                .setParameter("checksum", file.getCheckSum())
                .getSingleResult();
        session.close();
        final boolean duplicate = csCount == 1;
        if(duplicate) log.warn("Rejecting duplicate file {}", file.getFileName());
        return duplicate;
    }

    private boolean isProcessable() throws IOException, InterruptedException {
        if(Files.isDirectory(path) || Files.isHidden(path))
            return false;

        if(path.getFileName().toString().contains(" ")) {
            log.warn("Ignoring file with name contains spaces at '{}'", path);
            return false;
        }

        waitForGrowth();

        if (Files.size(path) == 0L) {
            log.warn("Ignoring zero byte file at '{}'", path);
            return false;
        }

        boolean dataFile = dataFileMatcher.matches(path.getFileName());
        boolean mappingFile = mappingMatcher.matches(path.getFileName());
        boolean martFile = martMatcher.matches(path.getFileName());

        if(dataFile){
            fileType = SinkFile.Type.DATA;
        } else
            if(mappingFile){
                fileType = SinkFile.Type.MAPPING;
            } else
                if(martFile){
                    fileType = SinkFile.Type.MART;
                } else {
                    return false;
                }

        if (ignoreSink) {
            if (path.startsWith(Paths.get(sinkConfig.dirRollback()))) {
                return false;
            }
            if (path.startsWith(Paths.get(sinkConfig.dirServed()))) {
                return false;
            }
            return !path.startsWith(Paths.get(sinkConfig.dirRotten()));
        }

        return true;
    }

    private void waitForGrowth() throws InterruptedException, IOException {
        boolean isGrowing;
        do {
            long initialWeight = Files.size(path);
            TimeUnit.MILLISECONDS.sleep(sinkConfig.waitGrowthTime());
            long finalWeight = Files.size(path);
            isGrowing = initialWeight < finalWeight;
        } while (isGrowing);
    }
}
