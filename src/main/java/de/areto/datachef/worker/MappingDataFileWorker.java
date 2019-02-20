package de.areto.datachef.worker;

import de.areto.common.util.SizeFormatter;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.worker.MappingDataFileWorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class MappingDataFileWorker extends MappingDataWorker<MappingDataFileWorkerCargo> {

    @Getter
    private final SinkFile sinkFile;

    public MappingDataFileWorker(@NonNull SinkFile file) {
        super(new MappingDataFileWorkerCargo(), file.getMappingName());
        this.sinkFile = file;

        this.cargo.setFileGroup(file.getFileGroup());
        this.cargo.setPublishDate(file.getPublishDate());
        this.cargo.setFileName(file.getFileName());
        this.cargo.setCheckSum(file.getCheckSum());
        this.cargo.setDataSize(file.getSize());
    }

    @Override
    boolean canProceed() {
        if (!super.canProceed()) return false;

        final String checkSumCountQuery = "select count(*) from MappingDataFileWorkerCargo c where c.checkSum = :checkSum";
        final Long checkSumCount = session.createQuery(checkSumCountQuery, Long.class)
                .setParameter("checkSum", cargo.getCheckSum())
                .getSingleResult();

        if (checkSumCount != 0) {
            log.error("{}: Rejected - file with checksum '{}' already processed", this, cargo.getCheckSum());
            addError("Rejected - file already processed");
            return false;
        }

        if (!mapping.getStagingMode().equals(StagingMode.FILE)) {
            addError("Rejected - Staging mode must be 'FILE'");
            return false;
        }

        return true;
    }

    @Override
    void doWork() {
        checkNotNull(mapping, "Mapping must be retrieved");

        log.info("{}: File '{}'", this, sinkFile.getFileName());

        for (SQLExpression expression : mapping.getManipulationExpressionsSorted()) {
            String sql = expression.getSqlCode();

            if (expression.getQueryType().equals(SQLExpression.QueryType.IMPORT_FILE))
                sql = MappingDataWorker.fillPlaceHolders(sinkFile, cargo.getDbId(), sql);

            if (expression.getQueryType().equals(SQLExpression.QueryType.RAW_2_COOKED))
                sql = MappingDataWorker.fillPlaceHolders(sinkFile, cargo.getDbId(), sql);

            executeSQLExpression(expression, sql);
        }

        if (log.isInfoEnabled()) {
            final String sizeFormatted = SizeFormatter.format(cargo.getDataSize());
            log.info("{}: Processed {} lines; {} of data", this, cargo.getPayloadSize(), sizeFormatted);
        }
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return new WorkerInfo.Builder()
                .setRunId(getRunId())
                .setName(mappingName)
                .setFileName(sinkFile.getFileName())
                .setWorkerClass(getClass())
                .create();
    }
}