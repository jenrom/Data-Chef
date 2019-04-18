package de.areto.datachef.worker;

import de.areto.common.util.SizeFormatter;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.worker.MappingDataFileWorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import static com.google.common.base.Preconditions.checkNotNull;
import static de.areto.datachef.config.Constants.DF_PUB_DATE;

@Slf4j
public class MappingDataFileWorker extends MappingDataWorker<MappingDataFileWorkerCargo> {

    @Getter
    private final SinkDataFile sinkFile;

    public MappingDataFileWorker(@NonNull SinkFile file) {
        this(new SinkDataFile(file));
    }

    public MappingDataFileWorker(@NonNull SinkDataFile file) {
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
                sql = fillPlaceHolders(sinkFile.getAbsolutePathString(), cargo.getDbId(), sql);

            if (expression.getQueryType().equals(SQLExpression.QueryType.RAW_2_COOKED))
                sql = fillPlaceHolders(sinkFile.getAbsolutePathString(), cargo.getDbId(), sql);

            executeSQLExpression(expression, sql);
        }

        if (log.isInfoEnabled()) {
            final String sizeFormatted = SizeFormatter.format(cargo.getDataSize());
            log.info("{}: Processed {} lines; {} of data", this, cargo.getPayloadSize(), sizeFormatted);
        }
    }
    /**
     * Replace placeholder Strings in previously created {@link SQLExpression}s if StageMode == FILE.
     *
     * @param filePath filePath
     * @param dbId Numerical ID representing the load id of the ELT process
     * @param sql  SQL as String that conains the placeholders
     * @return replaced and executable SQL
     */
    private  String fillPlaceHolders(@NonNull String filePath, @NonNull Long dbId, @NonNull String sql) {
        final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);
        String newSql = sql.replace(dvConfig.placeholderFilePath(), filePath);
        newSql = newSql.replaceAll(dvConfig.placeholderFileName(), cargo.getFileName());
        newSql = newSql.replaceAll(dvConfig.placeholderDbId(), dbId.toString());
        newSql = newSql.replaceAll(dvConfig.placeholderFileGroup(), cargo.getFileGroup());
        newSql = newSql.replaceAll(dvConfig.placeholderPublishDate(), DF_PUB_DATE.format(cargo.getPublishDate()));
        return newSql;
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