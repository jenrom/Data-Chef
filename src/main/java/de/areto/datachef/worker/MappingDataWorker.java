package de.areto.datachef.worker;

import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import java.time.LocalDateTime;
import java.util.Optional;

import static de.areto.datachef.config.Constants.DF_PUB_DATE;

@Slf4j
public abstract class MappingDataWorker<T extends WorkerCargo> extends DataWorker<T> {

    protected Mapping mapping;
    protected final String mappingName;

    MappingDataWorker(T cargo, String mappingName) {
        super(cargo);
        this.mappingName = mappingName;
        this.cargo.setName(mappingName);
    }

    MappingDataWorker(T cargo, String mappingName, long priority) {
        super(cargo, priority);
        this.mappingName = mappingName;
        this.cargo.setName(mappingName);
    }

    /**
     * Replace placeholder Strings in previously created {@link SQLExpression}s if {@link StagingMode} is
     * {@link StagingMode#CONNECTION} or {@link StagingMode#INSERT}.
     *
     * @param dbId        Numerical ID representing the load id of the ELT process
     * @param group       String representing the group information
     * @param publishDate {@link LocalDateTime} instance rendered as String
     * @param sql         replaced and executable SQL
     * @return replaced and executable SQL
     */
    public static String fillPlaceHolders(@NonNull Long dbId, @NonNull String group, @NonNull LocalDateTime publishDate, @NonNull String sql) {
        final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);
        String newSql = sql.replaceAll(dvConfig.placeholderDbId(), dbId.toString());
        newSql = newSql.replaceAll(dvConfig.placeholderFileGroup(), group);
        newSql = newSql.replaceAll(dvConfig.placeholderPublishDate(), DF_PUB_DATE.format(publishDate));
        return newSql;
    }

    @Override
    boolean canProceed() {
        final String workerCountQuery = "select count(*) from MappingWorkerCargo c where " +
                "c.name = :mappingName and c.status = :status";
        final Long workerCount = session.createQuery(workerCountQuery, Long.class)
                .setParameter("mappingName", cargo.getName())
                .setParameter("status", WorkerCargo.Status.OKAY)
                .getSingleResult();

        if (workerCount != 1) {
            log.error("{}: Rejected - no successful MappingWorkerCargo for Mapping '{}' found", this, cargo.getName());
            addError("Rejected - Mapping not found");
            return false;
        }

        final Optional<Mapping> mapping = session.byNaturalId(Mapping.class)
                .using("name", cargo.getName())
                .loadOptional();

        if (!mapping.isPresent()) {
            log.error("{}: Rejected - internal error - required Mapping '{}' not found", this, cargo.getName());
            addError("Rejected - Mapping not found");
            return false;
        } else {
            this.mapping = mapping.get();
            return true;
        }
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return new WorkerInfo.Builder()
                .setRunId(getRunId())
                .setName(mappingName)
                .setWorkerClass(getClass())
                .create();
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", super.toString(), cargo.getName());
    }
}
