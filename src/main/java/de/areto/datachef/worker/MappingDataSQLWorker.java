package de.areto.datachef.worker;

import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.worker.MappingDataSQLWorkerCargo;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class MappingDataSQLWorker extends MappingDataWorker<MappingDataSQLWorkerCargo> {

    public MappingDataSQLWorker(String mappingName) {
        super(new MappingDataSQLWorkerCargo(), mappingName);
    }

    public MappingDataSQLWorker(String mappingName, long priority) {
        super(new MappingDataSQLWorkerCargo(), mappingName, priority);
    }

    @Override
    boolean canProceed() {
        if(!super.canProceed()) return false;

        final boolean correctMode = mapping.getStagingMode().equals(StagingMode.CONNECTION)
                || mapping.getStagingMode().equals(StagingMode.INSERT);

        if (!correctMode) {
            addError("Staging mode must be 'CONNECTION' or 'INSERT'");
            return false;
        } else {
            return true;
        }
    }

    @Override
    void doWork() {
        checkNotNull(mapping, "Mapping must be retrieved");

        cargo.setStagingMode(mapping.getStagingMode());

        log.info("{}: Staging via '{}'", this, mapping.getStagingMode());

        for (SQLExpression expression : mapping.getManipulationExpressionsSorted()) {
            String sqlCode = expression.getSqlCode();

            if (expression.getQueryType().equals(SQLExpression.QueryType.RAW_2_COOKED)) {
                final String group = mapping.getStagingMode().toString();
                final LocalDateTime publishDate = cargo.getExecutionStartDateTime();
                sqlCode = MappingDataWorker.fillPlaceHolders(cargo.getDbId(), group, publishDate, sqlCode);
            }

            executeSQLExpression(expression, sqlCode);
        }

        if (log.isInfoEnabled()) {
            log.info("{}: Processed {} lines", this, cargo.getPayloadSize());
        }
    }
}
