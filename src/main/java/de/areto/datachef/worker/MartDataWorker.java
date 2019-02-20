package de.areto.datachef.worker;

import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.worker.MartDataWorkerCargo;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class MartDataWorker extends DataWorker<MartDataWorkerCargo> {
    protected Mart mart;

    public MartDataWorker(String martName) {
        super(new MartDataWorkerCargo());
        this.cargo.setName(martName);
    }

    @Override
    void doWork() throws Exception {
        checkNotNull(mart, "Mart must be retrieved");

        cargo.setMartType(mart.getMartType());

        log.info("{}: Loading via '{}'", this, mart.getMartType());

        for (SQLExpression expression : mart.getManipulationExpressionsSorted()) {
            String sqlCode = expression.getSqlCode();
            executeSQLExpression(expression, sqlCode);
        }

        if (log.isInfoEnabled()) {
            log.info("{}: Processed {} lines", this, cargo.getPayloadSize());
        }
    }

    @Override
    boolean canProceed() {
        final String workerCountQuery = "select count(*) from MartWorkerCargo c where " +
                "c.name = :martName and c.status = :status";
        final Long workerCount = session.createQuery(workerCountQuery, Long.class)
                .setParameter("martName", cargo.getName())
                .setParameter("status", WorkerCargo.Status.OKAY)
                .getSingleResult();

        if (workerCount != 1) {
            log.error("{}: Rejected - no successful MartWorkerCargo for Mart '{}' found", this, cargo.getName());
            addError("Rejected - Mart not found");
            return false;
        }

        final Optional<Mart> mart = session.byNaturalId(Mart.class)
                .using("name", cargo.getName())
                .loadOptional();

        if (!mart.isPresent()) {
            log.error("{}: Rejected - internal error - required Mart '{}' not found", this, cargo.getName());
            addError("Rejected - Mart not found");
            return false;
        } else {
            this.mart = mart.get();
            return true;
        }
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return new WorkerInfo.Builder()
                .setRunId(getRunId())
                .setName(cargo.getName())
                .setWorkerClass(getClass())
                .create();
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", super.toString(), cargo.getName());
    }
}
