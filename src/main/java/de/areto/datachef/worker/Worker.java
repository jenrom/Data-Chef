package de.areto.datachef.worker;

import com.google.common.base.Stopwatch;
import de.areto.common.concurrent.PriorityCallable;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.exceptions.DbSpoxException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpox;
import de.areto.datachef.jdbc.DbSpoxBuilder;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.compilation.SQLExpressionExecution;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.service.WorkerInThreadCallback;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;
import org.apache.commons.dbutils.DbUtils;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.resource.transaction.spi.TransactionStatus;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Slf4j
public abstract class Worker<T extends WorkerCargo> implements PriorityCallable<T> {

    @Getter
    private UUID runId = UUID.randomUUID();

    @Getter
    final T cargo;
    Session session;

    @Getter
    private boolean done = false;

    private final long priority;

    boolean saveExecutions = true;

    boolean createJobLog = true;

    DbSpox spox;

    Connection connection;

    private final List<WorkerInThreadCallback> inThreadCallbackList = new LinkedList<>();

    protected Worker(T cargo) {
        this(cargo, System.nanoTime());
    }

    protected Worker(T cargo, long priority) {
        this.cargo = cargo;
        this.priority = priority;
    }

    void addError(Exception e) {
        //e.printStackTrace();
        final StringBuilder builder = new StringBuilder();
        builder.append(e.getClass().getSimpleName());

        if (e.getMessage() != null) {
            builder.append(": ").append(e.getMessage());
        }

        cargo.addError(builder.toString());
    }

    void addError(String message, Object... args) {
        if (args.length > 0) {
            final String m = String.format(message, args);
            cargo.addError(m);
        } else {
            cargo.addError(message);
        }
    }

    public void addInThreadCallback(WorkerInThreadCallback callback) {
        this.inThreadCallbackList.add(callback);
    }

    void executeExpressionSilently(SQLExpression expression) {
        try {
            spox.executeScript(connection, expression.getSqlCode());
        } catch (SQLException e) {
            final String errMsg = e.getMessage() == null ? "" : ", reason: " + e.getMessage();
            final String msg = String.format("State: %s, Code: %d%s", e.getSQLState(), e.getErrorCode(), errMsg);
            addError(msg);
        } catch (IOException e) {
            addError(e);
        }
    }

    void executeSQLExpression(@NonNull SQLExpression expression) {
        executeSQLExpression(expression, expression.getSqlCode());
    }

    void executeSQLExpression(@NonNull SQLExpression expr, @NonNull String customCode) {
        final SQLExpressionExecution execution = new SQLExpressionExecution();
        execution.setExpression(expr);
        execution.setCustomSqlCode(customCode);
        execution.setExecutedTime(LocalDateTime.now());

        final Stopwatch stopwatch = Stopwatch.createStarted();

        try {
            final int[] results = spox.executeScript(connection, execution.getCustomSqlCode());
            final int updateCount = Arrays.stream(results).sum();
            execution.setUpdateCount(updateCount);
            execution.setErrorFlag(false);
            cargo.setPayloadSize(cargo.getPayloadSize() + updateCount);
        } catch (SQLException e) {
            execution.setUpdateCount(-1);
            execution.setErrorFlag(true);
            final String errMsg = e.getMessage() == null ? "" : ", reason: " + e.getMessage();
            final String msg = String.format("State: %s, Code: %d%s", e.getSQLState(), e.getErrorCode(), errMsg);
            execution.setErrorMessage(msg);

            if (log.isDebugEnabled()) log.debug("{}: {}", this, msg);

            final String globalErrorMsg = String.format("SQLExpression #%d resulted in an error", expr.getOrderNumber());
            addError(globalErrorMsg);
        } catch (IOException e) {
            execution.setUpdateCount(-1);
            execution.setErrorFlag(true);
            final String errMsg = e.getMessage() == null ? "" : ", reason: " + e.getMessage();
            execution.setErrorMessage(errMsg);

            if (log.isDebugEnabled()) log.debug("{}: {}", this, errMsg);

            final String globalErrorMsg = String.format("SQLExpression #%d resulted in an error", expr.getOrderNumber());
            addError(globalErrorMsg);
        } finally {
            execution.setRuntime(stopwatch.elapsed(TimeUnit.MILLISECONDS));

            if (saveExecutions)
                cargo.addExecution(execution);
        }
    }

    abstract void doWork() throws Exception;

    abstract boolean canProceed();

    public abstract WorkerInfo getWorkerInfo();

    @Override
    public long getPriority() {
        return priority;
    }

    @Override
    public T call() throws Exception {
        checkState(!done, "Worker already executed");

        if (!DWHSpox.get().isHealthy()) {
            throw new DbSpoxException("Data Warehouse is not reachable, rejecting...");
        }

        final Stopwatch watch = Stopwatch.createStarted();
        session = HibernateUtility.getSessionFactory().openSession();
        try {
            final boolean canProceed = canProceed();
            if (!canProceed) cargo.setStatus(WorkerCargo.Status.REJECTED);
            if (canProceed && setupSpox()) doTransactionalWork();
        } finally {
            DbUtils.closeQuietly(connection);
            session.close();
        }

        if (!inThreadCallbackList.isEmpty()) {
            for (WorkerInThreadCallback callback : inThreadCallbackList)
                callback.execute(cargo);
        }

        done = true;

        if (log.isInfoEnabled())
            log.info("{}: {} - execution took {}", this, cargo.getStatus().toString(), watch.stop());

        return cargo;
    }

    private boolean setupSpox() {
        final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
        spox = new DbSpoxBuilder().useConfig(dwhConfig).build();
        if (!spox.isReachable()) {
            addError("DWH is not reachable");
            return false;
        }

        try {
            connection = spox.getConnection();
            return true;
        } catch (SQLException e) {
            addError(e);
            return false;
        }
    }

    private void doTransactionalWork() {
        final Transaction transaction = session.beginTransaction();

        // Persisting cargo results in getting a dbId from Hibernate which will be used as ID for the ELT process...
        session.persist(cargo);

        cargo.setExecutionStartDateTime(LocalDateTime.now());
        try {
            executePreStatement();
            doWork();
            executePostStatement();
        } catch (Exception e) {
            this.addError(e);
        }
        cargo.setExecutionEndDateTime(LocalDateTime.now());

        if (cargo.hasErrors()) {
            cargo.setStatus(WorkerCargo.Status.ERROR);
        } else if (cargo.hasWarnings()) {
            cargo.setStatus(WorkerCargo.Status.WARNING);
        } else {
            cargo.setStatus(WorkerCargo.Status.OKAY);
        }

        if (createJobLog && !createJobLogEntry()) {
            cargo.setStatus(WorkerCargo.Status.ERROR);
        }

        if (transaction.getStatus().isNotOneOf(TransactionStatus.MARKED_ROLLBACK)) {
            transaction.commit();
        } else {
            transaction.rollback();
            addError("Rollback of transaction; Repository unstable!");
        }
    }

    private void executePostStatement() throws IOException, SQLException, RenderingException {
        if (!ConfigCache.getOrCreate(TemplateConfig.class).enablePostRunScript())
            return;

        checkNotNull(spox, "Spox not initialized");
        checkNotNull(connection, "Connection not initialized");

        final String preSql = MaintenanceStatementFactory.createWorkerPostRunStatement();
        int[] res = spox.executeScript(connection, preSql);

        cargo.addMessage(String.format("Successfully executed Post Run Script with %d statements", res.length));
    }

    private void executePreStatement() throws IOException, SQLException, RenderingException {
        if (!ConfigCache.getOrCreate(TemplateConfig.class).enablePreRunScript())
            return;

        checkNotNull(spox, "Spox not initialized");
        checkNotNull(connection, "Connection not initialized");

        final String preSql = MaintenanceStatementFactory.createWorkerPreRunStatement();
        int[] res = spox.executeScript(connection, preSql);

        cargo.addMessage(String.format("Successfully executed Pre Run Script with %d statements", res.length));
    }

    private boolean createJobLogEntry() {
        checkState(createJobLog, "Worker should not create a Job Log entry");
        checkNotNull(spox, "Spox not initialized");
        checkNotNull(connection, "Connection not initialized");

        try {
            final String insSql = MaintenanceStatementFactory.createInsertStatement(cargo);
            spox.executeScript(connection, insSql);
            return true;
        } catch (Exception e) {
            final String error = String.format("Unable to create job log entry. Reason %s%s",
                    e.getClass().getSimpleName(),
                    e.getMessage() == null ? "" : ", reason: " + e.getMessage());
            addError(error);
            return false;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Worker<?> worker = (Worker<?>) o;
        return Objects.equals(runId, worker.runId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId);
    }
}