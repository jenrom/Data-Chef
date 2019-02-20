package de.areto.datachef.worker;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.jdbc.DBObjectType;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.worker.DataWorkerCargo;
import de.areto.datachef.model.worker.MartWorkerCargo;
import de.areto.datachef.model.worker.RollbackMartWorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static de.areto.datachef.creator.expression.TableExpressionFactory.dropTableExpression;

@Slf4j
public class RollbackMartWorker extends RollbackWorker<RollbackMartWorkerCargo> {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);

    private final String martName;
    private MartWorkerCargo martWorkerCargo;
    private Mart mart;

    private final List<Long> updatableIds = new ArrayList<>();

    public RollbackMartWorker(@NonNull String martName) {
        super(new RollbackMartWorkerCargo());
        this.cargo.setRollbackType("Mart");
        this.martName = martName;
        cargo.setName(martName);
        this.saveExecutions = false;
    }

    @Override
    void doWork() throws Exception {
        log.info("{}: Starting rollback", this);

        if (martWorkerCargo.getMart() != null) {
            rollbackMart();
        } else {
            updatableIds.add(martWorkerCargo.getDbId());
            session.delete(martWorkerCargo);
        }

        updateJobLog(updatableIds);

        dropMartTable(dwhConfig.schemaNameDiner());

        moveFiles(martName);
    }

    private void rollbackMart() throws CreatorException {
        checkNotNull(martWorkerCargo.getMart(), "Mart must be present");

        cargo.setMart(martWorkerCargo.getMart());

        // Drop DataWorkerCargo and referenced SQLExpressionExecutions
        final String qDataWorker = "from DataWorkerCargo w where w.name = :martName";
        final List<DataWorkerCargo> dataCargoList = session.createQuery(qDataWorker, DataWorkerCargo.class)
                .setParameter("martName", martName).getResultList();

        for (DataWorkerCargo dataCargo : dataCargoList) {
            cargo.addMessage("Removing DataWorkerCargo #" + dataCargo.getDbId());
            updatableIds.add(dataCargo.getDbId());
            session.delete(dataCargo);
        }

        // Drop MartWorkerCargo and referenced SQLExpressionExecutions
        updatableIds.add(martWorkerCargo.getDbId());
        session.delete(martWorkerCargo);

        // Delete Mart, MartColumn
        session.delete(mart);
    }

    private void dropMartTable(String schemaName) throws SQLException, CreatorException {
        final Collection<DBTable> martTables = spox.getObjects(schemaName, null, DBObjectType.TABLE);

        if (martTables.stream().anyMatch(t -> t.getName().equals(martName))) {
            final String message = String.format("Dropping Mart %s.%s", schemaName, martName);
            cargo.addMessage(message);
            final SQLExpression dropDinerExpression = dropTableExpression(schemaName, martName);
            executeExpressionSilently(dropDinerExpression);
        }
    }

    @Override
    boolean canProceed() {
        final String qWorker = "from MartWorkerCargo c where c.name = :name";

        martWorkerCargo = session.createQuery(qWorker, MartWorkerCargo.class)
                .setParameter("name", martName)
                .getSingleResult();

        if (martWorkerCargo == null) {
            log.error("Rejecting - no executed MartWorkerCargo for Mart '{}'", martName);
            addError("Rejected - Requested MartWorkerCargo not found");
            return false;
        }

        mart = martWorkerCargo.getMart();

        if(mart == null) return true; // Rotten WorkerCargo...

        final String qDependency = "select m.name from CompilationUnit m where :name in elements(m.dependencyList)";
        final List<String> depNames = session.createQuery(qDependency, String.class)
                .setParameter("name", mart.getName())
                .getResultList();
        if(!depNames.isEmpty()) {
            log.error("Rejecting - Mart depends on '{}", depNames);
            addError(String.format("Rejected - Mart depends on '%s'", depNames));
            return false;
        }

        return true;
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return new WorkerInfo.Builder().setRunId(getRunId())
                .setName(martName)
                .setWorkerClass(getClass())
                .create();
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", super.toString(), martName);
    }
}
