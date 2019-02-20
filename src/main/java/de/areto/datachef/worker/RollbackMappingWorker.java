package de.areto.datachef.worker;

import de.areto.datachef.comparators.DVObjectDeleteComparator;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.creator.ViewQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.jdbc.DBObjectType;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.worker.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static de.areto.datachef.creator.expression.TableExpressionFactory.dropTableExpression;
import static de.areto.datachef.creator.expression.TableExpressionFactory.dropViewExpression;

@Slf4j
public class RollbackMappingWorker extends RollbackWorker<RollbackMappingWorkerCargo> {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);

    private final String mappingName;
    private MappingWorkerCargo mappingWorkerCargo;
    private Mapping mapping;

    private final List<Long> updatableIds = new ArrayList<>();

    public RollbackMappingWorker(@NonNull String mappingName) {
        super(new RollbackMappingWorkerCargo());
        this.cargo.setRollbackType("Mapping");
        this.mappingName = mappingName;
        cargo.setName(mappingName);
        this.saveExecutions = false;
    }

    @Override
    void doWork() throws Exception {
        log.info("{}: Starting rollback", this);

        if (mappingWorkerCargo.getMapping() != null) {
            rollbackMapping();
        } else {
            updatableIds.add(mappingWorkerCargo.getDbId());
            session.delete(mappingWorkerCargo);
        }

        updateJobLog(updatableIds);

        dropStage(dwhConfig.schemaNameRaw());
        dropStage(dwhConfig.schemaNameCooked());

        moveFiles(mappingName);
    }

    private void dropStage(String schemaName) throws SQLException, CreatorException {
        final Collection<DBTable> stageTables = spox.getObjects(schemaName, null, DBObjectType.TABLE);

        long rawTableCount = stageTables.stream().filter(t -> t.getName().equals(mappingName)).count();
        if (rawTableCount == 1) {
            final String message = String.format("Dropping Stage %s.%s", schemaName, mappingName);
            cargo.addMessage(message);
            final SQLExpression dropRawExpression = dropTableExpression(schemaName, mappingName);
            executeExpressionSilently(dropRawExpression);
        }
    }

    private void rollbackMapping() throws CreatorException {
        checkNotNull(mappingWorkerCargo.getMapping(), "Mapping must be present");

        cargo.setMapping(mappingWorkerCargo.getMapping());

        // Drop DataWorkerCargo and referenced SQLExpressionExecutions
        final String qDataWorker = "from DataWorkerCargo w where w.name = :mappingName";
        final List<DataWorkerCargo> dataCargoList = session.createQuery(qDataWorker, DataWorkerCargo.class)
                .setParameter("mappingName", mappingName).getResultList();

        for (DataWorkerCargo dataCargo : dataCargoList) {
            cargo.addMessage("Removing MappingDataFileWorkerCargo #" + dataCargo.getDbId());
            updatableIds.add(dataCargo.getDbId());
            session.delete(dataCargo);
        }

        final Collection<DVObject> mappedObjects = mapping.getMappedObjects();

        // Count references to mappings
        final String qObjectCount = "select " +
                "new " + ObjectCount.class.getName() + "(r.object, count(distinct r.mapping.dbId)) " +
                "from MappingObjectReference r " +
                "where r.object in :objects " +
                "group by r.object.dbId";

        final List<ObjectCount> objectReferenceCounts = session.createQuery(qObjectCount, ObjectCount.class)
                .setParameter("objects", mappedObjects)
                .getResultList();

        // Drop MappingWorkerCargo and referenced SQLExpressionExecutions
        updatableIds.add(mappingWorkerCargo.getDbId());
        session.delete(mappingWorkerCargo);

        // Delete Mapping, MappingColumn, MappingColumnReferences, MappingObjectReferences
        session.delete(mapping);

        // Drop not shared DVObjects
        final List<DVObject> dropList = objectReferenceCounts
                .stream().filter(c -> c.getCount().equals(1L))
                .map(ObjectCount::getObject)
                .sorted(new DVObjectDeleteComparator())
                .collect(Collectors.toList());

        for (DVObject object : dropList) {
            session.delete(object);
        }

        // Cache shared DVObjects
        final Set<DVObject> sharedObjects = new HashSet<>(mappedObjects);
        sharedObjects.removeAll(dropList);

        if (dropList.size() < mappedObjects.size()) {
            final String warnList = sharedObjects.stream().map(DVObject::toString)
                    .collect(Collectors.joining(","));

            final String warning = String.format("Objects [%s] may still contain data from this mapping", warnList);
            cargo.addWarning(warning);
        }

        // Drop History Satellites tables
        for (DVObject object : dropList) {
            if (object.isLink() && object.asLink().isHistoricized()) {
                final String message = String.format("Dropping History Satellite for %s %s",
                        object.getType(), object.getName());
                cargo.addMessage(message);
                final String tabName = dvConfig.satNamePrefix() + object.getName() + dvConfig.histSatSuffix();
                final SQLExpression expression = dropTableExpression(dwhConfig.schemaNameServed(), tabName);
                executeExpressionSilently(expression);
            }
        }

        // Drop DVObject tables
        for (DVObject object : dropList) {
            final String message = String.format("Dropping %s %s", object.getType(), object.getName());
            cargo.addMessage(message);
            final SQLExpression dropExpression = dropTableExpression(object);
            executeExpressionSilently(dropExpression);
        }

        // Drop views
        dropViews(mappedObjects);

        cargo.setPayloadSize((long) dropList.size());

        // Recreate Views for objects referenced by other Mappings...
        // Necessary, because Satellites could be dropped that are still joined in the view
        final Queue<SQLExpression> viewQueue = new ViewQueueCreator(sharedObjects, session).createExpressionQueue();
        for (SQLExpression viewExpr : viewQueue) {
            cargo.addMessage("Recreating: " + viewExpr.getDescription());
            executeExpressionSilently(viewExpr);
        }
    }

    private void dropViews(@NonNull Collection<DVObject> dropSet) {
        final String viewSchema = dwhConfig.schemaNameViews();
        try {
            final Collection<DBTable> views = spox.getObjects(viewSchema, null, DBObjectType.VIEW);
            for (DVObject o : dropSet) {
                if (o.isSatellite()) continue;

                for (DBTable view : views) {
                    if (view.getName().contains(o.getNamePrefix() + o.getName())) {
                        try {
                            final SQLExpression dropExpr = dropViewExpression(viewSchema, view.getName());
                            executeExpressionSilently(dropExpr);
                            cargo.addMessage(String.format("Dropping View %s.%s", viewSchema, view.getName()));
                        } catch (CreatorException e) {
                            final String msg = String.format("Unable to drop View %s.%s", viewSchema, view.getName());
                            addError(msg);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            final String msg = String.format("Unable to retrieve views in schema %s", dwhConfig.schemaNameViews());
            addError(msg);
        }
    }

    @Override
    boolean canProceed() {
        final String qWorker = "from MappingWorkerCargo c where c.name = :name";

        mappingWorkerCargo = session.createQuery(qWorker, MappingWorkerCargo.class)
                .setParameter("name", mappingName)
                .getSingleResult();

        if (mappingWorkerCargo == null) {
            log.error("Rejecting - no executed MappingWorkerCargo for Mapping '{}'", mappingName);
            addError("Rejected - Requested MappingWorkerCargo not found");
            return false;
        }

        if(mappingWorkerCargo.getMapping() == null)
            return true;

        mapping = mappingWorkerCargo.getMapping();

        final String qDependency = "select m.name from CompilationUnit m where :name in elements(m.dependencyList)";
        final List<String> depNames = session.createQuery(qDependency, String.class)
                .setParameter("name", mapping.getName())
                .getResultList();
        if(!depNames.isEmpty()) {
            log.error("Rejecting - Mapping depends on '{}", depNames);
            addError(String.format("Rejected - Mapping depends on '%s'", depNames));
            return false;
        }

        return true;
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return new WorkerInfo.Builder().setRunId(getRunId())
                .setName(mappingName)
                .setWorkerClass(getClass())
                .create();
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", super.toString(), mappingName);
    }
}
