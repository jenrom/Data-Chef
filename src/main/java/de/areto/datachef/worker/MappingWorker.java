package de.areto.datachef.worker;

import com.google.common.base.Stopwatch;
import de.areto.datachef.comparators.DVObjectLoadComparator;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.creator.DDLQueueCreator;
import de.areto.datachef.creator.DMLQueueCreator;
import de.areto.datachef.creator.ViewQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.jdbc.DbType;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.worker.MappingWorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import de.areto.datachef.parser.SinkScriptParser;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

@Slf4j
public class MappingWorker extends Worker<MappingWorkerCargo> {

    private final String mappingName;
    private long generatedObjects;
    private boolean fileMode;

    private Mapping mapping;
    private String mappingCode;

    @Getter
    private SinkFile sinkFile;

    private Queue<SQLExpression> ddlQueue;


    public MappingWorker(@NonNull SinkFile file) {
        this(file.getMappingName());
        this.sinkFile = file;
        this.fileMode = true;
    }

    public MappingWorker(@NonNull String mappingCode, @NonNull String mappingName) {
        this(mappingName);
        this.fileMode = false;
        this.mappingCode = mappingCode;
    }

    private MappingWorker(@NonNull String mappingName) {
        super(new MappingWorkerCargo());
        this.mappingName = mappingName;
        this.cargo.setName(mappingName);
    }

    @Override
    boolean canProceed() {
        final Long cargoCount = session.createQuery("select count(*) from MappingWorkerCargo c where c.name = :name", Long.class)
                .setParameter("name", mappingName)
                .getSingleResult();

        final Long mappingCount = session.createQuery("select count(*) from Mapping m where m.name = :name", Long.class)
                .setParameter("name", mappingName)
                .getSingleResult();

        if (cargoCount == 0 && mappingCount == 0) {
            return true;
        } else {
            log.error("{}: MappingWorkerCargo '{}' already processed, please rollback", this, cargo.getName());
            addError("Rejected - Already processed");
            return false;
        }
    }

    @Override
    void doWork() {
        if (fileMode) {
            try {
                this.mappingCode = sinkFile.getContentString();
                this.cargo.setCheckSum(sinkFile.getCheckSum());
            } catch (IOException e) {
                addError(e);
                return;
            }
        }

        log.info("{}: Starting...", this);

        if (!parseAndPersistMapping()) return;
        if (!createExpressions()) return;

        if (!cargo.hasErrors()) {
            executeExpressions();
        }

        cargo.setPayloadSize(generatedObjects);
    }

    private void executeExpressions() {
        /// !!! DDL !!!
        for (SQLExpression expression : ddlQueue) {
            executeSQLExpression(expression);
        }

        /// !!! VIEWS !!!
        try {
            final ViewQueueCreator viewQueueCreator = new ViewQueueCreator(mapping, session);
            final Queue<SQLExpression> viewQueue = viewQueueCreator.createExpressionQueue();
            for (SQLExpression viewExpr : viewQueue) {
                cargo.addMessage("Creating: " + viewExpr.getDescription());
                executeExpressionSilently(viewExpr);
            }
        } catch (CreatorException e) {
            addError(e);
        }
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return new WorkerInfo.Builder()
                .setRunId(getRunId())
                .setName(mappingName)
                .setFileName(fileMode ? sinkFile.getFileName() : "")
                .setWorkerClass(getClass())
                .create();
    }

    private boolean parseAndPersistMapping() {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final SinkScriptParser parser = new SinkScriptParser(mappingName, session);
        this.mapping = parser.parse(mappingCode);

        if (!mapping.isValid()) {
            for (String issue : mapping.getIssueList())
                addError(issue);
            return false;
        }

        final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
        if (dwhConfig.dbType().equals(DbType.SNOWFLAKE) || dwhConfig.dbType().equals(DbType.GENERIC_JDBC)) {
            if (mapping.getStagingMode().equals(StagingMode.CONNECTION)) {
                final String msgTpl = "Mapping: '%s' could not be successfully executed, because import via connection is not supported yet by the Data Warehause";
                final String msg = String.format(msgTpl, mappingName);
                addError(msg);
                return false;
            }
        }

        final List<DVObject> objects = mapping.getMappedObjects().stream()
                .sorted(new DVObjectLoadComparator()).collect(Collectors.toList());

        for (DVObject o : objects) {
            session.persist(o);
        }

        session.persist(mapping);
        this.cargo.setMapping(mapping);
        this.cargo.setStagingMode(mapping.getStagingMode());

        this.generatedObjects = mapping.getMappedObjects().size();

        for (DVObject object : mapping.getNewObjects()) {
            cargo.addMessage(String.format("Creating new object (%s) %s", object.getType(), object.getName()));
        }

        log.info("{}: Generated {} objects in {}", this, this.generatedObjects, stopwatch.stop());
        return true;
    }

    private boolean createExpressions() {
        try {
            final DDLQueueCreator ddlQueueCreator = new DDLQueueCreator(mapping);
            this.ddlQueue = ddlQueueCreator.createExpressionQueue();

            int defCounter = 1, manCounter = 1;

            for (SQLExpression expression : ddlQueue) {
                mapping.addDefinitionExpression(expression, defCounter);
                defCounter++;
            }

            final DMLQueueCreator dmlQueueCreator = new DMLQueueCreator(mapping);
            final Queue<SQLExpression> dmlQueue = dmlQueueCreator.createExpressionQueue();

            for (SQLExpression expression : dmlQueue) {
                mapping.addManipulationExpression(expression, manCounter);
                manCounter++;
            }

            return true;
        } catch (CreatorException e) {
            addError(e);
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", super.toString(), mappingName);
    }
}