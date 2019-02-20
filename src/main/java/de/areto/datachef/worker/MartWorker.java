package de.areto.datachef.worker;

import com.google.common.base.Stopwatch;
import de.areto.datachef.creator.MartDDLQueueCreator;
import de.areto.datachef.creator.MartDMLQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.TriggerMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.worker.MartWorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import de.areto.datachef.parser.MartScriptParser;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;


@Slf4j
public class MartWorker extends Worker<MartWorkerCargo> {

    private final String martName;
    private final SinkFile sinkFile;
    private String martScript;
    private Mart mart;
    private Queue<SQLExpression> ddlQueue;
    private Queue<SQLExpression> dmlQueue;

    /* Debugging:
    public MartWorker(@NonNull String martScript, @NonNull String martName) {
        super(new MartWorkerCargo());
        this.martScript = martScript;
        this.martName = martName;
    }
    */

    public MartWorker(@NonNull SinkFile martScriptFile) {
        super(new MartWorkerCargo());
        checkState(!martScriptFile.getMappingName().contains(" "), "Spaces in Mart Name are prohibited");
        this.martName = martScriptFile.getMappingName(); // reflect mart object here
        this.sinkFile = martScriptFile;
    }

    @Override
    boolean canProceed() {
        try {
            this.martScript = sinkFile.getContentString();
            this.cargo.setCheckSum(sinkFile.getCheckSum());
        } catch (IOException e) {
            addError(e);
            return false;
        }

        final Long cargoCount = session.createQuery("select count(*) from MartWorkerCargo c where c.name = :name", Long.class)
                .setParameter("name", martName)
                .getSingleResult();

        final Long martCount = session.createQuery("select count(*) from Mart m where m.name = :name", Long.class)
                .setParameter("name", martName)
                .getSingleResult();

        if (cargoCount == 0 && martCount == 0) {
            return true;
        } else {
            log.error("{}: MartWorkerCargo '{}' already processed, please rollback", this, martName);
            addError("Rejected - Already processed");
            return false;
        }
    }

    @Override
    void doWork() throws Exception {
        log.info("{}: Starting...", this);

        if (!parseAndPersistMart()) return;
        if (!createDdlExpressions()) return;
        if (!createDmlExpressions()) return;

        if (!cargo.hasErrors()) {
            executeDdlExpressions();
        }
    }

    private boolean parseAndPersistMart() {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        this.cargo.setName(martName);
        final MartScriptParser parser = new MartScriptParser(martName, session);
        this.mart = parser.parse(martScript);

        if (!mart.isValid()) {
            for (String issue : mart.getIssueList())
                addError(issue);
            return false;
        }

        session.persist(mart);

        this.cargo.setMart(mart);
        this.cargo.setMartType(mart.getMartType());
        this.cargo.setTriggeredByCron(mart.isTriggeredByCron());

        cargo.addMessage(String.format("Creating new mart (%s) %s", mart.getMartType(), mart.getName()));

        log.info("{}: Generated Mart ({}) {} in {}", this, this.mart.getMartType(), mart.getName() , stopwatch.stop());
        return true;
    }

    private boolean createDdlExpressions() {

        try {
            final MartDDLQueueCreator martDdlQueueCreator = new MartDDLQueueCreator(mart);
            this.ddlQueue = martDdlQueueCreator.createExpressionQueue();

            int defCounter = 1;

            for (SQLExpression expression : ddlQueue) {
                mart.addDefinitionExpression(expression, defCounter);
                defCounter++;
            }

            return true;
        } catch (CreatorException e){
            addError(e);
            return false;
        }
    }

    private void executeDdlExpressions() {
        for (SQLExpression expression : this.mart.getDefinitionExpressionsSorted()) {
            executeSQLExpression(expression);
        }
    }

    private boolean createDmlExpressions() {
        try {
            final MartDMLQueueCreator martDmlQueueCreator = new MartDMLQueueCreator(mart);
            this.dmlQueue = martDmlQueueCreator.createExpressionQueue();

            int defCounter = 1;

            for (SQLExpression expression : dmlQueue) {
                mart.addManipulationExpression(expression, defCounter);
                defCounter++;
            }

            return true;
        } catch (CreatorException e){
            addError(e);
            return false;
        }
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return new WorkerInfo.Builder()
                .setRunId(getRunId())
                .setName(martName)
                .setFileName(sinkFile.getFileName())
                .setWorkerClass(getClass())
                .create();
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", super.toString(), martName);
    }
}
