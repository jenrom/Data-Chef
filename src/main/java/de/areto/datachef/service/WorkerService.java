package de.areto.datachef.service;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import de.areto.common.concurrent.PriorityExecutors;
import de.areto.datachef.config.DataChefConfig;
import de.areto.datachef.model.compilation.CompilationUnit;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.worker.*;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.scheduler.CronDataWorkerTrigger;
import de.areto.datachef.scheduler.MousetrapController;
import de.areto.datachef.worker.MappingDataSQLWorker;
import de.areto.datachef.worker.MappingDataWorker;
import de.areto.datachef.worker.MartDataWorker;
import de.areto.datachef.worker.Worker;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;
import org.knowm.sundial.SundialJobScheduler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class WorkerService extends AbstractIdleService {

    private static final String CRON_PACKAGE = "de.areto.datachef.scheduler";

    private final DataChefConfig dataChefConfig = ConfigCache.getOrCreate(DataChefConfig.class);

    @Getter
    private final MousetrapController mousetrapController;

    private final Queue<WorkerInfo> workerInfoQueue;
    private final ExecutorService workerExecutor;
    private final ExecutorService workerInfoRemovalExecutor;

    @Getter
    private boolean healthy = false;

    public WorkerService() {
        this.workerInfoRemovalExecutor = Executors.newSingleThreadExecutor();
        this.workerInfoQueue = Queues.synchronizedQueue(new LinkedList<>());
        this.workerExecutor = PriorityExecutors.newSingleThreadExecutor();
        this.mousetrapController = new MousetrapController(this::executeTrigger);
    }

    @Override
    protected void startUp() {
        SundialJobScheduler.startScheduler(CRON_PACKAGE);
        this.registerScheduledExecutions();
        healthy = true;
    }

    @Override
    protected void shutDown() {
        this.workerExecutor.shutdown();
        this.workerInfoRemovalExecutor.shutdown();
        this.mousetrapController.clear();

        SundialJobScheduler.shutdown();

        healthy = false;
    }

    private void registerScheduledExecutions() {
        if (!dataChefConfig.activateCronSchedules()) return;

        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final List<Mapping> mappings = session.createQuery("from Mapping m", Mapping.class)
                    .getResultList();
            for (Mapping m : mappings) {
                registerSchedulesAndTraps(m);
            }

            final List<Mart> marts = session.createQuery("from Mart m", Mart.class)
                    .getResultList();
            for (Mart m : marts) {
                registerSchedulesAndTraps(m);
            }
        }
    }

    private void registerSchedulesAndTraps(CompilationUnit compilationUnit) {
        if (dataChefConfig.activateCronSchedules() && compilationUnit.isTriggeredByCron()) {
            registerCronSchedule(compilationUnit);
        }
        if (dataChefConfig.activateMouseTraps() && compilationUnit.isTriggeredByMousetrap()) {
            registerMousetrap(compilationUnit);
        }
    }

    private void registerMousetrap(CompilationUnit compilationUnit) {
        final String type = compilationUnit instanceof Mapping ? "Mapping" : "Mart";
        checkState(compilationUnit.isValid(), "%s must be valid", type);
        checkState(compilationUnit.isTriggeredByMousetrap(), "%s must be triggered by Mousetrap", type);

        if(compilationUnit instanceof Mapping) {
            final Mapping mapping = (Mapping) compilationUnit;
            checkState(!mapping.getStagingMode().equals(StagingMode.FILE),
                    "Mousetrap triggered Mapping not compatible with staging mode: %s",
                    mapping.getStagingMode());
        }

        if (mousetrapController.contains(compilationUnit.getName()))
            return;

        final Duration timeout;

        switch (compilationUnit.getTimeoutUnit()) {
            case "sec":
                timeout = Duration.ofSeconds(compilationUnit.getMousetrapTimeout());
                break;
            case "min":
                timeout = Duration.ofMinutes(compilationUnit.getMousetrapTimeout());
                break;
            case "hour":
                timeout = Duration.ofHours(compilationUnit.getMousetrapTimeout());
                break;
            case "day":
                timeout = Duration.ofDays(compilationUnit.getMousetrapTimeout());
                break;
            default:
                throw new IllegalStateException("Timeout unit unknown");
        }

        mousetrapController.createTrap(compilationUnit.getName(), compilationUnit.getDependencyList(), timeout);

        log.info("{}[{}]: Registered trapped execution after {}", type,
                compilationUnit.getName(), compilationUnit.getDependencyList());
    }

    private void registerCronSchedule(CompilationUnit compilationUnit) {
        final String type = compilationUnit instanceof Mapping ? "Mapping" : "Mart";
        checkState(compilationUnit.isValid(), "%s must be valid", type);
        checkState(compilationUnit.isTriggeredByCron(), "%s must be triggered via CRON", type);

        if(compilationUnit instanceof Mapping) {
            final Mapping mapping = (Mapping) compilationUnit;
            checkState(!mapping.getStagingMode().equals(StagingMode.FILE),
                    "Cron triggered Mapping not compatible with staging mode: %s",
                    mapping.getStagingMode());
        }

        checkState(compilationUnit.isCronExpressionValid(), "Cron expression must be valid");

        final Map<String, Object> params = new HashMap<>();
        params.put("unitName", compilationUnit.getName());
        params.put("type", type);

        final String jobName = getJobName(compilationUnit);
        final String triggerName = getTriggerName(compilationUnit);
        final String jobClass = CronDataWorkerTrigger.class.getName();

        if (SundialJobScheduler.getAllJobNames().contains(jobName)) {
            final String msg = String.format("Scheduled execution for %s '%s' already registered", type, compilationUnit.getName());
            throw new IllegalStateException(msg);
        }

        SundialJobScheduler.addJob(jobName, jobClass, params, true);
        SundialJobScheduler.addCronTrigger(triggerName, jobName, compilationUnit.getCronExpression());
        log.info("{}[{}]: Registered scheduled execution: {}", type, compilationUnit.getName(), compilationUnit.describeCronExpression());
    }

    private void tidyUpRollback(@NonNull RollbackMappingWorkerCargo rollbackCargo) {
        if(rollbackCargo.getMapping() != null) {
            final Mapping mapping = rollbackCargo.getMapping();
            if (dataChefConfig.activateCronSchedules() && mapping.isTriggeredByCron()) {
                final String jobName = getJobName(mapping);
                final String triggerName = getTriggerName(mapping);
                SundialJobScheduler.stopJob(jobName);
                SundialJobScheduler.removeTrigger(triggerName);
                SundialJobScheduler.removeJob(jobName);
                log.info("Mapping[{}]: Removing scheduled execution", mapping.getName());
            }

            if (dataChefConfig.activateMouseTraps() && mapping.isTriggeredByMousetrap()) {
                mousetrapController.removeTrap(mapping.getName());
                log.info("Mapping[{}]: Removing trapped execution", mapping.getName());
            }
        }
    }

    private void tidyUpRollback(@NonNull RollbackMartWorkerCargo rollbackCargo) {
        if(rollbackCargo.getMart() != null) {
            final Mart mart = rollbackCargo.getMart();
            if (dataChefConfig.activateCronSchedules() && mart.isTriggeredByCron()) {
                final String jobName = getJobName(mart);
                final String triggerName = getTriggerName(mart);
                SundialJobScheduler.stopJob(jobName);
                SundialJobScheduler.removeTrigger(triggerName);
                SundialJobScheduler.removeJob(jobName);
                log.info("Mart[{}]: Removing scheduled execution", mart.getName());
            }

            if (dataChefConfig.activateMouseTraps() && mart.isTriggeredByMousetrap()) {
                mousetrapController.removeTrap(mart.getName());
                log.info("Mart[{}]: Removing trapped execution", mart.getName());
            }
        }
    }

    private static String getJobName(@NonNull CompilationUnit compilationUnit) {

        return String.format("job_execute_mapping_%s", compilationUnit.getName());
    }

    private static String getTriggerName(@NonNull CompilationUnit compilationUnit) {
        return String.format("trigger_%s", getJobName(compilationUnit));
    }

    public int getQueueSize() {
        return workerInfoQueue.size();
    }

    public boolean isBusy() {
        return !workerInfoQueue.isEmpty();
    }

    public List<WorkerInfo> getQueueSnapshot() {
        final List<WorkerInfo> snapshotList = new ArrayList<>(workerInfoQueue.size());
        snapshotList.addAll(workerInfoQueue);
        return snapshotList;
    }

    private void removeWorkerInfo(@NonNull WorkerInfo workerInfo) {
        if (!workerInfoQueue.remove(workerInfo))
            log.error("Unable to remove WorkerInfo with run id '{}'", workerInfo.getRunId());
    }

    public ListenableFuture<? extends WorkerCargo> executeTrigger(@NonNull String unitName) {
        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final Optional<CompilationUnit> unit = session.byNaturalId(CompilationUnit.class)
                    .using("name", unitName).loadOptional();

            if(!unit.isPresent()) {
                final String msg = String.format("Compilation Unit '%s' not found", unitName);
                throw new IllegalArgumentException(msg);
            }

            checkState(unit.get().isValid(), "Compilation Unit has to be valid");

            return unit.get() instanceof Mart ?
                execute(new MartDataWorker(unitName)) : execute(new MappingDataSQLWorker(unitName, -1));
        }
    }

    public ListenableFuture<? extends WorkerCargo> execute(@NonNull Worker<? extends WorkerCargo> worker) {
        checkState(healthy, "Service must be healthy");
        checkState(!worker.isDone(), "Worker cannot be executed twice");

        if(worker instanceof MappingDataWorker || worker instanceof MartDataWorker) {
            worker.addInThreadCallback((cargo) -> {
                if(healthy && cargo != null && cargo.getStatus().equals(WorkerCargo.Status.OKAY))
                    mousetrapController.notify(cargo.getName());
            });
        }

        final Future<? extends WorkerCargo> future = workerExecutor.submit(worker);
        final ListenableFuture<? extends WorkerCargo> listenableFuture = JdkFutureAdapters
                .listenInPoolThread(future, workerInfoRemovalExecutor);

        final WorkerInfo workerInfo = worker.getWorkerInfo();
        workerInfo.setQueuedDateTime(LocalDateTime.now());
        workerInfoQueue.add(workerInfo);

        Futures.addCallback(listenableFuture, new WorkerCallback(workerInfo) {
            @Override
            public void workerCallback() {
                removeWorkerInfo(this.getWorkerInfo());
            }

            @Override
            public void handleSuccessResult(WorkerCargo result) {
                final boolean isOkay = result.getStatus().equals(WorkerCargo.Status.OKAY);

                if(isOkay && result instanceof MartWorkerCargo) {
                    final MartWorkerCargo martWorkerCargo = (MartWorkerCargo) result;
                    registerSchedulesAndTraps(martWorkerCargo.getMart());
                }

                if (isOkay && result instanceof MappingWorkerCargo) {
                    final MappingWorkerCargo mappingCargo = (MappingWorkerCargo) result;
                    registerSchedulesAndTraps(mappingCargo.getMapping());
                }

                if (result instanceof RollbackMappingWorkerCargo) {
                    tidyUpRollback((RollbackMappingWorkerCargo) result);
                }

                if (result instanceof RollbackMartWorkerCargo) {
                    tidyUpRollback((RollbackMartWorkerCargo) result);
                }
            }
        }, workerInfoRemovalExecutor);

        return listenableFuture;
    }
}