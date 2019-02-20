package de.areto.datachef.scheduler;

import com.google.common.util.concurrent.ListenableFuture;
import de.areto.datachef.Application;
import de.areto.datachef.model.worker.WorkerCargo;
import org.knowm.sundial.Job;
import org.knowm.sundial.exceptions.JobInterruptException;

import static com.google.common.base.Preconditions.checkNotNull;

public class CronDataWorkerTrigger extends Job implements DataWorkerTrigger {

    @Override
    public void doRun() throws JobInterruptException {
        final String unitName = this.getJobContext().get("unitName");
        checkNotNull(unitName, "Name of Compilation Unit has to be provided");
        trigger(unitName);
    }

    @Override
    public ListenableFuture<? extends WorkerCargo> trigger(String name) {
        return Application.get().getWorkerService().executeTrigger(name);
    }
}
