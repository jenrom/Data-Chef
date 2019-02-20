package de.areto.datachef.scheduler;

import de.areto.datachef.Application;
import lombok.extern.slf4j.Slf4j;
import org.knowm.sundial.Job;
import org.knowm.sundial.annotations.CronTrigger;
import org.knowm.sundial.exceptions.JobInterruptException;

@CronTrigger(cron = "0/10 * * * * ?")
@Slf4j
public class MousetrapChecker extends Job {
    @Override
    public void doRun() throws JobInterruptException {
        if(log.isDebugEnabled()) log.debug("Checking Mousetraps...");
        Application.get().getWorkerService().getMousetrapController().checkTraps();
    }
}
