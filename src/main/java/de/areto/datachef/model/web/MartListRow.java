package de.areto.datachef.model.web;

import de.areto.datachef.model.mart.MartType;
import de.areto.datachef.model.mart.TriggerMode;
import de.areto.datachef.model.worker.WorkerCargo;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class MartListRow {
    private long dbId;
    private String martName;
    private String type;
    private WorkerCargo.Status status;
    private LocalDateTime executionDateTime;
    private long runtime;
    private String triggerMode;

    // " m.dbId, m.name, m.executionStartDateTime, m.status, m.runtime, m.mart.martType, m.mart.triggeredByCron " +

    public MartListRow(long dbId, String martName, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, MartType martType, boolean triggeredByCron) {
        this.dbId = dbId;
        this.martName = martName;
        this.type = martType != null ? martType.toString() : "n/a";
        this.status = status;
        this.executionDateTime = executionDateTime;
        this.runtime = runtime;

        if(martType != null && triggeredByCron)
            this.triggerMode = "CRON";
        else if(martType != null && !triggeredByCron)
            this.triggerMode = "MOUSETRAP";
        else
            this.triggerMode = "n/a";
    }
}
