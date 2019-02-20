package de.areto.datachef.model.web;

import de.areto.datachef.model.mart.MartType;
import de.areto.datachef.model.worker.WorkerCargo;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class WorkerMartSQLListRow {
    private long dbId;
    private String martName;
    private MartType martType;
    private LocalDateTime executionDateTime;
    private WorkerCargo.Status status;
    private long runtime;
    private long payloadSize;

    public WorkerMartSQLListRow(long dbId, String martName, MartType martType, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize) {
        this.dbId = dbId;
        this.martName = martName;
        this.martType = martType;
        this.executionDateTime = executionDateTime;
        this.status = status;
        this.runtime = runtime;
        this.payloadSize = payloadSize;
    }
}
