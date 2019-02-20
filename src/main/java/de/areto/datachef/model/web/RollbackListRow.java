package de.areto.datachef.model.web;

import de.areto.datachef.model.worker.WorkerCargo;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class RollbackListRow {

    private long dbId;
    private String type;
    private String name;
    private LocalDateTime executionDateTime;
    private WorkerCargo.Status status;
    private long runtime;
    private long payloadSize;

    public RollbackListRow(long dbId, String cargoType, String name, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize) {
        this.dbId = dbId;
        this.type = cargoType;
        this.name = name;
        this.executionDateTime = executionDateTime;
        this.status = status;
        this.runtime = runtime;
        this.payloadSize = payloadSize;
    }
}
