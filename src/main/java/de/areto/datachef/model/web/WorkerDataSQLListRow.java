package de.areto.datachef.model.web;

import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.worker.WorkerCargo;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class WorkerDataSQLListRow {

    private long dbId;
    private String mappingName;
    private StagingMode stagingMode;
    private LocalDateTime executionDateTime;
    private WorkerCargo.Status status;
    private long runtime;
    private long payloadSize;

    public WorkerDataSQLListRow(long dbId, String mappingName, StagingMode stagingMode, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize) {
        this.dbId = dbId;
        this.mappingName = mappingName;
        this.stagingMode = stagingMode;
        this.executionDateTime = executionDateTime;
        this.status = status;
        this.runtime = runtime;
        this.payloadSize = payloadSize;
    }
}
