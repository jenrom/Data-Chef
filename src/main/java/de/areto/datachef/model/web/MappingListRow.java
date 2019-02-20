package de.areto.datachef.model.web;

import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.worker.WorkerCargo;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class MappingListRow {

    private long dbId;
    private String mappingName;
    private LocalDateTime executionDateTime;
    private WorkerCargo.Status status;
    private long runtime;
    private long payloadSize;
    private StagingMode stagingMode;

    public MappingListRow(long dbId, String mappingName, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize, StagingMode stagingMode) {
        this.dbId = dbId;
        this.mappingName = mappingName;
        this.executionDateTime = executionDateTime;
        this.status = status;
        this.runtime = runtime;
        this.payloadSize = payloadSize;
        this.stagingMode = stagingMode;
    }
}
