package de.areto.datachef.model.web;

import de.areto.datachef.model.worker.WorkerCargo;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class WorkerDataFileListRow {

    private long dbId;
    private String fileName;
    private String fileGroup;
    private long size;
    private LocalDateTime publishDate;
    private String mappingName;
    private LocalDateTime executionDateTime;
    private WorkerCargo.Status status;
    private long runtime;
    private long payloadSize;

    public WorkerDataFileListRow(long dbId, String fileName, String fileGroup, long size, LocalDateTime publishDate, String mappingName, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize) {
        this.dbId = dbId;
        this.fileName = fileName;
        this.fileGroup = fileGroup;
        this.size = size;
        this.publishDate = publishDate;
        this.mappingName = mappingName;
        this.executionDateTime = executionDateTime;
        this.status = status;
        this.runtime = runtime;
        this.payloadSize = payloadSize;
    }
}
