package de.areto.datachef.model.template;

import lombok.NonNull;

public class JobLogEntryBuilder {
    private Long loadId;
    private String loadTime;
    private String loadTimeEnd;
    private String jobType;
    private String name;
    private String mappingGroup = "";
    private String publishDate = "";
    private String recordSource = "";
    private Long payloadSize = 0L;
    private String fileChecksum = "";
    private Long dataSize = 0L;
    private String status;

    public JobLogEntryBuilder setLoadId(@NonNull Long loadId) {
        this.loadId = loadId;
        return this;
    }

    public JobLogEntryBuilder setLoadTime(@NonNull String loadTime) {
        this.loadTime = loadTime;
        return this;
    }

    public JobLogEntryBuilder setLoadTimeEnd(@NonNull String loadTimeEnd) {
        this.loadTimeEnd = loadTimeEnd;
        return this;
    }

    public JobLogEntryBuilder setJobType(@NonNull String jobType) {
        this.jobType = jobType;
        return this;
    }

    public JobLogEntryBuilder setName(@NonNull String name) {
        this.name = name;
        return this;
    }

    public JobLogEntryBuilder setMappingGroup(@NonNull String mappingGroup) {
        this.mappingGroup = mappingGroup;
        return this;
    }

    public JobLogEntryBuilder setPublishDate(@NonNull String publishDate) {
        this.publishDate = publishDate;
        return this;
    }

    public JobLogEntryBuilder setRecordSource(@NonNull String recordSource) {
        this.recordSource = recordSource;
        return this;
    }

    public JobLogEntryBuilder setPayloadSize(@NonNull Long payloadSize) {
        this.payloadSize = payloadSize;
        return this;
    }

    public JobLogEntryBuilder setFileChecksum(@NonNull String fileChecksum) {
        this.fileChecksum = fileChecksum;
        return this;
    }

    public JobLogEntryBuilder setDataSize(@NonNull Long dataSize) {
        this.dataSize = dataSize;
        return this;
    }

    public JobLogEntryBuilder setStatus(@NonNull String status) {
        this.status = status;
        return this;
    }

    public JobLogEntry createJobLogEntry() {
        return new JobLogEntry(loadId, loadTime, loadTimeEnd, jobType, name, mappingGroup, publishDate, recordSource, payloadSize, fileChecksum, dataSize, status);
    }
}