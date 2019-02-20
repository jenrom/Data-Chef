package de.areto.datachef.model.template;

import lombok.Data;

@Data
public class JobLogEntry {

    private final Long loadId;
    private final String loadTime;
    private final String loadTimeEnd;
    private final String jobType;
    private final String name;
    private String mappingGroup;
    private String publishDate;
    private String recordSource;
    private final Long payloadSize;
    private String fileChecksum;
    private Long dataSize;
    private final String status;

    public JobLogEntry(Long loadId, String loadTime, String loadTimeEnd, String jobType, String name, String mappingGroup, String publishDate, String recordSource, Long payloadSize, String fileChecksum, Long dataSize, String status) {
        this.loadId = loadId;
        this.loadTime = loadTime;
        this.loadTimeEnd = loadTimeEnd;
        this.jobType = jobType;
        this.name = name;
        this.mappingGroup = mappingGroup;
        this.publishDate = publishDate;
        this.recordSource = recordSource;
        this.payloadSize = payloadSize;
        this.fileChecksum = fileChecksum;
        this.dataSize = dataSize;
        this.status = status;
    }
}
