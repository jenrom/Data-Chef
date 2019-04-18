package de.areto.datachef.worker;

import de.areto.datachef.model.sink.SinkFile;
import lombok.NonNull;

import java.time.LocalDateTime;

public class SinkDataFile {
    private String fileGroup;
    private String fileName;
    private String checkSum;
    private Long size;
    private String mappingName;
    private LocalDateTime publishDate;
    private String absolutePathString;

    public SinkDataFile(String fileGroup, String fileName, String checkSum, Long size, String mappingName, LocalDateTime publishDate, String absolutePathString) {
        this.fileGroup = fileGroup;
        this.fileName = fileName;
        this.checkSum = checkSum;
        this.size = size;
        this.mappingName = mappingName;
        this.publishDate = publishDate;
        this.absolutePathString = absolutePathString;
    }

    public SinkDataFile(@NonNull SinkFile file) {
        this.fileGroup = file.getFileGroup();
        this.fileName = file.getFileName();
        this.checkSum = file.getCheckSum();
        this.size = file.getSize();
        this.mappingName = file.getMappingName();
        this.publishDate = file.getPublishDate();
        this.absolutePathString = file.getAbsolutePathString();
    }

    public String getMappingName() {
        return mappingName;
    }

    public String getFileGroup() {
        return fileGroup;
    }

    public LocalDateTime getPublishDate() {
        return publishDate;
    }

    public String getFileName() {
        return fileName;
    }

    public String getCheckSum() {
        return checkSum;
    }

    public Long getSize() {
        return size;
    }

    public @NonNull String getAbsolutePathString() {
        return absolutePathString;
    }
}
