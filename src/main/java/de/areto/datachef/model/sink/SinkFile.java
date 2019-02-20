package de.areto.datachef.model.sink;

import com.google.common.io.Files;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Objects;

@Getter
public class SinkFile implements Comparable<SinkFile> {

    public enum Type {
        DATA, MAPPING, MART
    }

    private final Path path;
    private final String fileGroup;
    private final LocalDateTime publishDate;
    private final String checkSum;
    private final String mappingName;
    private final Type type;
    private final long size;

    public SinkFile(@NonNull Path path, @NonNull String fileGroup, @NonNull LocalDateTime publishDate, @NonNull String checkSum, @NonNull String mappingName, @NonNull Type type, long size) {
        this.path = path;
        this.fileGroup = fileGroup;
        this.publishDate = publishDate;
        this.checkSum = checkSum;
        this.mappingName = mappingName;
        this.type = type;
        this.size = size;
    }

    public String getContentString() throws IOException {
        return Files.asCharSource(path.toFile(), Charset.defaultCharset()).read();
    }

    public String getFileName() {
        return path.getFileName().toString();
    }

    public String getAbsolutePathString() {
        return path.toAbsolutePath().toString();
    }

    @Override
    public int compareTo(SinkFile o) {
        return this.path.compareTo(o.getPath());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SinkFile sinkFile = (SinkFile) o;
        return Objects.equals(path, sinkFile.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }
}
