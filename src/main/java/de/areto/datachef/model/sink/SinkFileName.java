package de.areto.datachef.model.sink;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class SinkFileName {

    private final String path;
    private final String mappingName;
    private final String fileGroup;
    private final LocalDateTime publishDate;

}
