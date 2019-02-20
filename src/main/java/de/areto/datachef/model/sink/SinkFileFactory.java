package de.areto.datachef.model.sink;

import com.google.common.io.Files;
import de.areto.datachef.config.Constants;
import lombok.experimental.UtilityClass;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

@UtilityClass
public class SinkFileFactory {

    private final static String DF_PATTERN = "yyyyMMdd[_HHmm[ss]]";
    private final static DateTimeFormatter DF = createDateTimeFormatter();

    private static DateTimeFormatter createDateTimeFormatter() {
        final DateTimeFormatterBuilder tfb = new DateTimeFormatterBuilder();
        tfb.appendPattern(DF_PATTERN);
        tfb.parseDefaulting(ChronoField.HOUR_OF_DAY, 0);
        tfb.parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0);
        tfb.parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);
        return tfb.toFormatter();
    }

    public static SinkFile create(Path path, SinkFile.Type type) throws IOException {
        final SinkFileName sinkFileName = createFromPath(path);
        final String checkSum = calculateCheckSum(path);
        final long size = java.nio.file.Files.size(path);

        return new SinkFile(path, sinkFileName.getFileGroup(), sinkFileName.getPublishDate(), checkSum, sinkFileName.getMappingName(), type, size);
    }

    public static SinkFileName createFromPath(Path path) {
        final String fileName = path.getFileName().toString();
        final String baseName = Files.getNameWithoutExtension(fileName);
        final String nameParts[] = baseName.split("\\.");

        String fileGroup = "NA";
        LocalDateTime publishDate = LocalDateTime.now();

        if(nameParts.length == 3) {
            fileGroup = nameParts[1];
            LocalDateTime pb = LocalDateTime.from(DF.parse(nameParts[2]));
            publishDate = pb;
        }

        if(nameParts.length == 2) {
            if(nameParts[1].matches(Constants.SINK_FILE_NAME_PATTERN)) {
                publishDate = LocalDateTime.from(DF.parse(nameParts[1]));
            } else {
                fileGroup = nameParts[1];
            }
        }

        if(nameParts.length == 1) fileGroup = "NA";

        final String mappingName = nameParts[0];

        return new SinkFileName(path.toAbsolutePath().toString(), mappingName, fileGroup, publishDate);
    }

    public static String calculateCheckSum(Path p) throws IOException {
        try (FileInputStream fis = new FileInputStream(p.toFile())) {
            return DigestUtils.md5Hex(fis);
        }
    }
}
