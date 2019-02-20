package de.areto.datachef.model.sink;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class SinkFileFactoryTest {

    private SinkFile getSinkFile(String path) throws URISyntaxException, IOException {
        final URL tweetCsvUrl = SinkFileFactoryTest.class.getResource(path);
        final Path tweetCsvPath = Paths.get(tweetCsvUrl.toURI());
        return SinkFileFactory.create(tweetCsvPath, SinkFile.Type.DATA);
    }

    @Test
    public void testName1() throws Exception {
        final SinkFile file = getSinkFile("/test_sink/test_country.20161016.csv");
        assertEquals(2016, file.getPublishDate().getYear());
        assertEquals(10, file.getPublishDate().getMonthValue());
        assertEquals(16, file.getPublishDate().getDayOfMonth());
        assertEquals("NA", file.getFileGroup());
    }

    @Test
    public void testName2() throws Exception {
        final SinkFile file = getSinkFile("/test_sink/test_country.group1.20161017.csv");
        assertEquals(2016, file.getPublishDate().getYear());
        assertEquals(10, file.getPublishDate().getMonthValue());
        assertEquals(17, file.getPublishDate().getDayOfMonth());
        assertEquals("group1", file.getFileGroup());
    }

    @Test
    public void testName3() throws Exception {
        final SinkFile file = getSinkFile("/test_sink/employees.csv");
        final LocalDateTime now = LocalDateTime.now();
        assertEquals(now.getYear(), file.getPublishDate().getYear());
        assertEquals(now.getMonthValue(), file.getPublishDate().getMonthValue());
        assertEquals(now.getDayOfMonth(), file.getPublishDate().getDayOfMonth());
        assertEquals(now.getHour(), file.getPublishDate().getHour());
        assertEquals("NA", file.getFileGroup());
    }
}
