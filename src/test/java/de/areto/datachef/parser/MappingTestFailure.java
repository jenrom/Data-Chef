package de.areto.datachef.parser;

import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.sink.SinkFile;
import org.junit.Test;

import static de.areto.test.TestUtility.getSinkFileFromResource;
import static de.areto.test.TestUtility.parseAndPublish;
import static org.junit.Assert.assertFalse;

public class MappingTestFailure {

    @Test
    public void mappingShouldFail() throws Exception {
        SinkFile failFile = getSinkFileFromResource("/test_sink/failure.sink");
        final Mapping mapping = parseAndPublish(failFile);
        assertFalse(mapping.getIssueList().isEmpty());
        assertFalse(mapping.isValid());
    }
}
