package de.areto.datachef.parser;

import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.ConnectionType;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.persistence.HibernateUtility;
import org.hibernate.Session;
import org.junit.Test;

import java.util.Optional;

import static de.areto.test.TestUtility.*;
import static org.junit.Assert.*;

public class MappingTestDataDomain {

    @Test
    public void mappingCountryShouldBeValid() throws Exception {
        SinkFile ctrySinkFile = getSinkFileFromResource("/test_sink/test_data_domain.sink");
        final Mapping mapping = parseAndPublish(ctrySinkFile);
        assertTrue(mapping.isValid());
        assertEquals(StagingMode.CONNECTION, mapping.getStagingMode());
        assertEquals(ConnectionType.JDBC, mapping.getConnectionType());
        assertNotNull(mapping.getCsvType());
        assertEquals("german_csv", mapping.getCsvType().getName());
        assertEquals("datachef_repository", mapping.getConnectionName());
        assertNotNull(mapping.getCustomSqlCode());
        assertTrue(mapping.isTriggeredByCron());
        assertEquals("0/30 * * * * ?", mapping.getCronExpression());

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final Optional<Mapping> mappingOpt = session.byNaturalId(Mapping.class)
                    .using(Mapping.IDENTIFIER_COLUMN, ctrySinkFile.getMappingName())
                    .loadOptional();

            assertTrue(mappingOpt.isPresent());

            final Hub hubDataDomain = getObject(Hub.class, session, "data_domain");
            assertNotNull(hubDataDomain);
            assertEquals("data_domain", hubDataDomain.getName());
            assertFalse(hubDataDomain.hasAlias());
            assertEquals(1, hubDataDomain.getColumns().size());
            assertTrue(hubDataDomain.hasComment());
            assertTrue(hubDataDomain.getComments().contains("Data Domain"));

            final Satellite satDataDomain = getObject(Satellite.class, session, "data_domain");
            assertNotNull(satDataDomain);
            assertEquals("data_domain", satDataDomain.getName());
            assertEquals(hubDataDomain, satDataDomain.getParent());
            assertEquals(2, satDataDomain.getColumns().size());
        }
    }

}