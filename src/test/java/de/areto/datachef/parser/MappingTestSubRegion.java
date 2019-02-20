package de.areto.datachef.parser;

import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.persistence.HibernateUtility;
import org.hibernate.Session;
import org.junit.Test;

import java.util.Optional;

import static de.areto.test.TestUtility.getObject;
import static de.areto.test.TestUtility.getSinkFileFromResource;
import static de.areto.test.TestUtility.parseAndPublish;
import static org.junit.Assert.*;

public class MappingTestSubRegion {

    @Test
    public void mappingSubregionShouldBeValid() throws Exception {
        SinkFile subRegFile = getSinkFileFromResource("/test_sink/test_subregion.sink");
        final Mapping mapping = parseAndPublish(subRegFile);
        assertTrue(mapping.isValid());
        assertNull(mapping.getCustomSqlCode());
        assertEquals(StagingMode.FILE, mapping.getStagingMode());
        assertNotNull(mapping.getCsvType());
        assertEquals("german_csv", mapping.getCsvType().getName());
        assertNull(mapping.getConnectionName());

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final Optional<Mapping> mappingOpt = session.byNaturalId(Mapping.class)
                    .using(Mapping.IDENTIFIER_COLUMN, subRegFile.getMappingName())
                    .loadOptional();

            assertTrue(mappingOpt.isPresent());
            assertEquals(2, mappingOpt.get().getMappedObjects().size());

            final Hub hubSubRegion = getObject(Hub.class, session, "sub_region");
            assertTrue(hubSubRegion.hasComment());
            assertTrue(hubSubRegion.getComments().contains("Kommentar aus Mapping test_subregion"));
            assertEquals("sub_region", hubSubRegion.getName());
            assertTrue(hubSubRegion.hasAlias());
            assertEquals("sreg", hubSubRegion.getAliasName());
            assertEquals(1, hubSubRegion.getColumns().size());

            final Satellite satSubReg = getObject(Satellite.class, session, "sub_region");
            assertEquals("sub_region", satSubReg.getName());
            assertEquals(hubSubRegion, satSubReg.getParent());
            assertEquals(2, satSubReg.getColumns().size());

            final DVColumn colPopSize = satSubReg.getColumnByName("pop_size");
            assertNotNull(colPopSize);
            assertEquals("int16", colPopSize.getDataDomain().getName());
            assertFalse(colPopSize.hasComment());

            final DVColumn colDetail2 = satSubReg.getColumnByName("detail2");
            assertNotNull(colDetail2);
            assertEquals("int16", colDetail2.getDataDomain().getName());
            assertTrue(colDetail2.hasComment());
            assertTrue(colDetail2.getComments().contains("Size multipliziert mit 5"));
        }
    }
}
