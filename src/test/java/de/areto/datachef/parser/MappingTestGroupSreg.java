package de.areto.datachef.parser;

import com.google.common.collect.Sets;
import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.persistence.HibernateUtility;
import org.hibernate.Session;
import org.junit.Test;

import java.util.Optional;

import static de.areto.test.TestUtility.*;
import static org.junit.Assert.*;

public class MappingTestGroupSreg {

    @Test
    public void mappingSubregionShouldBeValid() throws Exception {
        parseAndPublish(getSinkFileFromResource("/test_sink/test_country.sink"));
        parseAndPublish(getSinkFileFromResource("/test_sink/test_subregion.sink"));

        SinkFile subRegFile = getSinkFileFromResource("/test_sink/test_group_sreg.sink");
        final Mapping mapping = parseAndPublish(subRegFile);
        assertTrue(mapping.isValid());
        assertNotNull(mapping.getCustomSqlCode());
        assertEquals(StagingMode.INSERT, mapping.getStagingMode());
        assertNotNull(mapping.getCsvType());
        assertNull(mapping.getConnectionName());
        assertTrue(mapping.isTriggeredByMousetrap());
        assertEquals(Sets.newHashSet("test_country", "test_subregion"), mapping.getDependencyList());

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final Optional<Mapping> mappingOpt = session.byNaturalId(Mapping.class)
                    .using(Mapping.IDENTIFIER_COLUMN, subRegFile.getMappingName())
                    .loadOptional();

            assertTrue(mappingOpt.isPresent());
            assertEquals(2, mappingOpt.get().getMappedObjects().size());

            final Hub hubSubRegion = getObject(Hub.class, session, "sub_region");
            assertEquals("sub_region", hubSubRegion.getName());
            assertTrue(hubSubRegion.hasAlias());
            assertEquals("sreg", hubSubRegion.getAliasName());
            assertEquals(1, hubSubRegion.getColumns().size());

            final Satellite satSubReg = getObject(Satellite.class, session, "bv_sreg");
            assertEquals("bv_sreg", satSubReg.getName());
            assertEquals(hubSubRegion, satSubReg.getParent());
            assertEquals(1, satSubReg.getColumns().size());

            final DVColumn colAvgPop = satSubReg.getColumnByName("avg_pop");
            assertNotNull(colAvgPop);
            assertEquals("decimal_measure_no_default", colAvgPop.getDataDomain().getName());
            assertFalse(colAvgPop.hasComment());
        }
    }
}
