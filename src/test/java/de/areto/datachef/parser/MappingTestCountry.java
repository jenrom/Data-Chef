package de.areto.datachef.parser;

import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.persistence.HibernateUtility;
import org.hibernate.Session;
import org.junit.Test;

import java.util.Objects;
import java.util.Optional;

import static de.areto.test.TestUtility.*;
import static org.junit.Assert.*;

public class MappingTestCountry {

    @Test
    public void mappingCountryShouldBeValid() throws Exception {
        SinkFile ctrySinkFile = getSinkFileFromResource("/test_sink/test_country.sink");
        final Mapping mapping = parseAndPublish(ctrySinkFile);
        assertTrue(mapping.isValid());
        assertNull(mapping.getCustomSqlCode());
        assertEquals(StagingMode.FILE, mapping.getStagingMode());
        assertNotNull(mapping.getCsvType());
        assertEquals("german_csv", mapping.getCsvType().getName());
        assertNull(mapping.getConnectionName());

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final Optional<Mapping> mappingOpt = session.byNaturalId(Mapping.class)
                    .using(Mapping.IDENTIFIER_COLUMN, ctrySinkFile.getMappingName())
                    .loadOptional();

            assertTrue(mappingOpt.isPresent());

            final Hub hubCountry = getObject(Hub.class, session, "country");
            assertNotNull(hubCountry);
            assertEquals("country", hubCountry.getName());
            assertTrue(hubCountry.hasAlias());
            assertEquals("ctry", hubCountry.getAlias());
            assertEquals(1, hubCountry.getColumns().size());
            assertTrue(hubCountry.hasComment());
            assertTrue(hubCountry.getComments().contains("Land"));

            final DVColumn columnCtryCode = hubCountry.getColumnByName("country_code");
            assertNotNull(columnCtryCode);
            assertEquals("country_code", columnCtryCode.getName());
            assertEquals("code", columnCtryCode.getDataDomain().getName());
            assertTrue(columnCtryCode.getComments().contains("Länder ISO Code"));
            assertTrue(columnCtryCode.isKeyColumn());

            final Satellite satCountry = getObject(Satellite.class, session, "country");
            assertNotNull(satCountry);
            final boolean equalsResult = Objects.equals(hubCountry, satCountry.getParent());
            assertTrue(equalsResult);
            assertEquals(1, satCountry.getColumns().size());

            final DVColumn columnName = satCountry.getColumnByName("name");
            assertNotNull(columnName);
            assertEquals("name", columnName.getName());
            assertEquals("name", columnName.getDataDomain().getName());
            assertTrue(columnName.hasComment());
            assertTrue(columnName.getComments().contains("Name des Landes"));

            final Satellite satCtryDetails = getObject(Satellite.class, session, "ctry_details");
            assertNotNull(satCtryDetails);
            assertEquals("ctry_details", satCtryDetails.getName());
            assertFalse(satCtryDetails.hasComment());
            assertEquals(2, satCtryDetails.getColumns().size());
            assertEquals(hubCountry, satCtryDetails.getParent());

            final DVColumn columnPopMio = satCtryDetails.getColumnByName("pop_mio");
            assertNotNull(columnPopMio);
            assertEquals("pop_mio", columnPopMio.getName());
            assertTrue(columnPopMio.getComments().contains("Population in Mio. Personen"));
            assertEquals("int16", columnPopMio.getDataDomain().getName());

            final DVColumn columnPopMio2 = satCtryDetails.getColumnByName("pop_mio2");
            assertNotNull(columnPopMio2);
            assertEquals("pop_mio2", columnPopMio2.getName());
            assertTrue(columnPopMio2.getComments().contains("Population verdoppelt"));
            assertEquals("int16", columnPopMio2.getDataDomain().getName());

            final Hub hubRegion = getObject(Hub.class, session, "region");
            assertNotNull(hubRegion);
            assertEquals("region", hubRegion.getName());
            assertTrue(hubRegion.hasAlias());
            assertEquals("reg", hubRegion.getAlias());
            assertEquals("reg", hubRegion.getAliasName());
            assertTrue(hubRegion.hasComment());
            assertTrue(hubRegion.getComments().contains("Region"));
            assertEquals(1, hubRegion.getColumns().size());

            final DVColumn columnRegion = hubRegion.getColumnByName("region");
            assertNotNull(columnRegion);
            assertEquals("region", columnRegion.getName());
            assertEquals("name", columnRegion.getDataDomain().getName());
            assertTrue(columnRegion.hasComment());
            assertTrue(columnRegion.getComments().contains("Regionsschlüssel"));
            assertTrue(columnRegion.isKeyColumn());

            Hub hubSubRegion = getObject(Hub.class, session, "sub_region");
            assertNotNull(hubSubRegion);
            assertEquals("sub_region", hubSubRegion.getName());
            assertTrue(hubSubRegion.hasAlias());
            assertEquals("sreg", hubSubRegion.getAliasName());
            assertEquals(1, hubSubRegion.getColumns().size());

            final DVColumn columnSubregion = hubSubRegion.getColumnByName("subregion");
            assertNotNull(columnSubregion);
            assertEquals("subregion", columnSubregion.getName());
            assertTrue(columnSubregion.hasComment());
            assertTrue(columnSubregion.getComments().contains("Subregionsschlüssel"));
            assertEquals("name", columnSubregion.getDataDomain().getName());

            final Link linkCtrySreg = getObject(Link.class, session, "ctry_sreg");
            assertNotNull(linkCtrySreg);
            assertEquals(2, linkCtrySreg.getLinkedHubs().size());
            assertTrue(linkCtrySreg.getLinkedHubs().contains(hubCountry));
            assertTrue(linkCtrySreg.getLinkedHubs().contains(hubSubRegion));
            assertFalse(linkCtrySreg.hasComment());
            assertTrue(linkCtrySreg.isHistoricized());

            final Satellite satCtrySreg = getObject(Satellite.class, session, "mem_info");
            assertNotNull(satCountry);
            assertEquals("mem_info", satCtrySreg.getName());
            assertEquals(linkCtrySreg, satCtrySreg.getParent());
            assertEquals(1, satCtrySreg.getColumns().size());

            final DVColumn columnMemSince = satCtrySreg.getColumnByName("mem_since");
            assertNotNull(columnMemSince);
            assertEquals("date", columnMemSince.getDataDomain().getName());
            assertTrue(columnMemSince.hasComment());
            assertTrue(columnMemSince.getComments().contains("Datum des Begins der Mitgliedschaft"));

            final Link linkRegSreg = getObject(Link.class, session, "reg_sreg");
            assertNotNull(linkRegSreg);
            assertEquals("reg_sreg", linkRegSreg.getName());
            assertFalse(linkRegSreg.hasComment());
            assertEquals(2, linkRegSreg.getLinkedHubs().size());
            assertTrue(linkRegSreg.getLinkedHubs().contains(hubRegion));
            assertTrue(linkRegSreg.getLinkedHubs().contains(hubSubRegion));

        }
    }

}