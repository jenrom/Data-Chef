package de.areto.datachef.parser;

import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.persistence.HibernateUtility;
import org.hibernate.Session;
import org.junit.Test;

import java.util.Optional;

import static de.areto.test.TestUtility.*;
import static org.junit.Assert.*;

public class MappingTestEmployees {

    @Test
    public void mappingEmployeesShouldBeValid() throws Exception {
        SinkFile employeeFile = getSinkFileFromResource("/test_sink/employees.sink");
        final Mapping mapping = parseAndPublish(employeeFile);
        assertTrue(mapping.isValid());
        assertNull(mapping.getCustomSqlCode());
        assertEquals(StagingMode.FILE, mapping.getStagingMode());
        assertNotNull(mapping.getCsvType());
        assertEquals("german_csv", mapping.getCsvType().getName());
        assertNull(mapping.getConnectionName());

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final Optional<Mapping> mappingOpt = session.byNaturalId(Mapping.class)
                    .using(Mapping.IDENTIFIER_COLUMN, employeeFile.getMappingName())
                    .loadOptional();

            assertTrue(mappingOpt.isPresent());

            final Hub hubEmployee = getObject(Hub.class, session, "employee");
            assertEquals("emp", hubEmployee.getAlias());
            assertEquals(1, hubEmployee.getColumns().size());
            assertEquals(1, hubEmployee.getComments().size());
            assertTrue(hubEmployee.getComments().contains("Angestellter"));

            final DVColumn columnPersNo = hubEmployee.getColumnByName("pers_no");
            assertNotNull(columnPersNo);
            assertEquals("int16", columnPersNo.getDataDomain().getName());
            assertTrue(columnPersNo.getComments().contains("Personalnummer"));
            assertTrue(columnPersNo.getComments().contains("Personalnummer (Vorgesetzter)"));

            final Hub hubCar = getObject(Hub.class, session, "car");
            assertTrue(hubCar.hasAlias());
            assertEquals(1, hubCar.getColumns().size());

            final Link lnkEmpDisc = getObject(Link.class, session, "emp_disciplinarian");
            assertEquals(2, lnkEmpDisc.getLegs().size());
            assertTrue(lnkEmpDisc.isSelfReference());
        }
    }
}
