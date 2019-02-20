package de.areto.datachef.jdbc;

import de.areto.datachef.exceptions.DataChefException;
import org.junit.Test;

import java.net.URL;

import static de.areto.datachef.jdbc.DriverUtility.loadExternalDriver;
import static org.junit.Assert.fail;

public class DriverUtilityTest {

    @Test
    public void driverShouldBeLoaded() throws Exception {
        try {
            loadExternalDriver(new URL("jar:file:./driver/exasol-jdbc-5.0.16.jar!/"), "com.exasol.jdbc.EXADriver");
            loadExternalDriver(new URL("jar:file:./driver/snowflake-jdbc-3.5.1.jar!/"), "net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (DataChefException e) {
            fail("Reason: " + e.getMessage() + " -> " + e.getCause());
        }
    }

}
