package de.areto.datachef.jdbc;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.Setup;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.model.jdbc.DBColumnType;
import de.areto.datachef.model.jdbc.DBObjectType;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.test.TestUtility;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Slf4j
@Ignore
public class SnowFlakeSpoxTest {

    private DbSpoxBuilder dbSpoxBuilder;
    private DbSpox dbSpox;

    private static final Logger LOG = LoggerFactory.getLogger(Setup.class);

    @Before
    public void before() {
        dbSpoxBuilder = new DbSpoxBuilder();
        dbSpoxBuilder.setCatalog("cookee");
        dbSpoxBuilder.setUsername("username");
        dbSpoxBuilder.setPassword("password");//warehouse=cookee_wh&
        dbSpoxBuilder.setConnectionString("jdbc:snowflake://xxxxx.eu-central-1.snowflakecomputing.com/?warehouse=cookee_wh&db=cookee");
        try{
            dbSpoxBuilder.setDriverURL(new URL("jar:file:./driver/snowflake-jdbc-3.5.3.jar!/"));
        }catch (MalformedURLException e){
            fail("Reason: " + e.getMessage() + " -> " + e.getCause());
        }
        dbSpoxBuilder.setDriverClass("net.snowflake.client.jdbc.SnowflakeDriver");
        dbSpoxBuilder.setDbType(DbType.SNOWFLAKE);
        dbSpox = dbSpoxBuilder.build();
        assertThat(dbSpox).isNotNull();
    }

    @Test
    public void shouldEstablishConnection() {
        assertThat(dbSpox.isReachable()).isTrue();
    }

    @Test
    public void typesShouldBeAvailable() throws Exception {
        final Map<String, DBColumnType> types = dbSpox.getTypes();
        assertThat(types).isNotEmpty();
    }

    @Test
    public void reservedWordsShouldBePresent() throws Exception {
        final Set<String> reservedWords = dbSpox.getReservedWords();
        assertThat(reservedWords).isNotEmpty();
    }

    @Test
    public void catalogShouldBePresent() throws Exception {
        final Set<String> catalogs = dbSpox.getCatalogs();
        assertThat(catalogs).isNotEmpty();
        assertThat(catalogs).contains("cookee");
        //assertThat(catalogs).hasSize(5);
    }

    @Test
    public void schemasShouldBePresent() throws Exception {
        final Set<String> schemas = dbSpox.getSchemas();
        assertThat(schemas).isNotEmpty();
    }

    @Test
    public void objectTypesShouldBeAvailable() throws Exception {
        final Collection<String> objectTypes = dbSpox.getObjectTypes();
        assertThat(objectTypes).isNotEmpty();
    }

    @Test
    public void tablesShouldBeAvailable() throws Exception {
        final Set<String> schemas = dbSpox.getSchemas();
        final String schemaName = "chef_raw";
        assertThat(schemas).contains(schemaName);
        final Collection<DBTable> tables = dbSpox.getObjects(schemaName, null, DBObjectType.TABLE);
        assertThat(tables).isNotEmpty();
        assertThat(tables).hasSize(1);
    }

    @Test
    public void testQuery() throws SQLException {
        final Connection connection = dbSpox.getConnection();
        Statement statement = connection.createStatement();

        List<String> sqlList = new ArrayList<>();
        sqlList.add("create or replace schema test_me");
        sqlList.add("create table test_me.test_me( test1 varchar(100), test2 decimal(2,0) )");
        sqlList.add("insert into test_me.test_me (test1, test2) values ( 'hallo', 5 )");
        sqlList.add("update test_me.test_me set test1 = 'hallo2' where test1 = 'hallo'");
        sqlList.add("delete from test_me.test_me where test1 = 'hallo2'");
        sqlList.add("drop schema test_me cascade");

        for(String sql : sqlList) {
            log.info("Executing SQL: {}", sql);
            final int res1 = statement.executeUpdate(sql);
            statement.close();
            assertThat(res1).isNotNegative();
        }
    }

    @Test
    public void testImport() throws Exception {
        final Connection connection = dbSpox.getConnection();
        Statement statement = connection.createStatement();
        try {
            statement.executeUpdate("drop schema test_me2 cascade");
            statement.close();
        } catch (Exception e) {
            // Schema already gone...
        }
        assertThat(statement.executeUpdate("CREATE SCHEMA TEST_ME2")).isEqualTo(0);
        statement.close();
        statement.executeUpdate("create or replace stage cookee.test_me2.test_import_stg comment = 'stage for test_import'");
        statement.close();
        final String createTableQuery = "CREATE OR REPLACE TABLE TEST_ME2.TEST_COUNTRY ( CTRY_CODE  VARCHAR(10) COMMENT 'Länder ISO Code', NAME  VARCHAR(100) COMMENT 'Name des Landes', REGION  VARCHAR(100) COMMENT 'Regionsschlüssel', SUBREGION  VARCHAR(100) COMMENT 'Subregionsschlüssel', MEM_SINCE  DATE COMMENT 'Datum des Begins der Mitgliedschaft', POP_MIO   DECIMAL(16,0) COMMENT 'Population in Mio. Personen')";
        assertThat(statement.executeUpdate(createTableQuery)).isEqualTo(0);
        statement.close();

        final SinkFile sinkFile = TestUtility.getSinkFileFromResource("/test_sink/test_country.20161016.csv");
        final String loadToStage = String.format("put file://%s @cookee.test_me2.test_import_stg",sinkFile.getAbsolutePathString());
        PreparedStatement pstmt1 = connection.prepareStatement(loadToStage);
        pstmt1.execute();
        pstmt1.close();

        final String importSql = String.format("COPY INTO TEST_ME2.TEST_COUNTRY FROM @cookee.test_me2.test_import_stg/test_country.20161016.csv file_format = ( type='csv'  ENCODING = 'UTF-8' RECORD_DELIMITER = '\n' FIELD_DELIMITER = ';' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)");
        PreparedStatement pstmt2 = connection.prepareStatement(importSql);
        final int importedRows = pstmt2.executeUpdate();
        assertThat(importedRows).isEqualTo(23);
        pstmt2.close();

        assertThat(statement.executeUpdate("drop schema test_me2 cascade")).isEqualTo(0);
        statement.close();
    }
}
