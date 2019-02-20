package de.areto.datachef.jdbc;

import de.areto.common.template.SQLTemplateRenderer;
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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
public class DbSpoxTest {

    private DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private DbSpox dbSpox;

    @Before
    public void before() {
        dbSpox = new DbSpoxBuilder().useConfig(dwhConfig).build();
        assertThat(dbSpox).isNotNull();
        assertThat(dbSpox).isInstanceOf(ExaSpox.class);
    }

    @Test
    public void shouldEstablishConnection() throws Exception {
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
    public void schemasShouldBePresent() throws Exception {
        final Set<String> schemas = dbSpox.getSchemas();
        assertThat(schemas).isNotEmpty();
        assertThat(schemas).contains("sys");
    }

    @Test
    public void objectTypesShouldBeAvailable() throws Exception {
        final Collection<String> objectTypes = dbSpox.getObjectTypes();
        assertThat(objectTypes).isNotEmpty();
    }

    @Test
    public void tablesShouldBeAvailable() throws Exception {
        final Set<String> schemas = dbSpox.getSchemas();
        assertThat(schemas).contains("sys");
        final String sysSchema = String.format("sys");
        final Collection<DBTable> tables = dbSpox.getObjects(sysSchema, null, DBObjectType.TABLE);
        assertThat(tables).isNotEmpty();
    }

    @Test
    public void testImport() throws Exception {
        try {
            dbSpox.execQuery("drop schema test_me2 cascade");
        } catch (Exception e) {
            // Schema already gone...
        }

        assertThat(dbSpox.execQuery("CREATE SCHEMA TEST_ME2")).isEqualTo(0);
        final String createTableQuery = "CREATE OR REPLACE TABLE TEST_ME2.TEST_COUNTRY ( CTRY_CODE VARCHAR(10) UTF8 COMMENT IS 'Länder ISO Code', NAME      VARCHAR(100) UTF8 COMMENT IS 'Name des Landes', REGION    VARCHAR(100) UTF8 COMMENT IS 'Regionsschlüssel', SUBREGION VARCHAR(100) UTF8 COMMENT IS 'Subregionsschlüssel', MEM_SINCE DATE COMMENT IS 'Datum des Begins der Mitgliedschaft', POP_MIO   DECIMAL(16,0) COMMENT IS 'Population in Mio. Personen') COMMENT IS 'Staging table for Mapping ''test_country'''";
        assertThat(dbSpox.execQuery(createTableQuery)).isEqualTo(0);
        final SinkFile sinkFile = TestUtility.getSinkFileFromResource("/test_sink/test_country.20161016.csv");
        final String importSql = String.format("IMPORT INTO TEST_ME2.TEST_COUNTRY FROM LOCAL CSV FILE '%s' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF' COLUMN SEPARATOR = ';' COLUMN DELIMITER = '\"' SKIP = 1 REJECT LIMIT 100", sinkFile.getAbsolutePathString());
        final int importedRows = dbSpox.execQuery(importSql);
        assertThat(importedRows).isEqualTo(23);
        assertThat(dbSpox.execQuery("drop schema test_me2 cascade")).isEqualTo(0);
    }

    @Test
    public void scriptShouldBeExecuted() throws Exception {
        Map<String, Object> context = new HashMap<>();
        context.put("dwhConfig", ConfigCache.getOrCreate(DWHConfig.class));

        final String template = ConfigCache.getOrCreate(TemplateConfig.class).setupDwhTemplate();
        SQLTemplateRenderer renderer = new SQLTemplateRenderer(template);
        final String sqlScript = renderer.render(context);
        final int[] res = dbSpox.executeScript(sqlScript);

        System.out.println("Executed: " + res.length + " statements");
        assertThat(res.length > 0);
    }

    @Test
    public void sqlShouldBeExecuted() throws Exception {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("create schema test_me");
        sqlList.add("create table test_me.test_me( test1 varchar(100), test2 decimal(2,0) )");
        sqlList.add("insert into test_me.test_me (test1, test2) values ( 'hallo', 5 )");
        sqlList.add("update test_me.test_me set test1 = 'hallo2' where test1 = 'hallo'");
        sqlList.add("delete from test_me.test_me where test1 = 'hallo2'");
        sqlList.add("drop schema test_me cascade");

        for(String sql : sqlList) {
            log.info("Executing SQL: {}", sql);
            final int res1 = dbSpox.execQuery(sql);
            assertThat(res1).isNotNegative();
        }

        final Short scalarResult = dbSpox.execScalar("select 5 from dual", Short.class);
        assertThat(scalarResult).isEqualTo((short) 5);
    }
}