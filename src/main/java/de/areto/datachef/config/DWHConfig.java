package de.areto.datachef.config;

import de.areto.datachef.jdbc.DbType;
import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.PreprocessorClasses;
import org.aeonbits.owner.Config.Sources;

import java.net.URL;

@LoadPolicy(LoadType.FIRST)
@Sources({
        "file:./config/dwh.config.properties"
})
@PreprocessorClasses(Trim.class)
public interface DWHConfig extends Accessible {

    @DefaultValue("EXASOL")
    DbType dbType();

    @DefaultValue("jar:file:./driver/exasol-jdbc-6.0.5.jar!/")
    URL jdbcDriverURL();

    @DefaultValue("com.exasol.jdbc.EXADriver")
    String jdbcDriverClass();

    @DefaultValue("true")
    boolean scanAndCheckSupportedDataTypes();

    @DefaultValue("jdbc:exa:127.0.0.1:8563")
    String jdbcConnectionString();

    String catalog();

    @DefaultValue("datachef")
    String username();

    @DefaultValue("datachef")
    String password();

    @DefaultValue("chef_diner")
    String schemaNameDiner();

    @DefaultValue("chef_raw")
    String schemaNameRaw();

    @DefaultValue("chef_cooked")
    String schemaNameCooked();

    @DefaultValue("chef_served")
    String schemaNameServed();

    @DefaultValue("chef_served")
    String schemaNameViews();

    @DefaultValue("chef_admin")
    String schemaNameAdmin();

    @DefaultValue("chef_job_log")
    String jobLogTableName();

    @DefaultValue("~~")
    String hashConcatinationString();

    @DefaultValue("NA")
    String nullValueReplacement();

    @DefaultValue("_err")
    String errorTableSuffix();

    @DefaultValue("true")
    boolean enableErrorTable();

    @DefaultValue("true")
    boolean generatePrimaryKeys();

    @DefaultValue("true")
    boolean generateForeignKeys();

    @DefaultValue("pk_")
    String primaryKeyPrefix();

    @DefaultValue("fk_")
    String foreignKeyPrefix();

    @DefaultValue("true")
    boolean truncateStage();

    @DefaultValue("2000")
    int commentMaxLength();

    @DefaultValue("false")
    boolean replaceTablesWhenCreated();
    
    @DefaultValue("datachef_db")
    String dbName();
    
    @DefaultValue("chef_stage")
    String snowflakeSchemaNameStage();
    
    @DefaultValue("datachef_stage")
    String snowflakeStageName();
    
    @DefaultValue("datachef_warehouse")
    String snowflakeWarehouseName();
    
    @DefaultValue("X-SMALL")
    String snowflakeWarehouseSize();
    
    @DefaultValue("standard")
    String snowflakeWarehouseType();
    
    
    @DefaultValue("120")
    String snowflakeAutoSuspend();
    
    @DefaultValue("true")
    String snowflakeAutoResume();
    
    @DefaultValue("true")
    String snowflakeInitiallySuspended();
    
}
