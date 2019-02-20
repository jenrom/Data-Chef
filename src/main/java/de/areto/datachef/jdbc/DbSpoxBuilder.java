package de.areto.datachef.jdbc;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.RepositoryConfig;

import java.net.URL;

public class DbSpoxBuilder {
    private String catalog;
    private String username;
    private String password;
    private String connectionString;
    private String driverClass;
    private URL driverURL;
    private DbType dbType;

    public DbSpoxBuilder setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public DbSpoxBuilder setUsername(String username) {
        this.username = username;
        return this;
    }

    public DbSpoxBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public DbSpoxBuilder setConnectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    public DbSpoxBuilder setDriverClass(String driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    public DbSpoxBuilder setDriverURL(URL driverURL) {
        this.driverURL = driverURL;
        return this;
    }

    public DbSpoxBuilder setDbType(DbType dbType) {
        this.dbType = dbType;
        return this;
    }

    public DbSpoxBuilder useConfig(DWHConfig config) {
        this.connectionString = config.jdbcConnectionString();
        this.catalog = config.catalog();
        this.username = config.username();
        this.password = config.password();
        this.driverClass = config.jdbcDriverClass();
        this.driverURL = config.jdbcDriverURL();
        this.dbType = config.dbType();
        return this;
    }

    public DbSpoxBuilder useConfig(RepositoryConfig config) {
        this.connectionString = config.jdbcConnectionString();
        this.catalog = config.catalog();
        this.driverClass = config.driverClass();
        this.username = config.username();
        this.password = config.password();
        this.dbType = DbType.GENERIC_JDBC;
        return this;
    }

    public DbSpox build() {
        if (dbType.equals(DbType.GENERIC_JDBC))
            return new DbSpox(catalog, username, password, connectionString, driverClass);

        if (dbType.equals(DbType.EXASOL))
            return new ExaSpox(username, password, connectionString, driverClass, driverURL);

        return new DbSpox(catalog, username, password, connectionString, driverClass, driverURL);
    }
}