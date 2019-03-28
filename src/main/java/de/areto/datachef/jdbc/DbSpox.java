package de.areto.datachef.jdbc;

import de.areto.datachef.model.jdbc.DBColumn;
import de.areto.datachef.model.jdbc.DBColumnType;
import de.areto.datachef.model.jdbc.DBObjectType;
import de.areto.datachef.model.jdbc.DBTable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.sql.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static de.areto.datachef.jdbc.DriverUtility.loadExternalDriver;

/**
 * Class that handles the communication with a JDBC based database. Alongside executing SQL statements {@link DbSpox}
 * is able to retrieve the meta data of the database as instances of {@link DBTable}, {@link DBColumn}
 * or {@link DBColumnType}. Moreover, supported types ({@link DBColumnType}) and a {@link Set}
 * of reserved words can be retrieved.
 * <p>
 * "Spox" is colloquial for "Spokesmen" and since this class is communicating or "speaking" with a database
 * the name was chosen.
 */
@Slf4j
public class DbSpox {

    private final String catalog;
    private final String username;
    private final String password;
    private final String connectionString;


    /**
     * Build a new {@link DbSpox}. Please use {@link DbSpoxBuilder} to create an instance publicly.
     *
     * @param catalog          Catalog
     * @param username         Username
     * @param password         Password
     * @param connectionString JDBC connection string
     * @param driverClass      JDBC driver class
     * @param driverURL        JDBC driver URL
     */
    DbSpox(String catalog, String username, String password, String connectionString, String driverClass, URL driverURL) {
        checkNotNull(catalog, "Catalog is null");
        checkNotNull(username, "Username is null");
        checkNotNull(password, "Password is null");
        checkNotNull(connectionString, "Connection string is null");
        checkNotNull(driverClass, "JDBC driver class is null");
        checkNotNull(driverURL, "JDBC driver URL is null");

        try {
            loadExternalDriver(driverURL, driverClass);
        } catch (Exception e) {
        	log.error("Error while loading jdbc Driver: {}",ExceptionUtils.getStackTrace(e));
            throw new IllegalStateException(e);
        }

        this.catalog = catalog;
        this.username = username;
        this.password = password;
        this.connectionString = connectionString;
    }

    // For Repository
    DbSpox(String catalog, String username, String password, String connectionString, String driverClass) {
        checkNotNull(catalog, "Catalog is null");
        checkNotNull(username, "Username is null");
        checkNotNull(password, "Password is null");
        checkNotNull(connectionString, "Connection string is null");
        checkNotNull(driverClass, "JDBC driver class is null");
        checkState(DbUtils.loadDriver(driverClass), "Driver not loaded");
        this.catalog = catalog;
        this.username = username;
        this.password = password;
        this.connectionString = connectionString;
    }

    /**
     * Check whether database is reachable.
     *
     * @return true if connection can be established, false if {@link SQLException} is thrown
     */
    public boolean isReachable() {
        try (Connection connection = getConnection()) {
            return connection.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Retrieves a {@link DBColumnType} of all the data types supported by this database.
     *
     * @return Map of all available {@link DBColumnType}
     * @throws SQLException if access error occurs
     */
    public Map<String, DBColumnType> getTypes() throws SQLException {
        try (Connection connection = getConnection()) {
            Map<String, DBColumnType> typeMap = new HashMap<>();
            final ResultSet res = connection.getMetaData().getTypeInfo();
            while (res.next()) {
                final DBColumnType t = new DBColumnType();
                t.setPrecision(res.getInt("PRECISION"));
                t.setType(res.getInt("DATA_TYPE"));
                t.setTypeName(res.getString("TYPE_NAME").toLowerCase());
                t.setCreateParams(res.getString("CREATE_PARAMS"));
                t.setNullable(res.getInt("NULLABLE") == DatabaseMetaData.typeNullable);
                t.setCaseSensitive(res.getBoolean("CASE_SENSITIVE"));
                t.setMinimumScale(res.getInt("MINIMUM_SCALE"));
                t.setMaximumScale(res.getInt("MAXIMUM_SCALE"));
                typeMap.put(t.getTypeName(), t);
            }
            return typeMap;
        }
    }

    /**
     * Retrieves a comma-separated list of all of this database's SQL keywords
     * that are NOT also SQL:2003 keywords. Keywords are returned lowercase.
     *
     * @return Set of reserved words
     * @throws SQLException if access error occurs
     */
    public Set<String> getReservedWords() throws SQLException {
        try (Connection connection = getConnection()) {
            final Set<String> reservedWordSet = new HashSet<>();
            String sqlKeywords = connection.getMetaData().getSQLKeywords();
            for (String word : sqlKeywords.split(",")) {
                reservedWordSet.add(word.toLowerCase());
            }
            return reservedWordSet;
        }
    }

    /**
     * Set<Catalog>
     * @return
     * @throws SQLException if access error occurs
     */
    public Set<String> getCatalogs()throws SQLException {
        try (Connection connection = getConnection()) {
            final Set<String> catalogSet = new HashSet<>();
            final DatabaseMetaData metaData = connection.getMetaData();
            final ResultSet catSet = metaData.getCatalogs();
            while (catSet.next()) {
                final String catalog = catSet.getString("TABLE_CAT").toLowerCase();
                catalogSet.add(catalog);
            }

            return catalogSet;
        }
    }

    /**
     * Map<Catalog, Schema>
     * @return
     * @throws SQLException if access error occurs
     */
    public Set<String> getSchemas() throws SQLException {
        try (Connection connection = getConnection()) {
            final Set<String> schemaSet = new HashSet<>();
            final DatabaseMetaData metaData = connection.getMetaData();
            final ResultSet schemas = metaData.getSchemas();

            while (schemas.next()) {
                final String catalog = schemas.getString("TABLE_CATALOG").toLowerCase();
                final String schemaName = schemas.getString("TABLE_SCHEM").toLowerCase();
                if(this.catalog.equals(catalog)) {
                    schemaSet.add(schemaName);
                }
            }

            return schemaSet;
        }
    }

    /**
     * Retrieves a {@link Collection} of available object types.
     * Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *
     * @return {@link Collection} of {@link String} containing available types
     * @throws SQLException if access error occurs
     */
    public Collection<String> getObjectTypes() throws SQLException {
        try (Connection connection = getConnection()) {
            final Set<String> typeSet = new HashSet<>();
            final ResultSet typeResultSet = connection.getMetaData().getTableTypes();
            while (typeResultSet.next()) {
                final String type = typeResultSet.getString("TABLE_TYPE");
                typeSet.add(type);
            }
            return typeSet;
        }
    }

    /**
     * Retrieve a {@link Collection} of {@link DBColumn} for a specified object, e.g. a table.
     *
     * @param schema            defining schema name
     * @param tableNamePattern  Pattern indicating the table name (can be null)
     * @param columnNamePattern Pattern indicating the column name (can be null)
     * @param connection        Open and active connection to the database
     * @return {@link Collection} of found {@link DBColumn}s
     * @throws SQLException if access error occurs
     */
    private Collection<DBColumn> getColumns(@NonNull String schema, @NonNull String tableNamePattern, String columnNamePattern, @NonNull Connection connection) throws SQLException {
        final Collection<DBColumn> columns = new ArrayList<>();
        final DatabaseMetaData metaData = connection.getMetaData();
        final ResultSet columnSet = metaData.getColumns(catalog.toUpperCase(), schema.toUpperCase(),
                tableNamePattern.toUpperCase(), columnNamePattern == null ? null : columnNamePattern.toUpperCase());
        while (columnSet.next()) {
            final DBColumn col = new DBColumn();
            col.setName(columnSet.getString("COLUMN_NAME").toLowerCase());
            col.setTypeName(columnSet.getString("TYPE_NAME").toLowerCase());
            col.setPrecision(columnSet.getInt("COLUMN_SIZE"));
            col.setScale(columnSet.getInt("DECIMAL_DIGITS"));
            col.setNullable(columnSet.getInt("NULLABLE")==DatabaseMetaData.columnNullable);
            col.setComment(columnSet.getString("REMARKS"));
            columns.add(col);
        }
        return columns;
    }

    /**
     * Retrieve a {@link Collection} of {@link DBTable} for a specified schema, {@link DBObjectType} and prefix
     *
     * @param schema defining schema name
     * @param prefix Prefix the object has to have
     * @param pType  {@link DBObjectType} defining the object
     * @return {@link Collection} of found {@link DBTable}s
     * @throws SQLException if access error occurs
     */
    public Collection<DBTable> getObjects(@NonNull String schema, String prefix, DBObjectType pType) throws SQLException {
        try (Connection connection = getConnection()) {
            final Collection<DBTable> tables = new ArrayList<>();
            final DatabaseMetaData metaData = connection.getMetaData();

            final ResultSet tableSet = metaData.getTables(this.catalog.toUpperCase(),
                    schema.toUpperCase(), prefix == null ? null : prefix.toUpperCase(), null);
            while (tableSet.next()) {
                final DBTable table = new DBTable();
                table.setName(tableSet.getString("TABLE_NAME").toLowerCase());
                table.setComment(tableSet.getString("REMARKS"));
                table.setSchema(schema);

                final String typeString = tableSet.getString("TABLE_TYPE");
                final DBObjectType objectType = DBObjectType.matchFromString(typeString);
                table.setType(objectType);

                if (pType != null && !objectType.equals(pType))
                    continue;

                final Collection<DBColumn> columns = getColumns(schema, table.getName(), null, connection);
                columns.forEach(table::addColumn);

                tables.add(table);
            }

            return tables;
        }
    }

    /**
     * Executes an SQL statement and retrieves the update count.
     * The update count reflects the affected rows.
     *
     * @param query SQL statement as {@link String}
     * @return number of updated or affected rows
     * @throws SQLException if a database access error occurs
     */
    public int execQuery(@NonNull String query) throws SQLException {
        try (Connection connection = getConnection()) {
            return execQuery(connection, query);
        }
    }

    /**
     * Executes an SQL statement and retrieves the update count.
     * The update count reflects the affected rows.
     * The {@link Connection} has to be managed, e.g. closed and/or committed, outside of the scope of this method.
     * Using one {@link Connection} for multiple statements saves execution time.
     *
     * @param connection Connection to the database, retrievable via {@link #getConnection()}.
     * @param query      SQL statement as {@link String}
     * @return number of updated or affected rows
     * @throws SQLException if a database access error occurs
     */
    public int execQuery(@NonNull Connection connection, @NonNull String query) throws SQLException {
        if (log.isTraceEnabled()) log.trace("Exec SQL: {}", query);
        final QueryRunner queryRunner = new QueryRunner();
        return queryRunner.update(connection, query);
    }

    /**
     * Executes an SQL statement and retrieves a scalar value of type {@link T}.
     *
     * @param query SQL statement as {@link String}
     * @param clazz Class of {@link T}
     * @param <T>   Type of expected value
     * @return Value of type {@link T}
     * @throws SQLException if a database access error occurs
     */
    public <T> T execScalar(@NonNull String query, @NonNull Class<T> clazz) throws SQLException {
        try (Connection connection = getConnection()) {
            if (log.isTraceEnabled()) log.trace("Exec SQL: {}", query);
            final QueryRunner queryRunner = new QueryRunner();
            final T result = queryRunner.query(connection, query, new ScalarHandler<T>());
            if (result.getClass().equals(clazz)) {
                return result;
            } else {
                throw new SQLException("Result is not of type '" + clazz + "'");
            }
        }
    }

    public int[] executeScript(@NonNull String sql) throws IOException, SQLException {
        try (Connection connection = getConnection()) {
            return this.executeScript(connection, sql);
        }
    }

    public int[] executeScript(@NonNull Connection connectoin, @NonNull String sql) throws IOException, SQLException {
        if(log.isTraceEnabled()) log.trace("SQL: {}", sql);
        final Reader reader = new BufferedReader(new StringReader(sql));
        final ScriptRunner runner = new ScriptRunner(connectoin, false, true);
        return runner.runScript(reader);
    }

    /**
     * Open and return a {@link Connection} to the database.
     *
     * @return {@link Connection} instance
     * @throws SQLException if a database access error occurs
     */
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(connectionString, username, password);
    }
}