package de.areto.datachef.jdbc;

import com.exasol.jdbc.EXAConnection;
import com.exasol.jdbc.EXAStatement;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * {@link ExaSpox} extends {@link DbSpox} in order to handle EXASOL specifics.
 * As of now, the EXASOL driver will throw an error if {@link Statement} is used to execute an CONNECTION statement.
 * The error disappears if an {@link EXAStatement} is used. If the EXASOL JDBC driver is used every obtained
 * {@link Statement} is an EXAStatement so the cast is safe.
 */
public class ExaSpox extends DbSpox {

    private static String DEFAULT_CATALOG = "exa_db";
    /**
     * Build a new {@link ExaSpox}. Please use {@link DbSpoxBuilder} to create an instance publicly.
     *
     * @param username         Username
     * @param password         Password
     * @param connectionString JDBC connection string
     * @param driverClass      JDBC driver class
     * @param driverURL        JDBC driver URL
     */
    ExaSpox(String username, String password, String connectionString, String driverClass, URL driverURL) {
        super(DEFAULT_CATALOG, username, password, connectionString, driverClass, driverURL);

    }

    /**
     * Executes an SQL statement and retrieves the update count. The update count reflects the affected rows.
     * This method only works if connected to a <a href="exasol.com">EXASOL</a> database!
     * The {@link Connection} has to be managed, e.g. closed and/or committed, outside of the scope of this method.
     *
     * @param connection Connection to the database, retrievable via {@link #getConnection()}.
     * @param query      SQL statement as {@link String}
     * @return number of updated or affected rows
     * @throws SQLException if a database access error occurs
     */
    @Override
    public int execQuery(Connection connection, String query) throws SQLException {
        if(connection instanceof EXAConnection) {
            final Statement statement = connection.createStatement();
            if(statement instanceof EXAStatement) {
                final EXAStatement exaStmnt = (EXAStatement) statement;
                exaStmnt.execute(query);
                return exaStmnt.getUpdateCount();
            } else {
                throw new IllegalStateException("Statement is not a " + EXAStatement.class);
            }
        } else {
            throw new IllegalStateException("Connection is not a " + EXAConnection.class);
        }
    }

    /**
     * Executes an SQL statement and retrieves the update count. The update count reflects the affected rows.
     * This method only works if connected to a <a href="exasol.com">EXASOL</a> database!
     *
     * @param query SQL statement as {@link String}
     * @return number of updated or affected rows
     * @throws SQLException if a database access error occurs
     */
    @Override
    public int execQuery(String query) throws SQLException {
        try (Connection connection = this.getConnection()) {
            return execQuery(connection, query);
        }
    }
}
