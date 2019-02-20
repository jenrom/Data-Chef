package de.areto.datachef.jdbc;

/*
 * Source via: https://stackoverflow.com/questions/1497569/how-to-execute-sql-script-file-using-jdbc
 * Original code (c) by Clinton Begin, 2004
 * Original license: Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
class ScriptRunner {

    private static final String DEFAULT_DELIMITER = ";";

    private final List<Integer> updateList = new ArrayList<>();
    private final Connection connection;
    private final boolean stopOnError;
    private final boolean autoCommit;

    @Getter
    private String delimiter = DEFAULT_DELIMITER;
    private boolean fullLineDelimiter = false;

    ScriptRunner(Connection connection, boolean autoCommit, boolean stopOnError) {
        this.connection = connection;
        this.autoCommit = autoCommit;
        this.stopOnError = stopOnError;
    }

    public void setDelimiter(String delimiter, boolean fullLineDelimiter) {
        this.delimiter = delimiter;
        this.fullLineDelimiter = fullLineDelimiter;
    }

    /**
     * Runs an SQL script (read in using the Reader parameter)
     *
     * @param reader the source of the script
     */
    int[] runScript(Reader reader) throws SQLException, IOException {
        boolean originalAutoCommit = connection.getAutoCommit();
        try {
            if (originalAutoCommit != this.autoCommit) {
                connection.setAutoCommit(this.autoCommit);
            }

            runScript(connection, reader);

            int[] res = new int[updateList.size()];
            for (int i = 0; i < updateList.size(); i++) {
                res[i] = updateList.get(i);
            }
            return res;
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    /**
     * Runs an SQL script (read in using the Reader parameter) using the
     * connection passed in
     *
     * @param conn   the connection to use for the script
     * @param reader the source of the script
     * @throws SQLException if any SQL errors occur
     * @throws IOException  if there is an error reading from the Reader
     */
    private void runScript(Connection conn, Reader reader) throws IOException, SQLException {
        StringBuffer command = null;
        try {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line;

            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuffer();
                }
                String trimmedLine = line.trim();
                if (trimmedLine.length() < 1) continue;
                if (trimmedLine.startsWith("--")) continue;
                if (trimmedLine.startsWith("//")) continue;
                if (trimmedLine.startsWith("--")) continue;
                if (trimmedLine.startsWith("/*")) continue;
                if (trimmedLine.startsWith("*")) continue;

                if (!fullLineDelimiter && trimmedLine.endsWith(getDelimiter()) || fullLineDelimiter && trimmedLine.equals(getDelimiter())) {
                    command.append(line.substring(0, line.lastIndexOf(getDelimiter())));
                    command.append(" ");

                    final Statement statement = conn.createStatement();

                    if (log.isTraceEnabled()) log.trace("Executing: {}", command);

                    int updateCount = -1;
                    if (stopOnError) {
                        statement.execute(command.toString());
                        updateCount = statement.getUpdateCount();
                    } else {
                        try {
                            statement.execute(command.toString());
                            updateCount = statement.getUpdateCount();
                        } catch (SQLException e) {
                            log.error("Error executing: {}", command.toString());
                            if (e.getMessage() != null) log.error(e.getMessage());
                        }
                    }

                    if (log.isTraceEnabled()) log.trace("Update count: {}", updateCount);
                    updateList.add(updateCount);

                    if (autoCommit && !conn.getAutoCommit()) {
                        conn.commit();
                    }

                    command = null;
                    statement.close();
                    Thread.yield();
                } else {
                    command.append(line);
                    command.append(" ");
                }
            }
            if (!autoCommit) {
                conn.commit();
            }
        } catch (SQLException | IOException e) {
            e.fillInStackTrace();
            throw e;
        } finally {
            conn.rollback();
        }
    }
}