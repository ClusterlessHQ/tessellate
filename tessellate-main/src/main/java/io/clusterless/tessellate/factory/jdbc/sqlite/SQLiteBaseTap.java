/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.util.URIs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.Properties;

/**
 * Abstract base class for SQLite tap implementations providing common functionality.
 * Subclasses must implement the specific deleteResource behavior.
 */
public abstract class SQLiteBaseTap extends Tap<Properties, Void, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SQLiteBaseTap.class);

    protected final Sink sinkModel;
    protected final String databasePath;
    protected final String tableName;
    protected transient Connection connection;

    public SQLiteBaseTap(SQLiteScheme scheme, Sink sinkModel) {
        super(scheme);
        this.sinkModel = sinkModel;

        URI uri = sinkModel.uris().get(0);
        this.databasePath = URIs.extractFilePath(uri);
        this.tableName = extractTableName(uri, sinkModel);
    }

    protected String extractTableName(URI uri, Sink sink) {
        String query = uri.getQuery();
        if (query != null) {
            for (String param : query.split("&")) {
                String[] keyValue = param.split("=", 2);
                if (keyValue.length == 2 && SQLiteConfig.TABLE_PARAM.equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
        }

        // Use table name from schema if available, otherwise default to 'data'
        String schemaTableName = sink.schema().tableName();
        if (schemaTableName != null && !schemaTableName.isEmpty()) {
            return schemaTableName;
        }

        return SQLiteConfig.DEFAULT_TABLE_NAME;
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        try {
            getConnection(conf);
            createTableIfNotExists(conf);

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("sqlite resource created successfully for table: {}", tableName);
            }

            return true;
        } catch (SQLException e) {
            throw new TapException("failed to create sqlite database resource", e);
        }
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        try {
            getConnection(conf);
            boolean exists = tableExists();

            return exists;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        // SQLite doesn't provide modification time at table level
        return System.currentTimeMillis();
    }

    @Override
    public boolean rollbackResource(Properties conf) throws IOException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.rollback();

                if (SQLiteConfig.isTraceEnabled(conf)) {
                    LOG.info("sqlite transaction rolled back successfully for table: {}", tableName);
                }

                return true;
            }

            return false;
        } catch (SQLException e) {
            throw new TapException("failed to rollback sqlite transaction", e);
        }
    }

    @Override
    public boolean commitResource(Properties conf) throws IOException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.commit();

                if (SQLiteConfig.isTraceEnabled(conf)) {
                    LOG.info("sqlite transaction committed successfully for table: {}", tableName);
                }

                return true;
            }

            return false;
        } catch (SQLException e) {
            throw new TapException("failed to commit sqlite transaction", e);
        }
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<? extends Properties> flowProcess, Void input) throws IOException {
        throw new UnsupportedOperationException("sqlite tap only supports sink operations");
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<? extends Properties> flowProcess, Void output) throws IOException {
        try {
            Properties conf = flowProcess.getConfigCopy();

            // Ensure database and table are created
            createResource(conf);

            SQLiteTupleEntryCollector collector = new SQLiteTupleEntryCollector(flowProcess, this);

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("sqlite tap opened for write successfully: table {}", tableName);
            }

            return collector;
        } catch (Exception e) {
            throw new TapException("failed to open sqlite for write", e);
        }
    }

    public Connection getConnection(Properties conf) throws SQLException {
        if (connection == null || connection.isClosed()) {
            String jdbcUrl = SQLiteConfig.JDBC_SQLITE_PREFIX + databasePath;
            connection = DriverManager.getConnection(jdbcUrl);

            // Configure SQLite for performance using configuration
            try (Statement stmt = connection.createStatement()) {
                if (SQLiteConfig.isWalModeEnabled(conf)) {
                    stmt.execute(SQLiteConfig.PRAGMA_JOURNAL_MODE_WAL);
                }

                stmt.execute(SQLiteConfig.PRAGMA_SYNCHRONOUS + SQLiteConfig.getSynchronousMode(conf));
                stmt.execute(SQLiteConfig.PRAGMA_CACHE_SIZE + SQLiteConfig.getCacheSize(conf));
                stmt.execute(SQLiteConfig.PRAGMA_TEMP_STORE + SQLiteConfig.getTempStore(conf));
            }

            connection.setAutoCommit(SQLiteConfig.isAutoCommitEnabled(conf));

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("sqlite connection established: {}", jdbcUrl);
            }
        }
        return connection;
    }


    public String getTableName() {
        return tableName;
    }

    public String getDatabasePath() {
        return databasePath;
    }

    protected void createTableIfNotExists(Properties conf) throws SQLException {
        if (tableExists()) {
            return;
        }

        SQLiteTypeMapper typeMapper = new SQLiteTypeMapper();
        StringBuilder createSQL = new StringBuilder("CREATE TABLE ");
        createSQL.append(tableName).append(" (");

        SQLiteScheme sqliteScheme = (SQLiteScheme) getScheme();
        Fields declaredFields = sqliteScheme.getSinkFields();

        for (int i = 0; i < declaredFields.size(); i++) {
            if (i > 0) createSQL.append(", ");

            String fieldName = declaredFields.get(i).toString();
            String sqlType = typeMapper.mapFieldToSQLiteType(declaredFields, i);

            createSQL.append(fieldName).append(" ").append(sqlType);
        }

        createSQL.append(")");

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSQL.toString());
            connection.commit();

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("sqlite table created: {} with sql: {}", tableName, createSQL.toString());
            }
        }
    }

    protected boolean tableExists() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return tables.next();
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<? extends Properties> flowProcess, Properties conf) {
        // Any sink-specific configuration can be done here
    }

    public void commitTransaction() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.commit();
        }
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.commit();
            connection.close();
        }
    }

    // Interface methods
    public Fields getSinkFields() {
        return ((SQLiteScheme) getScheme()).getSinkFields();
    }

    // Abstract method that subclasses must implement
    @Override
    public abstract boolean deleteResource(Properties conf) throws IOException;

    // Abstract method for identifier (differs between implementations)
    @Override
    public abstract String getIdentifier();
}