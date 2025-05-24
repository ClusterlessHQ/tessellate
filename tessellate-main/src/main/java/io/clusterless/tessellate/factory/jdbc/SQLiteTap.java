/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
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
 * A Cascading Tap for SQLite database operations.
 * This tap supports sink-only operations for writing data to SQLite databases.
 */
public class SQLiteTap extends Tap<Properties, Void, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SQLiteTap.class);

    private final Sink sinkModel;
    private final String databasePath;
    private final String tableName;
    private transient Connection connection;

    public SQLiteTap(SQLiteScheme scheme, Sink sinkModel) {
        super(scheme);
        this.sinkModel = sinkModel;

        URI uri = sinkModel.uris().get(0);
        this.databasePath = URIs.extractFilePath(uri);
        this.tableName = extractTableName(uri, sinkModel);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created SQLiteTap for database: {} table: {}", databasePath, tableName);
        }
    }


    private String extractTableName(URI uri, Sink sink) {
        String query = uri.getQuery();
        if (query != null) {
            for (String param : query.split("&")) {
                String[] keyValue = param.split("=", 2);
                if (keyValue.length == 2 && "table".equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
        }

        // Use table name from schema if available, otherwise default to 'data'
        String schemaTableName = sink.schema().tableName();
        if (schemaTableName != null && !schemaTableName.isEmpty()) {
            return schemaTableName;
        }

        return "data";
    }

    @Override
    public String getIdentifier() {
        return "sqlite://" + databasePath + "?table=" + tableName;
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Creating SQLite resource for database: {} table: {}", databasePath, tableName);
            }

            getConnection(conf);
            createTableIfNotExists(conf);

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("SQLite resource created successfully for table: {}", tableName);
            }

            return true;
        } catch (SQLException e) {
            throw new TapException("Failed to create SQLite database resource", e);
        }
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deleting SQLite resource for database: {} table: {}", databasePath, tableName);
            }

            if (connection != null && !connection.isClosed()) {
                connection.close();
            }

            // Note: We don't delete the database file, just close connection
            return true;
        } catch (SQLException e) {
            throw new TapException("Failed to close SQLite database connection", e);
        }
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        try {
            getConnection(conf);
            boolean exists = tableExists();

            if (LOG.isDebugEnabled()) {
                LOG.debug("SQLite table {} exists: {}", tableName, exists);
            }

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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Rolling back SQLite transaction for table: {}", tableName);
            }

            if (connection != null && !connection.isClosed()) {
                connection.rollback();

                if (SQLiteConfig.isTraceEnabled(conf)) {
                    LOG.info("SQLite transaction rolled back successfully for table: {}", tableName);
                }

                return true;
            }

            return false;
        } catch (SQLException e) {
            throw new TapException("Failed to rollback SQLite transaction", e);
        }
    }

    @Override
    public boolean commitResource(Properties conf) throws IOException {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing SQLite transaction for table: {}", tableName);
            }

            if (connection != null && !connection.isClosed()) {
                connection.commit();

                if (SQLiteConfig.isTraceEnabled(conf)) {
                    LOG.info("SQLite transaction committed successfully for table: {}", tableName);
                }

                return true;
            }

            return false;
        } catch (SQLException e) {
            throw new TapException("Failed to commit SQLite transaction", e);
        }
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<? extends Properties> flowProcess, Void input) throws IOException {
        throw new UnsupportedOperationException("SQLite tap only supports sink operations");
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<? extends Properties> flowProcess, Void output) throws IOException {
        try {
            Properties conf = flowProcess.getConfigCopy();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Opening SQLite tap for write: table {}", tableName);
            }

            // Ensure database and table are created
            createResource(conf);

            SQLiteTupleEntryCollector collector = new SQLiteTupleEntryCollector(flowProcess, this);

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("SQLite tap opened for write successfully: table {}", tableName);
            }

            return collector;
        } catch (Exception e) {
            throw new TapException("Failed to open SQLite for write", e);
        }
    }

    public Connection getConnection(Properties conf) throws SQLException {
        if (connection == null || connection.isClosed()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Creating new SQLite connection to: {}", databasePath);
            }

            String jdbcUrl = "jdbc:sqlite:" + databasePath;
            connection = DriverManager.getConnection(jdbcUrl);

            // Configure SQLite for performance using configuration
            try (Statement stmt = connection.createStatement()) {
                if (SQLiteConfig.isWalModeEnabled(conf)) {
                    stmt.execute("PRAGMA journal_mode=WAL");
                }

                stmt.execute("PRAGMA synchronous=" + SQLiteConfig.getSynchronousMode(conf));
                stmt.execute("PRAGMA cache_size=" + SQLiteConfig.getCacheSize(conf));
                stmt.execute("PRAGMA temp_store=" + SQLiteConfig.getTempStore(conf));
            }

            connection.setAutoCommit(SQLiteConfig.isAutoCommitEnabled(conf));

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("SQLite connection established: {}", jdbcUrl);
            }
        }
        return connection;
    }

    public Connection getConnection() throws SQLException {
        // Fallback method for compatibility
        Properties defaultConf = new Properties();
        SQLiteConfig.setDefaults(defaultConf);
        return getConnection(defaultConf);
    }

    public String getTableName() {
        return tableName;
    }

    public String getDatabasePath() {
        return databasePath;
    }

    private void createTableIfNotExists(Properties conf) throws SQLException {
        if (tableExists()) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating SQLite table: {}", tableName);
        }

        SQLiteTypeMapper typeMapper = new SQLiteTypeMapper();
        StringBuilder createSQL = new StringBuilder("CREATE TABLE ");
        createSQL.append(tableName).append(" (");

        for (int i = 0; i < getSourceFields().size(); i++) {
            if (i > 0) createSQL.append(", ");

            String fieldName = getSourceFields().get(i).toString();
            String sqlType = typeMapper.mapToSQLiteType(fieldName, sinkModel);

            createSQL.append(fieldName).append(" ").append(sqlType);
        }

        createSQL.append(")");

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSQL.toString());
            connection.commit();

            if (SQLiteConfig.isTraceEnabled(conf)) {
                LOG.info("SQLite table created: {} with SQL: {}", tableName, createSQL.toString());
            }
        }
    }

    private boolean tableExists() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return tables.next();
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<? extends Properties> flowProcess, Properties conf) {
        // Any sink-specific configuration can be done here
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing sink configuration for SQLite tap: {}", getIdentifier());
        }
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

            if (LOG.isDebugEnabled()) {
                LOG.debug("SQLite connection closed for database: {}", databasePath);
            }
        }
    }
}
