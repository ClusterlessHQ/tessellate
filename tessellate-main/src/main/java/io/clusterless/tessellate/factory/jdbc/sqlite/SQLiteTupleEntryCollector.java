/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import cascading.flow.FlowProcess;
import cascading.tap.TapException;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A TupleEntryCollector implementation for writing tuples to SQLite database.
 * This collector handles batched writes for optimal performance and provides
 * proper resource management and error handling.
 */
public class SQLiteTupleEntryCollector extends TupleEntryCollector {
    private static final Logger LOG = LoggerFactory.getLogger(SQLiteTupleEntryCollector.class);

    private final SQLiteBaseTap sqliteTap;
    private final FlowProcess<? extends Properties> flowProcess;
    private final Properties conf;
    private final cascading.tuple.Fields fields;
    private final int batchSize;
    private final boolean traceEnabled;

    private PreparedStatement insertStatement;
    private int batchCount = 0;
    private long totalRowsProcessed = 0;

    public SQLiteTupleEntryCollector(FlowProcess<? extends Properties> flowProcess, SQLiteBaseTap sqliteTap) {
        super(sqliteTap.getSinkFields());
        this.flowProcess = flowProcess;
        this.sqliteTap = sqliteTap;
        this.conf = flowProcess.getConfigCopy();
        this.fields = sqliteTap.getSinkFields();
        this.batchSize = SQLiteConfig.getBatchSize(conf);
        this.traceEnabled = SQLiteConfig.isTraceEnabled(conf);

    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
        try {
            if (insertStatement == null) {
                initializeStatement();
            }

            bindTupleToStatement(tupleEntry);
            insertStatement.addBatch();
            batchCount++;
            totalRowsProcessed++;

            if (batchCount >= batchSize) {
                executeBatch();
            }

            if (traceEnabled && totalRowsProcessed % (batchSize * 10) == 0) {
                LOG.info("sqlite processed {} rows for table: {}", totalRowsProcessed, sqliteTap.getTableName());
            }

        } catch (SQLException e) {
            throw new TapException("failed to insert data into sqlite table: " + sqliteTap.getTableName(), e);
        }
    }

    private void initializeStatement() throws SQLException {
        Connection connection = sqliteTap.getConnection(conf);
        String tableName = sqliteTap.getTableName();

        StringBuilder sql = new StringBuilder(SQLiteConfig.INSERT_INTO);
        sql.append(tableName).append(" (");

        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(fields.get(i).toString());
        }

        sql.append(SQLiteConfig.VALUES_CLAUSE);
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(")");

        insertStatement = connection.prepareStatement(sql.toString());

    }

    private void bindTupleToStatement(TupleEntry tupleEntry) throws SQLException {
        SQLiteTypeMapper typeMapper = new SQLiteTypeMapper();

        for (int i = 0; i < tupleEntry.size(); i++) {
            Object value = tupleEntry.getObject(i);

            if (value == null) {
                insertStatement.setNull(i + 1, java.sql.Types.NULL);
            } else {
                // Get CoercibleType if available from Fields type information
                cascading.tuple.type.CoercibleType<?> coercibleType = null;
                if (fields.hasTypes() && i < fields.size()) {
                    java.lang.reflect.Type fieldType = fields.getType(i);
                    if (fieldType instanceof cascading.tuple.type.CoercibleType) {
                        coercibleType = (cascading.tuple.type.CoercibleType<?>) fieldType;
                    }
                }

                // Apply type conversions using CoercibleType when available
                Object convertedValue = typeMapper.convertValueForSQLite(value, coercibleType);
                insertStatement.setObject(i + 1, convertedValue);
            }
        }
    }

    private void executeBatch() throws SQLException {
        if (batchCount > 0) {

            int[] results = insertStatement.executeBatch();
            sqliteTap.getConnection(conf).commit();

            if (traceEnabled) {
                LOG.info("sqlite batch executed: {} rows affected for table: {}",
                        batchCount, sqliteTap.getTableName());
            }

            batchCount = 0;
        }
    }

    @Override
    public void close() {
        try {
            // Execute any remaining batch
            if (insertStatement != null && batchCount > 0) {
                executeBatch();
            }

            // Close the prepared statement
            if (insertStatement != null) {
                insertStatement.close();
                insertStatement = null;
            }

            // Commit final transaction
            sqliteTap.commitTransaction();

            if (LOG.isDebugEnabled() || traceEnabled) {
                LOG.info("sqlite tuple entry collector closed, total rows processed: {} for table: {}",
                        totalRowsProcessed, sqliteTap.getTableName());
            }

        } catch (SQLException e) {
            LOG.error("error closing sqlite tuple entry collector", e);
            // Try to rollback on error
            try {
                Properties rollbackConf = new Properties();
                SQLiteConfig.setDefaults(rollbackConf);
                sqliteTap.rollbackResource(rollbackConf);
            } catch (Exception rollbackError) {
                LOG.error("failed to rollback sqlite transaction during close", rollbackError);
            }
        }
    }

    /**
     * Get the total number of rows processed by this collector
     */
    public long getTotalRowsProcessed() {
        return totalRowsProcessed;
    }

    /**
     * Get the current batch count
     */
    public int getCurrentBatchCount() {
        return batchCount;
    }
}
