/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * A Cascading Scheme for SQLite database operations.
 * This scheme is designed for sink-only operations and delegates actual
 * data writing to SQLiteTupleEntryCollector for better resource management.
 */
public class SQLiteScheme extends Scheme<Properties, Void, Void, Void, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SQLiteScheme.class);

    public SQLiteScheme(Fields fields) {
        super(fields, fields);
    }

    @Override
    public void sourceConfInit(FlowProcess<? extends Properties> flowProcess,
                               Tap<Properties, Void, Void> tap, Properties conf) {
        throw new UnsupportedOperationException("sqlite scheme only supports sink operations");
    }

    @Override
    public void sinkConfInit(FlowProcess<? extends Properties> flowProcess,
                             Tap<Properties, Void, Void> tap, Properties conf) {

        // Set default SQLite configuration
        SQLiteConfig.setDefaults(conf);

        // Configure SQLite-specific properties
        SQLiteBaseTap sqliteTap = (SQLiteBaseTap) tap;
        conf.setProperty(SQLiteConfig.PROP_DATABASE_PATH, sqliteTap.getDatabasePath());
        conf.setProperty(SQLiteConfig.PROP_TABLE_NAME, sqliteTap.getTableName());

        if (SQLiteConfig.isTraceEnabled(conf)) {
            LOG.info("sqlite trace enabled for database: {} table: {}",
                    sqliteTap.getDatabasePath(), sqliteTap.getTableName());
        }
    }

    @Override
    public void sinkPrepare(FlowProcess<? extends Properties> flowProcess,
                            SinkCall<Void, Void> sinkCall) throws IOException {

        SQLiteBaseTap sqliteTap = (SQLiteBaseTap) sinkCall.getTap();
        try {
            // Ensure database and table are ready
            sqliteTap.createResource(flowProcess.getConfigCopy());

            if (SQLiteConfig.isTraceEnabled(flowProcess.getConfigCopy())) {
                LOG.info("sqlite sink prepared successfully for table: {}", sqliteTap.getTableName());
            }
        } catch (Exception e) {
            throw new IOException("failed to prepare sqlite sink", e);
        }
    }

    @Override
    public void sinkCleanup(FlowProcess<? extends Properties> flowProcess,
                            SinkCall<Void, Void> sinkCall) throws IOException {

        SQLiteBaseTap sqliteTap = (SQLiteBaseTap) sinkCall.getTap();
        try {
            // Commit any pending transactions and close connections
            sqliteTap.commitTransaction();

            if (SQLiteConfig.isTraceEnabled(flowProcess.getConfigCopy())) {
                LOG.info("sqlite sink cleanup completed for table: {}", sqliteTap.getTableName());
            }
        } catch (Exception e) {
            LOG.warn("error during sqlite sink cleanup", e);
            // Don't throw exception during cleanup to avoid masking original errors
        }
    }

    @Override
    public boolean source(FlowProcess<? extends Properties> flowProcess,
                          SourceCall<Void, Void> sourceCall) throws IOException {
        throw new UnsupportedOperationException("sqlite scheme only supports sink operations");
    }

    @Override
    public void sink(FlowProcess<? extends Properties> flowProcess,
                     SinkCall<Void, Void> sinkCall) throws IOException {
        // For SQLite, the actual writing is handled by SQLiteTupleEntryCollector
        // This method is called by Cascading framework but we delegate to the collector
        throw new UnsupportedOperationException(
                "sqlite sink operations are handled by SQLiteTupleEntryCollector. " +
                        "this method should not be called in normal operation."
        );
    }
}
