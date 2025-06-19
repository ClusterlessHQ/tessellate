/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import cascading.tap.TapException;
import io.clusterless.tessellate.model.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * A Cascading Tap for SQLite database operations with multi-table support.
 * This tap supports sink-only operations for writing data to SQLite databases
 * where multiple tables can coexist. On deleteResource, only the specific table
 * is dropped, not the entire database file.
 */
public class SQLiteTableTap extends SQLiteBaseTap {
    private static final Logger LOG = LoggerFactory.getLogger(SQLiteTableTap.class);

    public SQLiteTableTap(SQLiteScheme scheme, Sink sinkModel) {
        super(scheme, sinkModel);
    }

    @Override
    public String getIdentifier() {
        return SQLiteConfig.SQLITE_SCHEME + databasePath + SQLiteConfig.TABLE_PARAM_PREFIX + tableName + SQLiteConfig.MODE_TABLE_PARAM;
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        try {
            if (connection != null && !connection.isClosed()) {
                // Drop only the specific table, not the entire database
                if (tableExists()) {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute(SQLiteConfig.DROP_TABLE + tableName);
                        connection.commit();
                        
                        if (SQLiteConfig.isTraceEnabled(conf)) {
                            LOG.info("sqlite table dropped successfully: {}", tableName);
                        }
                    }
                }
                
                connection.close();
            }

            return true;
        } catch (SQLException e) {
            throw new TapException("failed to delete sqlite table resource", e);
        }
    }
}