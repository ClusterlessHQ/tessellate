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
import java.util.Properties;

/**
 * A Cascading Tap for SQLite database operations with single-table support.
 * This tap supports sink-only operations for writing data to SQLite databases
 * where only one table is expected per database file. On deleteResource, the
 * entire database file is removed.
 */
public class SQLiteTap extends SQLiteBaseTap {
    private static final Logger LOG = LoggerFactory.getLogger(SQLiteTap.class);

    public SQLiteTap(SQLiteScheme scheme, Sink sinkModel) {
        super(scheme, sinkModel);
    }

    @Override
    public String getIdentifier() {
        return SQLiteConfig.SQLITE_SCHEME + databasePath + SQLiteConfig.TABLE_PARAM_PREFIX + tableName;
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }

            // Delete the entire database file for single-table databases
            java.io.File dbFile = new java.io.File(databasePath);
            if (dbFile.exists()) {
                boolean deleted = dbFile.delete();
                if (deleted && SQLiteConfig.isTraceEnabled(conf)) {
                    LOG.info("sqlite database file deleted successfully: {}", databasePath);
                }
                return deleted;
            }

            return true;
        } catch (SQLException e) {
            throw new TapException("failed to delete sqlite database file", e);
        }
    }
}