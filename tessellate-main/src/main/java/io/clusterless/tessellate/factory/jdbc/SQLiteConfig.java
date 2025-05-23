/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc;

import java.util.Properties;

/**
 * Configuration constants and utilities for SQLite operations
 */
public class SQLiteConfig {

    // Configuration property keys
    public static final String SQLITE_BATCH_SIZE = "cascading.sqlite.batch.size";
    public static final String SQLITE_WAL_MODE = "cascading.sqlite.wal.enabled";
    public static final String SQLITE_SYNC_MODE = "cascading.sqlite.synchronous.mode";
    public static final String SQLITE_CACHE_SIZE = "cascading.sqlite.cache.size";
    public static final String SQLITE_TEMP_STORE = "cascading.sqlite.temp.store";
    public static final String SQLITE_TRACE_ENABLED = "cascading.sqlite.trace.enabled";
    public static final String SQLITE_AUTO_COMMIT = "cascading.sqlite.auto.commit";

    // Default values
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final boolean DEFAULT_WAL_MODE = true;
    public static final String DEFAULT_SYNC_MODE = "NORMAL";
    public static final int DEFAULT_CACHE_SIZE = 10000;
    public static final String DEFAULT_TEMP_STORE = "memory";
    public static final boolean DEFAULT_TRACE_ENABLED = false;
    public static final boolean DEFAULT_AUTO_COMMIT = false;

    /**
     * Get batch size from configuration
     */
    public static int getBatchSize(Properties conf) {
        return Integer.parseInt(conf.getProperty(SQLITE_BATCH_SIZE, String.valueOf(DEFAULT_BATCH_SIZE)));
    }

    /**
     * Check if WAL mode is enabled
     */
    public static boolean isWalModeEnabled(Properties conf) {
        return Boolean.parseBoolean(conf.getProperty(SQLITE_WAL_MODE, String.valueOf(DEFAULT_WAL_MODE)));
    }

    /**
     * Get synchronous mode setting
     */
    public static String getSynchronousMode(Properties conf) {
        return conf.getProperty(SQLITE_SYNC_MODE, DEFAULT_SYNC_MODE);
    }

    /**
     * Get cache size setting
     */
    public static int getCacheSize(Properties conf) {
        return Integer.parseInt(conf.getProperty(SQLITE_CACHE_SIZE, String.valueOf(DEFAULT_CACHE_SIZE)));
    }

    /**
     * Get temp store setting
     */
    public static String getTempStore(Properties conf) {
        return conf.getProperty(SQLITE_TEMP_STORE, DEFAULT_TEMP_STORE);
    }

    /**
     * Check if tracing is enabled
     */
    public static boolean isTraceEnabled(Properties conf) {
        return Boolean.parseBoolean(conf.getProperty(SQLITE_TRACE_ENABLED, String.valueOf(DEFAULT_TRACE_ENABLED)));
    }

    /**
     * Check if auto commit is enabled
     */
    public static boolean isAutoCommitEnabled(Properties conf) {
        return Boolean.parseBoolean(conf.getProperty(SQLITE_AUTO_COMMIT, String.valueOf(DEFAULT_AUTO_COMMIT)));
    }

    /**
     * Set default SQLite configuration properties
     */
    public static void setDefaults(Properties conf) {
        conf.setProperty(SQLITE_BATCH_SIZE, String.valueOf(DEFAULT_BATCH_SIZE));
        conf.setProperty(SQLITE_WAL_MODE, String.valueOf(DEFAULT_WAL_MODE));
        conf.setProperty(SQLITE_SYNC_MODE, DEFAULT_SYNC_MODE);
        conf.setProperty(SQLITE_CACHE_SIZE, String.valueOf(DEFAULT_CACHE_SIZE));
        conf.setProperty(SQLITE_TEMP_STORE, DEFAULT_TEMP_STORE);
        conf.setProperty(SQLITE_TRACE_ENABLED, String.valueOf(DEFAULT_TRACE_ENABLED));
        conf.setProperty(SQLITE_AUTO_COMMIT, String.valueOf(DEFAULT_AUTO_COMMIT));
    }
}
