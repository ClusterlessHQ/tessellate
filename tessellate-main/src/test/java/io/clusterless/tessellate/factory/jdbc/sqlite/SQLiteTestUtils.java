/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Shared utilities for SQLite testing to reduce code duplication.
 */
public class SQLiteTestUtils {

    /**
     * Create default test configuration with tracing enabled and small batch size
     */
    public static Properties createTestConfig() {
        Properties testConfig = new Properties();
        SQLiteConfig.setDefaults(testConfig);
        testConfig.setProperty(SQLiteConfig.SQLITE_TRACE_ENABLED, "true");
        testConfig.setProperty(SQLiteConfig.SQLITE_BATCH_SIZE, "10"); // Small batch for testing
        return testConfig;
    }

    /**
     * Verify SQLite database contents including row count, column count, and sample data
     */
    public static void verifySQLiteContents(String dbPath, String tableName, int expectedRowCount, int expectedColumnCount) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            // Verify table exists
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
                assertTrue(tables.next(), "table " + tableName + " should exist");
            }

            // Verify row count
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {

                assertTrue(rs.next(), "count query should return a result");
                assertEquals(expectedRowCount, rs.getInt(1), "wrong number of rows in " + tableName);
            }

            // Verify column count
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1")) {

                ResultSetMetaData rsMetaData = rs.getMetaData();
                assertEquals(expectedColumnCount, rsMetaData.getColumnCount(), "wrong number of columns in " + tableName);
            }

            // Verify some sample data exists
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 3")) {

                List<String> firstColumnValues = new ArrayList<>();
                while (rs.next()) {
                    firstColumnValues.add(rs.getString(1));
                }

                assertFalse(firstColumnValues.isEmpty(), "should have at least one row of data");
            }
        }
    }

    /**
     * Verify that a specific column has the expected SQLite type
     */
    public static void verifyColumnType(String dbPath, String tableName, String columnName, String expectedType, String message) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {

            List<String> columnNames = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();

            while (rs.next()) {
                columnNames.add(rs.getString("name"));
                columnTypes.add(rs.getString("type"));
            }

            int index = columnNames.indexOf(columnName);
            assertTrue(index >= 0, "column " + columnName + " should exist");
            String actualType = columnTypes.get(index);
            assertEquals(expectedType, actualType, message + " (column: " + columnName + ")");
        }
    }

    /**
     * Verify that a table exists in the database
     */
    public static boolean tableExists(String dbPath, String tableName) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
                return tables.next();
            }
        }
    }

    /**
     * Get the number of tables in a SQLite database
     */
    public static int getTableCount(String dbPath) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")) {

            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    /**
     * Recursively delete a directory and all its contents
     */
    public static void deleteDirectory(File directory) {
        if (!directory.exists()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    /**
     * Verify timestamp data is properly stored in ISO-8601 format
     */
    public static void verifyTimestampData(String dbPath, String tableName, String timestampColumn) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + timestampColumn + " FROM " + tableName + " WHERE " + timestampColumn + " IS NOT NULL LIMIT 3")) {

            List<String> timestampValues = new ArrayList<>();
            while (rs.next()) {
                String timeStr = rs.getString(1);
                assertNotNull(timeStr, "timestamp should not be null");
                timestampValues.add(timeStr);
            }

            assertFalse(timestampValues.isEmpty(), "should have timestamp data");

            // Verify timestamps are in proper ISO-8601 format
            assertTrue(timestampValues.stream().anyMatch(ts -> ts.contains("T") && (ts.contains("Z") || ts.contains("+") || ts.contains("-"))),
                    "timestamps should be in ISO-8601 format");
        }
    }
}