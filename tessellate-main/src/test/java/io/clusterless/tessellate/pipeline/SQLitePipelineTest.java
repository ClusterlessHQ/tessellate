/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import io.clusterless.tessellate.junit.PathForOutput;
import io.clusterless.tessellate.junit.PathForResource;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PipelineOptionsMerge;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQLite pipeline functionality including data writing and verification.
 */
@ExtendWith(ResourceExtension.class)
public class SQLitePipelineTest {

    @Test
    void csvToSQLiteAbsolutePath(@PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException, SQLException {
        // Clean up any existing output directory
        File outputDir = Paths.get(output).toFile();
        if (outputDir.exists()) {
            deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Create SQLite database URI with absolute path
        String dbPath = Paths.get(output).resolve("test.db").toString();
        URI sqliteUri = URI.create("sqlite:///" + dbPath + "?table=csv_data");

        runCsvToSqlitePipeline(input, sqliteUri, "csv-to-sqlite-test", "csv_data");

        // Verify SQLite database contents
        verifySQLiteContents(dbPath, "csv_data", 13, 5);
    }

    @Test
    void csvToSQLiteRelativePath(@PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException, SQLException {
        // Clean up any existing output directory
        File outputDir = Paths.get(output).toFile();
        if (outputDir.exists()) {
            deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Calculate relative path from current working directory to the output directory
        String currentDir = System.getProperty("user.dir");
        String relativePath = Paths.get(currentDir).relativize(Paths.get(output)).resolve("relative.db").toString();
        URI sqliteUri = URI.create("sqlite://" + relativePath + "?table=header_data");

        runCsvToSqlitePipeline(input, sqliteUri, "csv-header-to-sqlite-test", "header_data");

        // Verify SQLite database contents - file should be directly in the output directory
        String dbPath = Paths.get(output).resolve("relative.db").toString();
        verifySQLiteContents(dbPath, "header_data", 13, 5);
    }

    @Test
    void awsS3LogToSQLiteWithTimestamps(@PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput URI output) throws IOException, SQLException {
        // Clean up any existing output directory
        File outputDir = Paths.get(output).toFile();
        if (outputDir.exists()) {
            deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Create SQLite database URI with absolute path
        String dbPath = Paths.get(output).resolve("s3log.db").toString();
        URI sqliteUri = URI.create("sqlite:///" + dbPath + "?table=s3_access_log");

        runS3LogToSqlitePipeline(input, sqliteUri, "s3log-to-sqlite-test", "s3_access_log");

        // Verify SQLite database contents - 4 log entries, 26 columns
        verifySQLiteContents(dbPath, "s3_access_log", 4, 26);

        // Verify that SQLite table schema reflects the AWS S3 access log schema types
        verifyS3LogTableSchema(dbPath, "s3_access_log");

        // Verify timestamp data is properly stored
        verifyTimestampData(dbPath, "s3_access_log");
    }

    private void runCsvToSqlitePipeline(URI input, URI sqliteUri, String pipelineName, String tableName) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName(pipelineName)
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(sqliteUri)
                        .withSchema(Schema.builder()
                                .withFormat(Format.sql)
                                .withTableName(tableName)
                                .build())
                        .build())
                .build();

        Pipeline pipeline = new Pipeline(pipelineOptions, def);
        pipeline.run();
    }

    private void runS3LogToSqlitePipeline(URI input, URI sqliteUri, String pipelineName, String tableName) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();
        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef def = PipelineDef.builder()
                .withName(pipelineName)
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withName("aws-s3-access-log")
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(sqliteUri)
                        .withSchema(Schema.builder()
                                .withFormat(Format.sql)
                                .withTableName(tableName)
                                .build())
                        .build())
                .build();

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(def));
        Pipeline pipeline = new Pipeline(pipelineOptions, merged);
        pipeline.run();
    }

    private void verifySQLiteContents(String dbPath, String tableName, int expectedRowCount, int expectedColumnCount) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            // Verify table exists
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
                assertTrue(tables.next(), "table " + tableName + " should exist");
            }

            // Verify row count and structure
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

    private void verifyTimestampData(String dbPath, String tableName) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            // Query timestamp column as string to verify Instant types are stored properly
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT time, bucketOwner, operation FROM " + tableName + " ORDER BY time LIMIT 3")) {

                List<String> timestampValues = new ArrayList<>();
                while (rs.next()) {
                    String timeStr = rs.getString("time");  // Get as string to avoid SQLite timestamp parsing issues
                    String bucketOwner = rs.getString("bucketOwner");
                    String operation = rs.getString("operation");

                    assertNotNull(timeStr, "timestamp should not be null");
                    assertNotNull(bucketOwner, "bucket owner should not be null");
                    assertNotNull(operation, "operation should not be null");

                    timestampValues.add(timeStr);
                }

                assertFalse(timestampValues.isEmpty(), "should have timestamp data");

                // Verify timestamps are properly stored in ISO-8601 format (should contain year 2021/2023)
                assertTrue(timestampValues.stream().anyMatch(ts -> ts.contains("2021") || ts.contains("2023")),
                        "timestamps should contain expected years");

                // Verify that timestamps are in proper ISO-8601 format
                assertTrue(timestampValues.stream().anyMatch(ts -> ts.contains("T") && ts.contains("Z")),
                        "timestamps should be in ISO-8601 format (contains T and Z)");
            }
        }
    }

    private void verifyS3LogTableSchema(String dbPath, String tableName) throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + dbPath;

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            // Get column information from SQLite table
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {

                List<String> columnNames = new ArrayList<>();
                List<String> columnTypes = new ArrayList<>();

                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String columnType = rs.getString("type");
                    columnNames.add(columnName);
                    columnTypes.add(columnType);
                }


                // Verify expected AWS S3 access log fields are present with appropriate types
                assertTrue(columnNames.contains("bucketOwner"), "should have bucketOwner column");
                assertTrue(columnNames.contains("bucket"), "should have bucket column");
                assertTrue(columnNames.contains("time"), "should have time column");
                assertTrue(columnNames.contains("remoteIP"), "should have remoteIP column");
                assertTrue(columnNames.contains("httpStatus"), "should have httpStatus column");
                assertTrue(columnNames.contains("bytesSent"), "should have bytesSent column");
                assertTrue(columnNames.contains("objectSize"), "should have objectSize column");
                assertTrue(columnNames.contains("totalTime"), "should have totalTime column");
                assertTrue(columnNames.contains("turnAroundTime"), "should have turnAroundTime column");

                // Verify that the type information from AWS S3 access log schema is properly mapped to SQLite types
                // Expected mappings based on aws-s3-access-log.json:
                // - bucketOwner|string -> TEXT
                // - time|Instant -> TEXT (stored as ISO-8601 string)
                // - httpStatus|Integer -> INTEGER
                // - bytesSent|Long -> INTEGER
                // - objectSize|Long -> INTEGER
                // - totalTime|Long -> INTEGER
                // - turnAroundTime|Long -> INTEGER

                verifyColumnType(columnNames, columnTypes, "bucketOwner", "TEXT", "string type should map to TEXT");
                verifyColumnType(columnNames, columnTypes, "bucket", "TEXT", "string type should map to TEXT");
                verifyColumnType(columnNames, columnTypes, "time", "TEXT", "instant type should map to TEXT for ISO-8601 storage");
                verifyColumnType(columnNames, columnTypes, "remoteIP", "TEXT", "string type should map to TEXT");
                // Verify that Integer and Long types from the AWS S3 access log schema are properly mapped to INTEGER
                verifyColumnType(columnNames, columnTypes, "httpStatus", "INTEGER", "httpStatus (Integer) should map to INTEGER");
                verifyColumnType(columnNames, columnTypes, "bytesSent", "INTEGER", "bytesSent (Long) should map to INTEGER");
                verifyColumnType(columnNames, columnTypes, "objectSize", "INTEGER", "objectSize (Long) should map to INTEGER");
                verifyColumnType(columnNames, columnTypes, "totalTime", "INTEGER", "totalTime (Long) should map to INTEGER");
                verifyColumnType(columnNames, columnTypes, "turnAroundTime", "INTEGER", "turnAroundTime (Long) should map to INTEGER");
            }
        }
    }

    private void verifyColumnType(List<String> columnNames, List<String> columnTypes, String columnName, String expectedType, String message) {
        int index = columnNames.indexOf(columnName);
        assertTrue(index >= 0, "column " + columnName + " should exist");
        String actualType = columnTypes.get(index);
        assertEquals(expectedType, actualType, message + " (column: " + columnName + ")");
    }

    private void deleteDirectory(File directory) {
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
}
