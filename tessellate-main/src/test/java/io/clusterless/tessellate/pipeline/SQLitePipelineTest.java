/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import io.clusterless.tessellate.factory.jdbc.sqlite.SQLiteConfig;
import io.clusterless.tessellate.factory.jdbc.sqlite.SQLiteTestUtils;
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
            SQLiteTestUtils.deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Create SQLite database URI with absolute path
        String dbPath = Paths.get(output).resolve("test.db").toString();
        URI sqliteUri = URI.create("sqlite:///" + dbPath + "?table=csv_data");

        runCsvToSqlitePipeline(input, sqliteUri, "csv-to-sqlite-test", "csv_data");

        // Verify SQLite database contents
        SQLiteTestUtils.verifySQLiteContents(dbPath, "csv_data", 13, 5);
    }

    @Test
    void csvToSQLiteRelativePath(@PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException, SQLException {
        // Clean up any existing output directory
        File outputDir = Paths.get(output).toFile();
        if (outputDir.exists()) {
            SQLiteTestUtils.deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Calculate relative path from current working directory to the output directory
        String currentDir = System.getProperty("user.dir");
        String relativePath = Paths.get(currentDir).relativize(Paths.get(output)).resolve("relative.db").toString();
        URI sqliteUri = URI.create("sqlite://" + relativePath + "?table=header_data");

        runCsvToSqlitePipeline(input, sqliteUri, "csv-header-to-sqlite-test", "header_data");

        // Verify SQLite database contents - file should be directly in the output directory
        String dbPath = Paths.get(output).resolve("relative.db").toString();
        SQLiteTestUtils.verifySQLiteContents(dbPath, "header_data", 13, 5);
    }

    @Test
    void awsS3LogToSQLiteWithTimestamps(@PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput URI output) throws IOException, SQLException {
        // Clean up any existing output directory
        File outputDir = Paths.get(output).toFile();
        if (outputDir.exists()) {
            SQLiteTestUtils.deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Create SQLite database URI with absolute path
        String dbPath = Paths.get(output).resolve("s3log.db").toString();
        URI sqliteUri = URI.create("sqlite:///" + dbPath + "?table=s3_access_log");

        runS3LogToSqlitePipeline(input, sqliteUri, "s3log-to-sqlite-test", "s3_access_log");

        // Verify SQLite database contents - 4 log entries, 26 columns
        SQLiteTestUtils.verifySQLiteContents(dbPath, "s3_access_log", 4, 26);

        // Verify that SQLite table schema reflects the AWS S3 access log schema types
        verifyS3LogTableSchema(dbPath, "s3_access_log");

        // Verify timestamp data is properly stored
        SQLiteTestUtils.verifyTimestampData(dbPath, "s3_access_log", "time");
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


    @Test
    void csvToSQLiteWithTableMode(@PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException, SQLException {
        // Clean up any existing output directory
        File outputDir = Paths.get(output).toFile();
        if (outputDir.exists()) {
            SQLiteTestUtils.deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Create SQLite database URI with table mode
        String dbPath = Paths.get(output).resolve("table_mode_test.db").toString();
        URI sqliteUri = URI.create("sqlite:///" + dbPath + "?" + SQLiteConfig.TABLE_PARAM + "=csv_data&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);

        runCsvToSqlitePipeline(input, sqliteUri, "csv-to-sqlite-table-mode-test", "csv_data");

        // Verify SQLite database contents
        SQLiteTestUtils.verifySQLiteContents(dbPath, "csv_data", 13, 5);
        
        // Verify table exists but database file persists (table mode behavior)
        assertTrue(SQLiteTestUtils.tableExists(dbPath, "csv_data"), "table should exist in table mode");
        assertEquals(1, SQLiteTestUtils.getTableCount(dbPath), "should have exactly one table");
    }

    @Test
    void multipleTablesSameDatabase(@PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException, SQLException {
        // Clean up any existing output directory
        File outputDir = Paths.get(output).toFile();
        if (outputDir.exists()) {
            SQLiteTestUtils.deleteDirectory(outputDir);
        }
        outputDir.mkdirs();

        // Create SQLite database with two different tables
        String dbPath = Paths.get(output).resolve("multi_table.db").toString();
        URI table1Uri = URI.create("sqlite:///" + dbPath + "?" + SQLiteConfig.TABLE_PARAM + "=table1&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);
        URI table2Uri = URI.create("sqlite:///" + dbPath + "?" + SQLiteConfig.TABLE_PARAM + "=table2&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);

        // Run pipeline for first table
        runCsvToSqlitePipeline(input, table1Uri, "multi-table-test-1", "table1");
        
        // Run pipeline for second table  
        runCsvToSqlitePipeline(input, table2Uri, "multi-table-test-2", "table2");

        // Verify both tables exist in same database
        SQLiteTestUtils.verifySQLiteContents(dbPath, "table1", 13, 5);
        SQLiteTestUtils.verifySQLiteContents(dbPath, "table2", 13, 5);
        assertTrue(SQLiteTestUtils.tableExists(dbPath, "table1"), "table1 should exist");
        assertTrue(SQLiteTestUtils.tableExists(dbPath, "table2"), "table2 should exist");
        assertEquals(2, SQLiteTestUtils.getTableCount(dbPath), "should have exactly two tables");
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

}
