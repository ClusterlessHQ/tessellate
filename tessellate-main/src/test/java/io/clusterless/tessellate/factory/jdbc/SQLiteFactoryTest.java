/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc;

import cascading.tuple.Fields;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Protocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Platform-agnostic tests for SQLite factory implementation.
 * Tests core functionality without dependencies on specific Cascading platforms.
 */
public class SQLiteFactoryTest {

    private SQLiteFactory factory;
    private Properties testConfig;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        factory = (SQLiteFactory) SQLiteFactory.INSTANCE;
        testConfig = new Properties();
        SQLiteConfig.setDefaults(testConfig);

        // Enable tracing for tests
        testConfig.setProperty(SQLiteConfig.SQLITE_TRACE_ENABLED, "true");
        testConfig.setProperty(SQLiteConfig.SQLITE_BATCH_SIZE, "10"); // Small batch for testing
    }

    @Test
    void testFactoryProtocols() {
        Set<Protocol> protocols = factory.getSinkProtocols();

        assertTrue(protocols.contains(Protocol.sqlite), "Factory should support sqlite protocol");
        assertEquals(1, protocols.size(), "Factory should only support sqlite protocol");
    }

    @Test
    void testFactoryFormats() {
        Set<Format> formats = factory.getFormats();

        assertTrue(formats.contains(Format.sql), "Factory should support sql format");
        assertEquals(1, formats.size(), "Factory should only support sql format");
    }

    @Test
    void testFactoryCompressions() {
        Set<Compression> compressions = factory.getCompressions();

        assertTrue(compressions.contains(Compression.none), "Factory should support no compression");
        assertEquals(1, compressions.size(), "Factory should only support no compression");
    }

    @Test
    void testSQLiteConfig() {
        // Test default values
        assertEquals(SQLiteConfig.DEFAULT_BATCH_SIZE, SQLiteConfig.getBatchSize(new Properties()));
        assertEquals(SQLiteConfig.DEFAULT_WAL_MODE, SQLiteConfig.isWalModeEnabled(new Properties()));
        assertEquals(SQLiteConfig.DEFAULT_SYNC_MODE, SQLiteConfig.getSynchronousMode(new Properties()));

        // Test custom values
        Properties customConfig = new Properties();
        customConfig.setProperty(SQLiteConfig.SQLITE_BATCH_SIZE, "500");
        customConfig.setProperty(SQLiteConfig.SQLITE_WAL_MODE, "false");
        customConfig.setProperty(SQLiteConfig.SQLITE_SYNC_MODE, "FULL");

        assertEquals(500, SQLiteConfig.getBatchSize(customConfig));
        assertFalse(SQLiteConfig.isWalModeEnabled(customConfig));
        assertEquals("FULL", SQLiteConfig.getSynchronousMode(customConfig));
    }

    @Test
    void testSQLiteTypeMapper() {
        SQLiteTypeMapper mapper = new SQLiteTypeMapper();

        // Test Java type to SQLite type mapping
        assertEquals("TEXT", mapper.mapJavaTypeToSQLite(String.class));
        assertEquals("INTEGER", mapper.mapJavaTypeToSQLite(Integer.class));
        assertEquals("INTEGER", mapper.mapJavaTypeToSQLite(Long.class));
        assertEquals("REAL", mapper.mapJavaTypeToSQLite(Double.class));
        assertEquals("INTEGER", mapper.mapJavaTypeToSQLite(Boolean.class));
        assertEquals("BLOB", mapper.mapJavaTypeToSQLite(byte[].class));
        assertEquals("TEXT", mapper.mapJavaTypeToSQLite(java.util.Date.class));
        assertEquals("TEXT", mapper.mapJavaTypeToSQLite(null));

        // Test value conversion
        assertEquals(1, mapper.convertValueForSQLite(true, Boolean.class));
        assertEquals(0, mapper.convertValueForSQLite(false, Boolean.class));
        assertNull(mapper.convertValueForSQLite(null, String.class));
        assertEquals("test", mapper.convertValueForSQLite("test", String.class));
    }

    @Test
    void testSQLiteTapCreation() throws Exception {
        Path dbPath = tempDir.resolve("test.db");
        URI sqliteUri = URI.create("sqlite://" + dbPath + "?table=test_table");

        Schema schema = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("test_table")
                .build();

        Sink sink = Sink.builder()
                .withOutput(sqliteUri)
                .withSchema(schema)
                .build();

        Fields fields = new Fields("name", "age", "city");
        fields = fields.applyTypes(String.class, Integer.class, String.class);

        SQLiteScheme scheme = new SQLiteScheme(fields, sink);
        SQLiteTap tap = new SQLiteTap(scheme, sink);

        // Test tap properties
        assertEquals("test_table", tap.getTableName());
        assertEquals(dbPath.toString(), tap.getDatabasePath());
        assertTrue(tap.getIdentifier().contains("sqlite://"));
        assertTrue(tap.getIdentifier().contains("test_table"));

        // Test resource creation
        assertFalse(tap.resourceExists(testConfig));
        assertTrue(tap.createResource(testConfig));
        assertTrue(tap.resourceExists(testConfig));

        // Test database connection and table creation
        Connection conn = tap.getConnection(testConfig);
        assertNotNull(conn);
        assertFalse(conn.getAutoCommit()); // Should be false for transactions

        // Verify table structure
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA table_info(test_table)")) {

            assertTrue(rs.next());
            assertEquals("name", rs.getString("name"));
            assertEquals("TEXT", rs.getString("type"));

            assertTrue(rs.next());
            assertEquals("age", rs.getString("name"));
            assertEquals("TEXT", rs.getString("type")); // Age mapped as TEXT due to default mapping

            assertTrue(rs.next());
            assertEquals("city", rs.getString("name"));
            assertEquals("TEXT", rs.getString("type"));
        }

        // Clean up
        tap.deleteResource(testConfig);
    }

    @Test
    void testSQLiteSchemeConfiguration() {
        Schema schema = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("config_test")
                .build();

        Sink sink = Sink.builder()
                .withSchema(schema)
                .build();

        Fields fields = new Fields("id", "data");
        SQLiteScheme scheme = new SQLiteScheme(fields, sink);

        assertNotNull(scheme.getFields());
        assertEquals(2, scheme.getFields().size());
        assertEquals("id", scheme.getFields().get(0).toString());
        assertEquals("data", scheme.getFields().get(1).toString());
        assertSame(sink, scheme.getSinkModel());
    }

    @Test
    void testURITableNameExtraction() throws Exception {
        // Test with query parameter
        Path dbPath = tempDir.resolve("uri_test.db");
        URI uriWithTable = URI.create("sqlite://" + dbPath.toString() + "?table=custom_table");

        Schema schema = Schema.builder().withFormat(Format.sql).build();
        Sink sink = Sink.builder().withOutput(uriWithTable).withSchema(schema).build();

        SQLiteScheme scheme = new SQLiteScheme(new Fields("test"), sink);
        SQLiteTap tap = new SQLiteTap(scheme, sink);

        assertEquals("custom_table", tap.getTableName());

        // Test with schema table name
        Schema schemaWithTable = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("schema_table")
                .build();

        URI uriWithoutTable = URI.create("sqlite://" + dbPath.toString());
        Sink sinkWithSchemaTable = Sink.builder()
                .withOutput(uriWithoutTable)
                .withSchema(schemaWithTable)
                .build();

        SQLiteTap tapWithSchemaTable = new SQLiteTap(scheme, sinkWithSchemaTable);
        assertEquals("schema_table", tapWithSchemaTable.getTableName());

        // Test default table name
        Schema defaultSchema = Schema.builder().withFormat(Format.sql).build();
        Sink defaultSink = Sink.builder()
                .withOutput(uriWithoutTable)
                .withSchema(defaultSchema)
                .build();

        SQLiteTap defaultTap = new SQLiteTap(scheme, defaultSink);
        assertEquals("data", defaultTap.getTableName());
    }

    @Test
    void testErrorHandling() {
        // Test invalid URI
        assertThrows(Exception.class, () -> {
            URI invalidUri = URI.create("sqlite://"); // No path
            Schema schema = Schema.builder().withFormat(Format.sql).build();
            Sink sink = Sink.builder().withOutput(invalidUri).withSchema(schema).build();

            SQLiteScheme scheme = new SQLiteScheme(new Fields("test"), sink);
            new SQLiteTap(scheme, sink);
        });
    }
}
