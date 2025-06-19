/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import cascading.tuple.Fields;
import io.clusterless.tessellate.junit.PathForOutput;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Protocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

import static java.nio.file.Files.createDirectories;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Platform-agnostic tests for SQLite factory implementation.
 * Tests core functionality without dependencies on specific Cascading platforms.
 */
@ExtendWith(ResourceExtension.class)
public class SQLiteFactoryTest {

    private SQLiteFactory factory;
    private Properties testConfig;

    @BeforeEach
    void setUp() {
        factory = (SQLiteFactory) SQLiteFactory.INSTANCE;
        testConfig = SQLiteTestUtils.createTestConfig();
    }

    @Test
    void testFactoryProtocols() {
        Set<Protocol> protocols = factory.getSinkProtocols();

        assertTrue(protocols.contains(Protocol.sqlite), "factory should support sqlite protocol");
        assertEquals(1, protocols.size(), "factory should only support sqlite protocol");
    }

    @Test
    void testFactoryFormats() {
        Set<Format> formats = factory.getFormats();

        assertTrue(formats.contains(Format.sql), "factory should support sql format");
        assertEquals(1, formats.size(), "factory should only support sql format");
    }

    @Test
    void testFactoryCompressions() {
        Set<Compression> compressions = factory.getCompressions();

        assertTrue(compressions.contains(Compression.none), "factory should support no compression");
        assertEquals(1, compressions.size(), "factory should only support no compression");
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

        // Test CoercibleType-based conversion
        assertEquals(1, mapper.convertValueForSQLite(true, (cascading.tuple.type.CoercibleType<?>) null)); // null CoercibleType falls back to direct conversion
        assertEquals(0, mapper.convertValueForSQLite(false, (cascading.tuple.type.CoercibleType<?>) null));
        assertNull(mapper.convertValueForSQLite(null, (cascading.tuple.type.CoercibleType<?>) null));
        assertEquals("test", mapper.convertValueForSQLite("test", (cascading.tuple.type.CoercibleType<?>) null));
    }

    @Test
    void testSQLiteTypeMapperWithFields() {
        SQLiteTypeMapper mapper = new SQLiteTypeMapper();

        // Test Fields-based type mapping (the method actually used in production)
        Fields fields = new Fields("name", "age", "active");
        fields = fields.applyTypes(String.class, Integer.class, Boolean.class);

        assertEquals("TEXT", mapper.mapFieldToSQLiteType(fields, 0));      // name -> String -> TEXT
        assertEquals("INTEGER", mapper.mapFieldToSQLiteType(fields, 1));   // age -> Integer -> INTEGER
        assertEquals("INTEGER", mapper.mapFieldToSQLiteType(fields, 2));   // active -> Boolean -> INTEGER

        // Test edge cases
        assertEquals("TEXT", mapper.mapFieldToSQLiteType(null, 0));
        assertEquals("TEXT", mapper.mapFieldToSQLiteType(fields, -1));
        assertEquals("TEXT", mapper.mapFieldToSQLiteType(fields, 10));
    }

    @Test
    void testSQLiteTypeMapperWithCoercibleTypes() {
        SQLiteTypeMapper mapper = new SQLiteTypeMapper();

        // Test mapTypeObjectToSQLite with Class objects (direct types)
        assertEquals("INTEGER", mapper.mapTypeObjectToSQLite(Integer.class));
        assertEquals("INTEGER", mapper.mapTypeObjectToSQLite(Long.class));
        assertEquals("TEXT", mapper.mapTypeObjectToSQLite(String.class));
        assertEquals("REAL", mapper.mapTypeObjectToSQLite(Double.class));
        assertEquals("INTEGER", mapper.mapTypeObjectToSQLite(Boolean.class));

        // Test null and unknown types
        assertEquals("TEXT", mapper.mapTypeObjectToSQLite(null));
        assertEquals("TEXT", mapper.mapTypeObjectToSQLite("unknown"));

        // Note: CoercibleType.getCanonicalType() functionality is verified in integration tests
        // The AWS S3 access log test verifies that WrappedCoercibleType instances with
        // IntegerObjectCoerce and LongObjectCoerce are properly mapped to INTEGER SQLite columns
    }

    @Test
    void testSQLiteTapCreation(@PathForOutput URI output) throws Exception {
        Path path = Paths.get(output);
        createDirectories(path);
        Path dbPath = path.resolve("test.db");
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

        SQLiteScheme scheme = new SQLiteScheme(fields);
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
            assertEquals("INTEGER", rs.getString("type")); // Age mapped as INTEGER due to Integer.class type

            assertTrue(rs.next());
            assertEquals("city", rs.getString("name"));
            assertEquals("TEXT", rs.getString("type"));
        }

        // Clean up
        tap.deleteResource(testConfig);

        // --- Test with relative file path ---
        // Relativize the output path to the current working directory, then use that as the base for the relative db path
        Path cwd = Paths.get("").toAbsolutePath();
        Path outputRelToCwd = cwd.relativize(path.toAbsolutePath());
        Path relDbPath = outputRelToCwd.resolve("relative_test.db");
        URI relSqliteUri = URI.create("sqlite://" + relDbPath + "?table=rel_table");

        Schema relSchema = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("rel_table")
                .build();

        Sink relSink = Sink.builder()
                .withOutput(relSqliteUri)
                .withSchema(relSchema)
                .build();

        SQLiteScheme relScheme = new SQLiteScheme(fields);
        SQLiteTap relTap = new SQLiteTap(relScheme, relSink);

        assertEquals("rel_table", relTap.getTableName());
        assertEquals(relDbPath.toString(), relTap.getDatabasePath());
        assertTrue(relTap.getIdentifier().contains("sqlite://"));
        assertTrue(relTap.getIdentifier().contains("rel_table"));

        // Clean up if file was created
        relTap.deleteResource(testConfig);
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
        SQLiteScheme scheme = new SQLiteScheme(fields);

        assertNotNull(scheme.getSinkFields());
        assertEquals(2, scheme.getSinkFields().size());
        assertEquals("id", scheme.getSinkFields().get(0).toString());
        assertEquals("data", scheme.getSinkFields().get(1).toString());
    }

    @Test
    void testURITableNameExtraction(@PathForOutput URI output) throws Exception {
        // Test with query parameter
        Path dbPath = Paths.get(output).resolve("uri_test.db");
        URI uriWithTable = URI.create("sqlite://" + dbPath + "?table=custom_table");

        Schema schema = Schema.builder().withFormat(Format.sql).build();
        Sink sink = Sink.builder().withOutput(uriWithTable).withSchema(schema).build();

        SQLiteScheme scheme = new SQLiteScheme(new Fields("test"));
        SQLiteTap tap = new SQLiteTap(scheme, sink);

        assertEquals("custom_table", tap.getTableName());

        // Test with schema table name
        Schema schemaWithTable = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("schema_table")
                .build();

        URI uriWithoutTable = URI.create("sqlite://" + dbPath);
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

            SQLiteScheme scheme = new SQLiteScheme(new Fields("test"));
            new SQLiteTap(scheme, sink);
        });
    }

    @Test
    void testSQLiteTapSingleTableMode(@PathForOutput URI output) throws Exception {
        Path path = Paths.get(output);
        createDirectories(path);
        Path dbPath = path.resolve("single_table_test.db");
        URI sqliteUri = URI.create("sqlite://" + dbPath + "?table=test_table");

        Schema schema = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("test_table")
                .build();

        Sink sink = Sink.builder()
                .withOutput(sqliteUri)
                .withSchema(schema)
                .build();

        Fields fields = new Fields("id", "name");
        fields = fields.applyTypes(Integer.class, String.class);

        // Test that factory creates SQLiteTap (default behavior)
        var tap = factory.getSink(null, sink, fields);
        assertTrue(tap instanceof SQLiteTap, "factory should create SQLiteTap by default");
        assertFalse(tap instanceof SQLiteTableTap, "factory should not create SQLiteTableTap by default");

        SQLiteTap sqliteTap = (SQLiteTap) tap;

        // Test resource creation
        assertFalse(sqliteTap.resourceExists(testConfig), "resource should not exist initially");
        assertTrue(sqliteTap.createResource(testConfig), "resource creation should succeed");
        assertTrue(sqliteTap.resourceExists(testConfig), "resource should exist after creation");
        assertTrue(dbPath.toFile().exists(), "database file should exist");

        // Test resource deletion - should remove entire database file
        assertTrue(sqliteTap.deleteResource(testConfig), "resource deletion should succeed");
        assertFalse(dbPath.toFile().exists(), "database file should be deleted in single-table mode");
    }

    @Test
    void testSQLiteTableTapMultiTableMode(@PathForOutput URI output) throws Exception {
        Path path = Paths.get(output);
        createDirectories(path);
        Path dbPath = path.resolve("multi_table_test.db");
        URI sqliteUri = URI.create("sqlite://" + dbPath + "?" + SQLiteConfig.TABLE_PARAM + "=test_table&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);

        Schema schema = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("test_table")
                .build();

        Sink sink = Sink.builder()
                .withOutput(sqliteUri)
                .withSchema(schema)
                .build();

        Fields fields = new Fields("id", "name");
        fields = fields.applyTypes(Integer.class, String.class);

        // Test that factory creates SQLiteTableTap when mode=table
        var tap = factory.getSink(null, sink, fields);
        assertTrue(tap instanceof SQLiteTableTap, "factory should create SQLiteTableTap when mode=table");

        SQLiteTableTap sqliteTableTap = (SQLiteTableTap) tap;
        
        // Test identifier includes mode parameter
        String identifier = sqliteTableTap.getIdentifier();
        assertTrue(identifier.contains(SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE), "identifier should include mode=table parameter");
        assertTrue(identifier.contains("sqlite://"), "identifier should use sqlite:// scheme");

        // Test resource creation
        assertFalse(sqliteTableTap.resourceExists(testConfig), "resource should not exist initially");
        assertTrue(sqliteTableTap.createResource(testConfig), "resource creation should succeed");
        assertTrue(sqliteTableTap.resourceExists(testConfig), "resource should exist after creation");
        assertTrue(dbPath.toFile().exists(), "database file should exist");

        // Verify table exists in database
        Connection conn = sqliteTableTap.getConnection(testConfig);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'")) {
            assertTrue(rs.next(), "test_table should exist in database");
        }

        // Test resource deletion - should only drop table, not entire file
        assertTrue(sqliteTableTap.deleteResource(testConfig), "resource deletion should succeed");
        assertTrue(dbPath.toFile().exists(), "database file should still exist in multi-table mode");

        // Verify table was dropped but database file still exists
        conn = sqliteTableTap.getConnection(testConfig);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'")) {
            assertFalse(rs.next(), "test_table should be dropped from database");
        }
        conn.close();

        // Clean up the database file manually since table mode doesn't delete it
        dbPath.toFile().delete();
    }

    @Test
    void testSQLiteSchemeErrorHandling() {
        Fields fields = new Fields("test");
        SQLiteScheme scheme = new SQLiteScheme(fields);
        
        // Test source operations throw UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> {
            scheme.sourceConfInit(null, null, null);
        }, "source operations should not be supported");
        
        assertThrows(UnsupportedOperationException.class, () -> {
            scheme.source(null, null);
        }, "source operations should not be supported");
        
        assertThrows(UnsupportedOperationException.class, () -> {
            scheme.sink(null, null);
        }, "direct sink operations should not be supported");
    }

    @Test
    void testSQLiteBaseTapSharedFunctionality(@PathForOutput URI output) throws Exception {
        Path path = Paths.get(output);
        createDirectories(path);
        Path dbPath = path.resolve("base_tap_test.db");
        URI sqliteUri = URI.create("sqlite://" + dbPath + "?table=base_test");

        Schema schema = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("base_test")
                .build();

        Sink sink = Sink.builder()
                .withOutput(sqliteUri)
                .withSchema(schema)
                .build();

        Fields fields = new Fields("id", "name", "timestamp");
        fields = fields.applyTypes(Integer.class, String.class, java.time.Instant.class);

        SQLiteScheme scheme = new SQLiteScheme(fields);
        SQLiteTap tap = new SQLiteTap(scheme, sink);

        // Test shared base functionality
        assertFalse(tap.resourceExists(testConfig), "resource should not exist initially");
        assertTrue(tap.createResource(testConfig), "resource creation should succeed");
        assertTrue(tap.resourceExists(testConfig), "resource should exist after creation");
        
        // Test connection reuse
        var conn1 = tap.getConnection(testConfig);
        var conn2 = tap.getConnection(testConfig);
        assertSame(conn1, conn2, "should reuse existing connection");
        
        // Test table name and database path extraction
        assertEquals("base_test", tap.getTableName());
        assertEquals(dbPath.toString(), tap.getDatabasePath());
        
        // Test transaction methods
        assertTrue(tap.commitResource(testConfig), "commit should succeed");
        assertTrue(tap.rollbackResource(testConfig), "rollback should succeed");
        
        // Clean up
        tap.deleteResource(testConfig);
    }

    @Test
    void testIdentifierConsistency(@PathForOutput URI output) throws Exception {
        Path path = Paths.get(output);
        createDirectories(path);
        Path dbPath = path.resolve("identifier_test.db");
        
        // Test SQLiteTap identifier (no mode parameter)
        URI sqliteUri = URI.create("sqlite://" + dbPath + "?table=test_table");
        Schema schema = Schema.builder().withFormat(Format.sql).withTableName("test_table").build();
        Sink sink = Sink.builder().withOutput(sqliteUri).withSchema(schema).build();
        Fields fields = new Fields("id");
        SQLiteScheme scheme = new SQLiteScheme(fields);
        
        SQLiteTap regularTap = new SQLiteTap(scheme, sink);
        String regularIdentifier = regularTap.getIdentifier();
        
        // Test SQLiteTableTap identifier (includes mode=table parameter)
        URI tableUri = URI.create("sqlite://" + dbPath + "?" + SQLiteConfig.TABLE_PARAM + "=test_table&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);
        Sink tableSink = Sink.builder().withOutput(tableUri).withSchema(schema).build();
        SQLiteTableTap tableTap = new SQLiteTableTap(scheme, tableSink);
        String tableIdentifier = tableTap.getIdentifier();
        
        // Both should use sqlite:// scheme
        assertTrue(regularIdentifier.startsWith("sqlite://"), "regular tap should use sqlite:// scheme");
        assertTrue(tableIdentifier.startsWith("sqlite://"), "table tap should use sqlite:// scheme");
        
        // Table tap should include mode parameter
        String modeTableParam = SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE;
        assertFalse(regularIdentifier.contains(modeTableParam), "regular tap should not include mode parameter");
        assertTrue(tableIdentifier.contains(modeTableParam), "table tap should include mode=table parameter");
        
        // Both should include table name
        assertTrue(regularIdentifier.contains("test_table"), "regular tap identifier should include table name");
        assertTrue(tableIdentifier.contains("test_table"), "table tap identifier should include table name");
    }

    @Test
    void testMultipleTablesInSameDatabase(@PathForOutput URI output) throws Exception {
        Path path = Paths.get(output);
        createDirectories(path);
        Path dbPath = path.resolve("shared_database.db");
        
        // Create two different table taps for the same database
        URI table1Uri = URI.create("sqlite://" + dbPath + "?" + SQLiteConfig.TABLE_PARAM + "=table1&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);
        URI table2Uri = URI.create("sqlite://" + dbPath + "?" + SQLiteConfig.TABLE_PARAM + "=table2&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);

        Schema schema1 = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("table1")
                .build();

        Schema schema2 = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("table2")
                .build();

        Sink sink1 = Sink.builder()
                .withOutput(table1Uri)
                .withSchema(schema1)
                .build();

        Sink sink2 = Sink.builder()
                .withOutput(table2Uri)
                .withSchema(schema2)
                .build();

        Fields fields = new Fields("id", "data");
        fields = fields.applyTypes(Integer.class, String.class);

        // Create both table taps
        var tap1 = (SQLiteTableTap) factory.getSink(null, sink1, fields);
        var tap2 = (SQLiteTableTap) factory.getSink(null, sink2, fields);

        // Create both tables
        assertTrue(tap1.createResource(testConfig), "table1 creation should succeed");
        assertTrue(tap2.createResource(testConfig), "table2 creation should succeed");
        assertTrue(dbPath.toFile().exists(), "database file should exist");

        // Verify both tables exist
        Connection conn = tap1.getConnection(testConfig);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs1 = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='table1'");
            assertTrue(rs1.next(), "table1 should exist");
            rs1.close();

            ResultSet rs2 = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='table2'");
            assertTrue(rs2.next(), "table2 should exist");
            rs2.close();
        }

        // Delete first table - database and second table should remain
        assertTrue(tap1.deleteResource(testConfig), "table1 deletion should succeed");
        assertTrue(dbPath.toFile().exists(), "database file should still exist");

        // Verify only table1 was dropped
        conn = tap2.getConnection(testConfig);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs1 = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='table1'");
            assertFalse(rs1.next(), "table1 should be dropped");
            rs1.close();

            ResultSet rs2 = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='table2'");
            assertTrue(rs2.next(), "table2 should still exist");
            rs2.close();
        }

        // Clean up
        tap2.deleteResource(testConfig);
        dbPath.toFile().delete();
    }

    @Test
    void testSQLiteTupleEntryCollectorErrorHandling(@PathForOutput URI output) throws Exception {
        Path path = Paths.get(output);
        createDirectories(path);
        Path dbPath = path.resolve("collector_error_test.db");
        URI sqliteUri = URI.create("sqlite://" + dbPath + "?table=error_test");

        Schema schema = Schema.builder()
                .withFormat(Format.sql)
                .withTableName("error_test")
                .build();

        Sink sink = Sink.builder()
                .withOutput(sqliteUri)
                .withSchema(schema)
                .build();

        Fields fields = new Fields("id", "data");
        fields = fields.applyTypes(Integer.class, String.class);

        SQLiteScheme scheme = new SQLiteScheme(fields);
        SQLiteTap tap = new SQLiteTap(scheme, sink);
        
        // Create the resource
        assertTrue(tap.createResource(testConfig), "resource creation should succeed");
        
        // Test that collector can be created and closed properly
        Properties flowConfig = new Properties();
        SQLiteConfig.setDefaults(flowConfig);
        cascading.flow.local.LocalFlowProcess flowProcess = new cascading.flow.local.LocalFlowProcess(flowConfig);
        var collector = new SQLiteTupleEntryCollector(flowProcess, tap);
        
        // Test batch tracking
        assertEquals(0, collector.getCurrentBatchCount(), "initial batch count should be 0");
        assertEquals(0, collector.getTotalRowsProcessed(), "initial rows processed should be 0");
        
        // Test closing without errors
        assertDoesNotThrow(() -> collector.close(), "collector close should not throw");
        
        // Clean up
        tap.deleteResource(testConfig);
    }

    @Test
    void testFactoryTapSelection() throws Exception {
        // Test factory selects correct tap type based on URI parameters
        Schema schema = Schema.builder().withFormat(Format.sql).withTableName("test").build();
        Fields fields = new Fields("id");
        
        // Regular SQLite URI should create SQLiteTap
        URI regularUri = URI.create("sqlite://test.db?table=test");
        Sink regularSink = Sink.builder().withOutput(regularUri).withSchema(schema).build();
        var regularTap = factory.getSink(null, regularSink, fields);
        assertTrue(regularTap instanceof SQLiteTap, "regular URI should create SQLiteTap");
        assertFalse(regularTap instanceof SQLiteTableTap, "regular URI should not create SQLiteTableTap");
        
        // SQLite URI with mode=table should create SQLiteTableTap
        URI tableUri = URI.create("sqlite://test.db?" + SQLiteConfig.TABLE_PARAM + "=test&" + SQLiteConfig.MODE_PARAM + "=" + SQLiteConfig.TABLE_MODE_VALUE);
        Sink tableSink = Sink.builder().withOutput(tableUri).withSchema(schema).build();
        var tableTap = factory.getSink(null, tableSink, fields);
        assertTrue(tableTap instanceof SQLiteTableTap, "mode=table URI should create SQLiteTableTap");
        
        // Other mode values should still create SQLiteTap
        URI otherModeUri = URI.create("sqlite://test.db?" + SQLiteConfig.TABLE_PARAM + "=test&" + SQLiteConfig.MODE_PARAM + "=other");
        Sink otherModeSink = Sink.builder().withOutput(otherModeUri).withSchema(schema).build();
        var otherModeTap = factory.getSink(null, otherModeSink, fields);
        assertTrue(otherModeTap instanceof SQLiteTap, "other mode values should create SQLiteTap");
        assertFalse(otherModeTap instanceof SQLiteTableTap, "other mode values should not create SQLiteTableTap");
    }
}
