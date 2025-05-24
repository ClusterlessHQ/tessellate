/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class URIsTest {
    @Test
    void trim() {
        assertEquals(URI.create("s3://bucket/"), URIs.trim(URI.create("s3://bucket/path/"), 1));
        assertEquals(URI.create("s3://bucket/path1/"), URIs.trim(URI.create("s3://bucket/path1/path2/"), 1));
        assertEquals(URI.create("s3://bucket/path1/"), URIs.trim(URI.create("s3://bucket/path1/path2"), 1));
        assertEquals(URI.create("s3://bucket/"), URIs.trim(URI.create("s3://bucket/path1/path2/"), 2));
        assertEquals(URI.create("s3://bucket/"), URIs.trim(URI.create("s3://bucket/path1/path2"), 2));
    }

    @Test
    void trimFilename() {
        assertEquals(URI.create("s3://bucket/path/"), URIs.trimFilename(URI.create("s3://bucket/path/"), true));
        assertEquals(URI.create("s3://bucket/path1/path2/"), URIs.trimFilename(URI.create("s3://bucket/path1/path2/"), true));
        assertEquals(URI.create("s3://bucket/path1/"), URIs.trimFilename(URI.create("s3://bucket/path1/path2"), true));
    }

    @Test
    void commonPathPrefix() {
        List<URI> uris = List.of(
                URI.create("s3://bucket/path1/path2/"),
                URI.create("s3://bucket/path1/path3/"),
                URI.create("s3://bucket/path1/path4/")
        );

        assertEquals(URI.create("s3://bucket/path1/"), URIs.findCommonPathPrefix(uris, 0));
        assertEquals(URI.create("s3://bucket/path1/"), URIs.findCommonPathPrefix(uris, 1));
        assertEquals(URI.create("s3://bucket/"), URIs.findCommonPathPrefix(uris, 2));
    }

    @Test
    void extractFilePath() {
        // Test absolute paths (3 slashes, empty authority)
        assertEquals("/absolute/path/to/file.db", URIs.extractFilePath(URI.create("sqlite:///absolute/path/to/file.db")));
        assertEquals("/home/user/data.db", URIs.extractFilePath(URI.create("file:///home/user/data.db")));

        // Test relative paths (2 slashes, authority contains first path segment)
        assertEquals("relative/path/to/file.db", URIs.extractFilePath(URI.create("sqlite://relative/path/to/file.db")));
        assertEquals("data/local.db", URIs.extractFilePath(URI.create("file://data/local.db")));
        assertEquals("./output/results.db", URIs.extractFilePath(URI.create("sqlite://./output/results.db")));

        // Test absolute paths (1 slash, no authority)
        assertEquals("/absolute/path", URIs.extractFilePath(URI.create("sqlite:/absolute/path")));
        assertEquals("/usr/local/data.db", URIs.extractFilePath(URI.create("file:/usr/local/data.db")));

        // Test authority only (no additional path)
        assertEquals("filename.db", URIs.extractFilePath(URI.create("sqlite://filename.db")));
        assertEquals("database", URIs.extractFilePath(URI.create("file://database")));
    }
}
