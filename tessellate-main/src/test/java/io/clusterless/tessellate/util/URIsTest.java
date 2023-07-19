/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class URIsTest {
    @Test
    void trim() {
        Assertions.assertEquals(URI.create("s3://bucket/"), URIs.trim(URI.create("s3://bucket/path/"), 1));
        Assertions.assertEquals(URI.create("s3://bucket/path1/"), URIs.trim(URI.create("s3://bucket/path1/path2/"), 1));
        Assertions.assertEquals(URI.create("s3://bucket/path1/"), URIs.trim(URI.create("s3://bucket/path1/path2"), 1));
        Assertions.assertEquals(URI.create("s3://bucket/"), URIs.trim(URI.create("s3://bucket/path1/path2/"), 2));
        Assertions.assertEquals(URI.create("s3://bucket/"), URIs.trim(URI.create("s3://bucket/path1/path2"), 2));
    }
}
