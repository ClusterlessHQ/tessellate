/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringHelperTest {
    @Test
    void camelCase() {
        assertEquals("FooBarId", StringHelper.convertConsecutiveUpperCase("FooBarID"));
        assertEquals("FooBarIdBaz", StringHelper.convertConsecutiveUpperCase("FooBarIDBaz"));
        assertEquals("FooBarIdBazUrl", StringHelper.convertConsecutiveUpperCase("FooBarIDBazURL"));
        assertEquals("FooBarIdBazUrl", StringHelper.convertConsecutiveUpperCase("FOOBarIDBazURL"));
        assertEquals("FooBaz.BarIdBazUrl", StringHelper.convertConsecutiveUpperCase("FOOBaz.BarIDBazURL"));
    }
}
