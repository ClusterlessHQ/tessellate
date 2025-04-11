/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FormatFieldsTest {
    @Test
    void lowerUnderscore() {
        assertEquals("foo_bar", FormatFieldsIntrinsic.lowerUnderscore("FooBar", String.class));
        assertEquals("foo_bar", FormatFieldsIntrinsic.lowerUnderscore("Foo Bar", String.class));
        assertEquals("foo_bar", FormatFieldsIntrinsic.lowerUnderscore("Foo_Bar", String.class));
        assertEquals("foo_bar", FormatFieldsIntrinsic.lowerUnderscore("Foo.Bar", String.class));
        assertEquals("$foo_bar", FormatFieldsIntrinsic.lowerUnderscore("$Foo.Bar", String.class));
    }

    @Test
    void upperUnderscore() {
        assertEquals("FOO_BAR", FormatFieldsIntrinsic.upperUnderscore("FooBar", String.class));
        assertEquals("FOO_BAR", FormatFieldsIntrinsic.upperUnderscore("Foo Bar", String.class));
        assertEquals("FOO_BAR", FormatFieldsIntrinsic.upperUnderscore("Foo_Bar", String.class));
        assertEquals("FOO_BAR", FormatFieldsIntrinsic.upperUnderscore("Foo.Bar", String.class));
        assertEquals("$FOO_BAR", FormatFieldsIntrinsic.upperUnderscore("$Foo.Bar", String.class));
    }

    @Test
    void camelCase() {
        assertEquals("FooBar", FormatFieldsIntrinsic.camelCase("FooBar", String.class));
        assertEquals("FooBar", FormatFieldsIntrinsic.camelCase("Foo Bar", String.class));
        assertEquals("FooBarId", FormatFieldsIntrinsic.camelCase("Foo Bar ID", String.class));
        assertEquals("FooBar", FormatFieldsIntrinsic.camelCase("Foo_Bar", String.class));
        assertEquals("FooBar", FormatFieldsIntrinsic.camelCase("Foo.Bar", String.class));
        assertEquals("FooBarId", FormatFieldsIntrinsic.camelCase("FooBarID", String.class));
        assertEquals("FooBarId", FormatFieldsIntrinsic.camelCase("Foo.BarID", String.class));
        assertEquals("FooBarIdUrl", FormatFieldsIntrinsic.camelCase("Foo.BarIDUrl", String.class));
        assertEquals("FooBarIdBazUrl", FormatFieldsIntrinsic.camelCase("Foo.BarIDBazURL", String.class));
    }
}
