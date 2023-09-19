/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FieldParserTest {
    @Test
    void parseFields() {
        assertNotNull(FieldParser.parseField("@field"));
        assertNotNull(FieldParser.parseField("@field|DateTime"));
        assertNotNull(FieldParser.parseField("@field|DateTime|yyyyMMdd"));
        assertNotNull(FieldParser.parseField("1|DateTime|yyyyMMdd"));
        assertNotNull(FieldParser.parseField("@field|Instant|twelfths|yyyyMMdd"));
    }

    @Test
    void parseFieldsList() {
        assertEquals(1, FieldParser.parseFieldList("@field").size());
        assertEquals(5, FieldParser.parseFieldList("@field1+@field|DateTime+@field|DateTime|yyyyMMdd+1|DateTime|yyyyMMdd+@field|Instant|twelfths|yyyyMMdd").size());
        assertEquals(2, FieldParser.parseFieldList("@field1+@field2").size());
        assertEquals(2, FieldParser.parseFieldList("@field1 + @field2").size());
        assertEquals(4, FieldParser.parseFieldList("@field1 + @field2+@field3 +@field4").size());
        assertEquals(5, FieldParser.parseFieldList("@field1+ @field|DateTime +@field|DateTime|yyyyMMdd+1|DateTime|yyyyMMdd + @field|Instant|twelfths|yyyyMMdd").size());
    }
}
