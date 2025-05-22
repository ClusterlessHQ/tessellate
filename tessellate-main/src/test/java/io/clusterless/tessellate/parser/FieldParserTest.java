/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import cascading.tuple.type.DateType;
import cascading.tuple.type.InstantType;
import clusterless.commons.temporal.IntervalUnits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SystemStubsExtension.class)
public class FieldParserTest {

    @SystemStub
    private EnvironmentVariables variables = new EnvironmentVariables();

    @Test
    void parseFields() {
        assertNotNull(FieldParser.parseField("@field"));
        assertNotNull(FieldParser.parseField("@field|DateTime"));
        assertNotNull(FieldParser.parseField("@field|DateTime|yyyyMMdd"));
        assertNotNull(FieldParser.parseField("1|DateTime|yyyyMMdd"));
        assertNotNull(FieldParser.parseField("@field|Instant|twelfths|yyyyMMdd"));
    }

    @Test
    void parseFieldsWithSpaces() {
        assertEquals("@field name", FieldParser.parseField("'@field name'").fieldRef().toString());
        assertEquals("@field name", FieldParser.parseField("'@field name'|DateTime").fieldRef().toString());
        assertEquals("@field name", FieldParser.parseField("'@field name'|DateTime|yyyyMMdd").fieldRef().toString());
        assertEquals("@field name", FieldParser.parseField("'@field name'|Instant|twelfths|yyyyMMdd").fieldRef().toString());
    }

    @Test
    void parseFieldsWithSlash() {
        assertEquals("@field/name", FieldParser.parseField("'@field/name'").fieldRef().toString());
        assertEquals("@field/name", FieldParser.parseField("'@field/name'|DateTime").fieldRef().toString());
        assertEquals("@field/name", FieldParser.parseField("'@field/name'|DateTime|yyyyMMdd").fieldRef().toString());
        assertEquals("@field/name", FieldParser.parseField("'@field/name'|Instant|twelfths|yyyyMMdd").fieldRef().toString());
    }

    @Test
    void parseFieldsWithDash() {
        assertEquals("@field-name", FieldParser.parseField("'@field-name'").fieldRef().toString());
        assertEquals("@field-name", FieldParser.parseField("'@field-name'|DateTime").fieldRef().toString());
        assertEquals("@field-name", FieldParser.parseField("'@field-name'|DateTime|yyyyMMdd").fieldRef().toString());
        assertEquals("@field-name", FieldParser.parseField("'@field-name'|Instant|twelfths|yyyyMMdd").fieldRef().toString());
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

    @Test
    void parseFieldsWithTypeDefaults() {
        assertEquals(
                "yyyyMMdd",
                assertInstanceOf(
                        DateType.class,
                        FieldsParser.INSTANCE.asFields(FieldParser.parseField("@field|DateTime|yyyyMMdd")).getType(0)
                ).getDateFormat().toLocalizedPattern()
        );

        variables.set(FieldsParser.DATE_TYPE_FORMAT, "yyyyMMdd");
        assertEquals(
                "yyyyMMdd",
                assertInstanceOf(
                        DateType.class,
                        FieldsParser.INSTANCE.asFields(FieldParser.parseField("@field|DateTime")).getType(0)
                ).getDateFormat().toLocalizedPattern()
        );

        assertEquals(
                FieldsParser.createPattern("yyyyMMdd").toString(),
                assertInstanceOf(
                        InstantType.class,
                        FieldsParser.INSTANCE.asFields(FieldParser.parseField("@field|Instant|twelfths|yyyyMMdd")).getType(0)
                ).getDateTimeFormatter().toString()
        );

        variables.set(FieldsParser.INSTANT_TYPE_FORMAT, "twelfths|yyyyMMdd");

        assertEquals(
                FieldsParser.createPattern("yyyyMMdd").toString(),
                assertInstanceOf(
                        InstantType.class,
                        FieldsParser.INSTANCE.asFields(FieldParser.parseField("@field|Instant")).getType(0)
                ).getDateTimeFormatter().toString()
        );

        variables.set(FieldsParser.INSTANT_TYPE_FORMAT, "twelfths");

        assertEquals(
                IntervalUnits.formatter(IntervalUnits.find("twelfths")).toString(),
                assertInstanceOf(
                        InstantType.class,
                        FieldsParser.INSTANCE.asFields(FieldParser.parseField("@field|Instant")).getType(0)
                ).getDateTimeFormatter().toString()
        );
    }
}
