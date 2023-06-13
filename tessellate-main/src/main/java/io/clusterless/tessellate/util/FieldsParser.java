/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import cascading.nested.json.JSONCoercibleType;
import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.DateType;
import cascading.tuple.type.InstantType;
import io.clusterless.tessellate.temporal.IntervalUnits;
import io.clusterless.tessellate.type.WrappedCoercibleType;

import java.lang.reflect.Type;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Parses a String into a Fields instance.
 * <p>
 * It accepts the following formats:
 * <pre>
 *     name
 *     name|primitive
 *     name|primitive|nullToken
 *     name|primitiveObject
 *     name|primitiveObject|nullToken
 *     name|DateTime
 *     name|DateTime|format
 *     name|Instant
 *     name|Instant|format
 * </pre>
 */
public class FieldsParser {
    public static final FieldsParser INSTANCE = new FieldsParser();

    private DateType defaultDateTimeType = new DateType("yyyy-MM-dd HH:mm:ss.SSSSSS z", TimeZone.getTimeZone("UTC"));
    private InstantType defaultInstantType = InstantType.ISO_MICROS;

    /**
     * Parses a String into a Fields instance with one field name and an optional type.
     *
     * @param value
     * @param defaultType
     * @return
     */
    public Fields parseSingleFields(String value, Type defaultType) {
        if (value == null) {
            throw new IllegalArgumentException("value may not be null");
        }

        if (value.isEmpty() || value.equals("ALL")) {
            return Fields.ALL;
        }

        if (value.equals("UNKNOWN")) {
            return Fields.UNKNOWN;
        }

        if (value.equals("NONE")) {
            return Fields.NONE;
        }

        String[] split = value.split("\\|", 2);

        Comparable<?> field = parseInt(split[0]);

        if (split.length == 1) {
            return defaultType == null ? new Fields(field) : new Fields(field, defaultType);
        }

        return new Fields(field, resolveType(split[1]));
    }

    protected Type resolveType(String typeName) {
        Type type = null;

        String[] split = typeName.split("\\|");

        typeName = split[0];

        String first = split.length >= 2 ? split[1] : null;
        String second = split.length >= 3 ? split[2] : null;

        if (typeName.equalsIgnoreCase("string")) {
            type = String.class;
        } else if (typeName.equalsIgnoreCase("json")) {
            type = JSONUtil.TYPE;
        } else if (typeName.equalsIgnoreCase("DateTime")) {
            if (first == null) {
                type = defaultDateTimeType;
            } else {
                type = new DateType(first, TimeZone.getTimeZone("UTC"));
            }
        } else if (typeName.equalsIgnoreCase("Instant")) {
            if (first == null) {
                type = defaultInstantType;
            } else {
                TemporalUnit unit = getUnit(first);
                if (unit != null) {
                    if (second == null) {
                        type = new InstantType(unit, IntervalUnits.formatter(unit));
                    } else {
                        DateTimeFormatter pattern = createPattern(second);
                        type = new InstantType(unit, () -> pattern);
                    }
                } else {
                    DateTimeFormatter pattern = createPattern(first);
                    type = new InstantType(ChronoUnit.MILLIS, () -> pattern);
                }
            }
        }

        if (type == null && !typeName.contains(".") && Character.isUpperCase(typeName.charAt(0))) {
            typeName = "java.lang." + typeName;
        }

        if (type == null) {
            type = getType(typeName);
        }

        if (!(type instanceof CoercibleType) && first != null) {
            type = new WrappedCoercibleType<>(Coercions.coercions.get(type), first);
        }

        return type;
    }

    public String resolveTypeName(Type type) {
        if (type instanceof JSONCoercibleType) {
            return "json";
        }
        if (type instanceof DateType) {
            return "DateTime";
        }
        if (type instanceof InstantType) {
            return "Instant";
        }
        if (type == String.class) {
            return "String";
        }

        String[] typeNames = Coercions.getTypeNames(new Type[]{type});

        return typeNames[0];
    }

    private static DateTimeFormatter createPattern(String splitType) {
        return DateTimeFormatter.ofPattern(splitType)
                .withZone(ZoneId.of("UTC"));
    }

    private static TemporalUnit getUnit(String s) {
        try {
            return IntervalUnits.find(s.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public static Comparable<?> parseInt(String fieldName) {
        try {
            return Integer.parseInt(fieldName);
        } catch (NumberFormatException e) {
            // i'm not a number!
        }

        return fieldName;
    }

    private static Type getType(String typeName) {
        try {
            return Coercions.asType(typeName);
        } catch (Exception exception) {
            throw new IllegalArgumentException("unknown type: " + typeName, exception);
        }
    }
}
