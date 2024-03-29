/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import cascading.nested.json.JSONCoercibleType;
import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.DateType;
import cascading.tuple.type.InstantType;
import clusterless.commons.temporal.IntervalUnits;
import io.clusterless.tessellate.parser.ast.Field;
import io.clusterless.tessellate.parser.ast.FieldType;
import io.clusterless.tessellate.parser.ast.FieldTypeParam;
import io.clusterless.tessellate.type.WrappedCoercibleType;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
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

        Field parsed = FieldParser.parseField(value);

        return asFields(parsed, defaultType);
    }

    public Fields asFields(List<Field> fields) {
        return asFields(fields, null);
    }

    public Fields asFields(List<Field> fields, Type defaultType) {
        return fields.stream()
                .map(f -> asFields(f, defaultType))
                .reduce(Fields.NONE, Fields::append);
    }

    @NotNull
    public Fields asFields(Field parsed) {
        return asFields(parsed, null);
    }

    public Fields asFields(Field parsed, Type defaultType) {
        Comparable<?> name = parsed.fieldRef().asComparable();
        if (parsed.fieldType().isEmpty()) {
            return defaultType == null ? new Fields(name) : new Fields(name, defaultType);
        }

        return new Fields(name, resolveType(parsed.fieldType().get()));
    }

    protected Type resolveType(FieldType fieldType) {
        Type type = null;

        String typeName = fieldType.name().name();

        String first = fieldType.param().map(FieldTypeParam::param1).orElse(null);
        String second = fieldType.param().map(FieldTypeParam::param2).orElse(null);

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
