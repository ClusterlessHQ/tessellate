/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc;

import cascading.tuple.Fields;
import cascading.tuple.type.CoercibleType;
import io.clusterless.tessellate.model.Sink;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

public class SQLiteTypeMapper {

    public String mapToSQLiteType(String fieldName, Sink sink) {
        // Try to get type information from schema declared fields
        if (sink.schema() != null && sink.schema().declared() != null) {
            for (io.clusterless.tessellate.model.Field field : sink.schema().declared()) {
                Fields fields = field.fields();
                if (fields != null) {
                    for (int i = 0; i < fields.size(); i++) {
                        if (fieldName.equals(fields.get(i).toString())) {
                            Object type = fields.getType(i);
                            if (type instanceof Class<?>) {
                                return mapJavaTypeToSQLite((Class<?>) type);
                            }
                        }
                    }
                }
            }
        }

        // Default to TEXT if no type information available
        return "TEXT";
    }

    public String mapJavaTypeToSQLite(Class<?> javaType) {
        if (javaType == null) {
            return "TEXT";
        }

        // Handle primitive wrapper types and their primitives
        if (javaType == String.class || javaType == char.class || javaType == Character.class) {
            return "TEXT";
        }

        if (javaType == Integer.class || javaType == int.class ||
                javaType == Long.class || javaType == long.class ||
                javaType == Short.class || javaType == short.class ||
                javaType == Byte.class || javaType == byte.class) {
            return "INTEGER";
        }

        if (javaType == Double.class || javaType == double.class ||
                javaType == Float.class || javaType == float.class ||
                javaType == BigDecimal.class) {
            return "REAL";
        }

        if (javaType == Boolean.class || javaType == boolean.class) {
            return "INTEGER"; // SQLite stores booleans as 0/1
        }

        // Date/Time types - store as TEXT in ISO format
        if (javaType == Date.class ||
                javaType == Instant.class ||
                javaType == LocalDate.class ||
                javaType == LocalDateTime.class ||
                javaType == LocalTime.class ||
                java.sql.Date.class.isAssignableFrom(javaType) ||
                java.sql.Time.class.isAssignableFrom(javaType) ||
                java.sql.Timestamp.class.isAssignableFrom(javaType)) {
            return "TEXT";
        }

        // Binary data
        if (javaType == byte[].class) {
            return "BLOB";
        }

        // CoercibleType handling (Cascading specific)
        if (CoercibleType.class.isAssignableFrom(javaType)) {
            try {
                CoercibleType<?> coercibleType = (CoercibleType<?>) javaType.getDeclaredConstructor().newInstance();
                Class<?> canonicalType = coercibleType.getCanonicalType();
                return mapJavaTypeToSQLite(canonicalType);
            } catch (Exception e) {
                return "TEXT"; // Fall back to TEXT if we can't instantiate
            }
        }

        // Default to TEXT for unknown types
        return "TEXT";
    }

    public Object convertValueForSQLite(Object value, Class<?> expectedType) {
        if (value == null) {
            return null;
        }

        // Boolean conversion for SQLite
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }

        // Date/Time conversion to ISO string format
        if (value instanceof Date) {
            return ((Date) value).toInstant().toString();
        }

        if (value instanceof Instant) {
            return value.toString();
        }

        if (value instanceof LocalDate) {
            return value.toString();
        }

        if (value instanceof LocalDateTime) {
            return value.toString();
        }

        if (value instanceof LocalTime) {
            return value.toString();
        }

        // For most other types, SQLite can handle the conversion
        return value;
    }
}
