/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import cascading.tuple.Fields;
import cascading.tuple.type.CoercibleType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

/**
 * Maps Java and Cascading field types to SQLite column types.
 * <p>
 * Key features:
 * - Uses Fields.getType(index) for efficient type lookup from pipeline currentFields
 * - Leverages CoercibleType.getCanonicalType() to resolve underlying Java types
 * - Maps Java types: String→TEXT, Integer/Long→INTEGER, Double/Float→REAL, Boolean→INTEGER, Instant→TEXT
 */
public class SQLiteTypeMapper {

    /**
     * Maps field types from Fields instance to SQLite types using field index.
     * This is the most efficient approach as it directly accesses the type array.
     * Used by SQLiteTap for table creation.
     *
     * @param fields     the Fields instance containing type information
     * @param fieldIndex the index of the field to map
     * @return SQLite column type (TEXT, INTEGER, REAL, BLOB)
     */
    public String mapFieldToSQLiteType(Fields fields, int fieldIndex) {
        if (fields == null || !fields.hasTypes() || fieldIndex < 0 || fieldIndex >= fields.size()) {
            return "TEXT";
        }

        java.lang.reflect.Type type = fields.getType(fieldIndex);
        if (type instanceof Class<?>) {
            return mapJavaTypeToSQLite((Class<?>) type);
        } else if (type != null) {
            return mapTypeObjectToSQLite(type);
        }

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

        // Note: CoercibleType instances are now handled in mapTypeObjectToSQLite()
        // This method focuses on direct Class<?> type mapping

        // Default to TEXT for unknown types
        return "TEXT";
    }

    /**
     * Maps type objects (like WrappedCoercibleType instances) to SQLite types.
     * Leverages CoercibleType.getCanonicalType() to resolve the actual underlying Java type.
     *
     * @param typeObject the type object to map (e.g., WrappedCoercibleType, CoercibleType, etc.)
     * @return SQLite column type (TEXT, INTEGER, REAL, BLOB)
     */
    public String mapTypeObjectToSQLite(Object typeObject) {
        if (typeObject == null) {
            return "TEXT";
        }

        // Handle CoercibleType (includes WrappedCoercibleType which implements CoercibleType)
        if (typeObject instanceof CoercibleType) {
            CoercibleType<?> coercibleType = (CoercibleType<?>) typeObject;
            Class<?> canonicalType = coercibleType.getCanonicalType();
            return mapJavaTypeToSQLite(canonicalType);
        }

        // Handle Class objects directly
        if (typeObject instanceof Class) {
            return mapJavaTypeToSQLite((Class<?>) typeObject);
        }

        // Handle custom types by attempting to extract a Class and delegate to mapJavaTypeToSQLite
        // This covers cases like InstantType or other custom type wrappers
        if (typeObject.getClass().getSimpleName().contains("Instant")) {
            return mapJavaTypeToSQLite(java.time.Instant.class);
        }

        // If we can't determine the type, check if it might be a wrapper around a known type
        // by examining the class name and mapping to standard Java types
        String typeName = typeObject.getClass().getSimpleName();
        if (typeName.contains("Integer")) {
            return mapJavaTypeToSQLite(Integer.class);
        }
        if (typeName.contains("Long")) {
            return mapJavaTypeToSQLite(Long.class);
        }
        if (typeName.contains("Double")) {
            return mapJavaTypeToSQLite(Double.class);
        }
        if (typeName.contains("Float")) {
            return mapJavaTypeToSQLite(Float.class);
        }
        if (typeName.contains("Boolean")) {
            return mapJavaTypeToSQLite(Boolean.class);
        }

        // Default to TEXT for completely unknown type objects
        return "TEXT";
    }

    /**
     * Converts a value for SQLite storage using CoercibleType interface when available.
     *
     * @param value         the value to convert
     * @param coercibleType the CoercibleType that can perform conversion, or null
     * @return the converted value suitable for SQLite storage
     */
    public Object convertValueForSQLite(Object value, CoercibleType<?> coercibleType) {
        if (value == null) {
            return null;
        }

        // If we have a CoercibleType, use it for conversion
        if (coercibleType != null) {
            try {
                // For SQLite, we generally want to convert temporal types to strings
                // and other types to their canonical representation
                Class<?> canonicalType = coercibleType.getCanonicalType();

                if (isTemporalType(canonicalType)) {
                    // For temporal types, use direct conversion to preserve format
                    return convertValueDirectly(value);
                } else if (canonicalType == Boolean.class || canonicalType == boolean.class) {
                    // Convert Boolean to Integer for SQLite (0/1)
                    Boolean boolValue = coercibleType.coerce(value, Boolean.class);
                    return boolValue != null ? (boolValue ? 1 : 0) : null;
                } else {
                    // For other types, convert to canonical type
                    return coercibleType.coerce(value, canonicalType);
                }
            } catch (Exception e) {
                // Fall back to direct conversion if coercion fails
            }
        }

        // Fallback for direct value conversion (when no CoercibleType available)
        return convertValueDirectly(value);
    }


    private boolean isTemporalType(Class<?> type) {
        return type == Date.class || type == Instant.class ||
                type == java.time.LocalDate.class || type == java.time.LocalDateTime.class ||
                type == java.time.LocalTime.class ||
                java.sql.Date.class.isAssignableFrom(type) ||
                java.sql.Time.class.isAssignableFrom(type) ||
                java.sql.Timestamp.class.isAssignableFrom(type);
    }

    private Object convertValueDirectly(Object value) {
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

        if (value instanceof java.time.LocalDate) {
            return value.toString();
        }

        if (value instanceof java.time.LocalDateTime) {
            return value.toString();
        }

        if (value instanceof java.time.LocalTime) {
            return value.toString();
        }

        // For most other types, SQLite can handle the conversion
        return value;
    }
}
