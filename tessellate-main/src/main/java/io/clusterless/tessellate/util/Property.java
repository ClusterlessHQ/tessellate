/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public class Property {
    public static void setIfNotNullFromEnvThenSystem(Properties properties, String env, String sys, @Nullable String defaultValue) {
        setIfNotNull(properties, sys, Optional.ofNullable(System.getenv(env)).orElse(Optional.ofNullable(System.getProperty(sys)).orElse(defaultValue)));
    }

    public static void setIfNotNullFromEnvThenSystem(Properties properties, String env, String sys) {
        setIfNotNull(properties, sys, Optional.ofNullable(System.getenv(env)).orElse(System.getProperty(sys)));
    }

    public static void setIfNotNullFromSystem(Properties properties, String key) {
        setIfNotNull(properties, key, System.getProperty(key));
    }

    public static void setIfNotNull(Properties properties, String key, String value) {
        if (value != null) {
            properties.setProperty(key, value);
        }
    }

    public static Properties createPropertiesFrom(Map<String, String> defaultProperties) {
        return createPropertiesFrom(defaultProperties.entrySet());
    }

    public static Properties createPropertiesFrom(Iterable<Map.Entry<String, String>> defaultProperties) {
        Properties properties = new Properties();

        for (Map.Entry<String, String> property : defaultProperties)
            properties.setProperty(property.getKey(), property.getValue());

        return properties;
    }

    public static Properties mergeAllProperties(Properties... properties) {
        Properties results = new Properties();

        for (int i = properties.length - 1; i >= 0; i--) {
            Properties current = properties[i];

            if (current == null)
                continue;

            Set<String> currentNames = current.stringPropertyNames();

            for (String currentName : currentNames) {
                results.setProperty(currentName, current.getProperty(currentName));
            }
        }

        return results;
    }

    public static Properties removeAllNulls(Properties current) {
        Properties results = new Properties();

        Set<String> currentNames = current.stringPropertyNames();

        for (String currentName : currentNames) {
            if (current.getProperty(currentName) != null) {
                results.setProperty(currentName, current.getProperty(currentName));
            }
        }

        return results;
    }
}
