/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.printer;

import cascading.nested.json.JSONCoercibleType;
import cascading.tuple.type.DateType;
import cascading.tuple.type.InstantType;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class TypeMap {
    public enum Dialect {
        athena
    }

    private static Consumer<Map<Type, String>> athena = map -> {
        map.put(String.class, "string");
        map.put(Boolean.class, "boolean");
        map.put(Byte.class, "tinyint");
        map.put(Short.class, "smallint");
        map.put(Integer.class, "int");
        map.put(Long.class, "bigint");
        map.put(Float.class, "float");
        map.put(Double.class, "double");
        map.put(JSONCoercibleType.class, "string");
        map.put(DateType.class, "timestamp");
        map.put(InstantType.class, "timestamp");
    };
    Map<Type, String> map = new HashMap<>();

    public TypeMap(Dialect dialect) {
        switch (dialect) {
            case athena:
                athena.accept(map);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + dialect);
        }
    }

    public String get(Type type) {
        return map.get(type);
    }
}
