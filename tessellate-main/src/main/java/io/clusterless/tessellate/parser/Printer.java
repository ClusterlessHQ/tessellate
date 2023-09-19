/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import io.clusterless.tessellate.parser.ast.Field;

import java.util.List;
import java.util.stream.Collectors;

public class Printer {
    public static String withParams(Object name, Object param) {
        return name + (param != null ? "|" + param : "");
    }

    public static String fields(List<Field> fields) {
        return fields.stream().map(Field::toString).collect(Collectors.joining("+"));
    }

    public static String literal(String literal) {
        if (literal == null) {
            return null;
        }

        if (literal.matches(".*[,{}'\": ]+.*")) {
            return String.format("'%s'", literal.replace("'", "''"));
        }

        return literal;
    }
}
