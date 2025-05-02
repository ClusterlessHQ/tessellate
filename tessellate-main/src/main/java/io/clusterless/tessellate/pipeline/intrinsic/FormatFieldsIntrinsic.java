/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.operation.Identity;
import cascading.tuple.Fields;
import clusterless.commons.util.Strings;
import io.clusterless.tessellate.parser.ast.Intrinsic;
import io.clusterless.tessellate.parser.ast.Operation;
import io.clusterless.tessellate.util.StringHelper;

import java.lang.reflect.Type;
import java.util.function.BiFunction;

public class FormatFieldsIntrinsic extends IntrinsicBuilder {

    public static final String FORMAT = "format";
    public static final String REGEX = "[/\\\\ .,-]";

    public FormatFieldsIntrinsic() {
        super("formatFields", FORMAT);
    }

    @Override
    public Result create(Fields currentFields, Operation operation) {
        Fields fromFields = fieldsParser().asFields(operation.arguments());

        // doesn't make sense to make an empty json object
        if (fromFields.isNone()) {
            fromFields = currentFields;
        }

        Intrinsic intrinsic = operation.exp();
        String format = intrinsic.params().getString(FORMAT).orElse("lowerUnderscore");

        BiFunction<Comparable, Type, Comparable> function;

        switch (format) {
            case "camelCase":
                function = FormatFieldsIntrinsic::camelCase;
                break;
            case "upperUnderscore":
                function = FormatFieldsIntrinsic::upperUnderscore;
                break;
            case "lowerUnderscore":
                function = FormatFieldsIntrinsic::lowerUnderscore;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + format);
        }

        Fields toFields = fromFields.rename(function);

        Identity identity = new Identity(toFields);

        return new Result(fromFields, identity, toFields);
    }

    public static Comparable<?> lowerUnderscore(Comparable<?> comparable, Type type) {
        String string = comparable.toString().replaceAll(REGEX, "_");

        if (!string.contains("_")) {
            return Strings.camelToLowerUnderscore(StringHelper.convertConsecutiveUpperCase(string));
        }

        return string.toLowerCase();
    }

    public static Comparable<?> upperUnderscore(Comparable<?> comparable, Type type) {
        String string = comparable.toString().replaceAll(REGEX, "_");

        if (!string.contains("_")) {
            return Strings.camelToUpperUnderscore(StringHelper.convertConsecutiveUpperCase(string));
        }

        return string.toUpperCase();
    }

    public static Comparable<?> camelCase(Comparable<?> comparable, Type type) {
        String string = comparable.toString().replaceAll(REGEX, "");
        return Strings.lowerUnderscoreToCamelCase(lowerUnderscore(string, type).toString());
    }
}
