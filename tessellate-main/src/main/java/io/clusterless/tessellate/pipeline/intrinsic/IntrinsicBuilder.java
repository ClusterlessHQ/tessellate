/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.operation.Function;
import cascading.tuple.Fields;
import io.clusterless.tessellate.parser.FieldsParser;
import io.clusterless.tessellate.parser.ast.Operation;

import java.util.Arrays;
import java.util.List;

public abstract class IntrinsicBuilder {
    public static class Result {

        final Fields arguments;
        final Function<?> function;
        final Fields results;

        public Result(Fields arguments, Function<?> function, Fields results) {
            this.arguments = arguments;
            this.function = function;
            this.results = results;
        }

        public Fields arguments() {
            return arguments;
        }

        public Function<?> function() {
            return function;
        }

        public Fields results() {
            return results;
        }
    }

    String name;
    List<String> params;

    public IntrinsicBuilder(String name, String... params) {
        this(name, Arrays.asList(params));
    }

    public IntrinsicBuilder(String name, List<String> params) {
        this.name = name;
        this.params = params;
    }

    public String name() {
        return name;
    }

    public List<String> params() {
        return params;
    }

    public FieldsParser fieldsParser() {
        return FieldsParser.INSTANCE;
    }

    public abstract Result create(Operation operation);

    protected static void requireParam(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
