/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.tuple.Fields;
import io.clusterless.tessellate.operation.HashFieldsFunction;
import io.clusterless.tessellate.parser.ast.Intrinsic;
import io.clusterless.tessellate.parser.ast.IntrinsicParams;
import io.clusterless.tessellate.parser.ast.Operation;

import java.util.Optional;

/**
 * {@code from_field1 + from_field2 ^hash{bits:64,modulo:...} +> into_field|type}
 * <p>
 * Hashes the argument fields, or every field when none are given, into a single result field.
 */
public class HashIntrinsic extends IntrinsicBuilder {
    public static final String BITS = "bits";
    public static final String MODULO = "modulo";

    public HashIntrinsic() {
        super("hash", BITS, MODULO);
    }

    @Override
    public Result create(Fields currentFields, Operation operation) {
        Fields fromFields = fieldsParser().asFields(operation.arguments());

        if (fromFields.isNone()) {
            fromFields = Fields.ALL;
        }

        Intrinsic intrinsic = operation.exp();
        IntrinsicParams params = intrinsic.params();

        int bits = parse(params, BITS, Integer::parseInt).orElse(64);

        if (bits != 64 && bits != 128) {
            throw new IllegalArgumentException("hash bits must be 64 or 128, was: " + bits);
        }

        Long modulo = parse(params, MODULO, Long::parseLong).orElse(null);

        if (modulo != null && modulo <= 0) {
            throw new IllegalArgumentException("hash modulo must be greater than zero, was: " + modulo);
        }

        if (modulo != null && bits != 64) {
            throw new IllegalArgumentException("hash modulo requires 64 bits, was: " + bits);
        }

        // a 128 bit hash does not fit a long, and a Long, unlike a long, keeps a null hash null
        Fields toFields = fieldsParser().asFields(operation.results(), bits == 128 ? String.class : Long.class);

        if (toFields.size() != 1) {
            throw new IllegalArgumentException("hash requires a single result field, was: " + toFields.print());
        }

        HashFieldsFunction function = new HashFieldsFunction(toFields, bits, modulo);

        return new Result(fromFields, function, toFields);
    }

    private static <T> Optional<T> parse(IntrinsicParams params, String param, java.util.function.Function<String, T> parser) {
        Optional<String> value = params.getString(param);

        try {
            return value.map(parser);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("hash " + param + " must be a number, was: " + value.orElse(null), e);
        }
    }
}
