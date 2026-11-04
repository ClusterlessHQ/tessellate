/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

/**
 * Hashes the string values of the argument fields with MurmurHash3 x64 128-bit (seed 0).
 * <p>
 * A single argument hashes the UTF-8 bytes of its value, so the result can be reproduced by any
 * MurmurHash3 x64 128-bit implementation. Many arguments hash each value with a presence byte and a
 * length prefix, so {@code "ab", "c"} and {@code "a", "bc"} differ. The result is null when every
 * argument is null.
 * <p>
 * The 64-bit hash is the first 8 bytes of the 128-bit hash read as a little-endian long, so its hex
 * string is the first 16 characters of the 128-bit hex string.
 */
public class HashFieldsFunction extends BaseOperation<HashFieldsFunction.Context> implements Function<HashFieldsFunction.Context> {
    private static final HashFunction MURMUR3 = Hashing.murmur3_128();

    private final int bits;
    private final Long modulo;

    public static class Context {
        protected final Tuple tuple = Tuple.size(1);
        protected final java.util.function.Function<HashCode, Object> render;
        protected String[] values;

        public Context(Type type, int bits, Long modulo) {
            this.render = renderer(type, bits, modulo);
        }

        public Tuple next(TupleEntry arguments) {
            if (values == null || values.length != arguments.size()) {
                values = new String[arguments.size()];
            }

            for (int i = 0; i < values.length; i++) {
                values[i] = arguments.getString(i);
            }

            HashCode hashCode = hash(values);

            tuple.set(0, hashCode == null ? null : render.apply(hashCode));

            return tuple;
        }
    }

    public HashFieldsFunction(Fields fieldDeclaration, int bits, Long modulo) {
        super(fieldDeclaration);
        this.bits = bits;
        this.modulo = modulo;

        if (fieldDeclaration.size() != 1) {
            throw new IllegalArgumentException("fieldDeclaration may only declare one field, was " + fieldDeclaration.print());
        }

        // fail at plan time, not on the first tuple
        renderer(fieldDeclaration.getType(0), bits, modulo);
    }

    public int bits() {
        return bits;
    }

    public Long modulo() {
        return modulo;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        operationCall.setContext(new Context(fieldDeclaration.getType(0), bits, modulo));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
        functionCall.getOutputCollector().add(functionCall.getContext().next(functionCall.getArguments()));
    }

    /**
     * Returns the hash of the given values, or null if every value is null.
     */
    public static HashCode hash(String... values) {
        if (values.length == 1) {
            return values[0] == null ? null : MURMUR3.hashString(values[0], StandardCharsets.UTF_8);
        }

        boolean allNull = true;
        Hasher hasher = MURMUR3.newHasher();

        for (String value : values) {
            if (value == null) {
                hasher.putByte((byte) 0);
                continue;
            }

            allNull = false;
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            hasher.putByte((byte) 1)
                    .putInt(bytes.length)
                    .putBytes(bytes);
        }

        return allNull ? null : hasher.hash();
    }

    /**
     * Returns the function that renders a hash as the declared result type.
     */
    public static java.util.function.Function<HashCode, Object> renderer(Type type, int bits, Long modulo) {
        boolean isString = type == String.class;
        boolean isLong = type == Long.class || type == Long.TYPE;

        if (!isString && !isLong) {
            throw new IllegalArgumentException("unsupported hash type: " + type);
        }

        if (modulo != null) {
            return isString ? h -> Long.toString(Math.floorMod(h.asLong(), modulo)) : h -> Math.floorMod(h.asLong(), modulo);
        }

        if (bits == 128) {
            if (!isString) {
                throw new IllegalArgumentException("a 128 bit hash requires a string type, was: " + type);
            }

            return HashCode::toString;
        }

        return isString ? h -> HashCode.fromLong(h.asLong()).toString() : HashCode::asLong;
    }
}
