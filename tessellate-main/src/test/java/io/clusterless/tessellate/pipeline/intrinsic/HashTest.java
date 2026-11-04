/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.tuple.Fields;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import io.clusterless.tessellate.operation.HashFieldsFunction;
import io.clusterless.tessellate.parser.StatementParser;
import io.clusterless.tessellate.parser.ast.Operation;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class HashTest {
    static final Fields CURRENT = new Fields("first", "second", "third");

    static IntrinsicBuilder.Result create(String statement) {
        return new HashIntrinsic().create(CURRENT, (Operation) StatementParser.parse(statement));
    }

    static Object render(String statement, String... values) {
        IntrinsicBuilder.Result result = create(statement);
        HashFieldsFunction function = (HashFieldsFunction) result.function();
        HashCode hashCode = HashFieldsFunction.hash(values);

        return HashFieldsFunction.renderer(result.results().getType(0), function.bits(), function.modulo()).apply(hashCode);
    }

    @Test
    void singleValueIsPlainMurmur3() {
        // reproducible by any MurmurHash3 x64 128-bit implementation over the UTF-8 bytes
        assertEquals(Hashing.murmur3_128().hashString("hello", StandardCharsets.UTF_8), HashFieldsFunction.hash("hello"));
    }

    @Test
    void manyValuesAreFramed() {
        assertNotEquals(HashFieldsFunction.hash("ab", "c"), HashFieldsFunction.hash("a", "bc"));
        assertNotEquals(HashFieldsFunction.hash("a", null), HashFieldsFunction.hash(null, "a"));
        assertNotEquals(HashFieldsFunction.hash("a", ""), HashFieldsFunction.hash("a", null));
        assertEquals(HashFieldsFunction.hash("a", "b"), HashFieldsFunction.hash("a", "b"));
    }

    @Test
    void allNullIsNull() {
        assertNull(HashFieldsFunction.hash((String) null));
        assertNull(HashFieldsFunction.hash(null, null));
        assertNotNull(HashFieldsFunction.hash(""));
    }

    @Test
    void defaultsToLong() {
        IntrinsicBuilder.Result result = create("first ^hash{} +> hashed");

        assertEquals(new Fields("first"), result.arguments());
        assertEquals(Long.class, result.results().getType(0));
        assertEquals(HashFieldsFunction.hash("hello").asLong(), render("first ^hash{} +> hashed", "hello"));
    }

    @Test
    void noArgumentsHashesAllFields() {
        assertEquals(Fields.ALL, create("^hash{} +> hashed").arguments());
    }

    @Test
    void stringIsHexAndPrefixOf128() {
        String hex64 = (String) render("first ^hash{} +> hashed|string", "hello");
        String hex128 = (String) render("first ^hash{bits:128} +> hashed", "hello");

        assertEquals(16, hex64.length());
        assertEquals(32, hex128.length());
        assertEquals(HashFieldsFunction.hash("hello").toString(), hex128);
        assertTrue(hex128.startsWith(hex64), hex64 + " " + hex128);
    }

    @Test
    void moduloBinsWithinRange() {
        for (String value : new String[]{"a", "b", "c", "d", "e", "f", "g", "h"}) {
            long bin = (Long) render("first ^hash{modulo:16} +> bin", value);

            assertTrue(bin >= 0 && bin < 16, "bin: " + bin);
            assertEquals(Math.floorMod(HashFieldsFunction.hash(value).asLong(), 16L), bin);
        }

        assertEquals(Long.toString((Long) render("first ^hash{modulo:16} +> bin", "a")), render("first ^hash{modulo:16} +> bin|string", "a"));
    }

    @Test
    void rejectsInvalidParams() {
        assertMessage("hash bits must be 64 or 128, was: 32", "first ^hash{bits:32} +> hashed");
        assertMessage("hash bits must be a number, was: big", "first ^hash{bits:big} +> hashed");
        assertMessage("hash modulo must be greater than zero, was: 0", "first ^hash{modulo:0} +> bin");
        assertMessage("hash modulo requires 64 bits, was: 128", "first ^hash{bits:128,modulo:4} +> bin");
        assertMessage("a 128 bit hash requires a string type, was: long", "first ^hash{bits:128} +> hashed|long");
        assertMessage("hash requires a single result field, was: ['a', 'b' | Long, Long]", "first ^hash{} +> a + b");
    }

    static void assertMessage(String expected, String statement) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> create(statement));
        assertEquals(expected, exception.getMessage());
    }
}
