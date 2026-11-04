/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.operation;

import cascading.CascadingTesting;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class HashFieldsTest {
    @Test
    void hashesEachTupleAsLong() {
        HashFieldsFunction function = new HashFieldsFunction(new Fields("hashed", Long.TYPE), 64, null);

        Assertions.assertEquals(
                HashFieldsFunction.hash("a", "b").asLong(),
                invoke(function, new Tuple("a", "b")).getLong(0)
        );
    }

    @Test
    void reusesContextAcrossTuples() {
        HashFieldsFunction function = new HashFieldsFunction(new Fields("hashed", String.class), 128, null);

        try (TupleListCollector tuples = CascadingTesting.invokeFunction(function, new Tuple[]{new Tuple("a"), new Tuple("b"), new Tuple((Object) null)}, Fields.RESULTS)) {
            Iterator<Tuple> iterator = tuples.iterator();

            Assertions.assertEquals(HashFieldsFunction.hash("a").toString(), iterator.next().getObject(0));
            Assertions.assertEquals(HashFieldsFunction.hash("b").toString(), iterator.next().getObject(0));
            Assertions.assertNull(iterator.next().getObject(0));
        }
    }

    @Test
    void allNullIsNullAsLong() {
        HashFieldsFunction function = new HashFieldsFunction(new Fields("hashed", Long.class), 64, null);

        Assertions.assertNull(invoke(function, new Tuple(null, null)).getObject(0));
    }

    @Test
    void hashesStringValueOfNonStrings() {
        HashFieldsFunction function = new HashFieldsFunction(new Fields("bin", Long.TYPE), 64, 10L);

        Assertions.assertEquals(
                Math.floorMod(HashFieldsFunction.hash("42").asLong(), 10L),
                invoke(function, new Tuple(42)).getLong(0)
        );
    }

    @Test
    void rejectsUnsupportedTypeAtPlanTime() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HashFieldsFunction(new Fields("hashed", Integer.class), 64, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HashFieldsFunction(new Fields("hashed", Long.TYPE), 128, null));
    }

    private static Tuple invoke(HashFieldsFunction function, Tuple arguments) {
        try (TupleListCollector tuples = CascadingTesting.invokeFunction(function, arguments, Fields.RESULTS)) {
            return tuples.iterator().next();
        }
    }
}
