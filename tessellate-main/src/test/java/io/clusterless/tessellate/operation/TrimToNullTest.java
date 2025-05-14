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
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TrimToNullTest {
    @Test
    void test() {
        verify(new Tuple(null, null, null, null, null), new Tuple(null, null, null, null, null));
        verify(new Tuple("a", "", "\n\t", null, null), new Tuple("a", null, null, null, null));
    }

    private static void verify(Tuple arguments, Tuple results) {
        TrimToNullFunction function = new TrimToNullFunction(Fields.size(results.size()));

        try (TupleListCollector tuples = CascadingTesting.invokeFunction(function, arguments, Fields.RESULTS)) {
            TupleEntry next = tuples.entryIterator().next();

            Assertions.assertEquals(results.size(), next.size());
            Assertions.assertEquals(results, next.getTuple());
        }
    }
}
