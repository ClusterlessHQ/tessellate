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

public class FixedWidthTest {
    @Test
    void padRight() {
        verify(4, new Tuple(), new Tuple(null, null, null, null, null));
        verify(4, new Tuple("a"), new Tuple("a", null, null, null, null));
        verify(4, new Tuple("a", "b"), new Tuple("a", "b", null, null, null));
        verify(4, new Tuple("a", "b", "c"), new Tuple("a", "b", "c", null, null));
        verify(4, new Tuple("a", "b", "c", "d"), new Tuple("a", "b", "c", "d", null));
        verify(4, new Tuple("a", "b", "c", "d", "e"), new Tuple("a", "b", "c", "d", "e"));
    }

    @Test
    void padLeft() {
        verify(0, new Tuple(), new Tuple(null, null, null, null, null));
        verify(0, new Tuple("a"), new Tuple(null, null, null, null, "a"));
        verify(0, new Tuple("a", "b"), new Tuple(null, null, null, "a", "b"));
        verify(0, new Tuple("a", "b", "c"), new Tuple(null, null, "a", "b", "c"));
        verify(0, new Tuple("a", "b", "c", "d"), new Tuple(null, "a", "b", "c", "d"));
        verify(0, new Tuple("a", "b", "c", "d", "e"), new Tuple("a", "b", "c", "d", "e"));
    }

    @Test
    void padMiddle() {
        verify(2, new Tuple(), new Tuple(null, null, null, null, null));
        verify(2, new Tuple("a"), new Tuple("a", null, null, null, null));
        verify(2, new Tuple("a", "b"), new Tuple("a", "b", null, null, null));
        verify(2, new Tuple("a", "b", "c"), new Tuple("a", "b", null, null, "c"));
        verify(2, new Tuple("a", "b", "c", "d"), new Tuple("a", "b", null, "c", "d"));
        verify(2, new Tuple("a", "b", "c", "d", "e"), new Tuple("a", "b", "c", "d", "e"));
    }

    private static void verify(int insertAt, Tuple arguments, Tuple results) {
        FixedWidthFunction function = new FixedWidthFunction(results.size(), insertAt);

        try (TupleListCollector tuples = CascadingTesting.invokeFunction(function, arguments, Fields.RESULTS)) {
            TupleEntry next = tuples.entryIterator().next();

            Assertions.assertEquals(results.size(), next.size());
            Assertions.assertEquals(results, next.getTuple());
        }
    }
}
