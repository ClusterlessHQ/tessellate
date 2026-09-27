/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Only the path under a registered prefix is filtered for {@code _} entries, so a prefix that itself sits under
 * a {@code _*} directory (the macOS temp root, a scratch dir) still records its writes.
 */
public class ObservedTest {
    @ParameterizedTest
    @ValueSource(strings = {"file:///x/_y/out", "file:///x/_y/out/"})
    void recordsWritesUnderAnUnderscorePrefix(String prefix) {
        Observed observed = new Observed();
        observed.writes(URI.create(prefix));

        observed.addWrite(URI.create("file:/x/_y/out/part-0"));
        observed.addWrite(URI.create("file:/x/_y/out/year=2023/part-1"));

        assertEquals(
                Set.of(URI.create("file:///x/_y/out/part-0"), URI.create("file:///x/_y/out/year=2023/part-1")),
                observed.writes(URI.create(prefix))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"file:///x/_y/out", "file:///x/_y/out/"})
    void skipsUnderscoreEntriesUnderThePrefix(String prefix) {
        Observed observed = new Observed();
        observed.writes(URI.create(prefix));

        observed.addWrite(URI.create("file:/x/_y/out/_SUCCESS"));
        observed.addWrite(URI.create("file:/x/_y/out/_temporary/0/part-0"));
        observed.addWrite(URI.create("file:/x/_y/out/year=2023/_temporary/part-0"));

        assertTrue(observed.writes(URI.create(prefix)).isEmpty(), () -> observed.writes().toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"file:///x/_y/in", "s3://bucket/_y/in"})
    void recordsReadsUnderAnUnderscorePrefix(String prefix) {
        Observed observed = new Observed();
        observed.reads(URI.create(prefix));

        observed.addRead(URI.create(prefix + "/part-0"));
        observed.addRead(URI.create(prefix + "/_SUCCESS"));

        assertEquals(1, observed.reads(URI.create(prefix)).size(), () -> observed.reads().toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"file:///x/_y/out", "s3://bucket/out"})
    void registeredPrefixWithoutWritesIsEmpty(String prefix) {
        Observed observed = new Observed();
        observed.writes(URI.create(prefix));

        observed.addWrite(URI.create("file:/x/_y/other/part-0"));

        assertTrue(observed.writes(URI.create(prefix)).isEmpty(), () -> observed.writes().toString());
    }
}
