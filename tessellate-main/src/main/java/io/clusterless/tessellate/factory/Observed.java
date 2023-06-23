/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import io.clusterless.tessellate.util.URIs;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class Observed {
    public static Observed INSTANCE = new Observed();

    Map<String, Set<URI>> reads = new LinkedHashMap<>();
    Map<String, Set<URI>> writes = new LinkedHashMap<>();

    public Map<String, Set<URI>> reads() {
        return reads;
    }

    public Map<String, Set<URI>> writes() {
        return writes;
    }

    public Set<URI> reads(URI prefix) {
        return reads.computeIfAbsent(URIs.cleanFileUrls(prefix).toString(), k -> new LinkedHashSet<>());
    }

    public Set<URI> writes(URI prefix) {
        return writes.computeIfAbsent(URIs.cleanFileUrls(prefix).toString(), k -> new LinkedHashSet<>());
    }

    public void addRead(URI uri) {
        add(reads, uri);
    }

    public void addWrite(URI uri) {
        add(writes, uri);
    }

    private static void add(Map<String, Set<URI>> map, final URI uri) {
        URI clean = URIs.cleanFileUrls(uri);

        if (clean.getPath().contains("/_")) {
            return;
        }

        map.entrySet()
                .stream()
                .filter(e -> clean.toString().startsWith(e.getKey()))
                .forEach(e -> e.getValue().add(clean));
    }
}
