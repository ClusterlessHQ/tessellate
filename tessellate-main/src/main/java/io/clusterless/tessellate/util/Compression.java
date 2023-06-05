/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public enum Compression {
    none(""),
    gzip(".gz"),
    snappy(".snappy"),
    lzo(".lzo"),
    brotli(".br"),
    lz4(".lz4"),
    lzstdzo(".zstd"),
    bzip2(".bz2");

    private final String extension;

    Compression(String extension) {
        this.extension = extension;
    }

    public static Compression find(List<URI> uris, Compression compression) {
        if (compression != null) {
            return compression;
        }

        Set<Compression> compressions = uris.stream().map(u -> find(u, null)).collect(Collectors.toSet());

        if (compressions.isEmpty()) {
            return none;
        }

        if (compressions.size() != 1) {
            throw new IllegalArgumentException("more than one compression found: " + compressions);
        }

        return compressions.stream().findFirst().get();
    }

    public static Compression find(URI uri, Compression compression) {
        if (compression != null) {
            return compression;
        }

        return find(uri.toString());
    }

    public static Compression find(String path) {
        for (Compression compression : values()) {
            if (compression == none) {
                continue;
            }

            if (path.endsWith(compression.extension())) {
                return compression;
            }
        }

        return none;
    }

    public String extension() {
        return extension;
    }
}
