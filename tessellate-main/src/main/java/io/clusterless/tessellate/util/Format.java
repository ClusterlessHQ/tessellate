/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import java.net.URI;
import java.util.*;

/**
 *
 */
public enum Format {
    csv("csv"),
    tsv("tsv"),
    delimited,
    parquet(true, "parquet"),
    text,
    json(text, "jsonl", "json"),
    regex(text, "log");

    private final Format parent;
    private final boolean alwaysEmbedsSchema;
    private final Set<String> extensions = new LinkedHashSet<>();

    Format(Format parent, String... extensions) {
        this.parent = parent;
        this.alwaysEmbedsSchema = false;
        this.extensions.addAll(List.of(extensions));
    }

    Format(Format parent, boolean alwaysEmbedsSchema, String... extensions) {
        this.parent = parent;
        this.alwaysEmbedsSchema = alwaysEmbedsSchema;
        this.extensions.addAll(List.of(extensions));
    }

    Format() {
        this.parent = this;
        this.alwaysEmbedsSchema = false;
    }

    Format(String... extensions) {
        this.parent = this;
        this.alwaysEmbedsSchema = false;
        this.extensions.addAll(List.of(extensions));
    }

    Format(boolean alwaysEmbedsSchema, String... extensions) {
        this.parent = this;
        this.alwaysEmbedsSchema = alwaysEmbedsSchema;
        this.extensions.addAll(List.of(extensions));
    }

    public Format parent() {
        return parent;
    }

    public boolean alwaysEmbedsSchema() {
        return alwaysEmbedsSchema;
    }

    public String extension() {
        if (extensions.isEmpty()) {
            return name();
        }

        return extensions.stream().findFirst().get();
    }

    public boolean hasFormat(URI uri) {
        String path = uri.getPath();
        Optional<String> found = extensions.stream().filter(path::endsWith).findFirst();

        return found.isPresent();
    }

    public static Optional<Format> find(URI uri) {
        return Arrays.stream(Format.values()).filter(format -> format.hasFormat(uri))
                .findFirst();
    }
}
