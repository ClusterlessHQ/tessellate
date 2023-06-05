/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public enum Format {
    csv("csv"),
    tsv("tsv"),
    parquet("parquet"),
    text,
    json(text, "jsonl", "json"),
    regex(text, "log");

    private final Format parent;
    private final Set<String> extensions = new LinkedHashSet<>();

    Format(Format parent, String... extensions) {
        this.parent = parent;
        this.extensions.addAll(List.of(extensions));
    }

    Format() {
        this.parent = this;
    }

    Format(String... extensions) {
        this.parent = this;
        this.extensions.addAll(List.of(extensions));
    }

    public Format parent() {
        return parent;
    }

    public String extension() {
        if (extensions.isEmpty()) {
            return name();
        }

        return extensions.stream().findFirst().get();
    }
}
