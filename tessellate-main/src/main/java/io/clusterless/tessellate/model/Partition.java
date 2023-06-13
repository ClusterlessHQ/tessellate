/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.Optional;

/**
 * ts->ymd|DateTime|yyyyMMdd
 */
public class Partition implements Model {
    public static final String TRANSLATE = "->";
    private final Field from;
    private final Field to;

    @JsonCreator
    public Partition(String partition) {
        Objects.requireNonNull(partition, "partition cannot be null");

        String[] parts = partition.split(TRANSLATE);
        if (parts.length == 1) {
            this.from = null;
            this.to = new Field(parts[0]);
        } else if (parts.length == 2) {
            this.from = new Field(parts[0]);
            this.to = new Field(parts[1]);
        } else {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }
    }

    public Partition(Field from, Field to) {
        this.from = from;
        this.to = to;
    }

    public Optional<Field> from() {
        return Optional.ofNullable(from);
    }

    public Field to() {
        return to;
    }

    @JsonValue
    public String toString() {
        if (from == null) {
            return to.toString();
        } else {
            return from + TRANSLATE + to;
        }
    }
}
