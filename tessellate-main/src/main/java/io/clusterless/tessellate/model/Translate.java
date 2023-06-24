/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import com.fasterxml.jackson.annotation.JsonValue;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;

public abstract class Translate implements Model {
    private final String declaration;
    protected final Field from;
    protected final Field to;

    public Translate(String declaration) {
        this.declaration = declaration;
        Objects.requireNonNull(declaration);

        String[] parts = declaration.split(translate());

        if (parts.length == 1 && requiresFrom()) {
            throw new IllegalArgumentException("invalid declaration, requires 'from->to': " + declaration);
        }

        if (parts.length == 1) {
            this.from = null;
            this.to = new Field(parts[0]);
        } else if (parts.length == 2) {
            this.from = new Field(parts[0]);
            this.to = new Field(parts[1]);
        } else {
            throw new IllegalArgumentException("invalid declaration: " + declaration);
        }
    }

    @NotNull
    protected abstract String translate();

    protected abstract boolean requiresFrom();

    public Optional<Field> from() {
        return Optional.ofNullable(from);
    }

    public Field to() {
        return to;
    }

    @JsonValue
    public String toString() {
        return declaration;
    }
}
