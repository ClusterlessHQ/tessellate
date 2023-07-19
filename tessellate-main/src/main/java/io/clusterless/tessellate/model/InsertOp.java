/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clusterless.tessellate.pipeline.Transforms;
import org.jetbrains.annotations.NotNull;

/**
 * <pre>
 * value=>field|type
 * </pre>
 */
public class InsertOp implements TransformOp, Model {
    private String declaration;
    @JsonIgnore
    private Field field;
    @JsonIgnore
    private String value;

    @JsonCreator
    public InsertOp(String declaration) {
        this.declaration = declaration;
        String[] split = declaration.split(translate());

        if (split.length != 2) {
            throw new IllegalArgumentException("invalid " + transform().name() + " declaration, expects 'value" + translate() + "field`, got: " + declaration);
        }

        this.field = new Field(split[1]);
        this.value = split[0];
    }

    @NotNull
    protected String translate() {
        return "=>";
    }

    public String declaration() {
        return declaration;
    }

    public Field field() {
        return field;
    }

    public String value() {
        return value;
    }

    public String toString() {
        return declaration;
    }

    @Override
    public Transforms transform() {
        return Transforms.insert;
    }
}
