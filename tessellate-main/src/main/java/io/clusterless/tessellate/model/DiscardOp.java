/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.clusterless.tessellate.pipeline.Transforms;

/**
 * <pre>
 * ts->
 * </pre>
 */
public class DiscardOp implements TransformOp, Model {
    private final String declaration;
    private final Field field;

    @JsonCreator
    public DiscardOp(String declaration) {
        this.declaration = declaration;

        String[] split = this.declaration.split("->");
        this.field = new Field(split[0]);
    }

    public Field field() {
        return field;
    }

    public String declaration() {
        return declaration;
    }

    public String toString() {
        return declaration;
    }

    @Override
    public Transforms transform() {
        return Transforms.discard;
    }
}
