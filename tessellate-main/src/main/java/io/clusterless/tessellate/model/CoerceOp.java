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
 * ts|DateTime|yyyyMMdd
 * </pre>
 */
public class CoerceOp implements TransformOp, Model {
    Field field;

    @JsonCreator
    public CoerceOp(String field) {
        this.field = new Field(field);
    }

    public CoerceOp(Field field) {
        this.field = field;
    }

    public Field field() {
        return field;
    }

    @Override
    public Transforms transform() {
        return Transforms.coerce;
    }

    public String toString() {
        return field.toString();
    }
}
