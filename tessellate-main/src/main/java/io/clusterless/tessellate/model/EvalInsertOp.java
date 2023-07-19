/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import cascading.tuple.type.CoercibleType;
import io.clusterless.tessellate.pipeline.Transforms;
import io.clusterless.tessellate.util.LiteralResolver;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.Map;

public class EvalInsertOp extends InsertOp {
    public EvalInsertOp(String declaration) {
        super(declaration);
    }

    @NotNull
    protected String translate() {
        return "!>";
    }

    public Object evaluate(Map<String, Object> context) {
        Class<?> resolvedType = getResolvedType();

        return LiteralResolver.resolve(value(), context, resolvedType);
    }

    protected Class<?> getResolvedType() {
        Type type = field().fields().getType(0);
        Class<?> resolvedType;
        if (type instanceof Class) {
            resolvedType = (Class) type;
        } else if (type instanceof CoercibleType) {
            resolvedType = ((CoercibleType) type).getCanonicalType();
        } else {
            throw new IllegalArgumentException("invalid type: " + type);
        }
        return resolvedType;
    }

    @Override
    public Transforms transform() {
        return Transforms.eval;
    }
}
