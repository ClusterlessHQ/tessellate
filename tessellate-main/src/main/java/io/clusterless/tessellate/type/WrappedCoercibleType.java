/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.type;

import cascading.tuple.type.CoercibleType;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.function.Function;

/**
 *
 */
public class WrappedCoercibleType<Canonical> implements CoercibleType<Canonical> {
    private CoercibleType<Canonical> coercibleType;
    private Function<Object, Object> function;
    private String nullToken;

    public WrappedCoercibleType(CoercibleType<Canonical> coercibleType, String nullToken) {
        this(coercibleType, value -> value == null ? null : value.equals(nullToken) ? null : value);
        this.nullToken = nullToken;
    }

    protected WrappedCoercibleType(CoercibleType<Canonical> coercibleType, Function<Object, Object> function) {
        this.coercibleType = coercibleType;
        this.function = function;
    }

    @Override
    public Class<Canonical> getCanonicalType() {
        return coercibleType.getCanonicalType();
    }

    @Override
    public Canonical canonical(Object value) {
        return coercibleType.canonical(function.apply(value));
    }

    @Override
    public <Coerce> Coerce coerce(Object value, Type to) {
        return coercibleType.coerce(value, to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrappedCoercibleType<?> that = (WrappedCoercibleType<?>) o;
        return Objects.equals(coercibleType, that.coercibleType) && Objects.equals(nullToken, that.nullToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coercibleType, nullToken);
    }
}
