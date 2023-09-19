/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import io.clusterless.tessellate.parser.Printer;

import java.util.Optional;

public class Field {
    FieldRef fieldRef;
    FieldType fieldType;

    public Field(FieldRef fieldRef, Optional<FieldType> fieldType) {
        this.fieldRef = fieldRef;
        this.fieldType = fieldType.orElse(null);
    }

    public FieldRef fieldRef() {
        return fieldRef;
    }

    public Optional<FieldType> fieldType() {
        return Optional.ofNullable(fieldType);
    }

    @Override
    public String toString() {
        return Printer.withParams(fieldRef.asComparable(), fieldType);
    }
}
