/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clusterless.tessellate.util.FieldsParser;

import java.util.Objects;

public class Field implements Model {
    @JsonIgnore
    private Fields fields;
    private final String field;

    @JsonCreator
    public Field(String field) {
        Objects.requireNonNull(field, "field may not be null");

        this.field = field;
        this.fields = FieldsParser.INSTANCE.parseSingleFields(this.field, null);
    }

    public Fields fields() {
        return fields;
    }

    @Override
    public String toString() {
        return field;
    }
}
