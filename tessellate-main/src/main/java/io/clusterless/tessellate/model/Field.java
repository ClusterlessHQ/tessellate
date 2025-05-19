/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.clusterless.tessellate.parser.FieldsParser;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Field implements Model {
    @JsonIgnore
    private Fields fields;
    private final String declaration;

    public static List<Field> asField(String... declarations) {
        return Arrays.stream(declarations).map(Field::new).collect(Collectors.toList());
    }

    // Constructor for deserialization from JSON string
    @JsonCreator
    public static Field fromString(String declaration) {
        Objects.requireNonNull(declaration, "field may not be null");
        return new Field(declaration);
    }

    @JsonCreator
    public Field(@JsonProperty("declaration") String declaration) {
        Objects.requireNonNull(declaration, "field may not be null");

        this.declaration = declaration;
        this.fields = FieldsParser.INSTANCE.parseSingleFields(this.declaration, null);
    }

    public Fields fields() {
        return fields;
    }

    public String declaration() {
        return declaration;
    }

    @Override
    public String toString() {
        return declaration;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return Objects.equals(fields, field.fields) && Objects.equals(declaration, field.declaration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(declaration);
    }
}
