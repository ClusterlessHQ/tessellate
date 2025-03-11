/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import io.clusterless.tessellate.parser.Printer;

import java.util.List;
import java.util.Objects;

public class Rel {
    String name;
    List<Field> fields;

    public Rel(String name, List<Field> fields) {
        this.name = name;
        this.fields = fields;
    }

    @Override
    public String toString() {
        return Printer.literal(name) + "|" + Printer.fields(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Rel rel = (Rel) o;
        return Objects.equals(name, rel.name) && Objects.equals(fields, rel.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fields);
    }
}
