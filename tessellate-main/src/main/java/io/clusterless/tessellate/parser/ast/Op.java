/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import java.util.Objects;

public class Op {
    String op = "";

    public Op() {
    }

    public Op(String op) {
        this.op = op;
    }

    public String op() {
        return op;
    }

    @Override
    public String toString() {
        return op;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Op op1 = (Op) o;
        return Objects.equals(op, op1.op);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op);
    }
}
