/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import com.google.common.base.Joiner;
import io.clusterless.tessellate.parser.Printer;

public class Assignment implements Statement {
    String literal;
    Op op;
    Field result;

    public Assignment(String literal, Op op, Field result) {
        this.literal = literal;
        this.op = op;
        this.result = result;
    }

    public String literal() {
        return literal;
    }

    public Op op() {
        return op;
    }

    public Field result() {
        return result;
    }

    @Override
    public String toString() {
        return Joiner.on("")
                .useForNull("")
                .join(
                        Printer.literal(literal()),
                        op(),
                        result
                );
    }
}
