/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import com.google.common.base.Joiner;
import io.clusterless.tessellate.parser.Printer;

import java.util.List;

public class FilterStatement implements Statement {
    List<Field> arguments;
    Exp exp;

    public FilterStatement(List<Field> arguments, Exp exp) {
        this.arguments = arguments;
        this.exp = exp;
    }

    public List<Field> arguments() {
        return arguments;
    }

    public <T extends Exp> T exp() {
        return (T) exp;
    }

    @Override
    public Op op() {
        return null;
    }

    @Override
    public String toString() {
        return Joiner.on(" ")
                .skipNulls()
                .join(
                        Printer.fields(arguments),
                        exp()
                );
    }
}
