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

import java.util.Collections;
import java.util.List;

public class Operation implements Statement {
    List<Field> arguments = Collections.emptyList();
    Exp exp;
    Op op = new Op();
    List<Field> results = Collections.emptyList();

    public Operation(List<Field> arguments, Exp exp, Op op, List<Field> results) {
        this.arguments = arguments;
        this.exp = exp;
        this.op = op;
        this.results = results;
    }

    public Operation(Exp exp, Op op, List<Field> results) {
        this.exp = exp;
        this.op = op;
        this.results = results;
    }

    public Operation(Field field) {
        this.arguments = List.of(field);
    }

    public Operation(Field argument, Op op, Field result) {
        this.arguments = List.of(argument);
        this.op = op;
        this.results = List.of(result);
    }

    public Operation(Field arguments, Op op) {
        this.arguments = List.of(arguments);
        this.op = op;
    }

    public List<Field> arguments() {
        return arguments;
    }

    public <T extends Exp> T exp() {
        return (T) exp;
    }

    public Op op() {
        return op;
    }

    public List<Field> results() {
        return results;
    }

    @Override
    public String toString() {
        return Joiner.on("")
                .useForNull("")
                .join(
                        Printer.fields(arguments),
                        exp(),
                        op(),
                        Printer.fields(results)
                );
    }
}
