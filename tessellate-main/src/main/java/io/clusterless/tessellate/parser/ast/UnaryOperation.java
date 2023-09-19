/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

public class UnaryOperation extends Operation {
    public UnaryOperation(Field argument, Op op, Field result) {
        super(argument, op, result);
    }

    public UnaryOperation(Field arguments, Op op) {
        super(arguments, op);
    }

    public UnaryOperation(Field field) {
        super(field);
    }
}
