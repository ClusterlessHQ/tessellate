/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import io.clusterless.tessellate.model.*;

import java.util.function.Function;

public enum Transforms {
    insert("=>", "^.+[=]>.+$", InsertOp::new),
    eval("!>", "^.+[!]>.+$", EvalInsertOp::new),
    copy("+>", "^.+[+]>.+$", CopyOp::new),
    rename("->", "^.+[-]>.+$", RenameOp::new),
    discard("->", "^.+[-]>$", DiscardOp::new),
    coerce("", "^.+$", CoerceOp::new);

    private final String operator;
    private final String match;
    private final Function<String, TransformOp> transform;

    Transforms(String operator, String match, Function<String, TransformOp> transform) {
        this.operator = operator;
        this.match = match;
        this.transform = transform;
    }

    public String operator() {
        return operator;
    }

    public boolean matches(String expression) {
        return expression.matches(match);
    }

    public TransformOp transform(String expression) {
        return transform.apply(expression);
    }
}
