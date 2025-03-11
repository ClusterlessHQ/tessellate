/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import java.util.List;

public class Join implements Statement {
    private final List<Rel> relations;
    private final JoinType joinType;
    private final Op op;
    private final List<Field> results;

    public Join(List<Rel> relations, JoinType joinType, Op op, List<Field> results) {
        this.relations = relations;
        this.joinType = joinType;
        this.op = op;
        this.results = results;
    }

    @Override
    public Op op() {
        return op;
    }

    public JoinType joinType() {
        return joinType;
    }

    public List<Rel> relations() {
        return relations;
    }

    public List<Field> results() {
        return results;
    }
}
