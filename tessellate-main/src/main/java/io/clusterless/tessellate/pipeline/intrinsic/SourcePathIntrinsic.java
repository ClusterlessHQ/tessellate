/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.tuple.Fields;
import io.clusterless.tessellate.operation.SourcePathFunction;
import io.clusterless.tessellate.parser.ast.Operation;

public class SourcePathIntrinsic extends IntrinsicBuilder {

    public SourcePathIntrinsic() {
        super("sourcePath");
    }

    @Override
    public Result create(Fields currentFields, Operation operation) {
        Fields toFields = fieldsParser().asFields(operation.results());

        if (toFields.isNone()) {
            toFields = new Fields("source_path", String.class);
        }

        if (toFields.size() != 1) {
            throw new IllegalArgumentException("results may only have one field");
        }

        SourcePathFunction function = new SourcePathFunction(toFields);

        return new Result(Fields.NONE, function, toFields);
    }
}
