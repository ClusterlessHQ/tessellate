/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.operation.Insert;
import cascading.tuple.Fields;
import io.clusterless.tessellate.parser.ast.Operation;

public class EnsureIntrinsic extends IntrinsicBuilder {

    public EnsureIntrinsic() {
        super("ensureFields");
    }

    @Override
    public Result create(Fields currentFields, Operation operation) {
        Fields toFields = fieldsParser().asFields(operation.results());

        if (currentFields.isNone()) {
            throw new IllegalArgumentException("this intrinsic requires fields to be declared, got: " + currentFields.print());
        }

        if (toFields.isNone()) {
            throw new IllegalArgumentException("result fields must be declared");
        }

        Fields insertFields = toFields.subtract(currentFields);

        Object[] values = new Object[insertFields.size()];

        Insert function = new Insert(insertFields, values);

        return new Result(Fields.ALL, function, toFields);
    }
}
