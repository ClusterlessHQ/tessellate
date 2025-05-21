/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.tuple.Fields;
import io.clusterless.tessellate.operation.TrimToNullFunction;
import io.clusterless.tessellate.parser.ast.Operation;

public class TrimToNullIntrinsic extends IntrinsicBuilder {

    public TrimToNullIntrinsic() {
        super("trimToNull");
    }

    @Override
    public Result create(Fields currentFields, Operation operation) {
        Fields fromFields = fieldsParser().asFields(operation.arguments());

        if (fromFields.isNone()) {
            fromFields = Fields.ALL;
        }

        Fields toFields = fieldsParser().asFields(operation.results());

        if (toFields.isNone() && fromFields.isAll()) {
            toFields = currentFields;
        } else if (toFields.isNone()) {
            toFields = currentFields.select(fromFields);
        }

        TrimToNullFunction function = new TrimToNullFunction(toFields);

        return new Result(fromFields, function, toFields);
    }
}
