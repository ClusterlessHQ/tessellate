/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.nested.json.JSONCreateFunction;
import cascading.tuple.Fields;
import io.clusterless.tessellate.parser.ast.Operation;

public class ToJsonIntrinsic extends IntrinsicBuilder {

    public ToJsonIntrinsic() {
        super("toJson");
    }

    @Override
    public Result create(Operation operation) {
        Fields fromFields = fieldsParser().asFields(operation.arguments());

        // doesn't make sense to make an empty json object
        if (fromFields.isNone()) {
            fromFields = Fields.ALL;
        }

        Fields toFields = fieldsParser().asFields(operation.results());

        if (toFields.isNone()) {
            toFields = new Fields("json");
        }

        if (toFields.size() != 1) {
            throw new IllegalArgumentException("results may only have one field");
        }

        JSONCreateFunction function = new JSONCreateFunction(toFields);

        return new Result(fromFields, function, toFields);
    }
}
