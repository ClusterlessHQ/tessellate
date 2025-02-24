/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.nested.json.JSONGetFunction;
import cascading.tuple.Fields;
import io.clusterless.tessellate.parser.ast.Operation;
import io.clusterless.tessellate.util.json.JSONUtil;

import java.util.Map;

public class FromJsonIntrinsic extends IntrinsicBuilder {

    public FromJsonIntrinsic() {
        super("fromJson");
    }

    @Override
    public Result create(Operation operation) {
        Fields fromFields = fieldsParser().asFields(operation.arguments());

        // doesn't make sense to make an empty json object
        if (fromFields.isNone()) {
            fromFields = new Fields("json");
        }

        if (fromFields.size() != 1) {
            throw new IllegalArgumentException("arguments may only have one field");
        }

        Fields toFields = fieldsParser().asFields(operation.results());

        if (toFields.isNone()) {
            throw new IllegalArgumentException("result fields must be declared and must match the properties on the json object");
        }

        if (!toFields.hasTypes()) {
            toFields = toFields.applyTypeToAll(String.class);
        }

        // need to escape / and ~ in field names
        Map<Fields, String> pathMap = JSONUtil.asPointerMap(toFields);

        JSONGetFunction function = new JSONGetFunction(pathMap);

        return new Result(fromFields, function, toFields);
    }
}
