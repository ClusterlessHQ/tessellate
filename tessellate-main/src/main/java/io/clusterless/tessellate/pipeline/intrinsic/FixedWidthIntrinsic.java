/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.tuple.Fields;
import io.clusterless.tessellate.operation.FixedWidthFunction;
import io.clusterless.tessellate.parser.ast.Intrinsic;
import io.clusterless.tessellate.parser.ast.Operation;

public class FixedWidthIntrinsic extends IntrinsicBuilder {

    public static final String WIDTH = "width";
    public static final String INSERT_AT = "insertAt";

    public FixedWidthIntrinsic() {
        super("fixedWidth", WIDTH, INSERT_AT);
    }

    @Override
    public Result create(Fields currentFields, Operation operation) {
        Fields toFields = fieldsParser().asFields(operation.results());
        Intrinsic intrinsic = operation.exp();

        Integer width = intrinsic.params().getInteger(WIDTH).orElse(null);
        Integer insertAt = intrinsic.params().getInteger(INSERT_AT).orElse(null);

        // if not given, we will insert at the end
        if (insertAt == null) {
            insertAt = -1;
        }

        if (width == null && toFields.isNone()) {
            throw new IllegalArgumentException("width must be specified, or result fields must be declared");
        }

        if (width == null) {
            width = toFields.size();
        } else if (toFields.isNone()) {
            toFields = Fields.size(width);
        }

        if (toFields.size() != width) {
            throw new IllegalArgumentException("result fields width must match fixed width");
        }

        // currentFields is equivalent to Field.ALL so we can safely copy over the type information
        if (!toFields.hasTypes() && currentFields.hasTypes()) {
            toFields = toFields.applyTypes(currentFields.getTypes());
        }

        FixedWidthFunction function = new FixedWidthFunction(toFields, width, insertAt);

        return new Result(Fields.ALL, function, toFields);
    }
}
