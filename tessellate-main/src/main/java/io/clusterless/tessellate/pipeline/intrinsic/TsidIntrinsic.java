/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline.intrinsic;

import cascading.tuple.Fields;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.clusterless.tessellate.operation.TsidFunction;
import io.clusterless.tessellate.parser.ast.Intrinsic;
import io.clusterless.tessellate.parser.ast.Operation;

import java.nio.charset.StandardCharsets;

public class TsidIntrinsic extends IntrinsicBuilder {

    public static final String COUNTER_TO_ZERO = "counterToZero";
    private static HashFunction SIP = Hashing.sipHash24();

    public static final String NODE = "node";
    public static final String NODE_COUNT = "nodeCount";
    public static final String EPOCH = "epoch";
    public static final String FORMAT = "format";

    public TsidIntrinsic() {
        super("tsid", NODE, NODE_COUNT, EPOCH, FORMAT);
    }

    @Override
    public Result create(Operation operation) {
        // default to long if not provided
        Fields toFields = fieldsParser().asFields(operation.results(), Long.TYPE);
        Intrinsic intrinsic = operation.exp();

        Integer nodeCount = intrinsic.params().getInteger(NODE_COUNT);
        Integer node = intrinsic.params().getInteger(NODE, s -> {
            requireParam(nodeCount, "nodeCount param required when providing a string node value");
            return SIP.hashString(s, StandardCharsets.UTF_8).asInt() % nodeCount;
        });

        Long epoch = intrinsic.params().getLong(EPOCH);
        String format = intrinsic.params().getString(FORMAT);
        Boolean counterZero = intrinsic.params().getBoolean(COUNTER_TO_ZERO);

        TsidFunction function = new TsidFunction(toFields, node, nodeCount, epoch, format, counterZero);

        return new Result(Fields.NONE, function, toFields);
    }
}
