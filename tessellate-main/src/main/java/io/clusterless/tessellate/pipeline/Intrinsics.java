/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import io.clusterless.tessellate.pipeline.intrinsic.IntrinsicBuilder;
import io.clusterless.tessellate.pipeline.intrinsic.TsidIntrinsic;

import java.util.HashMap;
import java.util.Map;

public class Intrinsics {

    static Map<String, IntrinsicBuilder> builders = new HashMap<>();

    private static void add(IntrinsicBuilder intrinsicBuilder) {
        builders.put(intrinsicBuilder.name(), intrinsicBuilder);
    }

    static {
        add(new TsidIntrinsic());
    }

    public static Map<String, IntrinsicBuilder> builders() {
        return builders;
    }
}
