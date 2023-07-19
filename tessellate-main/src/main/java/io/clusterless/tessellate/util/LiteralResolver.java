/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.mvel2.MVEL;

import java.util.HashMap;
import java.util.Map;

public class LiteralResolver {
    private static Map<String, Object> context = new HashMap<>();

    static {
        context.put("env", System.getenv());
        context.put("sys", System.getProperties());
    }

    public static Map<String, Object> context() {
        return new HashMap<>(context);
    }

    public static <T> T resolve(String expression, Class<T> type) {
        return resolve(expression, context, type);
    }

    public static <T> T resolve(String expression, Map<String, Object> context, Class<T> type) {
        return MVEL.eval(expression, context, type);
    }
}
