/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.mvel2.MVEL;

import java.util.Map;

public class LiteralResolver {

    public static MVELContext context() {
        return new MVELContext();
    }

    public static MVELContext context(Map<String, Object> source, Map<String, Object> sink) {
        return new MVELContext(source, sink);
    }

    public static String resolve(String expression) {
        return resolve(expression, context(), String.class);
    }

    public static <T> T resolve(String expression, Class<T> type) {
        return resolve(expression, context(), type);
    }

    public static <T> T resolve(String expression, MVELContext context, Class<T> type) {
        return MVEL.eval(expression, context, type);
    }

    public static String resolve(String expression, MVELContext context) {
        return MVEL.eval(expression, context, String.class);
    }
}
