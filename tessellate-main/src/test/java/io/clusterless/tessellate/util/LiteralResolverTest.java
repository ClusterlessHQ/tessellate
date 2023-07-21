/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.junit.jupiter.api.Test;

import static io.clusterless.tessellate.util.LiteralResolver.resolve;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LiteralResolverTest {
    @Test
    void resolveEnv() {
        MVELContext context = LiteralResolver.context();
        assertEquals(System.getenv("USER"), resolve("env.USER", context));
        assertEquals(System.getenv("USER"), resolve("env['USER']", context));
        assertEquals(System.getProperty("user.name"), resolve("sys['user.name']", context));

        assertEquals(context.rnd64(), resolve("rnd64", context));
        assertEquals(context.currentTimeISO8601(), resolve("currentTimeISO8601", context));
    }
}
