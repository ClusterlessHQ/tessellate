/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LiteralResolverTest {
    @Test
    void resolveEnv() {
        Assertions.assertEquals(System.getenv("USER"), LiteralResolver.resolve("env.USER", String.class));
        Assertions.assertEquals(System.getProperty("user.name"), LiteralResolver.resolve("sys['user.name']", String.class));
    }
}
