/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;

import java.util.Properties;
import java.util.Set;

public interface TapFactory {
    default void applyGlobalProperties(Properties properties) {
    }

    Set<Format> getFormats();

    Set<Compression> getCompressions();

    default boolean hasFormat(Format format) {
        return getFormats().contains(format);
    }

    default boolean hasCompression(Compression compression) {
        return getCompressions().contains(compression);
    }
}
