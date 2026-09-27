/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Protocol;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 *
 */
public interface SinkFactory extends TapFactory {
    Set<Protocol> getSinkProtocols();

    /**
     * The compressions this factory can write, a subset of {@link #getCompressions()} when some can only be read.
     */
    default Set<Compression> getSinkCompressions() {
        return getCompressions();
    }

    Tap<Properties, ?, ?> getSink(PipelineOptions pipelineOptions, Sink sinkModel, Fields currentFields) throws IOException;
}
