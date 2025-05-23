/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.SinkFactory;
import io.clusterless.tessellate.factory.TapFactory;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Protocol;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public class SQLiteFactory implements SinkFactory {
    public static TapFactory INSTANCE = new SQLiteFactory();

    @Override
    public Set<Protocol> getSinkProtocols() {
        return Set.of(Protocol.sqlite);
    }

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.sql);
    }

    @Override
    public Set<Compression> getCompressions() {
        return Set.of(Compression.none);
    }

    @Override
    public Tap<Properties, ?, ?> getSink(PipelineOptions pipelineOptions, Sink sinkModel, Fields currentFields) throws IOException {
        SQLiteScheme scheme = new SQLiteScheme(currentFields, sinkModel);
        return new SQLiteTap(scheme, sinkModel);
    }
}
