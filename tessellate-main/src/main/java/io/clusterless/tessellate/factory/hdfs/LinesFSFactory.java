/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs;

import cascading.tuple.Fields;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Protocol;

import java.util.Properties;
import java.util.Set;

public abstract class LinesFSFactory extends FSFactory {
    @Override
    public Set<Protocol> getSourceProtocols() {
        return Set.of(Protocol.hdfs, Protocol.s3);
    }

    @Override
    public Set<Protocol> getSinkProtocols() {
        return Set.of(Protocol.hdfs, Protocol.s3);
    }

    @Override
    public Set<Compression> getCompressions() {
        return Set.of(Compression.none, Compression.gzip, Compression.snappy, Compression.lz4, Compression.bzip2);
    }

    @Override
    public Properties getProperties(PipelineOptions pipelineOptions, Dataset dataset, Fields declaredFields) {
        Properties properties = super.getProperties(pipelineOptions, dataset, declaredFields);

        switch (dataset.schema().compression()) {
            case none:
                break;
            case gzip:
                properties.setProperty("mapreduce.map.output.compress.codec", org.apache.hadoop.io.compress.GzipCodec.class.getName());
                break;
            case snappy:
                properties.setProperty("mapreduce.map.output.compress.codec", org.apache.hadoop.io.compress.SnappyCodec.class.getName());
                break;
            case lz4:
                properties.setProperty("mapreduce.map.output.compress.codec", org.apache.hadoop.io.compress.Lz4Codec.class.getName());
                break;
            case bzip2:
                properties.setProperty("mapreduce.map.output.compress.codec", org.apache.hadoop.io.compress.BZip2Codec.class.getName());
                break;
        }

        return properties;
    }
}
