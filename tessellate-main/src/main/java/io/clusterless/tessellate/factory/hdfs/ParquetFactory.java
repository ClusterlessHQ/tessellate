/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs;

import cascading.scheme.Scheme;
import cascading.tap.parquet.TypedParquetScheme;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.TapFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.Protocol;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public class ParquetFactory extends FSFactory {
    public static TapFactory INSTANCE = new ParquetFactory();

    @Override
    public Set<Protocol> getProtocols() {
        return Set.of(Protocol.file, Protocol.hdfs, Protocol.s3);
    }

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.parquet);
    }

    @Override
    public Set<Compression> getCompressions() {
        return Set.of(Compression.none, Compression.gzip, Compression.snappy, Compression.lzo);
    }

    @Override
    protected Scheme createScheme(PipelineOptions pipelineOptions, Dataset dataset, Fields declaredFields) {
        CompressionCodecName compressionCodecName = compressionCodecName(dataset);

        return new TypedParquetScheme(declaredFields, compressionCodecName)
                .with(JSONUtil.TYPE);
    }

    @NotNull
    private static CompressionCodecName compressionCodecName(Dataset dataset) {
        CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;

        switch (dataset.schema().compression()) {
            case gzip:
                compressionCodecName = CompressionCodecName.GZIP;
                break;
            case snappy:
                compressionCodecName = CompressionCodecName.SNAPPY;
                break;
            case lzo:
                compressionCodecName = CompressionCodecName.LZO;
                break;
        }

        return compressionCodecName;
    }
}
