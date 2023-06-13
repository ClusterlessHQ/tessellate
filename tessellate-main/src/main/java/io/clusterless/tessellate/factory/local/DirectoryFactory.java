/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.local;

import cascading.nested.json.local.JSONTextLine;
import cascading.scheme.Scheme;
import cascading.scheme.local.CompressorScheme;
import cascading.scheme.local.Compressors;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.TapFactory;
import io.clusterless.tessellate.factory.local.tap.PrefixedDirTap;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.pipeline.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.Protocol;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 *
 */
public class DirectoryFactory extends FilesFactory {
    public static TapFactory INSTANCE = new DirectoryFactory();
    private int openWritesThreshold = 100;

    @Override
    public Set<Protocol> getProtocols() {
        return Set.of(Protocol.file);
    }

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.csv, Format.tsv, Format.json, Format.text);
    }

    @Override
    public Set<Compression> getCompressions() {
        return Set.of(Compression.none, Compression.gzip, Compression.snappy, Compression.brotli, Compression.lz4);
    }

    @Override
    public int openWritesThreshold() {
        return openWritesThreshold;
    }

    @Override
    protected Tap createTap(PipelineOptions pipelineOptions, Dataset dataset, Fields currentFields) {
        Scheme<Properties, InputStream, OutputStream, ?, ?> scheme;

        CompressorScheme.Compressor compressor = null;

        Schema schema = dataset.schema();
        switch (schema.compression()) {
            default:
            case none:
                break;
            case gzip:
                compressor = Compressors.GZIP;
                break;
            case snappy:
                compressor = Compressors.SNAPPY_FRAMED;
                break;
            case brotli:
                compressor = Compressors.BROTLI;
                break;
            case lz4:
                compressor = Compressors.LZ4_FRAMED;
                break;
        }

        Fields declaredFields = declaredFields(dataset);

        switch (schema.format()) {
            default:
            case text:
                scheme = new TextLine(new Fields("line"), compressor);
                break;
            case csv:
                scheme = new TextDelimited(declaredFields, compressor, schema.embedsSchema(), ",", "\"");
                break;
            case tsv:
                scheme = new TextDelimited(declaredFields, compressor, schema.embedsSchema(), "\t", "\"");
                break;
            case json:
                scheme = new JSONTextLine(JSONUtil.DATA_MAPPER, declaredFields, compressor) {
                    @Override
                    public String getExtension() {
                        return schema.format().extension();
                    }
                };
                break;
        }

        List<URI> uris = dataset.uris();

        if (uris.size() > 1) {
            throw new IllegalArgumentException("only one URI is supported");
        }

        URI uri = uris.get(0);

        boolean isDir = isLocalDirectory(uri);

        String prefix = null;

        if (isSink(dataset) && isDir) {
            prefix = getPartFileName((Sink) dataset, declaredFields);
        }

        Tap parentTap = createParentTap(uri, isDir, scheme, prefix);

        Optional<Partition> partition = createPartition(dataset);

        if (partition.isEmpty()) {
            return parentTap;
        }

        return new PartitionTap(parentTap, partition.get(), openWritesThreshold());
    }

    protected Tap createParentTap(URI uri, boolean isDir, Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String prefix) {
        if (isDir) {
            if (prefix == null) {
                return new PrefixedDirTap(scheme, uri.getPath(), SinkMode.KEEP);
            } else {
                return new PrefixedDirTap(scheme, uri.getPath(), SinkMode.KEEP, prefix);
            }
        } else {
            return new FileTap(scheme, uri.getPath(), SinkMode.KEEP);
        }
    }
}
