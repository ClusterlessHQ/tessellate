/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import cascading.nested.json.local.JSONTextLine;
import cascading.scheme.Scheme;
import cascading.scheme.local.CompressorScheme;
import cascading.scheme.local.Compressors;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tap.partition.NamedPartition;
import cascading.tuple.Fields;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.util.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
public class DirectoryFactory implements SourceFactory, SinkFactory {
    public static TapFactory INSTANCE = new DirectoryFactory();
    private int openWritesThreshold = 100;

    @Override
    public Set<Protocol> getProtocols() {
        return Set.of(Protocol.file);
    }

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.csv, Format.tsv, Format.json);
    }

    @Override
    public Set<Compression> getCompressions() {
        return Set.of(Compression.none, Compression.gzip, Compression.snappy, Compression.brotli, Compression.lz4);
    }

    public int openWritesThreshold() {
        return openWritesThreshold;
    }

    @Override
    public Tap getSource(Source sourceModel) {
        return createTap(sourceModel);
    }

    @Override
    public Tap getSink(Sink sinkModel) {
        return createTap(sinkModel);
    }

    private Tap createTap(Dataset dataset) {
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

        Fields declaredFields = Models.fieldAsFields(schema.declared(), String.class, isSink(dataset) ? Fields.ALL : Fields.UNKNOWN);
        switch (schema.format()) {
            default:
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

        boolean isDir = isDirectory(uri);

        String prefix = null;

        if (isSink(dataset) && isDir) {
            prefix = getPartFileName((Sink) dataset, declaredFields);
        }

        Tap parentTap = createParentTap(uri, isDir, scheme, prefix);

        List<Partition> partitions = dataset.partitions();

        if (partitions.isEmpty()) {
            return parentTap;
        }

        Fields partitionFields = Models.partitionsAsFields(partitions, String.class);

        DelimitedPartition partitioner;

        if (dataset.namedPartitions()) {
            partitioner = new NamedPartition(partitionFields, "/");
        } else {
            partitioner = new DelimitedPartition(partitionFields, "/");
        }

        return new PartitionTap(
                parentTap, partitioner, openWritesThreshold()
        );
    }

    private static boolean isSink(Dataset dataset) {
        return dataset instanceof Sink;
    }

    protected Tap createParentTap(URI uri, boolean isDir, Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String prefix) {
        if (isDir) {
            if (prefix == null) {
                return new PrefixedDirTap(scheme, uri.getPath(), SinkMode.UPDATE);
            } else {
                return new PrefixedDirTap(scheme, uri.getPath(), SinkMode.UPDATE, prefix);
            }
        } else {
            return new FileTap(scheme, uri.getPath(), SinkMode.KEEP);
        }
    }

    protected boolean isDirectory(URI uri) {
        Path path = uri.getScheme() == null ? Paths.get(uri.getPath()) : Paths.get(uri);

        if (Files.exists(path)) {
            return Files.isDirectory(path);
        } else {
            // assuming directory if no extension
            return path.getFileName().toString().lastIndexOf('.') == -1;
        }
    }

    protected String getPartFileName(Sink sinkModel, Fields declaredFields) {
        String result = "part";

        if (sinkModel.filename().prefix() != null && !sinkModel.filename().prefix().isEmpty()) {
            result = sinkModel.filename().prefix();
        }

        if (sinkModel.filename().includeFieldsHash()) {
            result = appendHyphen(result, Integer.toHexString(declaredFields.hashCode()));
        }

        if (sinkModel.filename().includeGuid()) {
            result = appendHyphen(result, getGUID(sinkModel));
        }

        return result;
    }

    protected String appendHyphen(String part, String append) {
        return String.format("%s-%s", part, append);
    }

    protected String getGUID(Sink options) {
        if (options.filename().providedGuid() != null) {
            return options.filename().providedGuid();
        }

        return UUID.randomUUID().toString();
    }
}
