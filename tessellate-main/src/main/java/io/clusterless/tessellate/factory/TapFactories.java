/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import com.google.common.collect.LinkedListMultimap;
import io.clusterless.tessellate.factory.hdfs.JSONFSFactory;
import io.clusterless.tessellate.factory.hdfs.ParquetFactory;
import io.clusterless.tessellate.factory.hdfs.TextFSFactory;
import io.clusterless.tessellate.factory.local.LocalDirectoryFactory;
import io.clusterless.tessellate.factory.local.StdOutFactory;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
public class TapFactories {
    private static final Logger LOG = LoggerFactory.getLogger(TapFactories.class);
    static Set<TapFactory> tapFactories = new LinkedHashSet<>(List.of(
            LocalDirectoryFactory.INSTANCE,
            ParquetFactory.INSTANCE,
            JSONFSFactory.INSTANCE,
            TextFSFactory.INSTANCE
    ));
    private static final LinkedListMultimap<Protocol, SourceFactory> sourceFactories = LinkedListMultimap.create();
    private static final LinkedListMultimap<Protocol, SinkFactory> sinkFactories = LinkedListMultimap.create();

    static {
        sourceFactories.put(null, (SourceFactory) LocalDirectoryFactory.INSTANCE);
        sinkFactories.put(null, (SinkFactory) LocalDirectoryFactory.INSTANCE);

        for (TapFactory tapFactory : tapFactories) {
            if (tapFactory instanceof SourceFactory) {
                Collection<Protocol> protocols = ((SourceFactory) tapFactory).getSourceProtocols();
                for (Protocol protocol : protocols) {
                    sourceFactories.put(protocol, (SourceFactory) tapFactory);
                }
            }

            if (tapFactory instanceof SinkFactory) {
                Collection<Protocol> protocols = ((SinkFactory) tapFactory).getSinkProtocols();
                for (Protocol protocol : protocols) {
                    sinkFactories.put(protocol, (SinkFactory) tapFactory);
                }
            }
        }
    }

    public static SourceFactory findSourceFactory(PipelineOptions pipelineOptions, Source sourceModel) throws IOException {
        if (sourceModel.manifest() != null) {
            LOG.info("reading manifest: {}", sourceModel.manifest());

            ManifestReader manifestReader = ManifestReader.from(sourceModel);

            if (manifestReader.isEmptyManifest()) {
                throw new ManifestEmptyException("manifest is empty: " + sourceModel.manifest());
            }

            List<URI> uris = manifestReader.uris(pipelineOptions);

            sourceModel.uris().addAll(uris);
        }

        List<URI> inputUris = sourceModel.uris();

        if (sourceModel.schema().format() == null) {
            sourceModel.schema().setFormat(Format.find(inputUris.get(0)).orElse(null));
        }

        Format format = sourceModel.schema().format();

        Compression compression = sourceModel.schema().compression();
        SourceFactory sourceFactory = findSourceFactory(inputUris, format, compression);

        LOG.info("found source factory, format: {}, compression: {}, factory: {}", format, compression, sourceFactory.getClass().getSimpleName());

        return sourceFactory;
    }

    public static SourceFactory findSourceFactory(List<URI> uris, Format format, Compression compression) {
        return findFactory(uris, format, compression, sourceFactories, TapFactory::getCompressions);
    }

    public static SinkFactory findSinkFactory(Sink sinkModel) {
        List<URI> inputUris = sinkModel.uris();

        if (inputUris.isEmpty()) {
            return new StdOutFactory();
        }

        Format format = sinkModel.schema().format();
        Compression compression = sinkModel.schema().compression();
        SinkFactory sinkFactory = findSinkFactory(inputUris, format, compression);

        LOG.info("found sink factory, format: {}, compression: {}, factory: {}", format, compression, sinkFactory.getClass().getSimpleName());

        return sinkFactory;
    }

    public static SinkFactory findSinkFactory(List<URI> uris, Format format, Compression compression) {
        return findFactory(uris, format, compression, sinkFactories, SinkFactory::getSinkCompressions);
    }

    /**
     * Finds the factory for the uris scheme, format and compression. The compression is checked even when the
     * scheme has a single factory, so an unsupported compression fails here, before the flow starts.
     *
     * @param compressions the compressions a factory supports on this side, read for a source, written for a sink
     */
    public static <T extends TapFactory> T findFactory(List<URI> uris, Format format, Compression compression, LinkedListMultimap<Protocol, T> factoriesMap, Function<T, Set<Compression>> compressions) {
        Set<String> schemes = uris.stream()
                .map(URI::getScheme)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        if (schemes.size() > 1) {
            throw new IllegalArgumentException("all uris must have common scheme, got: " + schemes);
        }

        Optional<String> scheme = schemes.stream().findFirst();
        Protocol protocol = scheme.map(Protocol::valueOf).orElse(Protocol.file);

        List<T> factories = factoriesMap.get(protocol); // null is ok

        if (factories.isEmpty()) {
            throw new IllegalArgumentException("no factory found for: " + scheme);
        }

        // if only one factory, it is the only candidate, else disambiguate factories by format
        List<T> candidates = factories.size() == 1 ? factories : factories.stream()
                .filter(factory -> factory.hasFormat(format.parent()))
                .toList();

        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("no factory found for: " + protocol + ", with format: " + format.parent());
        }

        Optional<T> first = candidates.stream()
                .filter(factory -> compressions.apply(factory).contains(compression))
                .findFirst();

        return first.orElseThrow(() -> {
            Set<Compression> supported = candidates.stream()
                    .flatMap(factory -> compressions.apply(factory).stream())
                    .collect(Collectors.toCollection(() -> EnumSet.noneOf(Compression.class)));

            return new IllegalArgumentException("unsupported compression: " + compression + ", for: " + protocol + ", with format: " + (format == null ? null : format.parent()) + ", supported: " + supported);
        });
    }

    public static List<SourceFactory> getSourceFactory(URI uri) {
        return sourceFactories.get(Protocol.fromString(uri.getScheme()));
    }

    public static List<SinkFactory> getSinkFactory(URI uri) {
        return sinkFactories.get(Protocol.fromString(uri.getScheme()));
    }

    public static Set<Protocol> getSourceProtocols() {
        return sourceFactories.keySet().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    public static Set<Protocol> getSinkProtocols() {
        return sinkFactories.keySet().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    public static Map<Protocol, Set<Format>> getSourceFormats() {
        return sourceFactories.entries().stream()
                .filter(e -> e.getKey() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getFormats(), TapFactories::merge));
    }

    public static Map<Protocol, Set<Compression>> getSourceCompression() {
        return sourceFactories.entries().stream()
                .filter(e -> e.getKey() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCompressions(), TapFactories::merge));
    }

    public static Map<Protocol, Set<Format>> getSinkFormats() {
        return sinkFactories.entries().stream()
                .filter(e -> e.getKey() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getFormats(), TapFactories::merge));
    }

    public static Map<Protocol, Set<Compression>> getSinkCompression() {
        return sinkFactories.entries().stream()
                .filter(e -> e.getKey() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSinkCompressions(), TapFactories::merge));
    }

    private static <T> Set<T> merge(Set<T> lhs, Set<T> rhs) {
        lhs = new HashSet<>(lhs);
        lhs.addAll(rhs);
        return lhs;
    }
}
