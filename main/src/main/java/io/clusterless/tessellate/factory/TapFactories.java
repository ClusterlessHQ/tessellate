/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import com.google.common.collect.LinkedListMultimap;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Protocol;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class TapFactories {
    static Set<TapFactory> tapFactories = Set.of(DirectoryFactory.INSTANCE);
    private static final LinkedListMultimap<Protocol, SourceFactory> sourceFactories = LinkedListMultimap.create();
    private static final LinkedListMultimap<Protocol, SinkFactory> sinkFactories = LinkedListMultimap.create();

    static {
        sourceFactories.put(null, (SourceFactory) DirectoryFactory.INSTANCE);
        sinkFactories.put(null, (SinkFactory) DirectoryFactory.INSTANCE);

        for (TapFactory tapFactory : tapFactories) {
            if (tapFactory instanceof SourceFactory) {
                Collection<Protocol> protocols = tapFactory.getProtocols();
                for (Protocol protocol : protocols) {
                    sourceFactories.put(protocol, (SourceFactory) tapFactory);
                }
            }

            if (tapFactory instanceof SinkFactory) {
                Collection<Protocol> protocols = tapFactory.getProtocols();
                for (Protocol protocol : protocols) {
                    sinkFactories.put(protocol, (SinkFactory) tapFactory);
                }
            }
        }
    }

    public static SourceFactory findSourceFactory(Source sourceModel) {
        List<URI> inputUris = sourceModel.uris();
        Format format = sourceModel.schema().format();
        Compression compression = sourceModel.schema().compression();
        return TapFactories.findSourceFactory(inputUris, format, compression);
    }

    public static SourceFactory findSourceFactory(List<URI> uris, Format format, Compression compression) {
        return findFactory(uris, format, compression, sourceFactories);
    }

    public static SinkFactory findSinkFactory(Sink sinkModel) {
        List<URI> inputUris = sinkModel.uris();
        Format format = sinkModel.schema().format();
        Compression compression = sinkModel.schema().compression();
        return TapFactories.findSinkFactory(inputUris, format, compression);
    }

    public static SinkFactory findSinkFactory(List<URI> uris, Format format, Compression compression) {
        return findFactory(uris, format, compression, sinkFactories);
    }

    public static <T extends TapFactory> T findFactory(List<URI> uris, Format format, Compression compression, LinkedListMultimap<Protocol, T> factoriesMap) {
        Set<String> schemes = uris.stream()
                .map(URI::getScheme)
                .collect(Collectors.toSet());

        if (schemes.size() > 1) {
            throw new IllegalArgumentException("all uris must have common scheme, got: " + schemes);
        }

        Optional<String> scheme = schemes.stream().findFirst();
        Protocol protocol = scheme.map(Protocol::valueOf).orElse(null);

        List<T> factories = factoriesMap.get(protocol); // null is ok

        if (factories.isEmpty()) {
            throw new IllegalArgumentException("no factory found for: " + scheme);
        }

        // if only one factory, return it
        if (factories.size() == 1) {
            return factories.get(0);
        }

        // disambiguate factories by format
        Optional<T> first = factories.stream()
                .filter(factory -> factory.hasFormat(format.parent()))
                .filter(factory -> factory.hasCompression(compression))
                .findFirst();

        return first.orElseThrow(() ->
                new IllegalArgumentException("no factory found for: " + scheme + ", with format: " + format + ", with compression: " + compression)
        );
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

    public static Map<Protocol, Set<Format>> getSinkFormats() {
        return sinkFactories.entries().stream()
                .filter(e -> e.getKey() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getFormats(), TapFactories::merge));
    }

    private static <T> Set<T> merge(Set<T> lhs, Set<T> rhs) {
        lhs.addAll(rhs);
        return lhs;
    }
}
