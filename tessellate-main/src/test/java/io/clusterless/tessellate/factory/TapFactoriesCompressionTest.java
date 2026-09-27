/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import com.google.common.collect.LinkedListMultimap;
import io.clusterless.tessellate.factory.local.LocalDirectoryFactory;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Protocol;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Brotli can only be read, commons-compress has no brotli compressor and parquet has no brotli codec on the
 * classpath, so sinks must not offer it and a brotli sink must be rejected before the flow starts.
 */
public class TapFactoriesCompressionTest {
    private static final List<URI> FILE_OUTPUT = List.of(URI.create("file:///tmp/output/"));

    private static IllegalArgumentException assertRejected(List<URI> uris, Format format, Compression compression) {
        return assertThrows(IllegalArgumentException.class, () -> TapFactories.findSinkFactory(uris, format, compression));
    }

    @Test
    void csvSinkRejectsBrotli() {
        IllegalArgumentException exception = assertRejected(FILE_OUTPUT, Format.csv, Compression.brotli);

        String message = exception.getMessage();
        assertTrue(message.contains("brotli"), message);
        assertTrue(message.contains("file"), message);
        assertTrue(message.contains("csv"), message);
        assertTrue(message.contains("[none, gzip, snappy, lz4]"), message);
    }

    @Test
    void parquetSinkRejectsBrotli() {
        IllegalArgumentException exception = assertRejected(FILE_OUTPUT, Format.parquet, Compression.brotli);

        String message = exception.getMessage();
        assertTrue(message.contains("brotli"), message);
        assertTrue(message.contains("parquet"), message);
        assertTrue(message.contains("[none, gzip, snappy, lz4]"), message);
    }

    @Test
    void parquetSinkRejectsBrotliOnS3() {
        assertRejected(List.of(URI.create("s3://bucket/output/")), Format.parquet, Compression.brotli);
    }

    @Test
    void singleSinkFactoryRejectsBrotli() {
        LinkedListMultimap<Protocol, SinkFactory> factories = LinkedListMultimap.create();
        factories.put(Protocol.file, (SinkFactory) LocalDirectoryFactory.INSTANCE);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TapFactories.findFactory(FILE_OUTPUT, Format.csv, Compression.brotli, factories, SinkFactory::getSinkCompressions));

        assertTrue(exception.getMessage().contains("[none, gzip, snappy, lz4]"), exception::getMessage);
    }

    @Test
    void sinkCompressionsExcludeBrotli() {
        for (Protocol protocol : List.of(Protocol.file, Protocol.hdfs, Protocol.s3)) {
            Set<Compression> compressions = TapFactories.getSinkCompression().get(protocol);

            assertNotNull(compressions, protocol::name);
            assertFalse(compressions.contains(Compression.brotli), () -> protocol + ": " + compressions);
            assertTrue(compressions.contains(Compression.gzip), () -> protocol + ": " + compressions);
        }
    }

    @Test
    void sourceCompressionsKeepBrotliForFile() {
        Set<Compression> compressions = TapFactories.getSourceCompression().get(Protocol.file);

        assertTrue(compressions.contains(Compression.brotli), compressions::toString);
    }

    @Test
    void csvSourceAcceptsBrotli() {
        SourceFactory factory = TapFactories.findSourceFactory(List.of(URI.create("file:///tmp/input.csv.br")), Format.csv, Compression.brotli);

        assertSame(LocalDirectoryFactory.INSTANCE, factory);
    }

    @Test
    void csvSinkAcceptsGzip() {
        SinkFactory factory = TapFactories.findSinkFactory(FILE_OUTPUT, Format.csv, Compression.gzip);

        assertSame(LocalDirectoryFactory.INSTANCE, factory);
    }
}
