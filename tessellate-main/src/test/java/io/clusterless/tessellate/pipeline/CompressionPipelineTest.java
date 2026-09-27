/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.CascadingTesting;
import io.clusterless.tessellate.junit.PathForOutput;
import io.clusterless.tessellate.junit.PathForResource;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PipelineOptionsMerge;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trips each sink compression through a pipeline, reads brotli input, and rejects a brotli sink,
 * which can only be read, before the flow starts.
 */
@ExtendWith(ResourceExtension.class)
public class CompressionPipelineTest {

    private static Sink sink(URI output, Format format, Compression compression) {
        return Sink.builder()
                .withOutput(output)
                .withSchema(Schema.builder()
                        .withFormat(format)
                        .withCompression(compression)
                        .withEmbedsSchema(true)
                        .build())
                .withFilename(Filename.builder()
                        .withPrefix("test")
                        .withIncludeGuid(true)
                        .withProvidedGuid("guid")
                        .build())
                .build();
    }

    private static Source csvSource(URI input, Compression compression) {
        return Source.builder()
                .withInputs(List.of(input))
                .withSchema(Schema.builder()
                        .withFormat(Format.csv)
                        .withCompression(compression)
                        .withEmbedsSchema(true)
                        .build())
                .build();
    }

    private static void assertSinkEntries(Pipeline pipeline, int length, int size) throws IOException {
        CascadingTesting.validateEntries(
                pipeline.flow().openSink(),
                l -> assertEquals(length, l, "wrong file length"),
                l -> assertEquals(size, l, "wrong tuple size"),
                l -> {
                }
        );
    }

    @Test
    void readBrotliCsv(@PathForResource("/data/delimited-header.csv.br") URI input, @PathForOutput URI output) throws IOException {
        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(csvSource(input, Compression.brotli))
                .withSink(sink(output, Format.csv, Compression.none))
                .build();

        Pipeline pipeline = new Pipeline(new PipelineOptions(), def);

        pipeline.run();

        assertSinkEntries(pipeline, 13, 5); // headers are declared so aren't counted
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = {"none", "gzip", "snappy", "lz4"})
    void writeCsv(Compression compression, @PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException {
        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(csvSource(input, Compression.none))
                .withSink(sink(output, Format.csv, compression))
                .build();

        Pipeline pipeline = new Pipeline(new PipelineOptions(), def);

        assertEquals(0, pipeline.run());

        assertSinkEntries(pipeline, 13, 5);
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = {"none", "gzip", "snappy", "lz4"})
    void writeParquet(Compression compression, @PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withName("aws-s3-access-log")
                                .build())
                        .build())
                .withSink(sink(output, Format.parquet, compression))
                .build();

        PipelineDef merged = new PipelineOptionsMerge(pipelineOptions).merge(JSONUtil.valueToTree(def));
        Pipeline pipeline = new Pipeline(pipelineOptions, merged);

        assertEquals(0, pipeline.run());

        assertSinkEntries(pipeline, 4, merged.source().schema().declared().size());
    }

    @ParameterizedTest
    @EnumSource(value = Format.class, names = {"csv", "parquet"})
    void brotliSinkRejectedBeforeFlow(Format format, @PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) {
        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(csvSource(input, Compression.none))
                .withSink(sink(output, format, Compression.brotli))
                .build();

        Pipeline pipeline = new Pipeline(new PipelineOptions(), def);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, pipeline::build);

        assertTrue(exception.getMessage().contains("[none, gzip, snappy, lz4]"), exception::getMessage);
        assertNull(pipeline.flow());
    }
}
