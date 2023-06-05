/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.JSONUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
@ExtendWith(ResourceExtension.class)
public class PipelineTest {

    @Test
    void noHeaders(@PathForResource("/data/delimited.csv") URI input, @PathForOutput URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(false)
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.tsv)
                                .withEmbedsSchema(false)
                                .build())
                        .build())
                .build();

        Pipeline pipeline = new Pipeline(pipelineOptions, def);

        pipeline.run();

        CascadingTesting.validateEntries(
                pipeline.flow().openSink(),
                l -> assertEquals(12, l, "wrong length"),
                l -> assertEquals(5, l, "wrong size"),
                l -> {
                }
        );
    }

    @Test
    void headers(@PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.tsv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .build();

        Pipeline pipeline = new Pipeline(pipelineOptions, def);

        pipeline.run();

        CascadingTesting.validateEntries(
                pipeline.flow().openSink(),
                l -> assertEquals(13, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(5, l, "wrong size"),
                l -> {
                }
        );
    }

    @Test
    void awsS3Access(@PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withName("aws-s3-access-log")
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.tsv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .build();

        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(def));

        Pipeline pipeline = new Pipeline(pipelineOptions, merged);

        pipeline.run();

        CascadingTesting.validateEntries(
                pipeline.flow().openSink(),
                l -> assertEquals(2, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(18, l, "wrong size"),
                l -> {
                }
        );
    }
}
