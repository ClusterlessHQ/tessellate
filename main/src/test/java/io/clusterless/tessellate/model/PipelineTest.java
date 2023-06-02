/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import cascading.tuple.Fields;
import io.clusterless.tessellate.junit.PathForOutput;
import io.clusterless.tessellate.junit.PathForResource;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.pipeline.Pipeline;
import io.clusterless.tessellate.pipeline.PipelineOptions;
import io.clusterless.tessellate.util.Format;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.List;

/**
 *
 */
@ExtendWith(ResourceExtension.class)
public class PipelineTest {

    @Test
    void noHeaders(@PathForResource("/data/delimited.csv") URI input, @PathForOutput URI output) {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withDeclared(List.of(new Field(Fields.UNKNOWN)))
                                .withFormat(Format.csv)
                                .withEmbedsSchema(false)
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withDeclared(List.of(new Field(Fields.ALL)))
                                .withFormat(Format.tsv)
                                .withEmbedsSchema(false)
                                .build())
                        .build())
                .build();

        Pipeline pipeline = new Pipeline(pipelineOptions, def);

        pipeline.run();
    }

    @Test
    void headers(@PathForResource("/data/delimited-header.csv") URI input, @PathForOutput URI output) {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withDeclared(List.of(new Field(Fields.UNKNOWN)))
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withDeclared(List.of(new Field(Fields.ALL)))
                                .withFormat(Format.tsv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .build();

        Pipeline pipeline = new Pipeline(pipelineOptions, def);

        pipeline.run();
    }
}
