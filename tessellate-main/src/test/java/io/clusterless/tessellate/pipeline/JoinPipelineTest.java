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
import io.clusterless.tessellate.util.Format;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.clusterless.tessellate.util.AltAssertions.assertFilenameParts;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ResourceExtension.class)
public class JoinPipelineTest {
    @Test
    void joinInner(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +inner{} -> lhs_value", 11, 3);
    }

    @Test
    void joinInnerRetain(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +inner{} +> lhs_value", 11, 4);
    }

    @Test
    void joinOuter(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +outer{} -> lhs_value", 16, 3);
    }

    @Test
    void joinOuterRetain(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +outer{} +> lhs_value", 16, 4);
    }

    @Test
    void joinLeft(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +left{} -> lhs_value", 11, 3);
    }

    @Test
    void joinLeftRetain(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +left{} +> lhs_value", 11, 4);
    }

    @Test
    void joinRight(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +right{} -> lhs_value", 16, 3);
    }

    @Test
    void joinRightRetain(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +right{} +> lhs_value", 16, 4);
    }

    @Test
    void joinInnerFilter(@PathForResource("/data/lhs.csv") URI lhs, @PathForResource("/data/rhs.csv") URI rhs, @PathForOutput URI output) throws IOException {
        joinTest(lhs, rhs, output, "lhs(lhs_id|int) rhs(rhs_id|int) +inner{}", 11, 2);
    }

    private static void joinTest(URI lhs, URI rhs, URI output, String transform, int length, int size) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSources(Map.of(
                        "lhs",
                        Source.builder()
                                .withInputs(List.of(lhs))
                                .withSchema(Schema.builder()
                                        .withFormat(Format.csv)
                                        .withEmbedsSchema(true)
                                        .build())
                                .build(),
                        "rhs",
                        Source.builder()
                                .withInputs(List.of(rhs))
                                .withSchema(Schema.builder()
                                        .withFormat(Format.csv)
                                        .withEmbedsSchema(true)
                                        .build())
                                .build()
                ))
                .withTransform(
                        new Transform(transform)
                )
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.tsv)
                                .withEmbedsSchema(true)
                                .build())
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
                                .build())
                        .build())
                .build();

        Pipeline pipeline = new Pipeline(pipelineOptions, def);

        pipeline.run();

        CascadingTesting.validateEntries(
                pipeline.flow().openSink(),
                l -> assertEquals(length, l, "wrong file length"), // headers are declared so aren't counted
                l -> assertEquals(size, l, "wrong tuple size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
    }
}
