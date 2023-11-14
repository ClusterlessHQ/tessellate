/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import com.adelean.inject.resources.junit.jupiter.GivenTextResource;
import com.adelean.inject.resources.junit.jupiter.TestWithResources;
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PipelineOptionsMerge;
import io.clusterless.tessellate.parser.ast.Assignment;
import io.clusterless.tessellate.parser.ast.UnaryOperation;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestWithResources
public class PipelineOptionsMergerTest {
    @Test
    void usingOptions(@GivenTextResource("/config/pipeline-mvel.json") String pipelineJson) throws IOException {
        List<URI> inputs = List.of(URI.create("s3://foo/input"));
        URI output = URI.create("s3://foo/output");

        PipelineOptions pipelineOptions = new PipelineOptions();
        pipelineOptions.inputOptions().setInputs(inputs);
        pipelineOptions.outputOptions().setOutput(output);

        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef merged = merger.merge(JSONUtil.readTree(pipelineJson));

        assertEquals(inputs, merged.source().inputs());
        assertEquals(output, merged.sink().output());

        assertEquals("1689820455", ((Assignment) merged.transform().statements().get(5)).literal());
        assertEquals("_seven", ((UnaryOperation) merged.transform().statements().get(6)).results().get(0).fieldRef().asComparable());
    }

    @Test
    void fromSchema(@GivenTextResource("/config/pipeline-named-schema.json") String pipelineJson) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef merged = merger.merge(JSONUtil.readTree(pipelineJson));

        assertEquals(26, merged.source().schema().declared().size());
        assertEquals(3, merged.source().partitions().size());
    }
}
