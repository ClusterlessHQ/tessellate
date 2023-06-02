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
import io.clusterless.tessellate.util.JSONUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestWithResources
public class PipelineOptionsMergerTest {
    @Test
    void name(@GivenTextResource("/config/pipeline.json") String pipelineJson) throws IOException {
        List<URI> inputs = List.of(URI.create("s3://foo/input"));
        URI output = URI.create("s3://foo/output");

        PipelineOptions pipelineOptions = new PipelineOptions();
        pipelineOptions.setInputs(inputs);
        pipelineOptions.setOutput(output);

        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef merged = merger.merge(JSONUtil.readTree(pipelineJson));

        assertEquals(inputs, merged.source().inputs());
        assertEquals(output, merged.sink().output());
    }
}
