/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import io.hosuaby.inject.resources.junit.jupiter.GivenTextResource;
import io.hosuaby.inject.resources.junit.jupiter.TestWithResources;
import io.clusterless.tessellate.factory.ManifestWriter;
import io.clusterless.tessellate.model.Field;
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PipelineOptionsMerge;
import io.clusterless.tessellate.parser.ast.AssignmentStatement;
import io.clusterless.tessellate.parser.ast.UnaryOperation;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.junit.jupiter.api.Test;
import org.mvel2.PropertyAccessException;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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

        assertEquals("1689820455", ((AssignmentStatement) merged.transform().statements().get(5)).literal());
        assertEquals("_seven", ((UnaryOperation) merged.transform().statements().get(6)).results().get(0).fieldRef().asComparable());
    }

    @Test
    void usingOptionsMissing() throws IOException {
        List<URI> inputs = List.of(URI.create("s3://foo/input"));
        URI output = URI.create("s3://foo/output");
        List<Field> declared = List.of(new Field("json"));

        PipelineOptions pipelineOptions = new PipelineOptions();
        pipelineOptions.inputOptions().setInputs(inputs);
        pipelineOptions.outputOptions().setOutput(output);
        pipelineOptions.outputOptions().setOutputFields(declared);
        pipelineOptions.outputOptions().setOutputFormat(Format.json);

        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef merged = merger.merge();

        assertEquals(inputs, merged.source().inputs());
        assertEquals(output, merged.sink().output());
        assertEquals(declared, merged.sink().schema().declared());
        assertEquals(Format.json, merged.sink().schema().format());
    }

    /**
     * The pipeline file is an MVEL template, {@code @{source.*}} must resolve against the merged source and
     * {@code @{sink.*}} against the merged sink.
     */
    @Test
    void templateResolvesSourceAndSink() throws IOException {
        String pipelineJson = """
                {
                  "source": {
                    "inputs": ["s3://bucket/input"],
                    "schema": {"declared": ["one|string"], "format": "csv"}
                  },
                  "transform": [
                    "@{source.manifestLot}=>source_lot|string",
                    "@{sink.manifestLot}=>sink_lot|string"
                  ]
                }
                """;

        PipelineOptions pipelineOptions = new PipelineOptions();
        new CommandLine(pipelineOptions).parseArgs("--input-manifest-lot", "input-lot", "-l", "output-lot");

        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef merged = merger.merge(JSONUtil.readTree(pipelineJson));

        assertEquals("input-lot", merged.source().manifestLot());
        assertEquals("output-lot", merged.sink().manifestLot());
        assertEquals("input-lot", ((AssignmentStatement) merged.transform().statements().get(0)).literal());
        assertEquals("output-lot", ((AssignmentStatement) merged.transform().statements().get(1)).literal());
    }

    private static final String LOT_PIPELINE = """
            {
              "source": {
                "inputs": ["s3://bucket/input"],
                "schema": {"declared": ["one|string"], "format": "csv"}
              },
              "transform": [
                "@{sink.manifestLot}=>sink_lot|string"
              ]
            }
            """;

    private static PipelineDef mergeLot(String pipelineJson, String... args) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();
        new CommandLine(pipelineOptions).parseArgs(args);

        return new PipelineOptionsMerge(pipelineOptions).merge(JSONUtil.readTree(pipelineJson));
    }

    /**
     * A lot id passes through from birth, so without an output lot the sink inherits the input lot, both in the
     * model and in {@code @{sink.*}} templates.
     */
    @Test
    void sinkInheritsSourceLot() throws IOException {
        PipelineDef merged = mergeLot(LOT_PIPELINE, "--input-manifest-lot", "input-lot");

        assertEquals("input-lot", merged.source().manifestLot());
        assertEquals("input-lot", merged.sink().manifestLot());
        assertEquals("input-lot", ((AssignmentStatement) merged.transform().statements().get(0)).literal());
    }

    private static final String TEMPLATED_LOT_PIPELINE = """
            {
              "source": {
                "inputs": ["s3://bucket/input"],
                "schema": {"declared": ["one|string"], "format": "csv"},
                "manifestLot": "lot-@{rnd64Next()}"
              },
              "transform": [
                "@{source.manifestLot}=>source_lot|string",
                "@{sink.manifestLot}=>sink_lot|string"
              ]
            }
            """;

    /**
     * A templated source lot is resolved once, so the inherited sink lot and every {@code @{source.*}} and
     * {@code @{sink.*}} reference carry the same value.
     */
    @Test
    void sinkInheritsResolvedTemplatedSourceLot() throws IOException {
        PipelineDef merged = mergeLot(TEMPLATED_LOT_PIPELINE);

        String lot = merged.source().manifestLot();

        assertNotNull(lot);
        assertFalse(lot.contains("@{"), lot);
        assertEquals(lot, merged.sink().manifestLot());
        assertEquals(lot, ((AssignmentStatement) merged.transform().statements().get(0)).literal());
        assertEquals(lot, ((AssignmentStatement) merged.transform().statements().get(1)).literal());
    }

    @Test
    void sinkInheritsTemplatedSourceLotOnce() throws IOException {
        String pipelineJson = """
                {
                  "source": {
                    "inputs": ["s3://bucket/input"],
                    "schema": {"declared": ["one|string"], "format": "csv"},
                    "manifestLot": "@{rnd64Next()}"
                  }
                }
                """;

        PipelineDef merged = mergeLot(pipelineJson);

        String lot = merged.source().manifestLot();

        assertFalse(lot.contains("@{"), lot);
        assertEquals(lot, merged.sink().manifestLot());
    }

    @Test
    void sinkInheritsResolvedTemplatedInputLot() throws IOException {
        PipelineDef merged = mergeLot(LOT_PIPELINE, "--input-manifest-lot", "lot-@{rnd64Next()}");

        String lot = merged.source().manifestLot();

        assertFalse(lot.contains("@{"), lot);
        assertEquals(lot, merged.sink().manifestLot());
        assertEquals(lot, ((AssignmentStatement) merged.transform().statements().get(0)).literal());
    }

    @Test
    void outputLotOverridesTemplatedSourceLot() throws IOException {
        PipelineDef merged = mergeLot(TEMPLATED_LOT_PIPELINE, "-l", "output-lot");

        String lot = merged.source().manifestLot();

        assertFalse(lot.contains("@{"), lot);
        assertEquals("output-lot", merged.sink().manifestLot());
        assertEquals(lot, ((AssignmentStatement) merged.transform().statements().get(0)).literal());
        assertEquals("output-lot", ((AssignmentStatement) merged.transform().statements().get(1)).literal());
    }

    /**
     * An output manifest requires a lot, an inherited lot satisfies it.
     */
    @Test
    void manifestAcceptsInheritedLot() throws IOException {
        PipelineDef merged = mergeLot(LOT_PIPELINE, "--input-manifest-lot", "input-lot", "-t", "file:///tmp/manifest/lot={lot}/state={state}/manifest.json");

        assertNotSame(ManifestWriter.NULL, ManifestWriter.from(merged.sink(), null));
    }

    @Test
    void outputLotOverridesSourceLot() throws IOException {
        PipelineDef merged = mergeLot(LOT_PIPELINE, "--input-manifest-lot", "input-lot", "-l", "output-lot");

        assertEquals("output-lot", merged.sink().manifestLot());
        assertEquals("output-lot", ((AssignmentStatement) merged.transform().statements().get(0)).literal());
    }

    @Test
    void pipelineSinkLotOverridesSourceLot() throws IOException {
        String pipelineJson = """
                {
                  "source": {
                    "inputs": ["s3://bucket/input"],
                    "schema": {"declared": ["one|string"], "format": "csv"}
                  },
                  "sink": {"manifestLot": "pipeline-lot"}
                }
                """;

        PipelineDef merged = mergeLot(pipelineJson, "--input-manifest-lot", "input-lot");

        assertEquals("input-lot", merged.source().manifestLot());
        assertEquals("pipeline-lot", merged.sink().manifestLot());
    }

    @Test
    void noLotIsNotInvented() throws IOException {
        String pipelineJson = """
                {
                  "source": {
                    "inputs": ["s3://bucket/input"],
                    "schema": {"declared": ["one|string"], "format": "csv"}
                  }
                }
                """;

        PipelineDef merged = mergeLot(pipelineJson);

        assertNull(merged.source().manifestLot());
        assertNull(merged.sink().manifestLot());
    }

    /**
     * A lot referenced but never given fails naming the property, with or without a sink block in the pipeline.
     */
    @Test
    void missingSinkLotReferenceFails() {
        PropertyAccessException exception = assertThrows(PropertyAccessException.class, () -> mergeLot(LOT_PIPELINE));

        assertTrue(exception.getMessage().contains("could not access: manifestLot"), exception::getMessage);
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
