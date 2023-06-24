/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.CascadingTesting;
import com.github.hal4j.uritemplate.URITemplate;
import io.clusterless.tessellate.junit.PathForOutput;
import io.clusterless.tessellate.junit.PathForResource;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PipelineOptionsMerge;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.URIs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
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
    void awsS3AccessLog(@PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput URI output) throws IOException {
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
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size(), l, "wrong size"),
                l -> {
                }
        );
    }

    @Test
    void awsS3AccessLogWithTransforms(@PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withName("aws-s3-access-log")
                                .build())
                        .build())
                .withTransform(new Transform(
                        "time+>ymd|DateTime|yyyyMMdd", // copy: "time|Instant|dd/MMM/yyyy:HH:mm:ss Z"
                        "httpStatus->httpStatusString|String",// rename: "httpStatus|Integer"
                        "httpStatusString|Integer",// coerce: "httpStatusString|String"
                        "requestID->",// discard: "httpStatusString|String"
                        "200=>code|Integer" // insert
                ))
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
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 1, l, "wrong size"),
                l -> {
                }
        );
    }

    @Test
    void writeReadParquet(@PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput("intermediate") URI intermediate, @PathForOutput("output") URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();
        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef writeAsParquet = PipelineDef.builder()
                .withName("write")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withName("aws-s3-access-log")
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(intermediate)
                        .withSchema(Schema.builder()
                                .withFormat(Format.parquet)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .build();

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(writeAsParquet));
        Pipeline pipelineWrite = new Pipeline(pipelineOptions, merged);

        pipelineWrite.run();

        CascadingTesting.validateEntries(
                pipelineWrite.flow().openSink(),
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size(), l, "wrong size"),
                l -> {
                }
        );

        PipelineDef readAsParquet = PipelineDef.builder()
                .withName("write")
                .withSource(Source.builder()
                        .withInputs(List.of(intermediate))
                        .withSchema(Schema.builder()
                                .withFormat(Format.parquet)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .build();

        Pipeline pipelineRead = new Pipeline(pipelineOptions, merger.merge(JSONUtil.valueToTree(readAsParquet)));

        pipelineRead.run();

        CascadingTesting.validateEntries(
                pipelineRead.flow().openSink(),
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size(), l, "wrong size"),
                l -> {
                }
        );
    }

    @Test
    void writeReadParquetPartitioned(@PathForResource("/data/aws-s3-access-log.txt") URI input, @PathForOutput("intermediate") URI intermediate, @PathForOutput("output") URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();
        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef writeAsParquet = PipelineDef.builder()
                .withName("write")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withName("aws-s3-access-log")
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(intermediate)
                        .withSchema(Schema.builder()
                                .withFormat(Format.parquet)
                                .withEmbedsSchema(true)
                                .build())
                        .withNamedPartitions(true)
                        .withPartitions(List.of(
                                new Partition("time+>year|DateTime|yyyy"), // DateTime can parse year, month, and day. Instant cannot,
                                new Partition("time+>month|DateTime|MM"),
                                new Partition("time+>day|DateTime|dd")
                        ))
                        .build())
                .build();

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(writeAsParquet));
        Pipeline pipelineWrite = new Pipeline(pipelineOptions, merged);

        pipelineWrite.run();

        CascadingTesting.validateEntries(
                pipelineWrite.flow().openSink(),
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 3, l, "wrong size"),
                l -> {
                }
        );

        PipelineDef readAsParquet = PipelineDef.builder()
                .withName("read")
                .withSource(Source.builder()
                        .withInputs(List.of(intermediate))
                        .withSchema(Schema.builder()
                                .withFormat(Format.parquet)
                                .withEmbedsSchema(true)
                                .build())
                        .withNamedPartitions(true)
                        .withPartitions(List.of(
                                new Partition("year|DateTime|yyyy"),
                                new Partition("month|DateTime|MM"),
                                new Partition("day|DateTime|dd")
                        ))
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .build();

        Pipeline pipelineRead = new Pipeline(pipelineOptions, merger.merge(JSONUtil.valueToTree(readAsParquet)));

        pipelineRead.run();

        CascadingTesting.validateEntries(
                pipelineRead.flow().openSink(),
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 3, l, "wrong size"),
                l -> {
                }
        );
    }

    /**
     * * {@code {provider-service}://{manifest-store}/{dataset-name}/{dataset-version}/{lot}/{state}[/{attempt}]/manifest.{ext}}
     * <pre>
     *      {
     * 	"sources": {
     * 		"main": {
     * 			"name": "source",
     * 			"version": "20230101",
     * 			"pathURI": "s3://test-clusterless-simple-python-copy-086903124729-us-west-2/ingress/",
     * 			"subscribe": true
     *                }* 	},
     * 	"sinks": {
     * 		"main": {
     * 			"name": "sink",
     * 			"version": "20230101",
     * 			"pathURI": "s3://test-clusterless-simple-python-copy-086903124729-us-west-2/copy/",
     * 			"publish": true
     *        }
     *    }    ,
     * 	"sourceManifestPaths": {
     * 		"main": "s3://test-clusterless-manifest-086903124729-us-west-2/datasets/name=ingress-python-example-source/version=20230101/lot={lot}/state=complete/manifest.json"
     *    },
     * 	"sinkManifestPaths": {
     * 		"main": "s3://test-clusterless-manifest-086903124729-us-west-2/datasets/name=ingress-python-example-copy/version=20230101/lot={lot}/state={state}{/attempt*}/manifest.json"
     *    },
     * 	"workloadProps": {
     *
     *    }
     * }
     *  </pre>
     */
    @Test
    void writeReadParquetPartitionedWithManifests(
            @PathForResource("/data/aws-s3-access-log.txt") URI input,
            @PathForOutput("intermediateManifest") URI intermediateManifest,
            @PathForOutput("intermediate") URI intermediate,
            @PathForOutput("output") URI output
    ) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();
        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        intermediateManifest = URIs.copyWithPathAppend(intermediateManifest, "/lot={lot}/state={state}{/attempt*}/manifest.json");

        PipelineDef writeAsParquet = PipelineDef.builder()
                .withName("write")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withName("aws-s3-access-log")
                                .build())
                        .build())
                .withSink(Sink.builder()
                        .withOutput(intermediate)
                        .withManifest(intermediateManifest)
                        .withManifestLot("20211112PT5M000")
                        .withSchema(Schema.builder()
                                .withFormat(Format.parquet)
                                .withEmbedsSchema(true)
                                .build())
                        .withNamedPartitions(true)
                        .withPartitions(List.of(
                                new Partition("time+>year|DateTime|yyyy"), // DateTime can parse year, month, and day. Instant cannot,
                                new Partition("time+>month|DateTime|MM"),
                                new Partition("time+>day|DateTime|dd")
                        ))
                        .build())
                .build();

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(writeAsParquet));
        Pipeline pipelineWrite = new Pipeline(pipelineOptions, merged);

        pipelineWrite.run();

        CascadingTesting.validateEntries(
                pipelineWrite.flow().openSink(),
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 3, l, "wrong size"),
                l -> {
                }
        );

        URI resolvedIntermediateManifest = new URITemplate(URLDecoder.decode(intermediateManifest.toString(), StandardCharsets.UTF_8))
                .expand("lot", "20211112PT5M000")
                .expand("state", "complete")
                .discard("attempt")
                .toURI();

        PipelineDef readAsParquet = PipelineDef.builder()
                .withName("read")
                .withSource(Source.builder()
                        .withManifest(resolvedIntermediateManifest)
                        .withSchema(Schema.builder()
                                .withFormat(Format.parquet)
                                .withEmbedsSchema(true)
                                .build())
                        .withNamedPartitions(true)
                        .withPartitions(List.of(
                                new Partition("year|DateTime|yyyy"),
                                new Partition("month|DateTime|MM"),
                                new Partition("day|DateTime|dd")
                        ))
                        .build())
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .build();

        Pipeline pipelineRead = new Pipeline(pipelineOptions, merger.merge(JSONUtil.valueToTree(readAsParquet)));

        pipelineRead.run();

        CascadingTesting.validateEntries(
                pipelineRead.flow().openSink(),
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 3, l, "wrong size"),
                l -> {
                }
        );
    }
}
