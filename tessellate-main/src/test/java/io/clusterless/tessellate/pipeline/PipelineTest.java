/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.CascadingTesting;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.github.hal4j.uritemplate.URITemplate;
import io.clusterless.tessellate.junit.PathForOutput;
import io.clusterless.tessellate.junit.PathForResource;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PipelineOptionsMerge;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.URIs;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                l -> assertEquals(12, l, "wrong length"),
                l -> assertEquals(5, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
    }

    @Test
    void badWidth(@PathForResource("/data/delimited-variable-width.csv") URI input, @PathForOutput URI output) throws IOException {
        Transform transform = new Transform(
                "^fixedWidth{ width:5, insertAt:3 } ->"
        );

        fixedWidthBase(input, output, transform);
    }

    @Test
    void badWidthWithFields(@PathForResource("/data/delimited-variable-width.csv") URI input, @PathForOutput URI output) throws IOException {
        Transform transform = new Transform(
                "^fixedWidth{ width:5, insertAt:3 } -> _0+_1+_2+_3+_4"
        );

        fixedWidthBase(input, output, transform);
    }

    private static void fixedWidthBase(URI input, URI output, Transform transform) throws IOException {
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
                .withTransform(transform)
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .build())
                        .build())
                .build();

        Pipeline pipeline = new Pipeline(pipelineOptions, def);

        pipeline.run();

        CascadingTesting.validateEntries(
                pipeline.flow().openSink(),
                l -> assertEquals(5, l, "wrong length"),
                l -> assertEquals(5, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test", "", ".csv", 1);
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
                l -> assertEquals(13, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(5, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
    }

    @Test
    void headersPartitioned(@PathForResource("/data/partitioned/x=1/y=2/z=3/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException {
        List<SourcePartition> partitions = List.of(
                new SourcePartition("x"),
                new SourcePartition("y"),
                new SourcePartition("z")
        );

        Fields expected = new Fields("first", "second", "third", "fourth", "fifth", "x", "y", "z");
        headersPartitionedBase(input, output, partitions, expected);
    }

    @Test
    void headersPartitionedRenamed(@PathForResource("/data/partitioned/x=1/y=2/z=3/delimited-header.csv") URI input, @PathForOutput URI output) throws IOException {
        List<SourcePartition> partitions = List.of(
                new SourcePartition("x -> a"),
                new SourcePartition("y -> b"),
                new SourcePartition("z -> c")
        );

        Fields expected = new Fields("first", "second", "third", "fourth", "fifth", "a", "b", "c");
        headersPartitionedBase(input, output, partitions, expected);
    }

    private static void headersPartitionedBase(URI input, URI output, List<SourcePartition> partitions, Fields expected) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(URIs.trim(input, 4)))
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .withPartitions(partitions)
                        .build())
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

        TupleEntryIterator iterator = pipeline.flow().openSink();

        assertEquals(expected, iterator.getFields().unApplyTypes());

        CascadingTesting.validateEntries(
                iterator,
                l -> assertEquals(13, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(8, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
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
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
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

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
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
                        "^tsid{node:1,nodeCount:256} +> id|long",
                        "time+>ymd|DateTime|yyyyMMdd", // copy: "time|Instant|dd/MMM/yyyy:HH:mm:ss Z"
                        "httpStatus->httpStatusString|String",// rename: "httpStatus|Integer"
                        "httpStatusString|Integer",// coerce: "httpStatusString|String"
                        "requestID->",// discard: "httpStatusString|String"
                        "200=>code|Integer", // insert
                        "=> _empty|Integer" // insert
                ))
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

        PipelineOptionsMerge merger = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(def));

        Pipeline pipeline = new Pipeline(pipelineOptions, merged);

        pipeline.run();

        CascadingTesting.validateEntries(
                pipeline.flow().openSink(),
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 1 + 1 + 1, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
    }

    @Test
    void writeReadParquet(@PathForResource("/data/aws-s3-access-log.txt") URI input,
                          @PathForOutput("intermediate") URI intermediate,
                          @PathForOutput("output") URI output) throws IOException {
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
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
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
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
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

        assertFilenameParts(intermediate, "test-", "-guid", ".parquet", 1);
    }

    @Test
    void writeReadParquetPartitioned(@PathForResource("/data/aws-s3-access-log.txt") URI input,
                                     @PathForOutput("intermediate") URI intermediate,
                                     @PathForOutput("output") URI output) throws IOException {
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
                                new SinkPartition("time+>year|DateTime|yyyy"), // DateTime can parse year, month, and day. Instant cannot,
                                new SinkPartition("time+>month|DateTime|MM"),
                                new SinkPartition("time+>day|DateTime|dd")
                        ))
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
                                .build())
                        .build())
                .build();

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(writeAsParquet));
        Pipeline pipelineWrite = new Pipeline(pipelineOptions, merged);

        pipelineWrite.run();

        TupleEntryIterator iterator = pipelineWrite.flow().openSink();

        assertEquals(new Fields(
                "bucketOwner",
                "bucket",
                "time",
                "remoteIP",
                "requester",
                "requestID",
                "operation",
                "key",
                "requestURI",
                "httpStatus",
                "errorCode",
                "bytesSent",
                "objectSize",
                "totalTime",
                "turnAroundTime",
                "referrer",
                "userAgent",
                "versionID",
                "hostId",
                "signatureVersion",
                "cipherSuite",
                "authenticationType",
                "hostHeader",
                "tlsVersion",
                "accessPointArn",
                "aclRequired",
                "year",
                "month",
                "day"
        ), iterator.getFields().unApplyTypes());

        CascadingTesting.validateEntries(
                iterator,
                l -> assertEquals(4, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 3, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(intermediate, "test-", "-guid", ".parquet", 3);

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
                                new SourcePartition("year|DateTime|yyyy"),
                                new SourcePartition("month|DateTime|MM"),
                                new SourcePartition("day|DateTime|dd")
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

        TupleEntryIterator finalIterator = pipelineRead.flow().openSink();

        assertEquals(new Fields(
                "bucketOwner",
                "bucket",
                "time",
                "remoteIP",
                "requester",
                "requestID",
                "operation",
                "key",
                "requestURI",
                "httpStatus",
                "errorCode",
                "bytesSent",
                "objectSize",
                "totalTime",
                "turnAroundTime",
                "referrer",
                "userAgent",
                "versionID",
                "hostId",
                "signatureVersion",
                "cipherSuite",
                "authenticationType",
                "hostHeader",
                "tlsVersion",
                "accessPointArn",
                "aclRequired",
                "year",
                "month",
                "day"
        ), finalIterator.getFields().unApplyTypes());

        CascadingTesting.validateEntries(
                finalIterator,
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
                        .withManifestTemplate(intermediateManifest.toString())
                        .withManifestLot("20211112PT5M000")
                        .withSchema(Schema.builder()
                                .withFormat(Format.parquet)
                                .withEmbedsSchema(true)
                                .build())
                        .withNamedPartitions(true)
                        .withPartitions(List.of(
                                new SinkPartition("time+>year|DateTime|yyyy"), // DateTime can parse year, month, and day. Instant cannot,
                                new SinkPartition("time+>month|DateTime|MM"),
                                new SinkPartition("time+>day|DateTime|dd")
                        ))
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
                                .build())
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

        assertFilenameParts(intermediate, "test-", "-guid", ".parquet", 3);

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
                                new SourcePartition("year|DateTime|yyyy"),
                                new SourcePartition("month|DateTime|MM"),
                                new SourcePartition("day|DateTime|dd")
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

    @Test
    void toJsonAndBack(
            @PathForResource("/data/delimited-header.csv") URI input,
            @PathForOutput("intermediate") URI intermediate,
            @PathForOutput("output") URI output) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef writeJsonDef = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .withTransform(
                        new Transform(
                                "^toJson{} -> json"
                        )
                )
                .withSink(Sink.builder()
                        .withOutput(intermediate)
                        .withSchema(Schema.builder()
                                .withFormat(Format.json)
                                .withEmbedsSchema(false)
                                .build())
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
                                .build())
                        .build())
                .build();

        Pipeline writeJson = new Pipeline(pipelineOptions, writeJsonDef);

        writeJson.run();

        CascadingTesting.validateEntries(
                writeJson.flow().openSink(),
                l -> assertEquals(13, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(1, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(intermediate, "test-", "-guid", ".jsonl", 1);

        PipelineDef readJsonDef = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(intermediate))
                        .withSchema(Schema.builder()
                                .withDeclared(Field.asField("json"))
                                .withFormat(Format.json)
                                .withEmbedsSchema(true)
                                .build())
                        .build())
                .withTransform(
                        new Transform(
                                "json ^fromJson{} -> first + second + third + fourth + fifth"
                        )
                )
                .withSink(Sink.builder()
                        .withOutput(output)
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .withFilename(Filename.builder()
                                .withPrefix("test")
                                .withIncludeGuid(true)
                                .withProvidedGuid("guid")
                                .build())
                        .build())
                .build();

        Pipeline readJson = new Pipeline(pipelineOptions, readJsonDef);

        readJson.run();

        CascadingTesting.validateEntries(
                readJson.flow().openSink(),
                l -> assertEquals(13, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(5, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".csv", 1);
    }

    private static void assertFilenameParts(URI output, String prefix, String guid, String extension, int fileCount) throws IOException {
        final int[] count = {0};
        try (Stream<Path> pathStream = Files.find(Paths.get(output), 10, (path, attr) -> !path.toString().startsWith(".") && path.toString().endsWith(extension))) {
            pathStream
                    .forEach(path -> {
                        count[0]++;
                        String filename = path.getFileName().toString();
                        assertTrue(filename.startsWith(prefix), "wrong filename: " + filename);
                        assertTrue(filename.contains(guid), "wrong filename: " + filename);
                    });
        }

        assertEquals(fileCount, count[0], "wrong number of files");
    }

    @Test
    void headersBadWidth(
            @PathForResource("/data/delimited-header-bad-width.csv") URI input,
            @PathForOutput("output") URI output,
            @PathForOutput("errors") URI errors
    ) throws IOException {
        PipelineOptions pipelineOptions = new PipelineOptions();

        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(Source.builder()
                        .withInputs(List.of(input))
                        .withSchema(Schema.builder()
                                .withFormat(Format.csv)
                                .withEmbedsSchema(true)
                                .build())
                        .withErrorPath(errors)
                        .build())
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
                l -> assertEquals(11, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(5, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
        assertFilenameParts(errors, "errors-source", "-", ".csv.gz", 1);
    }

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
                l -> assertEquals(length, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(size, l, "wrong size"),
                l -> {
                }
        );

        assertFilenameParts(output, "test-", "-guid", ".tsv", 1);
    }
}
