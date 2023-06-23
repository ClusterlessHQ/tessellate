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
import io.clusterless.tessellate.junit.PathForResource;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.junit.URLForOutput;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.URIs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
@Testcontainers
@ExtendWith(SystemStubsExtension.class)
@ExtendWith(ResourceExtension.class)
public class PipelineIntegrationTest {
    public static final String TEST_BUCKET = "test-bucket";

    static DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:2.1.0");

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(
                    LocalStackContainer.Service.S3
            );

    protected String defaultRegion() {
        return localstack.getRegion();
    }

    @SystemStub
    private EnvironmentVariables environmentVariables = new EnvironmentVariables()
            .set("AWS_PROFILE", null)
            .set("AWS_ACCESS_KEY_ID", localstack.getAccessKey())
            .set("AWS_SECRET_ACCESS_KEY", localstack.getSecretKey())
            .set("AWS_DEFAULT_REGION", localstack.getRegion())
            .set("AWS_S3_ENDPOINT", localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());

    @BeforeEach
    public void bootstrap() {
        try (S3Client s3 = S3Client.builder()
                .region(Region.of(defaultRegion()))
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
                .build()) {
            s3.createBucket(b -> b.bucket(TEST_BUCKET));
        }
    }

    @Test
    void writeReadParquet(@PathForResource("/data/aws-s3-access-log.txt") URI input,
                          @URLForOutput(scheme = "s3", host = TEST_BUCKET, path = "intermediate") URI intermediate,
                          @URLForOutput(scheme = "s3", host = TEST_BUCKET, path = "output") URI output) throws IOException {
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
                l -> assertEquals(2, l, "wrong length"), // headers are declared so aren't counted
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
                l -> assertEquals(2, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size(), l, "wrong size"),
                l -> {
                }
        );
    }

    @Test
    void writeReadParquetPartitioned(
            @PathForResource("/data/aws-s3-access-log.txt") URI input,
            @URLForOutput(scheme = "s3", host = TEST_BUCKET, path = "intermediate") URI intermediate,
            @URLForOutput(scheme = "s3", host = TEST_BUCKET, path = "output") URI output) throws IOException {
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
                                new Partition("time->year|DateTime|yyyy"), // DateTime can parse year, month, and day. Instant cannot,
                                new Partition("time->month|DateTime|MM"),
                                new Partition("time->day|DateTime|dd")
                        ))
                        .build())
                .build();

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(writeAsParquet));
        Pipeline pipelineWrite = new Pipeline(pipelineOptions, merged);

        pipelineWrite.run();

        CascadingTesting.validateEntries(
                pipelineWrite.flow().openSink(),
                l -> assertEquals(2, l, "wrong length"), // headers are declared so aren't counted
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
                l -> assertEquals(2, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 3, l, "wrong size"),
                l -> {
                }
        );
    }

    @Test
    void writeReadParquetPartitionedWithManifests(
            @PathForResource("/data/aws-s3-access-log.txt") URI input,
            @URLForOutput(scheme = "s3", host = TEST_BUCKET, path = "intermediateManifest") URI intermediateManifest,
            @URLForOutput(scheme = "s3", host = TEST_BUCKET, path = "intermediate") URI intermediate,
            @URLForOutput(scheme = "s3", host = TEST_BUCKET, path = "output") URI output
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
                                new Partition("time->year|DateTime|yyyy"), // DateTime can parse year, month, and day. Instant cannot,
                                new Partition("time->month|DateTime|MM"),
                                new Partition("time->day|DateTime|dd")
                        ))
                        .build())
                .build();

        PipelineDef merged = merger.merge(JSONUtil.valueToTree(writeAsParquet));
        Pipeline pipelineWrite = new Pipeline(pipelineOptions, merged);

        pipelineWrite.run();

        CascadingTesting.validateEntries(
                pipelineWrite.flow().openSink(),
                l -> assertEquals(2, l, "wrong length"), // headers are declared so aren't counted
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
                l -> assertEquals(2, l, "wrong length"), // headers are declared so aren't counted
                l -> assertEquals(merged.source().schema().declared().size() + 3, l, "wrong size"),
                l -> {
                }
        );
    }
}
