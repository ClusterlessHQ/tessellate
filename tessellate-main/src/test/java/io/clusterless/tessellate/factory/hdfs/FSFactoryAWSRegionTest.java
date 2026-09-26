/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs;

import io.clusterless.tessellate.options.PipelineOptions;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Region precedence for S3A: the {@code fs.s3a.endpoint.region} system property beats the
 * per-side {@code --input-/--output-aws-region}, which beats the global {@code --aws-region}.
 * With none of them set, tess leaves the region to the SDK chain and S3A discovery.
 */
@ExtendWith(SystemStubsExtension.class)
public class FSFactoryAWSRegionTest {
    @SystemStub
    private EnvironmentVariables environment = new EnvironmentVariables();

    @SystemStub
    private SystemProperties systemProperties = new SystemProperties();

    private static PipelineOptions parse(String... args) {
        PipelineOptions pipelineOptions = new PipelineOptions();
        new CommandLine(pipelineOptions).parseArgs(args);
        return pipelineOptions;
    }

    private static Properties apply(PipelineOptions pipelineOptions, boolean isSink) {
        return new ParquetFactory().applyAWSProperties(pipelineOptions, new Properties(), isSink);
    }

    @Test
    void perSideRegionBeatsGlobal() {
        PipelineOptions pipelineOptions = parse("--input-aws-region", "us-west-2", "--aws-region", "us-east-1");

        assertEquals("us-west-2", apply(pipelineOptions, false).getProperty(Constants.AWS_REGION));
        assertEquals("us-east-1", apply(pipelineOptions, true).getProperty(Constants.AWS_REGION));
    }

    @Test
    void outputRegionOnlyAppliesToSink() {
        PipelineOptions pipelineOptions = parse("--output-aws-region", "us-east-2");

        assertFalse(apply(pipelineOptions, false).containsKey(Constants.AWS_REGION));
        assertEquals("us-east-2", apply(pipelineOptions, true).getProperty(Constants.AWS_REGION));
    }

    @Test
    void systemPropertyBeatsOptions() {
        systemProperties.set(Constants.AWS_REGION, "eu-west-1");

        PipelineOptions pipelineOptions = parse(
                "--input-aws-region", "us-west-2",
                "--output-aws-region", "us-east-2",
                "--aws-region", "us-east-1"
        );

        assertEquals("eu-west-1", apply(pipelineOptions, false).getProperty(Constants.AWS_REGION));
        assertEquals("eu-west-1", apply(pipelineOptions, true).getProperty(Constants.AWS_REGION));
    }

    @Test
    void noRegionLeavesPropertyUnset() {
        PipelineOptions pipelineOptions = parse();

        assertFalse(apply(pipelineOptions, false).containsKey(Constants.AWS_REGION));
        assertFalse(apply(pipelineOptions, true).containsKey(Constants.AWS_REGION));
    }

    @Test
    void environmentRegionIsLeftToTheSDK() {
        environment.set("AWS_REGION", "ap-south-1");

        PipelineOptions pipelineOptions = parse();

        assertFalse(apply(pipelineOptions, false).containsKey(Constants.AWS_REGION));
        assertFalse(apply(pipelineOptions, true).containsKey(Constants.AWS_REGION));
    }
}
