/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import picocli.CommandLine;

import java.nio.file.Path;

/**
 * pipeline
 * inputPath
 * outputPath
 * errorPath
 */
public class PipelineOptions implements AWSOptions {
    @CommandLine.Option(names = {"-p", "--pipeline"}, description = "pipeline file")
    private Path pipelinePath;

    @CommandLine.Mixin
    private InputOptions inputOptions = new InputOptions();
    @CommandLine.Mixin
    private OutputOptions outputOptions = new OutputOptions();

    @CommandLine.Option(names = {"-d", "--debug"}, description = "debug output for pipeline")
    private boolean debug;

    @CommandLine.Option(names = {"--aws-endpoint"}, description = "aws endpoint")
    protected String endpoint;

    @CommandLine.Option(names = {"--aws-region"}, description = "aws region")
    protected String region;

    @CommandLine.Option(names = {"--aws-assumed-role-arn"}, description = "aws assumed role arn")
    protected String assumedRoleARN;

    public Path pipelinePath() {
        return pipelinePath;
    }

    public InputOptions inputOptions() {
        return inputOptions;
    }

    public OutputOptions outputOptions() {
        return outputOptions;
    }

    public boolean debug() {
        return debug;
    }

    @Override
    public String awsEndpoint() {
        return endpoint;
    }

    @Override
    public String aswRegion() {
        return region;
    }

    @Override
    public String awsAssumedRoleARN() {
        return assumedRoleARN;
    }
}
