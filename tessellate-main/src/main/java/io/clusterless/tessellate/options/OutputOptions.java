/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.options;

import picocli.CommandLine;

import java.net.URI;

public class OutputOptions implements AWSOptions {
    @CommandLine.Option(names = {"-o", "--output"}, description = "output uris")
    private URI output;
    @CommandLine.Option(names = {"-t", "--output-manifest"}, description = "output manifest uri template")
    private String outputManifest;
    @CommandLine.Option(names = {"-l", "--output-lot"}, description = "output lot")
    private String outputLot;
    @CommandLine.Option(names = {"--output-aws-endpoint"}, description = "aws endpoint")
    protected String awsEndpoint;
    @CommandLine.Option(names = {"--output-aws-region"}, description = "aws region")
    protected String awsRegion;
    @CommandLine.Option(names = {"--output-aws-assumed-role-arn"}, description = "aws assumed role arn")
    protected String awsAssumedRoleARN;

    public URI output() {
        return output;
    }

    public OutputOptions setOutput(URI output) {
        this.output = output;
        return this;
    }

    public String outputManifest() {
        return outputManifest;
    }

    public OutputOptions setOutputManifest(String outputManifest) {
        this.outputManifest = outputManifest;
        return this;
    }

    public String outputLot() {
        return outputLot;
    }

    public OutputOptions setOutputLot(String outputLot) {
        this.outputLot = outputLot;
        return this;
    }

    @Override
    public String awsEndpoint() {
        return awsEndpoint;
    }

    @Override
    public String aswRegion() {
        return awsRegion;
    }

    @Override
    public String awsAssumedRoleARN() {
        return awsAssumedRoleARN;
    }
}
