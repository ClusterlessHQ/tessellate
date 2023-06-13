/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import picocli.CommandLine;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

public class InputOptions implements AWSOptions {
    @CommandLine.Option(names = {"-i", "--input"}, description = "input uris")
    private List<URI> inputs = new LinkedList<>();
    @CommandLine.Option(names = {"--input-aws-endpoint"}, description = "aws endpoint")
    protected String awsEndpoint;
    @CommandLine.Option(names = {"--input-aws-region"}, description = "aws region")
    protected String awsRegion;
    @CommandLine.Option(names = {"--input-aws-assumed-role-arn"}, description = "aws assumed role arn")
    protected String awsAssumedRoleARN;

    public InputOptions setInputs(List<URI> inputs) {
        this.inputs = inputs;
        return this;
    }

    public List<URI> inputs() {
        return inputs;
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
