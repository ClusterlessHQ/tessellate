/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.options;

import io.clusterless.tessellate.model.Field;
import io.clusterless.tessellate.util.Format;
import picocli.CommandLine;

import java.net.URI;
import java.util.List;

public class OutputOptions implements AWSOptions {
    @CommandLine.Option(names = {"-o", "--output"}, description = "output uris")
    private URI output;
    @CommandLine.Option(names = {"--output-fields"}, description = "output fields", converter = FieldConverter.class, split = "[\\+,]")
    private List<Field> outputFields;
    @CommandLine.Option(names = {"--output-format"}, description = "output format")
    private Format outputFormat;
    @CommandLine.Option(names = {"-t", "--output-manifest-template"}, description = "output manifest uri template")
    private String outputManifestTemplate;
    @CommandLine.Option(names = {"-l", "--output-manifest-lot"}, description = "output lot")
    private String outputLot;
    @CommandLine.Option(names = {"--output-aws-endpoint"}, description = "aws endpoint")
    protected String awsEndpoint;
    @CommandLine.Option(names = {"--output-aws-region"}, description = "aws region")
    protected String awsRegion;
    @CommandLine.Option(names = {"--output-aws-assumed-role-arn"}, description = "aws assumed role arn")
    protected String awsAssumedRoleARN;
    @CommandLine.Option(names = {"--output-errors"}, description = "output errors uri")
    private URI outputErrors;

    public URI output() {
        return output;
    }

    public OutputOptions setOutput(URI output) {
        this.output = output;
        return this;
    }

    public List<Field> outputFields() {
        return outputFields;
    }

    public void setOutputFields(List<Field> outputFields) {
        this.outputFields = outputFields;
    }

    public Format outputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(Format outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String outputManifestTemplate() {
        return outputManifestTemplate;
    }

    public OutputOptions setOutputManifestTemplate(String outputManifestTemplate) {
        this.outputManifestTemplate = outputManifestTemplate;
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

    public URI outputErrors() {
        return outputErrors;
    }
}
