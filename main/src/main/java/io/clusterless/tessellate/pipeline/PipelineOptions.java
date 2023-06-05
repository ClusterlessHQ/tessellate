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
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

/**
 * pipeline
 * inputPath
 * outputPath
 * errorPath
 */
public class PipelineOptions {
    @CommandLine.Option(names = {"-p", "--pipeline"}, description = "pipeline file")
    private Path pipelinePath;

    @CommandLine.Option(names = {"-i", "--input"}, description = "input uris")
    private List<URI> inputs = new LinkedList<>();

    @CommandLine.Option(names = {"-o", "--output"}, description = "output uris")
    private URI output;

    @CommandLine.Option(names = {"-d", "--debug"}, description = "debug output for pipeline")
    private boolean debug;

    public PipelineOptions setInputs(List<URI> inputs) {
        this.inputs = inputs;
        return this;
    }

    public PipelineOptions setOutput(URI output) {
        this.output = output;
        return this;
    }

    public Path pipelinePath() {
        return pipelinePath;
    }

    public List<URI> inputs() {
        return inputs;
    }

    public URI output() {
        return output;
    }

    public boolean debug() {
        return debug;
    }
}
