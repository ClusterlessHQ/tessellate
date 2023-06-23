/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate;

import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.pipeline.Pipeline;
import io.clusterless.tessellate.pipeline.PipelineOptions;
import io.clusterless.tessellate.pipeline.PipelineOptionsMerge;
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.MetricsPrinter;
import io.clusterless.tessellate.util.Verbosity;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 *
 */
@CommandLine.Command(
        name = "tessellate",
        mixinStandardHelpOptions = true,
        version = "1.0-wip"
)
public class Main implements Callable<Integer> {
    @CommandLine.Mixin
    protected Verbosity verbosity = new Verbosity();

    @CommandLine.Mixin
    protected MetricsPrinter metrics = new MetricsPrinter();

    @CommandLine.Mixin
    protected PipelineOptions pipelineOptions = new PipelineOptions();

    @CommandLine.Option(names = "--print-project", description = "show project template, will not run pipeline")
    protected boolean printProject = false;

    public static void main(String[] args) {
        Main main = new Main();

        CommandLine commandLine = new CommandLine(main);

        try {
            commandLine.parseArgs(args);
        } catch (CommandLine.MissingParameterException | CommandLine.UnmatchedArgumentException e) {
            System.err.println(e.getMessage());
            commandLine.usage(System.out);
            System.exit(-1);
        }

        if (args.length == 0 || commandLine.isUsageHelpRequested()) {
            commandLine.usage(System.out);
            return;
        } else if (commandLine.isVersionHelpRequested()) {
            commandLine.printVersionHelp(System.out);
            return;
        }

        int exitCode = 0;

        try {
            exitCode = commandLine.execute(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());

            if (main.verbosity().isVerbose()) {
                e.printStackTrace(System.err);
            }

            System.exit(-1); // get exit code from exception
        }

        System.exit(exitCode);
    }

    public Main() {
    }

    public Verbosity verbosity() {
        return verbosity;
    }

    @Override
    public Integer call() throws IOException {
        PipelineOptionsMerge merge = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef pipelineDef = merge.merge();

        if (printProject) {
            System.out.println(JSONUtil.writeAsStringSafePretty(pipelineDef));
            return 0;
        }

        return executePipeline(pipelineDef);
    }

    private Integer executePipeline(PipelineDef pipelineDef) throws IOException {
        try {
            Pipeline pipeline = new Pipeline(pipelineOptions, pipelineDef);

            metrics.start(pipeline);

            pipeline.build();

            return pipeline.run();
        } finally {
            metrics.stop();
        }
    }
}
