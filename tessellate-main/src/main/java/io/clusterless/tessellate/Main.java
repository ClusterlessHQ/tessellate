/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate;

import io.clusterless.tessellate.factory.TapFactories;
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PipelineOptionsMerge;
import io.clusterless.tessellate.pipeline.Pipeline;
import io.clusterless.tessellate.util.MetricsPrinter;
import io.clusterless.tessellate.util.Verbosity;
import io.clusterless.tessellate.util.VersionProvider;
import io.clusterless.tessellate.util.json.JSONUtil;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 *
 */
@CommandLine.Command(
        name = "tess",
        mixinStandardHelpOptions = true,
        versionProvider = VersionProvider.class,
        sortOptions = false
)
public class Main implements Callable<Integer> {
    public enum Show {
        formats,
        protocols,
        compression
    }

    @CommandLine.Mixin
    protected Verbosity verbosity = new Verbosity();

    @CommandLine.Mixin
    protected MetricsPrinter metrics = new MetricsPrinter();

    @CommandLine.Mixin
    protected PipelineOptions pipelineOptions = new PipelineOptions();

    public enum PrintScope {
        simple,
        all
    }

    @CommandLine.Option(
            names = "--print-pipeline",
            arity = "0..1",
            description = {
                    "show pipeline template, will not run pipeline",
                    "Optional values: ${COMPLETION-CANDIDATES}"
            },
            fallbackValue = "simple"
    )
    protected PrintScope printPipeline;

    @CommandLine.Option(
            names = "--show-source",
            description = {
                    "show protocols, formats, or compression options",
                    "Possible values: ${COMPLETION-CANDIDATES}"
            }
    )
    protected Show showSource;

    @CommandLine.Option(
            names = "--show-sink",
            description = {
                    "Show protocols, formats or compression options",
                    "Possible values: ${COMPLETION-CANDIDATES}"
            }
    )
    protected Show showSink;

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

        if (main.showSource != null) {
            if (main.showSource == Show.protocols) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSourceProtocols()));
            } else if (main.showSource == Show.formats) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSourceFormats()));
            } else if (main.showSource == Show.compression) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSourceCompression()));
            }
            return;
        }

        if (main.showSink != null) {
            if (main.showSink == Show.protocols) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSinkProtocols()));
            } else if (main.showSink == Show.formats) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSinkFormats()));
            } else if (main.showSink == Show.compression) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSinkCompression()));
            }
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
        Path path = pipelineOptions.pipelinePath();

        if (path != null && !Files.exists(path)) {
            System.err.println("pipeline file does not exist: " + path);
            return -1;
        }

        PipelineOptionsMerge merge = new PipelineOptionsMerge(pipelineOptions);

        PipelineDef pipelineDef = merge.merge();

        if (printPipeline != null) {
            switch (printPipeline) {
                case all:
                    System.out.println(JSONUtil.writeAsStringSafePretty(pipelineDef));
                    break;
                case simple:
                    System.out.println(JSONUtil.writeRWAsPrettyStringSafe(pipelineDef));
                    break;
            }
            return 0;
        }

        return executePipeline(pipelineDef);
    }

    private Integer executePipeline(PipelineDef pipelineDef) throws IOException {
        try {
            Pipeline pipeline = new Pipeline(pipelineOptions, pipelineDef);

            metrics.start(pipeline);

            pipeline.build();

            if (pipeline.state() == Pipeline.State.EMPTY_MANIFEST) {
                return 0;
            }

            return pipeline.run();
        } finally {
            metrics.stop();
        }
    }
}
