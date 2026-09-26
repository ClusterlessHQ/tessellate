/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import io.clusterless.tessellate.util.Versions;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
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
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

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

    /**
     * The only place tess exits the JVM, see {@link #run(String[])} for the exit codes.
     */
    public static void main(String[] args) {
        System.exit(run(args));
    }

    /**
     * Parses and executes the command line once and returns the exit code:
     * <ul>
     *     <li>{@link CommandLine.ExitCode#OK} (0) on success, including --help, --version, --show-*, --print-*,
     *     and an empty manifest</li>
     *     <li>{@link CommandLine.ExitCode#SOFTWARE} (1) on an unexpected error</li>
     *     <li>{@link CommandLine.ExitCode#USAGE} (2) on a usage error, an invalid option or a missing pipeline file</li>
     *     <li>{@link Pipeline#FLOW_FAILED} (3) when the flow fails</li>
     * </ul>
     * stdout carries only sink data and the requested machine outputs, everything else goes to stderr.
     */
    static int run(String[] args) {
        Main main = new Main();

        CommandLine commandLine = new CommandLine(main)
                .setParameterExceptionHandler(Main::handleParameterException)
                .setExecutionExceptionHandler((e, cmd, parseResult) -> main.handleExecutionException(e, cmd));

        if (args.length == 0) {
            commandLine.usage(commandLine.getOut());
            return CommandLine.ExitCode.OK;
        }

        return commandLine.execute(args);
    }

    private static int handleParameterException(CommandLine.ParameterException e, String[] args) {
        CommandLine commandLine = e.getCommandLine();
        PrintWriter err = commandLine.getErr();

        err.println(e.getMessage());
        commandLine.usage(err);
        err.flush();

        return CommandLine.ExitCode.USAGE;
    }

    private int handleExecutionException(Exception e, CommandLine commandLine) {
        PrintWriter err = commandLine.getErr();

        err.println(e.getMessage() != null ? e.getMessage() : e.toString());

        if (verbosity().isVerbose()) {
            e.printStackTrace(err);
        }

        err.flush();

        return CommandLine.ExitCode.SOFTWARE;
    }

    public Main() {
    }

    public Verbosity verbosity() {
        return verbosity;
    }

    @Override
    public Integer call() throws IOException {
        if (showSource != null) {
            if (showSource == Show.protocols) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSourceProtocols()));
            } else if (showSource == Show.formats) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSourceFormats()));
            } else if (showSource == Show.compression) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSourceCompression()));
            }
            return CommandLine.ExitCode.OK;
        }

        if (showSink != null) {
            if (showSink == Show.protocols) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSinkProtocols()));
            } else if (showSink == Show.formats) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSinkFormats()));
            } else if (showSink == Show.compression) {
                System.out.println(JSONUtil.writeAsStringSafePretty(TapFactories.getSinkCompression()));
            }
            return CommandLine.ExitCode.OK;
        }

        Path path = pipelineOptions.pipelinePath();

        if (path != null && !Files.exists(path)) {
            System.err.println("pipeline file does not exist: " + path);
            return CommandLine.ExitCode.USAGE;
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
            return CommandLine.ExitCode.OK;
        }

        return executePipeline(pipelineDef);
    }

    private Integer executePipeline(PipelineDef pipelineDef) throws IOException {
        try {
            LOG.info("tessellate version: {}", Versions.clsVersion());

            Pipeline pipeline = new Pipeline(pipelineOptions, pipelineDef);

            metrics.start(pipeline);

            pipeline.build();

            if (pipeline.state() == Pipeline.State.EMPTY_MANIFEST) {
                return CommandLine.ExitCode.OK;
            }

            return pipeline.run();
        } finally {
            metrics.stop();
        }
    }
}
