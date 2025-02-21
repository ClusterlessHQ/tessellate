/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.flow.stream.duct.DuctException;
import cascading.operation.Debug;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.pipe.assembly.Copy;
import cascading.tap.Tap;
import cascading.tap.TrapProps;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.*;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PrintOptions;
import io.clusterless.tessellate.parser.ast.Statement;
import io.clusterless.tessellate.printer.SchemaPrinter;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static cascading.flow.FlowDef.flowDef;

public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    public static final String HEAD = "head";
    public static final String TAIL = "tail";
    public static final String TRANSFORM = "transform";

    public enum State {
        NONE,
        EMPTY_MANIFEST,
        READY,
        COMPLETE
    }

    private final PipelineOptions pipelineOptions;
    private final PipelineDef pipelineDef;
    private State state = State.NONE;
    private Flow flow;
    private Properties commonProperties = new Properties();
    private LocalFlowProcess localFlowProcess;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public Pipeline(PipelineOptions pipelineOptions, PipelineDef pipelineDef) {
        this.pipelineOptions = pipelineOptions;
        this.pipelineDef = pipelineDef;
    }

    public PipelineOptions pipelineOptions() {
        return pipelineOptions;
    }

    public PipelineDef pipelineDef() {
        return pipelineDef;
    }

    public LocalFlowProcess flowProcess() {
        if (localFlowProcess == null) {
            localFlowProcess = new LocalFlowProcess(commonProperties);
        }
        return localFlowProcess;
    }

    public State state() {
        return state;
    }

    public Flow flow() {
        return flow;
    }

    public boolean hasFlow() {
        return flow != null;
    }

    public boolean isRunning() {
        return hasFlow() && running.get();
    }

    public void build() throws IOException {
        SourceFactory sourceFactory;
        try {
            sourceFactory = TapFactories.findSourceFactory(pipelineOptions, pipelineDef.source());
        } catch (ManifestEmptyException e) {
            SinkFactory sinkFactory = TapFactories.findSinkFactory(pipelineDef.sink());

            sinkFactory.applyGlobalProperties(commonProperties);

            ManifestWriter manifestWriter = ManifestWriter.from(pipelineDef.sink(), null);

            manifestWriter.writeManifest(commonProperties);

            state = State.EMPTY_MANIFEST;

            return;
        }

        sourceFactory.applyGlobalProperties(commonProperties);

        Tap<Properties, ?, ?> sourceTap = sourceFactory.getSource(pipelineOptions, pipelineDef.source());

        if (pipelineDef.source().schema().embedsSchema() || pipelineDef.source().schema().format().alwaysEmbedsSchema()) {
            sourceTap.retrieveSourceFields(flowProcess());
        }

        // get source fields here so that any partition fields will be captured
        Fields sourceFields = sourceTap.getSourceFields();

        if (!sourceFields.hasTypes()) {
            sourceFields = sourceFields.applyTypeToAll(String.class);
        }

        PipelineContext context = new PipelineContext(LOG, sourceFields, new Pipe(HEAD));

        logCurrentFields(context.currentFields);

        Schema sourceSchema = pipelineDef.source().schema();
        if (sourceSchema.format() == Format.regex) {
            Fields declaredFields = Models.fieldAsFields(sourceSchema.declared(), String.class, Fields.ALL);
            Pipe pipe = new Each(context.pipe, new Fields("line"), new RegexParser(declaredFields, sourceSchema.pattern()), Fields.SWAP);
            LOG.info("parsing lines with regex: {}", sourceSchema.pattern());
            Fields currentFields = context.currentFields.subtract(new Fields("line")).append(declaredFields);
            logCurrentFields(currentFields);
            context.update(currentFields, pipe);
        }

        context.name(TRANSFORM);
        // todo: group like transforms together if there are no interdependencies
        for (Statement statement : pipelineDef.transform().statements()) {
            context = new Transformer(statement).resolve(context);
        }

        Fields partitionFields = Fields.NONE;

        if (!pipelineDef.sink().partitions().isEmpty()) {
            // todo: honor the -> and +> operators when declaring partitions
            for (SinkPartition partition : pipelineDef().sink().partitions()) {
                if (partition.from().isPresent()) {
                    Pipe pipe = new Copy(context.pipe, partition.from().get().fields(), partition.to().fields());
                    partitionFields = partitionFields.append(partition.to().fields());
                    context.update(context.currentFields, pipe);
                } else if (context.currentFields.contains(partition.to().fields())) {
                    partitionFields = partitionFields.append(partition.to().fields());
                } else {
                    Pipe pipe = new Coerce(context.pipe, partition.to().fields());
                    // change the type information
                    partitionFields = partitionFields.rename(partition.to().fields(), partition.to().fields());
                    context.update(context.currentFields, pipe);
                }
            }
        }

        LOG.info("sink partitions fields: {}", partitionFields);

        // watch the progress on the console
        if (pipelineOptions().debug()) {
            context.update(context.currentFields, new Each(context.pipe, new Debug(true)));
        }

        context.name(TAIL);
        LOG.info("sinking into fields: {}", context.currentFields);

        SinkFactory sinkFactory = TapFactories.findSinkFactory(pipelineDef.sink());

        sinkFactory.applyGlobalProperties(commonProperties);

        Tap<Properties, ?, ?> sinkTap = sinkFactory.getSink(pipelineOptions, pipelineDef.sink(), context.currentFields);

        Map<String, Tap> traps = new HashMap<>();

        if (pipelineDef.source().errorPath() != null) {
            Tap<Properties, ?, ?> errorTap = createTrap(pipelineDef.source().errorPath(), "errors-source", sinkFactory);
            traps.put(HEAD, errorTap);
        }

        if (pipelineDef.sink().errorPath() != null) {
            Tap<Properties, ?, ?> errorTap = createTrap(pipelineDef.sink().errorPath(), "errors-sink", sinkFactory);
            traps.put(TAIL, errorTap);
        }

        if (!traps.isEmpty()) {
            commonProperties = TrapProps.trapProps()
                    .setRecordThrowableMessage(true)
                    .setRecordElementTrace(true)
                    .setRecordThrowableStackTrace(true)
                    .buildProperties(commonProperties);
        }

        FlowDef flowDef = flowDef()
                .setName("pipeline")
                .addSource(HEAD, sourceTap)
                .addSink(TAIL, sinkTap)
                .addTail(context.pipe)
                .addTraps(traps);

        flow = new LocalFlowConnector(commonProperties).connect(flowDef);

        state = State.READY;
    }

    private Tap<Properties, ?, ?> createTrap(URI errorPath, String prefix, SinkFactory sinkFactory) throws IOException {
        Sink errorSink = Sink.builder()
                .withOutput(errorPath)
                .withFilename(Filename.builder()
                        .withPrefix(prefix)
                        .build())
                .withSchema(Schema.builder()
                        .withFormat(Format.csv)
                        .withCompression(Compression.gzip)
                        .withDeclared(Field.asField("ALL"))
                        .build())
                .build();

        return sinkFactory.getSink(pipelineOptions, errorSink, Fields.ALL);
    }

    private static void logCurrentFields(Fields currentFields) {
        LOG.info("current fields: {}", currentFields);
    }

    public Integer run() throws IOException {
        if (state == State.NONE) {
            build();
        }

        if (state != State.READY) {
            throw new IllegalStateException("pipeline is not ready to run");
        }

        if (pipelineOptions().printOptions().printOutputSchema()) {
            Tap<?, ?, ?> tap = flow.getSink();
            PrintOptions.PrintFormat printFormat = pipelineOptions().printOptions().printFormat();
            SchemaPrinter schemaPrinter = new SchemaPrinter(tap, printFormat);

            schemaPrinter.print(System.out);
            return 0;
        }

        running.set(true);

        try {
            try {
                flow.complete();
            } catch (CascadingException e) {
                return handleCascadingException(e);
            }
        } finally {
            running.set(false);
        }

        state = State.COMPLETE;

        return 0;
    }

    private Integer handleCascadingException(CascadingException cascadingException) {
        Throwable cause = cascadingException.getCause();

        if (cause instanceof DuctException) {
            LOG.error("flow failed with: {}: {}", cause.getMessage(), cause.getCause().getMessage(), cascadingException);
            System.err.println("flow failed with: " + cause.getMessage() + ": " + cause.getCause().getMessage());
            return -1;
        }

        if (cause instanceof CascadingException) {
            LOG.error("flow failed with: {}: {}", cause.getMessage(), cause.getCause().getMessage(), cascadingException);
            System.err.println("flow failed with: " + cause.getMessage() + ": " + cause.getCause().getMessage());
            return -1;
        }

        LOG.error("flow failed with: {}", cascadingException.getMessage(), cascadingException);
        System.err.println("flow failed with: " + cascadingException.getMessage());

        return -1;
    }
}
