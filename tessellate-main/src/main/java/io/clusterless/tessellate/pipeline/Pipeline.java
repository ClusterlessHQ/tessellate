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
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.pipe.assembly.Copy;
import cascading.pipe.assembly.Discard;
import cascading.tap.Tap;
import cascading.tap.TrapProps;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.*;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.options.PrintOptions;
import io.clusterless.tessellate.parser.ast.Join;
import io.clusterless.tessellate.parser.ast.Rel;
import io.clusterless.tessellate.parser.ast.Statement;
import io.clusterless.tessellate.printer.SchemaPrinter;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Models;
import io.clusterless.tessellate.util.URIs;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
        Source primarySource = findPrimarySource();

        // if source is a manifest, and the manifest points to empty data, we drop an empty manifest at the sink
        SourceFactory primarySourceFactory = findPrimarySourceFactory(primarySource);

        if (primarySourceFactory == null) return;

        primarySourceFactory.applyGlobalProperties(commonProperties);

        Tap<Properties, ?, ?> primarySourceTap = primarySourceFactory.getSource(pipelineOptions, primarySource);

        if (primarySource.schema().embedsSchema() || primarySource.schema().format().alwaysEmbedsSchema()) {
            primarySourceTap.retrieveSourceFields(flowProcess());
        }

        // get source fields here so that any partition fields will be captured
        Fields sourceFields = primarySourceTap.getSourceFields();

        if (!sourceFields.hasTypes()) {
            sourceFields = sourceFields.applyTypeToAll(String.class);
        }

        PipelineContext context = new PipelineContext(LOG, sourceFields, new Pipe(HEAD));

        logCurrentFields(context.currentFields);

        Schema sourceSchema = primarySource.schema();
        // this is mirrored in the Tap Factories where the Schema is instantiated
        if ((sourceSchema.format() == Format.text || sourceSchema.format() == Format.regex) && sourceSchema.embedsSchema()) {
            // this is a hack to skip the first line
            LOG.info("sourcing format: {}, embedSchema is true, skipping first line, but not using the schema", sourceSchema.format());
            Fields num = new Fields("num");
            Pipe pipe = new Each(context.pipe, num, new ExpressionFilter("num == 0"));
            pipe = new Discard(pipe, num);
            Fields currentFields = context.currentFields.subtract(num);
            logCurrentFields(currentFields);
            context.update(currentFields, pipe);
        }

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

        if (primarySource.errorPath() != null) {
            Tap<Properties, ?, ?> errorTap = createTrap(primarySource.errorPath(), "errors-source", sinkFactory);
            traps.put(HEAD, errorTap);
            LOG.info("trapping input errors at: {}", errorTap.getIdentifier());
        }

        if (pipelineDef.sink().errorPath() != null) {
            Tap<Properties, ?, ?> errorTap = createTrap(pipelineDef.sink().errorPath(), "errors-sink", sinkFactory);
            traps.put(TAIL, errorTap);
            LOG.info("trapping output errors at: {}", errorTap.getIdentifier());
        }

        if (!traps.isEmpty()) {
            commonProperties = TrapProps.trapProps()
                    .setRecordThrowableMessage(true)
                    .setRecordElementTrace(true)
                    .setRecordThrowableStackTrace(true)
                    .buildProperties(commonProperties);
        }

        Map<String, Tap> joinSources = createJoinSources(sinkFactory, traps);

        FlowDef flowDef = flowDef()
                .setName("pipeline")
                .addSource(HEAD, primarySourceTap)
                .addSources(joinSources)
                .addSink(TAIL, sinkTap)
                .addTail(context.pipe)
                .addTraps(traps);

        flow = new LocalFlowConnector(commonProperties).connect(flowDef);

        state = State.READY;
    }

    private Map<String, Tap> createJoinSources(SinkFactory sinkFactory, Map<String, Tap> traps) throws IOException {
        Map<String, Tap> results = new HashMap<>();

        Map<String, Source> secondarySources = findSecondarySources();

        for (Map.Entry<String, Source> entry : secondarySources.entrySet()) {
            String name = entry.getKey();
            Source source = entry.getValue();
            SourceFactory sourceFactory = TapFactories.findSourceFactory(pipelineOptions, source);
            sourceFactory.applyGlobalProperties(commonProperties);
            results.put(name, sourceFactory.getSource(pipelineOptions, source));

            if (source.errorPath() != null) {
                Tap<Properties, ?, ?> errorTap = createTrap(source.errorPath(), "errors-source-" + name, sinkFactory);
                traps.put(HEAD, errorTap);
                LOG.info("trapping input errors for: {}, at: {}", name, errorTap.getIdentifier());
            }
        }

        return results;
    }

    private @Nullable SourceFactory findPrimarySourceFactory(Source primarySource) throws IOException {
        SourceFactory primarySourceFactory;
        try {
            primarySourceFactory = TapFactories.findSourceFactory(pipelineOptions, primarySource);
        } catch (ManifestEmptyException e) {
            SinkFactory sinkFactory = TapFactories.findSinkFactory(pipelineDef.sink());

            sinkFactory.applyGlobalProperties(commonProperties);

            ManifestWriter manifestWriter = ManifestWriter.from(pipelineDef.sink(), null);

            manifestWriter.writeManifest(commonProperties);

            state = State.EMPTY_MANIFEST;

            return null;
        }

        return primarySourceFactory;
    }

    private Source findPrimarySource() {
        Source source = pipelineDef.source();

        if (source != null) {
            return source;
        }

        List<Join> joins = pipelineDef.transform().statements(Join.class);

        if (joins.isEmpty()) {
            throw new IllegalStateException("no source defined");
        }

        List<String> names = joins.stream()
                .map(Join::rhsRelations)
                .flatMap(List::stream)
                .map(Rel::name)
                .distinct()
                .collect(Collectors.toList());

        if (names.size() != 1) {
            throw new IllegalStateException("multiple rhs sources defined, may only be one primary source, got: " + names);
        }

        return pipelineDef.sources().get(names.get(0));
    }

    private Map<String, Source> findSecondarySources() {
        List<Join> joins = pipelineDef.transform().statements(Join.class);

        List<String> names = joins.stream()
                .map(Join::lhsRelations)
                .flatMap(List::stream)
                .map(Rel::name)
                .distinct()
                .collect(Collectors.toList());

        return pipelineDef.sources().entrySet().stream()
                .filter(entry -> names.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Tap<Properties, ?, ?> createTrap(URI errorPath, String prefix, SinkFactory sinkFactory) throws IOException {
        Sink errorSink = Sink.builder()
                // for FileTap the final / gets removed
                .withOutput(URIs.copyAsDirectory(URIs.cleanFileUrls(errorPath)))
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
