/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.Insert;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.pipe.assembly.Copy;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import io.clusterless.tessellate.factory.SinkFactory;
import io.clusterless.tessellate.factory.SourceFactory;
import io.clusterless.tessellate.factory.TapFactories;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static cascading.flow.FlowDef.flowDef;

public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private final PipelineOptions pipelineOptions;
    private final PipelineDef pipelineDef;
    private Flow flow;
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
            localFlowProcess = new LocalFlowProcess();
        }
        return localFlowProcess;
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
        SourceFactory sourceFactory = TapFactories.findSourceFactory(pipelineDef.source());
        Tap<Properties, ?, ?> sourceTap = sourceFactory.getSource(pipelineOptions, pipelineDef.source());

        if (pipelineDef.source().schema().embedsSchema() || pipelineDef.source().schema().format().alwaysEmbedsSchema()) {
            sourceTap.retrieveSourceFields(flowProcess());
        }

        // get source fields here so that any partition fields will be captured
        Fields currentFields = sourceTap.getSourceFields();

        Pipe pipe = new Pipe("head");

        Schema sourceSchema = pipelineDef.source().schema();
        if (sourceSchema.format() == Format.regex) {
            Fields declaredFields = Models.fieldAsFields(sourceSchema.declared(), String.class, Fields.ALL);
            pipe = new Each(pipe, new Fields("line"), new RegexParser(declaredFields, sourceSchema.pattern()), Fields.RESULTS);
            LOG.info("parsing lines with regex: {}", sourceSchema.pattern());
            currentFields = declaredFields;
        }

        // todo: group like transforms together if there are no interdependencies
        for (TransformOp transformOp : pipelineDef.transform().transformOps()) {
            switch (transformOp.transform()) {
                case insert:
                    InsertOp insertOp = (InsertOp) transformOp;
                    Fields insertFields = insertOp.field().fields();
                    String value = insertOp.value() == null || insertOp.value().isEmpty() ? null : insertOp.value();
                    Object literal = Coercions.coerce(value, insertFields.getType(0));
                    LOG.info("transform insert: fields: {}, value: {}", insertFields, literal);
                    pipe = new Each(pipe, new Insert(insertFields, literal), Fields.ALL);
                    currentFields = currentFields.append(insertFields);
                    break;
                case coerce:
                    CoerceOp coerceOp = (CoerceOp) transformOp;
                    Fields coerceFields = coerceOp.field().fields();
                    LOG.info("transform coerce: fields: {}", coerceFields);
                    pipe = new Coerce(pipe, coerceFields);
                    currentFields = currentFields.rename(coerceFields, coerceFields); // change the type information
                    break;
                case copy:
                    CopyOp copyOp = (CopyOp) transformOp;
                    Fields copyFromFields = copyOp.from().orElseThrow().fields();
                    Fields copyToFields = copyOp.to().fields();
                    LOG.info("transform copy: from: {}, to: {}", copyFromFields, copyToFields);
                    pipe = new Copy(pipe, copyFromFields, copyToFields);
                    currentFields = currentFields.append(copyToFields);
                    break;
                case rename:
                    RenameOp renameOp = (RenameOp) transformOp;
                    Fields renameFromFields = renameOp.from().orElseThrow().fields();
                    Fields renameToFields = renameOp.to().fields();
                    LOG.info("transform rename: from: {}, to: {}", renameFromFields, renameToFields);
                    pipe = new Rename(pipe, renameFromFields, renameToFields);
                    currentFields = currentFields.rename(renameFromFields, renameToFields);
                    break;
                case discard:
                    DiscardOp discardOp = (DiscardOp) transformOp;
                    Fields discardFields = discardOp.field().fields();
                    LOG.info("transform discard: fields: {}", discardFields);
                    pipe = new Discard(pipe, discardFields);
                    currentFields = currentFields.subtract(discardFields);
                    break;
            }
        }

        Fields partitionFields = Fields.NONE;

        if (!pipelineDef.sink().partitions().isEmpty()) {
            // todo: honor the -> and +> operators when declaring partitions
            for (Partition partition : pipelineDef().sink().partitions()) {
                if (partition.from().isPresent()) {
                    pipe = new Copy(pipe, partition.from().get().fields(), partition.to().fields());
                    partitionFields = partitionFields.append(partition.to().fields());
                } else {
                    pipe = new Coerce(pipe, partition.to().fields());
                    // change the type information
                    partitionFields = partitionFields.rename(partition.to().fields(), partition.to().fields());
                }
            }
        }

        LOG.info("coercing into partitions fields: {}", partitionFields);

        // watch the progress on the console
        if (pipelineOptions().debug()) {
            pipe = new Each(pipe, new Debug(true));
        }

        LOG.info("sinking into fields: {}", currentFields);

        SinkFactory sinkFactory = TapFactories.findSinkFactory(pipelineDef.sink());

        Tap<Properties, ?, ?> sinkTap = sinkFactory.getSink(pipelineOptions, pipelineDef.sink(), currentFields);

        flow = new LocalFlowConnector().connect(flowDef()
                .setName("pipeline")
                .addSource(pipe, sourceTap)
                .addSink(pipe, sinkTap)
                .addTail(pipe));
    }

    public Integer run() throws IOException {

        if (flow == null) {
            build();
        }

        running.set(true);
        try {
            flow.complete();
        } finally {
            running.set(false);
        }

        return 0;
    }
}
