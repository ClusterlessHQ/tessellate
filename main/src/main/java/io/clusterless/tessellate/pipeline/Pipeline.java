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
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import io.clusterless.tessellate.factory.SinkFactory;
import io.clusterless.tessellate.factory.SourceFactory;
import io.clusterless.tessellate.factory.TapFactories;
import io.clusterless.tessellate.model.PipelineDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.FlowDef.flowDef;

public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private final PipelineOptions pipelineOptions;
    private final PipelineDef pipelineDef;
    private Flow flow;

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

    public void build() {
        SourceFactory sourceFactory = TapFactories.findSourceFactory(pipelineDef.source());
        SinkFactory sinkFactory = TapFactories.findSinkFactory(pipelineDef.sink());

        Tap sourceTap = sourceFactory.getSource(pipelineDef.source());

        Pipe pipe = new Pipe("head");

        // watch the progress on the console
        pipe = new Each(pipe, new Debug(true));

        Tap sinkTap = sinkFactory.getSink(pipelineDef.sink());

        flow = new LocalFlowConnector().connect(flowDef()
                .setName("pipeline")
                .addSource(pipe, sourceTap)
                .addSink(pipe, sinkTap)
                .addTail(pipe));
    }

    public Integer run() {

        if (flow == null) {
            build();
        }

        flow.complete();

        return 0;
    }
}
