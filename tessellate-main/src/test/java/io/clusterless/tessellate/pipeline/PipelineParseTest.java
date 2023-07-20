/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.DateType;
import cascading.tuple.type.InstantType;
import com.adelean.inject.resources.junit.jupiter.GivenTextResource;
import com.adelean.inject.resources.junit.jupiter.TestWithResources;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.temporal.IntervalDateTimeFormatter;
import io.clusterless.tessellate.type.WrappedCoercibleType;
import io.clusterless.tessellate.util.JSONUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@TestWithResources
public class PipelineParseTest {
    @Test
    void name(@GivenTextResource("config/pipeline.json") String pipelineJson) throws JsonProcessingException {
        PipelineDef pipeline = JSONUtil.CONFIG_MAPPER.readValue(pipelineJson, PipelineDef.class);

        assertEquals(new Fields("one", Integer.TYPE), pipeline.source().schema().declared().get(0).fields());
        assertEquals(new Fields("two", Integer.TYPE), pipeline.source().schema().declared().get(1).fields());
        assertEquals(new Fields("three", new WrappedCoercibleType(Coercions.INTEGER, "-")), pipeline.source().schema().declared().get(2).fields());
        assertEquals(IntervalDateTimeFormatter.TWELFTH_FORMATTER.toString(), ((InstantType) pipeline.source().schema().declared().get(3).fields().getType(0)).getDateTimeFormatter().toString());

        List<Partition> partitions = pipeline.source().partitions();

        assertEquals(3, partitions.size());
        assertEquals(new Fields("one"), partitions.get(0).to().fields());
        assertEquals(new Fields("two"), partitions.get(1).from().orElseThrow().fields());
        assertEquals(new Fields("@two"), partitions.get(1).to().fields());
        assertEquals(new Fields("three"), partitions.get(2).from().orElseThrow().fields());
        assertEquals(new Fields("@three", new DateType("yyyyMMdd", TimeZone.getTimeZone("UTC"))), partitions.get(2).to().fields());

        Transform transform = pipeline.transform();

        assertEquals(6, transform.transformOps().size());
        assertInstanceOf(CoerceOp.class, transform.transformOps().get(0));
        assertInstanceOf(RenameOp.class, transform.transformOps().get(1));
        assertInstanceOf(CopyOp.class, transform.transformOps().get(2));
        assertInstanceOf(DiscardOp.class, transform.transformOps().get(3));
        assertInstanceOf(InsertOp.class, transform.transformOps().get(4));
        assertInstanceOf(EvalInsertOp.class, transform.transformOps().get(5));
    }
}
