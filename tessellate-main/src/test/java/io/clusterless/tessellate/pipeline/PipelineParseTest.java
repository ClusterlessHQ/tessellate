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
import io.clusterless.tessellate.model.Partition;
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.model.Transform;
import io.clusterless.tessellate.parser.ast.Op;
import io.clusterless.tessellate.temporal.IntervalDateTimeFormatter;
import io.clusterless.tessellate.type.WrappedCoercibleType;
import io.clusterless.tessellate.util.JSONUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

        assertEquals(6, transform.statements().size());
        assertEquals(new Op(), transform.statements().get(0).op());
        assertEquals(new Op("->"), transform.statements().get(1).op());
        assertEquals(new Op("+>"), transform.statements().get(2).op());
        assertEquals(new Op("->"), transform.statements().get(3).op());
        assertEquals(new Op("=>"), transform.statements().get(4).op());
        assertEquals(new Op("=>"), transform.statements().get(5).op());
    }
}
