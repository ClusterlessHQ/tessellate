/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs;

import cascading.nested.json.hadoop3.JSONTextLine;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.TapFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.json.JSONUtil;

import java.util.Set;

public class JSONFSFactory extends LinesFSFactory {
    public static TapFactory INSTANCE = new JSONFSFactory();

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.json);
    }

    @Override
    protected Scheme createScheme(Dataset dataset, Fields declaredFields) {
        if (dataset.schema().compression() == Compression.none) {
            return new JSONTextLine(JSONUtil.DATA_MAPPER, declaredFields);
        } else {
            return new JSONTextLine(JSONUtil.DATA_MAPPER, declaredFields, TextLine.Compress.ENABLE);
        }
    }
}
