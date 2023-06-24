/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.TapFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;

import java.util.Set;

public class TextFSFactory extends LinesFSFactory {
    public static TapFactory INSTANCE = new TextFSFactory();

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.text, Format.csv, Format.tsv);
    }

    @Override
    protected Scheme createScheme(PipelineOptions pipelineOptions, Dataset dataset, Fields declaredFields) {
        Schema schema = dataset.schema();
        TextLine.Compress compress = schema.compression() == Compression.none ? TextLine.Compress.DISABLE : TextLine.Compress.ENABLE;

        switch (schema.format()) {
            default:
            case text:
                return new TextLine(new Fields("line"), new Fields("line"), compress);
            case csv:
                return new TextDelimited(declaredFields, compress, schema.embedsSchema(), ",", "\"");
            case tsv:
                return new TextDelimited(declaredFields, compress, schema.embedsSchema(), "\t", "\"");
        }
    }
}
