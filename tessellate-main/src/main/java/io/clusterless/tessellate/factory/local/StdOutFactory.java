/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.local;

import cascading.nested.json.local.JSONTextLine;
import cascading.scheme.Scheme;
import cascading.scheme.local.CompressorScheme;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.Tap;
import cascading.tap.local.StdOutTap;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.SinkFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Models;
import io.clusterless.tessellate.util.Protocol;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Set;

public class StdOutFactory implements SinkFactory {
    @Override
    public Set<Protocol> getSinkProtocols() {
        return Set.of(Protocol.stdout);
    }

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.tsv, Format.csv, Format.json);
    }

    @NotNull
    protected Scheme<Properties, InputStream, OutputStream, ?, ?> createScheme(Dataset dataset, Fields declaredFields) {
        CompressorScheme.Compressor compressor = null;

        Scheme<Properties, InputStream, OutputStream, ?, ?> scheme;

        Schema schema = dataset.schema();
        Format format = schema.format() == null ? Format.csv : schema.format();

        switch (format) {
            default:
            case text:
                Fields sinkFields = new Fields("line");
                Fields sourceFields = dataset.schema().embedsSchema() ? new Fields("num", "line").applyTypes(Long.TYPE, String.class) : sinkFields;
                scheme = new TextLine(sourceFields, sinkFields, compressor);
                break;
            case delimited:
                DelimitedParser delimited = new DelimitedParser(schema.delimiterChar(), schema.quoteChar(), null, schema.strictParsing(), true);
                return new TextDelimited(declaredFields, compressor, schema.embedsSchema(), schema.embedsSchema(), delimited);
            case csv:
                DelimitedParser csv = new DelimitedParser(",", schema.quoteChar(), null, schema.strictParsing(), true);
                scheme = new TextDelimited(declaredFields, compressor, schema.embedsSchema(), csv);
                break;
            case tsv:
                DelimitedParser tsv = new DelimitedParser("\t", schema.quoteChar(), null, schema.strictParsing(), true);
                scheme = new TextDelimited(declaredFields, compressor, schema.embedsSchema(), tsv);
                break;
            case json:
                scheme = new JSONTextLine(JSONUtil.DATA_MAPPER, declaredFields, compressor) {
                    @Override
                    public String getExtension() {
                        return format.extension();
                    }
                };
                break;
        }

        return scheme;
    }

    @Override
    public Tap<Properties, ?, ?> getSink(PipelineOptions pipelineOptions, Sink sinkModel, Fields currentFields) throws IOException {
        Fields declaredFields = Models.fieldAsFields(sinkModel.schema().declared(), String.class, Fields.ALL);

        Fields resultFields = declaredFields.isAll() ? currentFields : declaredFields;

        return new StdOutTap(createScheme(sinkModel, resultFields));
    }

    @Override
    public Set<Compression> getCompressions() {
        return Set.of(Compression.none);
    }
}
