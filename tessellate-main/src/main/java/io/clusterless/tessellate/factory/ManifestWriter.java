/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import cascading.flow.local.LocalFlowProcess;
import cascading.nested.json.hadoop3.JSONTextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.hadoop.LocalHfsAdaptor;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.github.hal4j.uritemplate.URITemplate;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.util.URIs;
import io.clusterless.tessellate.util.json.JSONUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * s3://test-clusterless-manifest-086903124729-us-west-2/datasets/name=ingress-python-example-copy/version=20230101/lot={lot}/state={state}{/attempt*}/manifest.json
 */
public class ManifestWriter {
    public static ManifestWriter NULL = new ManifestWriter() {
        @Override
        public void writeManifest(Properties conf) {
            // do nothing
        }
    };

    public static ManifestWriter from(Dataset dataset, URI uriPrefix) {
        if (!(dataset instanceof Sink)) {
            return NULL;
        }
        Sink sink = (Sink) dataset;

        if (!sink.hasManifest()) {
            return NULL;
        }

        return new ManifestWriter(sink, uriPrefix);
    }

    private final URITemplate template;
    private final String lot;
    private final URI uriPrefix;

    private ManifestWriter() {
        this.template = null;
        this.lot = null;
        this.uriPrefix = null;
    }

    public ManifestWriter(Sink dataset, URI uriPrefix) {
        String manifestTemplate = dataset.manifestTemplate();
        this.template = new URITemplate(URLDecoder.decode(manifestTemplate, StandardCharsets.UTF_8));
        this.lot = dataset.manifestLot();
        this.uriPrefix = URIs.makeAbsolute(uriPrefix);

        if (this.lot == null) {
            throw new IllegalArgumentException("lot is required when manifest is set");
        }
    }

    public void writeManifest(Properties conf) throws IOException {
        Set<URI> writes = Observed.INSTANCE.writes(uriPrefix);

        if (writes.isEmpty()) {
            writeManifest(conf, "empty", writes);
        } else {
            writeManifest(conf, "complete", writes);
        }
    }

    private void writeManifest(Properties conf, String state, Set<URI> writes) throws IOException {
        URI complete = template
                .expand("lot", lot)
                .expand("state", state)
                .discard("attempt")
                .toURI();

        Map<String, Object> manifest = new LinkedHashMap<>();

        manifest.put("state", state);
        manifest.put("comment", null);
        manifest.put("lotId", lot);
        manifest.put("uriType", "identifier");
        manifest.put("uris", writes);

        Properties properties = new Properties(conf);

        properties.put("cascading.tapcollector.partname", "manifest-data.json");

        try (TupleEntryCollector tupleEntryCollector = openForWrite(properties, complete)) {
            tupleEntryCollector.add(new Tuple(JSONUtil.valueToTree(manifest)));
        }
    }

    private static TupleEntryCollector openForWrite(Properties conf, URI complete) throws IOException {
        return new LocalHfsAdaptor(new Hfs(new JSONTextLine(), complete.toString(), SinkMode.UPDATE)).openForWrite(new LocalFlowProcess(conf));
    }
}
