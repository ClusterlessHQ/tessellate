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
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.URIs;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * s3://test-clusterless-manifest-086903124729-us-west-2/datasets/name=ingress-python-example-copy/version=20230101/lot={lot}/state={state}{/attempt*}/manifest.json
 */
public class ManifestWriter {
    public static ManifestWriter NULL = new ManifestWriter() {
        @Override
        public void writeSuccess(Properties conf) {
            // do nothing
        }
    };

    public static ManifestWriter from(Dataset dataset, URI uriPrefix) {
        if (!(dataset instanceof Sink)) {
            return NULL;
        }
        Sink sink = (Sink) dataset;

        if (sink.manifest() == null) {
            return NULL;
        }

        return new ManifestWriter(sink, uriPrefix);
    }

    private final URI manifestURI;
    private final String lot;
    private final URI uriPrefix;

    private ManifestWriter() {
        this.manifestURI = null;
        this.lot = null;
        this.uriPrefix = null;
    }

    public ManifestWriter(Sink dataset, URI uriPrefix) {
        this(dataset.manifest(), dataset.manifestLot(), uriPrefix);
    }

    public ManifestWriter(URI manifestURI, String lot, URI uriPrefix) {
        this.manifestURI = manifestURI;
        this.lot = lot;
        this.uriPrefix = URIs.makeAbsolute(uriPrefix);

        if (this.lot == null) {
            throw new IllegalArgumentException("lot is required when manifest is set");
        }
    }

    public void writeSuccess(Properties conf) throws IOException {
        URITemplate template = new URITemplate(URLDecoder.decode(manifestURI.toString(), StandardCharsets.UTF_8));

        URI complete = template
                .expand("lot", lot)
                .expand("state", "complete")
                .discard("attempt")
                .toURI();

        Map<String, Object> manifest = new LinkedHashMap<>();

        manifest.put("state", "complete");
        manifest.put("comment", null);
        manifest.put("lotId", lot);
        manifest.put("uriType", "identifier");
        manifest.put("uris", Observed.INSTANCE.writes(uriPrefix));

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
