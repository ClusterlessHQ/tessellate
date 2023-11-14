/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import cascading.flow.local.LocalFlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import com.fasterxml.jackson.databind.JsonNode;
import io.clusterless.tessellate.model.Field;
import io.clusterless.tessellate.model.Schema;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.URIs;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class ManifestReader {
    private static final Logger LOG = LoggerFactory.getLogger(ManifestReader.class);
    public static final int SHOW_DUPLICATES = 20;

    public static ManifestReader from(Source source) {
        return new ManifestReader(source);
    }

    private final URI manifestURI;
    private final List<URI> uris;
    private List<URI> manifestUris;

    public ManifestReader(Source source) {
        this.manifestURI = source.manifest();
        this.uris = clean(source.uris());
    }

    public boolean isEmptyManifest() {
        return manifestURI == null ? uris.isEmpty() : manifestURI.toString().contains("state=empty");
    }

    public List<URI> uris(PipelineOptions pipelineOptions) throws IOException {
        if (manifestURI == null) {
            return uris;
        }

        if (manifestUris != null) {
            return manifestUris;
        }

        JsonNode node = null;

        try (TupleEntryIterator entryIterator = openForRead(pipelineOptions, manifestURI)) {
            while (entryIterator.hasNext()) {
                TupleEntry next = entryIterator.next();
                node = (JsonNode) next.getObject(0);
            }
        }

        if (node == null) {
            throw new IllegalStateException("manifest: " + manifestURI + ", is empty");
        }

        List<URI> found = JSONUtil.toList(node.get("uris"));

        LOG.info("found uris: {}, in manifest: {}", found.size(), manifestURI);

//        if (found.isEmpty()) {
//            throw new IllegalStateException("manifest: " + manifestURI + ", is empty");
//        }

        manifestUris = clean(found);

        return manifestUris;
    }

    protected List<URI> clean(List<URI> uris) {
        List<URI> distinct = uris.stream()
                .map(URIs::copyWithoutQuery)
                .map(URIs::makeAbsolute)
                .distinct()
                .collect(Collectors.toList());

        long duplicates = uris.size() - distinct.size();

        if (duplicates <= 0) {
            return uris;
        }

        Set<URI> items = new HashSet<>();

        uris.stream()
                .filter(n -> !items.add(n))
                .collect(Collectors.toSet());

        LOG.warn("removing duplicates: {}, showing: {}", duplicates, Math.min(duplicates, SHOW_DUPLICATES));

        items.stream().limit(SHOW_DUPLICATES).forEach(u -> LOG.warn("duplicate: {}", u));

        uris = distinct;

        return uris;
    }

    private TupleEntryIterator openForRead(PipelineOptions pipelineOptions, URI uri) throws IOException {
        Source source = Source.builder()
                .withSchema(Schema.builder()
                        .withFormat(Format.json)
                        .withDeclared(List.of(new Field("json|json")))
                        .build())
                .withInputs(List.of(uri))
                .build();

        SourceFactory sourceFactory = TapFactories.findSourceFactory(pipelineOptions, source);

        Tap<Properties, ?, ?> sourceTap = sourceFactory.getSource(pipelineOptions, source);

        return sourceTap.openForRead(new LocalFlowProcess(new Properties()));
    }
}
