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
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import com.fasterxml.jackson.databind.JsonNode;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.URIs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class ManifestReader {
    private static final Logger LOG = LoggerFactory.getLogger(ManifestReader.class);
    public static final int SHOW_DUPLICATES = 20;

    public static ManifestReader from(Dataset dataset) {
        if (!(dataset instanceof Source)) {
            return new ManifestReader(dataset.uris());
        }

        return new ManifestReader(((Source) dataset));
    }

    private final URI manifestURI;
    private final List<URI> uris;
    private final int numPartitions;
    private List<URI> manifestUris;

    public ManifestReader(Source source) {
        this.manifestURI = source.manifest();
        this.uris = source.uris();
        this.numPartitions = source.partitions().size();
    }

    public ManifestReader(List<URI> uris) {
        this.uris = uris;
        this.manifestURI = null;
        this.numPartitions = 0;
    }

    public List<URI> uris(Properties conf) throws IOException {
        if (manifestURI == null) {
            return dedupe(uris);
        }

        if (manifestUris != null) {
            return manifestUris;
        }

        JsonNode node = null;

        try (TupleEntryIterator entryIterator = openForRead(conf, manifestURI)) {
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

        if (found.isEmpty()) {
            throw new IllegalStateException("manifest: " + manifestURI + ", is empty");
        }

        manifestUris = dedupe(found);

        return manifestUris;
    }

    public boolean urisFromManifest() {
        return manifestUris != null;
    }

    public URI findCommonRoot(Properties conf) throws IOException {
        List<URI> uris = uris(conf);

        // uri is likely a directory or single file, let the Hfs tap handle it
        if (!urisFromManifest() && uris.size() == 1) {
            return uris.get(0);
        }

        Set<String> roots = uris.stream()
                .map(u -> URIs.trim(u, numPartitions + 1))
                .map(Objects::toString)
                .collect(Collectors.toSet());

        String commonPrefix = StringUtils.getCommonPrefix(roots.toArray(new String[0]));

        if (commonPrefix.isEmpty()) {
            throw new IllegalArgumentException("to many unique roots, got: " + roots);
        }

        return URI.create(commonPrefix);
    }

    protected List<URI> dedupe(List<URI> uris) {
        List<URI> distinct = uris.stream()
                .map(URIs::copyWithoutQuery)
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

    private static TupleEntryIterator openForRead(Properties conf, URI uri) throws IOException {
        return new LocalHfsAdaptor(new Hfs(new JSONTextLine(), uri.toString(), SinkMode.KEEP)).openForRead(new LocalFlowProcess(conf));
    }
}
