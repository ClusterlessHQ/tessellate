/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import heretical.pointer.operation.BuildSpec;
import heretical.pointer.operation.json.JSONBuilder;
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.util.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The PipelineOptionsMerge class is used to merge the command line sourced {@link PipelineOptions} into a
 * {@link PipelineDef}.
 */
public class PipelineOptionsMerge {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineOptionsMerge.class);
    static BuildSpec spec = new BuildSpec()
            .putInto("inputs", "/source/inputs")
            .putInto("inputManifest", "/source/manifest")
            .putInto("output", "/sink/output")
            .putInto("outputManifest", "/sink/manifest");

    private static final Map<Comparable, Function<PipelineOptions, JsonNode>> argumentLookups = new HashMap<>();

    static {
        argumentLookups.put("inputs", pipelineOptions -> nullOrNode(pipelineOptions.inputOptions().inputs()));
        argumentLookups.put("output", pipelineOptions -> nullOrNode(pipelineOptions.outputOptions().output()));
    }

    private static JSONBuilder builder = new JSONBuilder(spec);

    PipelineOptions pipelineOptions;
    private Map<Comparable, Object> arguments;

    public PipelineOptionsMerge(PipelineOptions pipelineOptions) {
        this.pipelineOptions = pipelineOptions;
    }

    public Map<Comparable, Object> arguments() {
        if (arguments != null) {
            return arguments;
        }

        arguments = argumentLookups.entrySet()
                .stream()
                .map(e -> {
                    JsonNode apply = e.getValue().apply(pipelineOptions);
                    return apply == null ? null : Map.entry(e.getKey(), apply);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return arguments;
    }

    public PipelineDef merge() {
        Path path = pipelineOptions.pipelinePath();

        JsonNode jsonNode;

        if (path != null) {
            LOG.info("pipeline path: {}", path);
            jsonNode = JSONUtil.readTreeSafe(path.toFile());
        } else {
            jsonNode = JSONUtil.CONFIG_MAPPER.valueToTree(new PipelineDef());
        }

        return merge(jsonNode);
    }

    protected PipelineDef merge(JsonNode jsonNode) {
        // apply cli arguments
        builder.build((k, t) -> arguments().get(k), jsonNode);

        // merge the stored schema with any provided values
        loadAndMerge(jsonNode, "/source");
        loadAndMerge(jsonNode, "/sink");

        LOG.info("pipeline: {}", JSONUtil.writeAsStringSafe(jsonNode));

        return JSONUtil.treeToValueSafe(jsonNode, PipelineDef.class);
    }

    private void loadAndMerge(JsonNode jsonNode, String target) {
        JsonNode schemaName = jsonNode.at(target + "/schema/name");

        if (schemaName.isMissingNode() || schemaName.isNull()) {
            return;
        }

        ObjectNode schema = (ObjectNode) jsonNode.at(target + "/schema");

        String resourceName = "schemas/" + schemaName.textValue() + ".json";

        LOG.info("loading schema: {}", resourceName);

        ClassLoader classLoader = this.getClass().getClassLoader();
        try (InputStream resourceAsStream = classLoader.getResourceAsStream(resourceName)) {

            if (resourceAsStream == null) {
                throw new IllegalStateException("could not load: " + resourceName);
            }

            JSONUtil.CONFIG_MAPPER.readerForUpdating(schema)
                    .readTree(resourceAsStream);
        } catch (IOException e) {
            throw new IllegalStateException("failed loading schema: " + schemaName.textValue(), e);
        }
    }

    private static JsonNode nullOrNode(Object output) {
        if (output == null) {
            return null;
        }
        return JSONUtil.valueToTree(output);
    }

    private static JsonNode nullOrNode(Collection<?> objects) {
        if (objects == null || objects.isEmpty()) {
            return null;
        }

        return JSONUtil.valueToTree(objects);
    }
}
