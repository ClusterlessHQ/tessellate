/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.options;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import heretical.pointer.operation.BuildSpec;
import heretical.pointer.operation.json.JSONBuilder;
import heretical.pointer.path.NestedPointer;
import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.util.JSONUtil;
import io.clusterless.tessellate.util.LiteralResolver;
import org.jetbrains.annotations.NotNull;
import org.mvel2.templates.TemplateRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static heretical.pointer.path.json.JSONNestedPointerCompiler.COMPILER;

/**
 * The PipelineOptionsMerge class is used to merge the command line sourced {@link PipelineOptions} into a
 * {@link PipelineDef}.
 */
public class PipelineOptionsMerge {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineOptionsMerge.class);

    // map cli option to pipeline model path
    private static BuildSpec buildSpec = new BuildSpec()
            .putInto("inputs", "/source/inputs")
            .putInto("inputManifest", "/source/manifest")
            .putInto("inputManifestLot", "/source/manifestLot")
            .putInto("output", "/sink/output")
            .putInto("outputManifest", "/sink/manifest")
            .putInto("outputManifestLot", "/sink/manifestLot");
    private static JSONBuilder builder = new JSONBuilder(buildSpec);

    // all uris that should be resolved relative to the pipeline file path
    private static List<NestedPointer<JsonNode, ArrayNode>> uris = List.of(
            COMPILER.nested("/source/inputs/*"),
            COMPILER.nested("/source/manifest"),
            COMPILER.nested("/sink/output"),
            COMPILER.nested("/sink/manifest")
    );

    private static final Map<Comparable, Function<PipelineOptions, JsonNode>> argumentLookups = new HashMap<>();

    static {
        argumentLookups.put("inputs", pipelineOptions -> nullOrNode(pipelineOptions.inputOptions().inputs()));
        argumentLookups.put("inputManifest", pipelineOptions -> nullOrNode(pipelineOptions.inputOptions().inputManifest()));
        argumentLookups.put("inputManifestLot", pipelineOptions -> nullOrNode(pipelineOptions.inputOptions().inputLot()));
        argumentLookups.put("output", pipelineOptions -> nullOrNode(pipelineOptions.outputOptions().output()));
        argumentLookups.put("outputManifest", pipelineOptions -> nullOrNode(pipelineOptions.outputOptions().outputManifest()));
        argumentLookups.put("outputManifestLot", pipelineOptions -> nullOrNode(pipelineOptions.outputOptions().outputLot()));
    }

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

        JsonNode pipelineDef;

        if (path != null) {
            LOG.info("pipeline path: {}", path);
            pipelineDef = JSONUtil.readTreeSafe(path.toFile());
            // resolve uris to pipeline file directory
            for (NestedPointer<JsonNode, ArrayNode> pointer : uris) {
                pointer.apply(pipelineDef, n -> resolve(path, n));
            }
        } else {
            pipelineDef = JSONUtil.CONFIG_MAPPER.valueToTree(new PipelineDef());
        }

        return merge(pipelineDef);
    }

    private JsonNode resolve(Path path, JsonNode jsonNode) {
        if (jsonNode.isNull()) {
            return jsonNode;
        }

        URI uri = JSONUtil.treeToValueSafe(jsonNode, URI.class);

        if (!(uri.getScheme() == null || uri.getScheme().equals("file"))) {
            return jsonNode;
        }

        return JSONUtil.valueToTree(path.toUri().resolve(uri).normalize());
    }

    public PipelineDef merge(JsonNode pipelineDef) {
        // apply cli arguments
        builder.build((k, t) -> arguments().get(k), pipelineDef);

        // merge the stored schema with any provided values
        loadAndMerge(pipelineDef, "/source");
        loadAndMerge(pipelineDef, "/sink");

        String mergedPipelineDef = JSONUtil.writeAsStringSafe(pipelineDef);
        Map<String, Object> context = getContext(mergedPipelineDef);
        String resolved = TemplateRuntime.eval(mergedPipelineDef, context).toString();
        LOG.info("pipeline: {}", resolved);

        return JSONUtil.stringToValue(resolved, PipelineDef.class);
    }

    @NotNull
    private static Map<String, Object> getContext(String mergedPipelineDef) {
        Map<String, Object> context = LiteralResolver.context();
        Map map = JSONUtil.stringToValue(mergedPipelineDef, Map.class);

        context.put("source", map.get("source"));
        context.put("sink", map.get("sink"));
        return context;
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
