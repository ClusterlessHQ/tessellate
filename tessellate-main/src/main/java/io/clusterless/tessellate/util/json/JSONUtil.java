/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util.json;

import cascading.nested.json.JSONCoercibleType;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

public class JSONUtil {
    public static final ObjectMapper DATA_MAPPER;
    public static final ObjectMapper CONFIG_MAPPER;
    public static final JSONCoercibleType TYPE;

    public static final ObjectReader CONFIG_READER;

    static {
        CONFIG_MAPPER = createObjectMapper();

        // prevents json object from being created with duplicate names at the same level
        CONFIG_MAPPER.setConfig(CONFIG_MAPPER.getDeserializationConfig()
                .with(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY));

        CONFIG_MAPPER.setConfig(CONFIG_MAPPER.getSerializationConfig()
                .withoutFeatures(
                        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                        SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS,
                        SerializationFeature.FAIL_ON_EMPTY_BEANS
                )
        );

        DATA_MAPPER = new ObjectMapper();

        DATA_MAPPER.registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module());

        // prevents json object from being created with duplicate names at the same level
        DATA_MAPPER.setConfig(DATA_MAPPER.getDeserializationConfig()
                .with(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY));

        DATA_MAPPER.setConfig(DATA_MAPPER.getSerializationConfig()
                .withFeatures(
                        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                        SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS
                )
        );

        CONFIG_READER = CONFIG_MAPPER
                .enable(JsonParser.Feature.ALLOW_COMMENTS)
                .enable(JsonParser.Feature.ALLOW_YAML_COMMENTS)
                .reader();

        TYPE = new JSONCoercibleType(DATA_MAPPER);
    }

    private static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module());
    }

    public static final ObjectWriter CONFIG_WRITER = CONFIG_MAPPER.writer();

    public static final ObjectWriter CONFIG_WRITER_PRETTY = CONFIG_MAPPER.writerWithDefaultPrettyPrinter();

    public static String writeAsStringSafe(Object object) {
        try {
            return CONFIG_WRITER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String writeRWAsPrettyStringSafe(Object object) {
        return writeViewAsPrettyStringSafe(object, Views.Simple.class);
    }

    public static String writePartitionedAsPrettyStringSafe(Object object) {
        return writeViewAsPrettyStringSafe(object, Views.Partitioned.class);
    }

    private static String writeViewAsPrettyStringSafe(Object object, Class<?> view) {
        try {
            return createObjectMapper()
                    .disable(MapperFeature.DEFAULT_VIEW_INCLUSION)
                    .writerWithDefaultPrettyPrinter()
                    .withView(view).writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String writeAsStringSafePretty(Object object) {
        try {
            return CONFIG_WRITER_PRETTY
                    .writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static JsonNode readTree(InputStream inputStream) throws IOException {
        return CONFIG_READER.readTree(inputStream);
    }

    public static JsonNode readTree(String json) throws IOException {
        return CONFIG_READER.readTree(json);
    }

    public static <J extends JsonNode> J readTreeSafe(File file) {
        try {
            return readTree(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <J extends JsonNode> J readTree(File file) throws IOException {
        if (!file.exists()) {
            throw new FileNotFoundException("does not exist: " + file);
        }

        return (J) CONFIG_READER.readTree(new FileInputStream(file));
    }

    public static <T> T readObjectSafe(byte[] bytes, Class<T> type) {
        try {
            return CONFIG_READER.readValue(bytes, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T readObjectSafe(String json, Class<T> type) {
        try {
            return CONFIG_READER.readValue(json, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T readObjectSafe(Path path, Class<T> type) {
        try {
            return CONFIG_READER.readValue(path.toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T treeToValueSafe(JsonNode n, Class<T> type) {
        try {
            return CONFIG_READER.treeToValue(n, type);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static JsonNode valueToTree(Object value) {
        try {
            return CONFIG_READER.readTree(CONFIG_WRITER.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static JsonNode stringToTree(String value) {
        try {
            return CONFIG_READER.readTree(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T stringToValue(String value, Class<T> type) {
        try {
            return CONFIG_READER.readValue(value, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T treeToValue(JsonNode n, Class<T> type) throws JsonProcessingException {
        return CONFIG_READER.treeToValue(n, type);
    }

    public static List<URI> toList(JsonNode node) {
        try {
            return JSONUtil.CONFIG_READER.treeToValue(node, TypeFactory.defaultInstance().constructParametricType(LinkedList.class, URI.class));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
