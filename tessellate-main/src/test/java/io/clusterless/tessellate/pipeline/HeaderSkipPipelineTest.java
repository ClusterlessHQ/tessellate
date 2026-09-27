/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.tuple.TupleEntryIterator;
import io.clusterless.tessellate.junit.PathForOutput;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.model.*;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Format;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A text or regex source that embeds its schema has its header, the line where num is 0, skipped in each file,
 * while every other line is kept, including a line whose content is "0".
 */
@ExtendWith(ResourceExtension.class)
public class HeaderSkipPipelineTest {

    private static Path write(Path dir, String name, String... lines) throws IOException {
        return Files.write(dir.resolve(name), List.of(lines));
    }

    private static Source source(URI input, Schema schema) {
        return Source.builder()
                .withInputs(List.of(input))
                .withSchema(schema)
                .build();
    }

    private static Sink sink(URI output, Format format) {
        return Sink.builder()
                .withOutput(output)
                .withSchema(Schema.builder()
                        .withFormat(format)
                        .withEmbedsSchema(false)
                        .build())
                .withFilename(Filename.builder()
                        .withPrefix("test")
                        .build())
                .build();
    }

    private static List<String> run(Source source, Sink sink) throws IOException {
        PipelineDef def = PipelineDef.builder()
                .withName("test")
                .withSource(source)
                .withSink(sink)
                .build();

        Pipeline pipeline = new Pipeline(new PipelineOptions(), def);

        assertEquals(0, pipeline.run());

        List<String> results = new ArrayList<>();

        try (TupleEntryIterator iterator = pipeline.flow().openSink()) {
            iterator.forEachRemaining(entry -> results.add(entry.getTuple().toString("|", false)));
        }

        return results;
    }

    private static Schema textSchema(boolean embedsSchema) {
        return Schema.builder()
                .withFormat(Format.text)
                .withEmbedsSchema(embedsSchema)
                .build();
    }

    @Test
    void textSkipsHeader(@TempDir Path input, @PathForOutput URI output) throws IOException {
        Path file = write(input, "data.txt", "header", "first", "0", "last");

        List<String> results = run(source(file.toUri(), textSchema(true)), sink(output, Format.text));

        assertThat(results).containsExactlyInAnyOrder("first", "0", "last");
    }

    @Test
    void textKeepsLinesWhoseNumContainsZero(@TempDir Path input, @PathForOutput URI output) throws IOException {
        List<String> lines = new ArrayList<>(List.of("header"));
        IntStream.rangeClosed(1, 20).mapToObj(i -> "line-" + i).forEach(lines::add);

        Path file = write(input, "data.txt", lines.toArray(String[]::new));

        List<String> results = run(source(file.toUri(), textSchema(true)), sink(output, Format.text));

        assertThat(results).containsExactlyInAnyOrderElementsOf(lines.subList(1, lines.size()));
    }

    @Test
    void textKeepsHeaderWhenSchemaNotEmbedded(@TempDir Path input, @PathForOutput URI output) throws IOException {
        Path file = write(input, "data.txt", "header", "first", "0", "last");

        List<String> results = run(source(file.toUri(), textSchema(false)), sink(output, Format.text));

        assertThat(results).containsExactlyInAnyOrder("header", "first", "0", "last");
    }

    @Test
    void textSkipsHeaderInEachFile(@TempDir Path input, @PathForOutput URI output) throws IOException {
        write(input, "a.txt", "header-a", "a1", "a2");
        write(input, "b.txt", "header-b", "b1", "b2");

        List<String> results = run(source(input.toUri(), textSchema(true)), sink(output, Format.text));

        assertThat(results).containsExactlyInAnyOrder("a1", "a2", "b1", "b2");
    }

    @Test
    void regexSkipsHeader(@TempDir Path input, @PathForOutput URI output) throws IOException {
        Path file = write(input, "data.log", "key value", "a 1", "b 2", "0 0");

        Schema schema = Schema.builder()
                .withFormat(Format.regex)
                .withEmbedsSchema(true)
                .withPattern("^(\\S+) (\\S+)$")
                .withDeclared(Field.asField("key|String", "value|String"))
                .build();

        List<String> results = run(source(file.toUri(), schema), sink(output, Format.tsv));

        assertThat(results).containsExactlyInAnyOrder("a|1", "b|2", "0|0");
    }
}
