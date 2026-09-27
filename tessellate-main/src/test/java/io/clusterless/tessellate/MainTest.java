/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate;

import io.clusterless.tessellate.junit.PathForResource;
import io.clusterless.tessellate.junit.ResourceExtension;
import io.clusterless.tessellate.pipeline.Pipeline;
import io.clusterless.tessellate.util.Verbosity;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.stream.SystemErr;
import uk.org.webcompere.systemstubs.stream.SystemOut;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the real entry path, {@link Main#run(String[])}, and asserts the exit code, stdout, and stderr.
 * <p>
 * stdout carries only sink data and the documented machine outputs; logs, usage on error, and error
 * messages go to stderr. Exit codes: 0 success, 1 unexpected error, 2 usage error, 3 flow failure.
 */
@ExtendWith({SystemStubsExtension.class, ResourceExtension.class})
public class MainTest {
    private static final String HEADER = "first,second,third,fourth,fifth";

    @SystemStub
    private SystemOut systemOut = new SystemOut();

    @SystemStub
    private SystemErr systemErr = new SystemErr();

    @TempDir
    private Path tempDir;

    @AfterEach
    void resetVerbosity() {
        // -v sets the root logger level globally, so restore the default for later tests
        Verbosity.disable();
    }

    private Path pipelineFile(URI input) throws IOException {
        String json = """
                {
                  "source": { "inputs": ["%s"], "schema": { "format": "csv", "embedsSchema": true } },
                  "sink": { "schema": { "format": "csv", "embedsSchema": true } }
                }
                """.formatted(input);

        return Files.writeString(tempDir.resolve("pipeline.json"), json);
    }

    private String out() {
        return systemOut.getText();
    }

    private String err() {
        return systemErr.getText();
    }

    /**
     * The stdout sink re-quotes values, so compare the header and the line count against the input.
     */
    private void assertOnlyData(URI input) throws IOException {
        List<String> expected = Files.readAllLines(Path.of(input));
        List<String> actual = out().lines().toList();

        assertEquals(HEADER, actual.get(0), this::out);
        assertEquals(expected.size(), actual.size(), this::out);
    }

    private void assertJSONOnStdout() throws IOException {
        assertFalse(out().isBlank());
        assertNotNull(JSONUtil.readTree(out()), this::out);
    }

    private static void assertNoStackTrace(String text) {
        assertFalse(text.contains("\tat "), () -> "unexpected stack trace: " + text);
    }

    @Test
    void unknownOptionIsUsageError() {
        assertEquals(CommandLine.ExitCode.USAGE, Main.run(new String[]{"--bogus"}));

        assertEquals("", out());
        assertTrue(err().contains("Unknown option: '--bogus'"), this::err);
        assertTrue(err().contains("Usage: tess"), this::err);
    }

    @Test
    void invalidEnumValueIsUsageError() {
        assertEquals(CommandLine.ExitCode.USAGE, Main.run(new String[]{"--show-source", "bogus"}));

        assertEquals("", out());
        assertTrue(err().contains("Invalid value for option '--show-source'"), this::err);
        assertNoStackTrace(err());
    }

    @Test
    void missingOptionValueIsUsageError() {
        assertEquals(CommandLine.ExitCode.USAGE, Main.run(new String[]{"-i"}));

        assertEquals("", out());
        assertTrue(err().contains("Missing required parameter"), this::err);
        assertNoStackTrace(err());
    }

    @Test
    void missingPipelineFileIsUsageError() {
        Path missing = tempDir.resolve("missing.json");

        assertEquals(CommandLine.ExitCode.USAGE, Main.run(new String[]{"-p", missing.toString()}));

        assertEquals("", out());
        assertTrue(err().contains("pipeline file does not exist: " + missing), this::err);
    }

    @Test
    void dataOnStdout(@PathForResource("/data/delimited-header.csv") URI input) throws IOException {
        Path pipeline = pipelineFile(input);

        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"-p", pipeline.toString()}));

        assertOnlyData(input);
        assertEquals("", err());
    }

    @Test
    void verboseLogsOnStderr(@PathForResource("/data/delimited-header.csv") URI input) throws IOException {
        Path pipeline = pipelineFile(input);

        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"-v", "-p", pipeline.toString()}));

        assertOnlyData(input);
        assertTrue(err().contains("tessellate version:"), this::err);
    }

    @Test
    void flowFailure(@PathForResource("/data/delimited-header-bad-width.csv") URI input) throws IOException {
        Path pipeline = pipelineFile(input);

        assertEquals(Pipeline.FLOW_FAILED, Main.run(new String[]{"-p", pipeline.toString()}));

        assertTrue(err().contains("flow failed with:"), this::err);
    }

    @Test
    void brotliSinkRejectedBeforeFlow(@PathForResource("/data/delimited-header.csv") URI input) throws IOException {
        String json = """
                {
                  "source": { "inputs": ["%s"], "schema": { "format": "csv", "embedsSchema": true } },
                  "sink": { "output": "%s", "schema": { "format": "csv", "compression": "brotli" } }
                }
                """.formatted(input, tempDir.resolve("output").toUri());

        Path pipeline = Files.writeString(tempDir.resolve("pipeline.json"), json);

        assertEquals(CommandLine.ExitCode.SOFTWARE, Main.run(new String[]{"-p", pipeline.toString()}));

        assertEquals("", out());
        assertTrue(err().contains("[none, gzip, snappy, lz4]"), this::err);
        assertFalse(err().contains("flow failed with:"), this::err);
        assertNoStackTrace(err());
    }

    @Test
    void exceptionInCall() throws IOException {
        Path pipeline = Files.writeString(tempDir.resolve("pipeline.json"), "{ not json");

        assertEquals(CommandLine.ExitCode.SOFTWARE, Main.run(new String[]{"-p", pipeline.toString()}));

        assertEquals("", out());
        assertFalse(err().isBlank());
        assertNoStackTrace(err());
    }

    @Test
    void exceptionInCallVerbose() throws IOException {
        Path pipeline = Files.writeString(tempDir.resolve("pipeline.json"), "{ not json");

        assertEquals(CommandLine.ExitCode.SOFTWARE, Main.run(new String[]{"-v", "-p", pipeline.toString()}));

        assertEquals("", out());
        assertTrue(err().contains("\tat "), this::err);
    }

    @Test
    void showSource() throws IOException {
        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"--show-source", "formats"}));

        assertJSONOnStdout();
        assertTrue(out().contains("\"csv\""), this::out);
        assertEquals("", err());
    }

    @Test
    void showSink() throws IOException {
        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"--show-sink", "protocols"}));

        assertJSONOnStdout();
        assertTrue(out().contains("\"file\""), this::out);
        assertEquals("", err());
    }

    @Test
    void printPipeline(@PathForResource("/data/delimited-header.csv") URI input) throws IOException {
        Path pipeline = pipelineFile(input);

        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"-p", pipeline.toString(), "--print-pipeline"}));

        assertJSONOnStdout();
        assertTrue(out().contains("\"source\""), this::out);
        assertEquals("", err());
    }

    @Test
    void printOutputSchema(@PathForResource("/data/delimited-header.csv") URI input) throws IOException {
        Path pipeline = pipelineFile(input);

        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"-p", pipeline.toString(), "--print-output-schema"}));

        assertTrue(out().contains("first"), this::out);
        assertEquals("", err());
    }

    @Test
    void help() {
        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"--help"}));

        assertTrue(out().contains("Usage: tess"), this::out);
        assertEquals("", err());
    }

    @Test
    void noArgumentsPrintsUsage() {
        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{}));

        assertTrue(out().contains("Usage: tess"), this::out);
        assertEquals("", err());
    }

    @Test
    void version() {
        assertEquals(CommandLine.ExitCode.OK, Main.run(new String[]{"--version"}));

        assertFalse(out().isBlank());
        assertEquals("", err());
    }
}
