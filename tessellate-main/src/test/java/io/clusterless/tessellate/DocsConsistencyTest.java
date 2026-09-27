/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate;

import io.clusterless.tessellate.factory.TapFactories;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Protocol;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The Antora pages hand-copy option, protocol, and format names, so check them against the picocli
 * annotations and the registered tap factories.
 */
public class DocsConsistencyTest {
    private static final Path ANTORA = Path.of("src/main/antora");
    private static final Path SOURCE_SINK = ANTORA.resolve("modules/reference/pages/source-sink.adoc");

    // tokens in the pages that are not tess options
    private static final Set<String> NOT_TESS_OPTIONS = Set.of(
            "--rm" // docker run, install.adoc
    );

    private static final Pattern OPTION = Pattern.compile("(?<![\\w-])--[a-z][a-z-]*");
    private static final Pattern PROTOCOL = Pattern.compile("`([a-z0-9]+)(\\(s\\))?://`");
    private static final Pattern TERM = Pattern.compile("^`?([a-z0-9/:]+?)`?::(?!:)");

    private static Map<Path, String> pages() {
        try (Stream<Path> paths = Files.walk(ANTORA)) {
            return paths.filter(path -> path.toString().endsWith(".adoc"))
                    .collect(Collectors.toMap(path -> path, DocsConsistencyTest::read, (l, r) -> l, TreeMap::new));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The definition list terms under the given section title, up to the next section.
     */
    private static Set<String> terms(String page, String section) {
        Set<String> terms = new LinkedHashSet<>();
        boolean inSection = false;

        for (String line : page.split("\n")) {
            if (line.startsWith("== ")) {
                inSection = line.equals("== " + section);
                continue;
            }

            Matcher matcher = TERM.matcher(line);
            if (inSection && matcher.find()) {
                terms.add(matcher.group(1));
            }
        }

        assertFalse(terms.isEmpty(), "no terms found in section: " + section);

        return terms;
    }

    private static Set<Protocol> registeredProtocols() {
        Set<Protocol> protocols = EnumSet.noneOf(Protocol.class);
        protocols.addAll(TapFactories.getSourceProtocols());
        protocols.addAll(TapFactories.getSinkProtocols());
        return protocols;
    }

    private static Set<Format> registeredFormats() {
        Set<Format> formats = EnumSet.noneOf(Format.class);
        TapFactories.getSourceFormats().values().forEach(formats::addAll);
        TapFactories.getSinkFormats().values().forEach(formats::addAll);
        return formats;
    }

    @Test
    void optionsAreReal() {
        Set<String> options = new CommandLine(new Main()).getCommandSpec().options().stream()
                .flatMap(option -> Arrays.stream(option.names()))
                .collect(Collectors.toSet());

        List<String> unknown = new ArrayList<>();

        pages().forEach((path, page) -> {
            Matcher matcher = OPTION.matcher(page);
            while (matcher.find()) {
                String token = matcher.group();
                if (!options.contains(token) && !NOT_TESS_OPTIONS.contains(token)) {
                    unknown.add(ANTORA.relativize(path) + ": " + token);
                }
            }
        });

        assertEquals(List.of(), unknown, "options named in the docs that tess does not have");
    }

    @Test
    void protocolsAreRegistered() {
        Set<String> registered = registeredProtocols().stream()
                .map(Protocol::name)
                .collect(Collectors.toSet());

        List<String> unknown = new ArrayList<>();

        pages().forEach((path, page) -> {
            Matcher matcher = PROTOCOL.matcher(page);
            while (matcher.find()) {
                List<String> schemes = matcher.group(2) == null ? List.of(matcher.group(1)) : List.of(matcher.group(1), matcher.group(1) + "s");
                for (String scheme : schemes) {
                    if (!registered.contains(scheme)) {
                        unknown.add(ANTORA.relativize(path) + ": " + scheme + "://");
                    }
                }
            }
        });

        assertEquals(List.of(), unknown, "protocols named in the docs that no factory handles");
    }

    @Test
    void sourceSinkProtocolsMatchFactories() {
        Set<String> documented = terms(read(SOURCE_SINK), "Protocols").stream()
                .map(term -> term.replace("://", ""))
                .collect(Collectors.toCollection(TreeSet::new));

        Set<String> registered = registeredProtocols().stream()
                .map(Protocol::name)
                .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(registered, documented);
    }

    @Test
    void sourceSinkFormatsMatchFactories() {
        // a term like text/regex names a format and its sub-formats, a sub-format is handled by its parent's factory
        Set<Format> documented = terms(read(SOURCE_SINK), "Formats").stream()
                .flatMap(term -> Arrays.stream(term.split("/")))
                .map(Format::valueOf)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Format.class)));

        Set<Format> registered = registeredFormats();

        Set<Format> unhandled = documented.stream()
                .filter(format -> !registered.contains(format.parent()))
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Format.class)));

        Set<Format> undocumented = EnumSet.copyOf(registered);
        undocumented.removeAll(documented);

        assertEquals(Set.of(), unhandled, "formats named in source-sink.adoc that no factory handles");
        assertEquals(Set.of(), undocumented, "formats a factory handles that source-sink.adoc does not name");
    }
}
