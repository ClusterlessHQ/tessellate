/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AltAssertions {
    public static void assertFilenameParts(URI output, String prefix, String guid, String extension, int fileCount) throws IOException {
        final int[] count = {0};
        try (Stream<Path> pathStream = Files.find(Paths.get(output), 10, (path, attr) -> !path.toString().startsWith(".") && path.toString().endsWith(extension))) {
            pathStream
                    .forEach(path -> {
                        count[0]++;
                        String filename = path.getFileName().toString();
                        assertTrue(filename.startsWith(prefix), "wrong filename: " + filename);
                        assertTrue(filename.contains(guid), "wrong filename: " + filename);
                    });
        }

        assertEquals(fileCount, count[0], "wrong number of files");
    }
}
