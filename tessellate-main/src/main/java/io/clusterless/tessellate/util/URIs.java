/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class URIs {
    public static URI copyWithoutQuery(URI uri) {
        try {
            return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to copy uri");
        }
    }

    public static URI copyWithScheme(URI uri, String scheme) {
        try {
            return new URI(scheme, uri.getHost(), uri.getPath(), uri.getQuery());
        } catch (URISyntaxException exception) {
            throw new IllegalArgumentException(exception.getMessage(), exception);
        }
    }

    public static URI copyWithHost(URI uri, String host) {
        try {
            return new URI(uri.getScheme(), host, uri.getPath(), uri.getQuery());
        } catch (URISyntaxException exception) {
            throw new IllegalArgumentException(exception.getMessage(), exception);
        }
    }

    public static URI copyWithQuery(URI uri, String query) {
        try {
            return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), query, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to copy uri");
        }
    }

    public static URI copyWithPath(URI uri, String path) {
        try {
            return new URI(uri.getScheme(), uri.getAuthority(), path, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to copy uri");
        }
    }

    public static URI copyWithPathAppend(URI uri, String path) {
        try {
            return new URI(uri.getScheme(), uri.getAuthority(), Paths.get(uri.getPath(), path).toString(), null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to copy uri");
        }
    }

    public static URI copyWithDecodedPath(URI uri) {
        try {
            String decodedPath = URLDecoder.decode(uri.getPath(), StandardCharsets.UTF_8);
            return new URI(uri.getScheme(), uri.getAuthority(), decodedPath, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to copy uri");
        }
    }

    public static URI trimFilename(URI uri, boolean hasPartitions) {
        if (!hasPartitions) {
            return uri;
        }

        return trimFilename(uri);
    }

    public static URI copyAsDirectory(URI uri) {
        String path = uri.getPath();

        if (path == null) {
            return uri;
        }

        if (path.endsWith("/")) {
            return uri;
        }

        return copyWithPath(uri, path + "/");
    }

    public static URI trimFilename(URI uri) {
        String path = uri.getPath();

        if (path == null) {
            return uri;
        }

        if (path.endsWith("/")) {
            return uri;
        }

        return trim(uri, 1);
    }

    public static URI trim(URI uri, int trim) {
        if (trim == 0) {
            return uri;
        }

        String path = uri.getPath();

        if (path == null) {
            return uri;
        }

        String[] split = path.substring(1).split("/");

        if (split.length == trim) {
            return copyWithPath(uri, "/");
        }

        StringJoiner joiner = new StringJoiner("/", "/", "/");

        for (int i = 0; i < split.length - trim; i++) {
            joiner.add(split[i]);
        }

        return copyWithPath(uri, joiner.toString());
    }

    public static URI cleanFileUrls(URI uri) {
        return "file".equals(uri.getScheme()) ? URIs.copyWithHost(uri, "") : uri.normalize();
    }

    public static URI makeAbsolute(URI uri) {
        if (uri == null) {
            return null;
        }

        if (uri.isAbsolute()) {
            return uri;
        }

        return Paths.get(uri.getPath()).toAbsolutePath().toUri();
    }

    /**
     * Find the common path prefix of a list of URIs. Where path considers the elements of the URI path,
     * not substrings of the path.
     * <p>
     * For example, the common prefix of "s3://bucket/path1/path2" and "s3://bucket/path1/path3" is "s3://bucket/path1/".
     */
    @NotNull
    public static URI findCommonPathPrefix(List<URI> uris, int numPartitions) {
        if (uris.isEmpty()) {
            throw new IllegalArgumentException("URI list is empty");
        }

        List<String[]> elements = uris.stream()
                .map(u -> trimFilename(u, numPartitions != 0))
                .map(u -> trim(u, numPartitions))
                .map(u -> u.toString().split("/"))
                .collect(Collectors.toList());

        List<String> common = new ArrayList<>();
        for (int i = 0; i < elements.get(0).length; i++) {
            String element = elements.get(0)[i];
            int pos = i;
            boolean allMatch = elements.stream().allMatch(parts -> parts.length > pos && parts[pos].equals(element));

            if (allMatch) {
                common.add(element);
            } else {
                break;
            }
        }

        if (common.isEmpty()) {
            throw new IllegalArgumentException("No common prefix found");
        }

        String commonPrefix = String.join("/", common);
        return URI.create(commonPrefix + "/");
    }
}
