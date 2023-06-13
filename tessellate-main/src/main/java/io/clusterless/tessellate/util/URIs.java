/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import java.util.regex.Pattern;

public class URIs {
    private static Pattern pattern = Pattern.compile("(\\{\\{(?!\\{)(.*)}}(?!}))");

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

    /**
     * @param raw
     * @return
     * @throws IllegalArgumentException
     */
    public static URI encodeEscapedToURI(String raw) {
        String encoded = encodeEscaped(raw);

        try {
            return new URI(encoded);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage() + ", use {{...}} to url encode portions of the path", e);
        }
    }

    public static String encodeEscaped(String raw) {
        return pattern.matcher(raw)
                .replaceAll((r) -> URLEncoder.encode(r.group(2), StandardCharsets.UTF_8));
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

        StringJoiner joiner = new StringJoiner("/", "/", "/");

        for (int i = 0; i < split.length - trim; i++) {
            joiner.add(split[i]);
        }

        return copyWithPath(uri, joiner.toString());
    }
}
