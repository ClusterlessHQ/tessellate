/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.local.tap;

import cascading.CascadingException;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tap.local.DirTap;
import cascading.tap.type.TapWith;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Properties;

public class PrefixedDirTap extends DirTap {
    private String prefix = "part";

    public PrefixedDirTap(Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String directory, SinkMode sinkMode) {
        super(scheme, directory, sinkMode);
    }

    public PrefixedDirTap(Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory, SinkMode sinkMode) {
        super(scheme, directory, sinkMode);
    }

    public PrefixedDirTap(Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path directory, SinkMode sinkMode, String prefix) {
        super(scheme, directory, sinkMode);
        this.prefix = prefix;
    }

    public PrefixedDirTap(Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String path, SinkMode keep, String prefix) {
        super(scheme, path, keep);
        this.prefix = prefix;
    }

    @Override
    protected String getOutputFileBasename() {
        return prefix;
    }

    @Override
    protected PathMatcher getPathMatcher() {
        // Hadoop FS writes _SUCCESS and .crc files, so we need to ignore them
        return path -> {
            String string = path.getFileName().toString();
            return string.charAt(0) != '_' && string.charAt(0) != '.';
        };
    }

    protected TapWith<Properties, InputStream, OutputStream> create(Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, Path path, SinkMode sinkMode) {
        try {
            return new PrefixedDirTap(scheme, path, sinkMode, prefix);
        } catch (CascadingException exception) {
            throw new TapException("unable to create a new instance of: " + getClass().getName(), exception);
        }
    }
}
