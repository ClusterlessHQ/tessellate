/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

public enum Protocol {
    file, hdfs, s3, http, https;

    public static Protocol fromString(String protocol) {
        switch (protocol) {
            case "file":
                return file;
            case "hdfs":
                return hdfs;
            case "s3":
                return s3;
            case "http":
                return http;
            case "https":
                return https;
            default:
                throw new IllegalArgumentException("Unknown protocol: " + protocol);
        }
    }
}
