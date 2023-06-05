/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import cascading.tuple.Fields;
import cascading.tuple.type.DateType;

import java.util.TimeZone;

public class TemporalTypes {
    public static Fields STANDARD_TIME = new Fields("time", new DateType("yyyy-MM-dd'T'HH:mm'Z'", TimeZone.getTimeZone("UTC")));

    public static final DateType DATE_TIME_TYPE_MILLIS = new DateType("yyyy-MM-dd HH:mm:ss.SSS z", TimeZone.getTimeZone("UTC"));
    public static final DateType DATE_TIME_TYPE_MICROS = new DateType("yyyy-MM-dd HH:mm:ss.SSSSSS z", TimeZone.getTimeZone("UTC"));
    public static final DateType DATE_TIME_TYPE_NANOS = new DateType("yyyy-MM-dd HH:mm:ss.SSSSSSSSS z", TimeZone.getTimeZone("UTC"));
}
