/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class MVELContext {
    protected Random random = new Random(System.currentTimeMillis());
    protected String rnd32 = Integer.toString(Math.abs(random.nextInt()));
    protected String rnd64 = Long.toString(Math.abs(random.nextLong()));
    protected InetAddress localHost = getLocalHost();
    protected Instant now = Instant.now();
    protected ZonedDateTime utc = now.atZone(ZoneId.of("UTC"));

    Map<String, Object> source = new HashMap<>();
    Map<String, Object> sink = new HashMap<>();

    public MVELContext() {
    }

    public MVELContext(Map<String, Object> source, Map<String, Object> sink) {
        this.source = source;
        this.sink = sink;
    }

    public Map<String, String> env() {
        return System.getenv();
    }

    public Properties sys() {
        return System.getProperties();
    }

    public String pid() {
        return Long.toString(ProcessHandle.current().pid());
    }

    public String rnd32() {
        return rnd32;
    }

    public String rnd64() {
        return rnd64;
    }

    public String rnd32Next() {
        return Integer.toString(Math.abs(random.nextInt()));
    }

    public String rnd64Next() {
        return Long.toString(Math.abs(random.nextLong()));
    }

    public String hostAddress() {
        return localHost == null ? "" : localHost.getHostAddress();
    }

    public String hostName() {
        return localHost == null ? "" : localHost.getCanonicalHostName();
    }

    public String currentTimeMillis() {
        return Long.toString(now.toEpochMilli());
    }

    public String currentTimeYear() {
        return Integer.toString(utc.getYear());
    }

    public String currentTimeMonth() {
        return String.format("%02d", utc.getMonthValue());
    }

    public String currentTimeDay() {
        return String.format("%02d", utc.getDayOfMonth());
    }

    public String currentTimeHour() {
        return String.format("%02d", utc.getHour());
    }

    public String currentTimeMinute() {
        return String.format("%02d", utc.getMinute());
    }

    public String currentTimeSecond() {
        return String.format("%02d", utc.getSecond());
    }

    public String currentTimeISO8601() {
        return now.toString();
    }

    public Map<String, Object> source() {
        return source;
    }

    public Map<String, Object> sink() {
        return sink;
    }

    private static InetAddress getLocalHost() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
