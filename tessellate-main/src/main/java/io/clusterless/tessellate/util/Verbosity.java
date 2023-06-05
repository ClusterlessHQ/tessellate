/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import picocli.CommandLine;

import java.util.logging.Level;
import java.util.logging.LogManager;

public class Verbosity {
    static {
        disable();
    }

    private int level = 0;

    public static void setLoggingLevel(Level level) {
        LogManager.getLogManager().getLogger("").setLevel(level);
    }

    public static void disable() {
        setLoggingLevel(Level.OFF);
    }

    public static void debug() {
        setLoggingLevel(Level.FINE);
    }

    public static void info() {
        setLoggingLevel(Level.INFO);
    }

    @CommandLine.Option(
            names = {"-v", "--verbose"},
            scope = CommandLine.ScopeType.INHERIT,
            description = {
                    "Specify multiple -v options to increase verbosity.",
                    "For example, `-v -v -v` or `-vvv`"})
    public void setVerbose(boolean[] verbosity) {
        setLoggingLevel(verbosity.length);
    }

    public int level() {
        return level;
    }

    protected void setLoggingLevel(int level) {
        this.level = level;
        switch (level) {
            case 0:
                disable();
                break;
            case 1:
                info();
                break;
            default:
                debug();
                break;
        }
    }
}
