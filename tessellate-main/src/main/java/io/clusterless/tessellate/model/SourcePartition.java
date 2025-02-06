/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.jetbrains.annotations.NotNull;

/**
 * ts->ymd|DateTime|yyyyMMdd
 */
public class SourcePartition extends Translate implements Partition {
    @JsonCreator
    public SourcePartition(String partition) {
        super(partition);
    }

    @Override
    @NotNull
    protected String translate() {
        return "\\s*[-]>\\s*";
    }

    @Override
    protected boolean requiresFrom() {
        return false;
    }
}
