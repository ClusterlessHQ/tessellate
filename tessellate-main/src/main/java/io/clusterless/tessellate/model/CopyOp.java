/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.clusterless.tessellate.pipeline.Transforms;
import org.jetbrains.annotations.NotNull;

/**
 * <pre>
 * ts+>ymd|DateTime|yyyyMMdd
 * </pre>
 */
public class CopyOp extends Translate implements TransformOp {
    @JsonCreator
    public CopyOp(String partition) {
        super(partition);
    }

    @Override
    @NotNull
    protected String translate() {
        return "[+]>";
    }

    @Override
    protected boolean requiresFrom() {
        return true;
    }

    @Override
    public Transforms transform() {
        return Transforms.copy;
    }
}
