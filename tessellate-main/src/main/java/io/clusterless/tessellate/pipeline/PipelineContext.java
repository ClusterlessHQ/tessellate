/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import org.slf4j.Logger;


public class PipelineContext {
    public final Logger log;
    public Fields currentFields;
    public Pipe pipe;

    public PipelineContext(Logger log, Fields currentFields, Pipe pipe) {
        this.log = log;
        this.currentFields = currentFields;
        this.pipe = pipe;
    }

    public PipelineContext update(Fields currentFields, Pipe pipe) {
        this.currentFields = currentFields;
        this.pipe = pipe;
        logCurrentFields(this.currentFields);

        return this;
    }

    public void logCurrentFields(Fields currentFields) {
        log.info("current fields: {}", currentFields);
    }
}
