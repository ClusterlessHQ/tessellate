/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import io.clusterless.tessellate.model.PipelineDef;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.pipeline.Pipeline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.stream.SystemErr;
import uk.org.webcompere.systemstubs.stream.SystemOut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * --metrics-print writes its ANSI status lines to stderr so they never interleave with stdout sink data.
 */
@ExtendWith(SystemStubsExtension.class)
public class MetricsPrinterTest {
    private static final String ESC = "\u001B[";

    @SystemStub
    private SystemOut systemOut = new SystemOut();

    @SystemStub
    private SystemErr systemErr = new SystemErr();

    @Test
    void metricsOnStderr() throws InterruptedException {
        Pipeline pipeline = new Pipeline(new PipelineOptions(), PipelineDef.builder().build());

        MetricsPrinter metrics = new MetricsPrinter();
        metrics.enabled = true;
        metrics.interval = 1;

        metrics.start(pipeline);

        try {
            long deadline = System.currentTimeMillis() + 5_000;

            while (!systemErr.getText().contains(ESC) && !systemOut.getText().contains(ESC) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
        } finally {
            metrics.stop();
        }

        assertEquals("", systemOut.getText());
        assertTrue(systemErr.getText().contains(ESC), "no metrics status printed to stderr");
    }
}
