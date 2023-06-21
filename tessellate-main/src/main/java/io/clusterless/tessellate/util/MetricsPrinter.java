/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import cascading.stats.CascadingStats;
import cascading.stats.FlowStats;
import io.clusterless.tessellate.pipeline.Pipeline;
import org.fusesource.jansi.Ansi;
import picocli.CommandLine;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static cascading.flow.StepCounters.*;
import static org.fusesource.jansi.Ansi.ansi;

public class MetricsPrinter {
    @CommandLine.Option(names = {"--metrics-print"}, description = "print metrics to the console")
    boolean enabled;

    @CommandLine.Option(names = {"--metrics-interval"}, description = "metrics print interval in seconds")
    int interval = 15;

    private AtomicBoolean completed = new AtomicBoolean(false);
    private Timer timer;
    private PrintStream printStream = System.out;

    private class AnsiPrinterTimerTask extends TimerTask {
        protected Consumer<PrintStream> doOnce = this::clearSpace;
        protected final Pipeline pipeline;

        public AnsiPrinterTimerTask(Pipeline pipeline) {
            this.pipeline = pipeline;
        }

        protected void clearSpace(PrintStream printStream) {
            printStream.println();
            printStream.println();

            doOnce = (ps) -> {
            };
        }

        @Override
        public void run() {
            if (completed.get()) {
                timer.cancel();
                return;
            }

            doOnce.accept(printStream);

            String status = createStatus(pipeline);

            Ansi ansi = ansi()
                    .cursorUp(2)
                    .cursorToColumn(1)
                    .eraseLine()
                    .a(status)
                    .cursorDown(2)
                    .cursorToColumn(1);

            printStream.print(ansi.toString());
        }
    }

    public void stop() {
        completed.set(true);
    }

    public void start(Pipeline pipeline) {
        if (!enabled) {
            return;
        }

        timer = new Timer("pipeline-metrics-printer", true);

        timer.scheduleAtFixedRate(new AnsiPrinterTimerTask(pipeline), interval * 1000L, interval * 1000L);
    }

    protected String createStatus(Pipeline pipeline) {
        if (!pipeline.isRunning()) {
            return "\n";
        }

        return String.format("%s\n%s", currentDurationStatus(pipeline), currentTupleStatus(pipeline));
    }

    private String currentTupleStatus(Pipeline pipeline) {
        FlowStats flowStats = pipeline.flow().getFlowStats();

        long tuplesRead = flowStats.getCounterValue(Tuples_Read);
        long tuplesWritten = flowStats.getCounterValue(Tuples_Written);
        long tuplesTrapped = flowStats.getCounterValue(Tuples_Trapped);

        return String.format("tuples: read: %,d, trapped: %,d, written: %,d", tuplesRead, tuplesTrapped, tuplesWritten);
    }

    private String currentDurationStatus(Pipeline pipeline) {
        FlowStats flowStats = pipeline.flow().getFlowStats();

        if (flowStats.getStatus() == CascadingStats.Status.PENDING) {
            return "";
        }

        long beginTime = flowStats.getCounterValue(Process_Begin_Time);

        if (beginTime == 0) {
            return "";
        }

        long currentDuration = System.currentTimeMillis() - beginTime;
        long readDuration = flowStats.getCounterValue(Read_Duration);
        long writeDuration = flowStats.getCounterValue(Write_Duration);
        long intermediateDuration = currentDuration - readDuration - writeDuration;

        double readPercent = 100D * readDuration / currentDuration;
        double writePercent = 100D * writeDuration / currentDuration;
        double computePercent = 100D * intermediateDuration / currentDuration;

        String durationMessage = "duration: total: %s, read: %s (%.2f%%), int: %s (%.2f%%), write: %s (%.2f%%)";

        return String.format(
                durationMessage,
                Duration.ofMillis(currentDuration),
                Duration.ofMillis(readDuration), readPercent,
                Duration.ofMillis(intermediateDuration), computePercent,
                Duration.ofMillis(writeDuration), writePercent
        );
    }
}
