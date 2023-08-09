/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.options;

import picocli.CommandLine;

public class PrintOptions {
    public enum PrintFormat {
        JSON,
        SQL
    }

    @CommandLine.Option(names = {"--print-output-schema"}, description = "print output schema, exits after printing")
    protected boolean printOutputSchema = false;

    @CommandLine.Option(names = {"--print-format"}, description = "print format")
    protected PrintFormat printFormat = PrintFormat.JSON;

    public boolean printOutputSchema() {
        return printOutputSchema;
    }

    public PrintFormat printFormat() {
        return printFormat;
    }
}
