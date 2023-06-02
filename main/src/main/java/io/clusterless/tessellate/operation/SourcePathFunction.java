/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.io.File;

/**
 * Inserts the current source path into the tuple stream.
 */
public class SourcePathFunction extends BaseOperation<Tuple> implements Function<Tuple> {
    final String defaultPath;
    final boolean fileNameOnly;

    public SourcePathFunction(Fields fieldDeclaration) {
        this(fieldDeclaration, null, false);
    }

    public SourcePathFunction(Fields fieldDeclaration, String defaultPath) {
        this(fieldDeclaration, defaultPath, false);
    }

    public SourcePathFunction(Fields fieldDeclaration, boolean fileNameOnly) {
        this(fieldDeclaration, null, fileNameOnly);
    }

    public SourcePathFunction(Fields fieldDeclaration, String defaultPath, boolean fileNameOnly) {
        super(fieldDeclaration.hasTypes() ? fieldDeclaration : fieldDeclaration.applyTypes(String.class));
        this.defaultPath = defaultPath;
        this.fileNameOnly = fileNameOnly;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
        operationCall.setContext(Tuple.size(1));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
        String sourcePath = flowProcess.getFlowProcessContext().getSourcePath();

        if (sourcePath == null)
            sourcePath = defaultPath;
        else if (fileNameOnly)
            sourcePath = getFileName(sourcePath);

        functionCall.getContext().set(0, sourcePath);

        functionCall.getOutputCollector().add(functionCall.getContext());
    }

    protected String getFileName(String sourcePath) {
        int i = sourcePath.lastIndexOf(File.separatorChar);

        if (i == -1)
            return sourcePath;

        return sourcePath.substring(i + 1);
    }
}
