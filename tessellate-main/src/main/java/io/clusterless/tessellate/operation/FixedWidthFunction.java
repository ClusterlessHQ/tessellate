/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * FixedWidthFunction is a {@link Function} that pads a Tuple to a fixed width by inserting nulls at a given index.
 */
public class FixedWidthFunction extends BaseOperation<Tuple> implements Function<Tuple> {
    private final int width;
    private final int insertAt;

    /**
     * @param width    the width of the output Tuple
     * @param insertAt the index to insert the input Tuple into the output Tuple, negative values are from the end
     */
    public FixedWidthFunction(int width, int insertAt) {
        this(Fields.size(width), width, insertAt);
    }

    public FixedWidthFunction(Fields fieldDeclaration, int width, int insertAt) {
        super(fieldDeclaration);
        this.width = width;
        this.insertAt = insertAt < 0 ? width + insertAt : insertAt;

        if (fieldDeclaration.size() != width) {
            throw new IllegalArgumentException("fieldDeclaration width must match fixed width");
        }

        if (this.insertAt < 0 || this.insertAt >= width) {
            throw new IllegalArgumentException("insertAt must be between 0 and width, got: " + this.insertAt);
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
        TupleEntry arguments = functionCall.getArguments();

        int argumentsSize = arguments.size();

        if (argumentsSize == width) {
            functionCall.getOutputCollector().add(arguments.getTuple());
            return;
        }

        // we do this here because by definition the argument sizes are variable throughout the source data
        if (argumentsSize > width) {
            throw new OperationException("arguments size is greater than width, this does not truncate");
        }

        Tuple result = Tuple.size(width);

        int current = 0;
        for (int i = 0; i < Math.min(insertAt, argumentsSize); i++) {
            result.set(i, arguments.getObject(current++));
        }

        if (argumentsSize <= insertAt) {
            functionCall.getOutputCollector().add(result);
            return;
        }

        int completed = current;
        int gap = width - argumentsSize;
        int remaining = argumentsSize - current;

        for (int i = 0; i < remaining; i++) {
            result.set(i + completed + gap, arguments.getObject(current++));
        }

        functionCall.getOutputCollector().add(result);
    }
}
