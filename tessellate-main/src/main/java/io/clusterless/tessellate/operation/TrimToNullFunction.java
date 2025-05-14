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
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TrimToNullFunction extends BaseOperation<Object[]> implements Function<Object[]> {

    public TrimToNullFunction(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Object[]> operationCall) {
        Object[] context = new Object[]{
                new TupleEntry(fieldDeclaration, Tuple.size(fieldDeclaration.size())),
                new Object[fieldDeclaration.size()]
        };
        operationCall.setContext(context);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Object[]> functionCall) {
        Object[] context = functionCall.getContext();
        TupleEntry result = (TupleEntry) context[0];
        Object[] values = (Object[]) context[1];

        TupleEntry arguments = functionCall.getArguments();
        for (int i = 0; i < arguments.size(); i++) {
            String value = arguments.getString(i);

            if (value == null) {
                values[i] = null;
            } else {
                String intermediate = value.trim();

                if (intermediate.isEmpty()) {
                    values[i] = null;
                } else {
                    values[i] = arguments.getObject(i);
                }
            }
        }

        result.setCanonicalValues(values);

        functionCall.getOutputCollector().add(result);
    }
}
