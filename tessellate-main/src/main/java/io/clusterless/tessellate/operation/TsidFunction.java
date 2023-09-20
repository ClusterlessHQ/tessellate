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
import com.github.f4b6a3.tsid.Tsid;
import com.github.f4b6a3.tsid.TsidFactory;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.function.Supplier;

public class TsidFunction extends BaseOperation<TsidFunction.Context> implements Function<TsidFunction.Context> {
    private final Integer node;
    private final Integer nodeCount;
    private final Long epoch;
    private final String format;
    private final Boolean counterToZero;

    public static class Context {

        protected final Tuple tuple = Tuple.size(1);
        protected final TsidFactory factory;
        protected final Supplier<?> supplier;

        public Context(Type type, Integer node, Integer nodeCount, Long epoch, String format, Boolean counterToZero) {
            // pass nulls instead of defaults so that the builder can pick up env/property defaults
            TsidFactory.Builder builder = TsidFactory.builder()
                    .withNode(node)
                    .withNodeBits(nodeCount != null ? (int) Math.ceil(Math.log(nodeCount) / Math.log(2)) : null)
                    .withCustomEpoch(Instant.ofEpochMilli(epoch == null ? Tsid.TSID_EPOCH : epoch));

            // zeros the counter every millisecond
            if (counterToZero != null && counterToZero) {
                builder.withRandomFunction(byte[]::new);
            }

            this.factory = builder
                    .build();

            if (type == String.class) {
                if (format == null) {
                    supplier = () -> factory.create().toString();
                } else {
                    supplier = () -> factory.create().format(format);
                }
            } else if (type == Long.class || type == Long.TYPE) {
                supplier = () -> factory.create().toLong();
            } else {
                throw new IllegalArgumentException("unsupported type: " + type);
            }
        }

        public Tuple next() {
            tuple.set(0, supplier.get());
            return tuple;
        }
    }

    public TsidFunction(Fields fieldDeclaration, Integer node, Integer nodeCount, Long epoch, String format, Boolean counterToZero) {
        super(0, fieldDeclaration);
        this.node = node;
        this.nodeCount = nodeCount;
        this.epoch = epoch;
        this.format = format;
        this.counterToZero = counterToZero;

        if (fieldDeclaration.size() != 1)
            throw new IllegalArgumentException("fieldDeclaration may only declare one field, was " + fieldDeclaration.print());
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        operationCall.setContext(new Context(fieldDeclaration.getType(0), node, nodeCount, epoch, format, counterToZero));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
        functionCall.getOutputCollector().add(functionCall.getContext().next());
    }
}
