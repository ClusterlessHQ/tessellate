/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.pipe.assembly.Copy;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import io.clusterless.tessellate.parser.FieldsParser;
import io.clusterless.tessellate.parser.ast.*;
import io.clusterless.tessellate.pipeline.intrinsic.IntrinsicBuilder;

import java.util.List;

public class Transformer {
    private final FieldsParser fieldsParser = FieldsParser.INSTANCE;
    private final Statement statement;

    public Transformer(Statement statement) {
        this.statement = statement;
    }

    PipelineContext resolve(PipelineContext context) {
        switch (statement.op().op()) {
            case "":
                return handleCoerce(context);

            case "=>":
                return handleAssignment(context);

            case "+>":
                return copyAndEval(context);

            case "->":
                return discardAndEval(context);

            default:
                throw new IllegalStateException("Unexpected value: " + statement.op().op());
        }
    }

    private PipelineContext discardAndEval(PipelineContext context) {
        Operation operation = (Operation) statement;

        if (operation.exp() != null) {
            IntrinsicBuilder.Result result = create(context, operation);
            Pipe pipe = new Each(context.pipe, result.arguments(), result.function(), Fields.REPLACE);
            Fields currentFields = context.currentFields.subtract(result.arguments()).append(result.results());
            return context.update(currentFields, pipe);
        }

        Fields fromFields = fieldsParser.asFields(operation.arguments());
        Fields toFields = fieldsParser.asFields(operation.results());

        if (toFields.isNone()) {
            context.log.info("transform discard: fields: {}", fromFields);
            Pipe pipe = new Discard(context.pipe, fromFields);
            Fields currentFields = context.currentFields.subtract(fromFields);
            return context.update(currentFields, pipe);
        } else {
            context.log.info("transform rename: from: {}, to: {}", fromFields, toFields);
            Pipe pipe = new Rename(context.pipe, fromFields, toFields);
            Fields currentFields = context.currentFields.rename(fromFields, toFields);
            return context.update(currentFields, pipe);
        }
    }

    private PipelineContext copyAndEval(PipelineContext context) {
        Operation operation = (Operation) statement;

        if (operation.exp() == null) {
            Fields fromFields = fieldsParser.asFields(operation.arguments());
            Fields toFields = fieldsParser.asFields(operation.results());

            context.log.info("transform copy: from: {}, to: {}", fromFields, toFields);
            Pipe pipe = new Copy(context.pipe, fromFields, toFields);
            Fields currentFields = context.currentFields.append(toFields);
            return context.update(currentFields, pipe);
        } else {
            IntrinsicBuilder.Result result = create(context, operation);
            Pipe pipe = new Each(context.pipe, result.arguments(), result.function(), Fields.ALL);
            Fields currentFields = context.currentFields.append(result.results());
            return context.update(currentFields, pipe);
        }
    }

    private PipelineContext handleCoerce(PipelineContext context) {
        List<Field> arguments = ((UnaryOperation) statement).arguments();
        Fields coerceFields = fieldsParser.asFields(arguments);
        context.log.info("transform coerce: fields: {}", coerceFields);
        Pipe pipe = new Coerce(context.pipe, coerceFields);
        Fields currentFields = context.currentFields.rename(coerceFields, coerceFields); // change the type information

        return context.update(currentFields, pipe);
    }

    private PipelineContext handleAssignment(PipelineContext context) {
        String value = ((Assignment) statement).literal();
        Fields toFields = fieldsParser.asFields(((Assignment) statement).result(), null);
        Object literal = Coercions.coerce(value, toFields.getType(0));
        context.log.info("transform insert: fields: {}, value: {}", toFields, literal);
        Pipe pipe = new Each(context.pipe, new Insert(toFields, literal), Fields.ALL);
        Fields currentFields = context.currentFields.append(toFields);

        return context.update(currentFields, pipe);
    }

    private IntrinsicBuilder.Result create(PipelineContext context, Operation operation) {
        if (operation.exp() instanceof Intrinsic) {
            Intrinsic intrinsic = operation.exp();

            IntrinsicBuilder intrinsicBuilder = Intrinsics.builders().get(intrinsic.name().name());

            if (intrinsicBuilder == null) {
                throw new IllegalArgumentException("unknown intrinsic function: " + intrinsic.name());
            }

            IntrinsicBuilder.Result result = intrinsicBuilder.create(operation);

            context.log.info("transform {}: from: {}, to: {}, having: {}", intrinsicBuilder.name(), result.arguments(), result.results(), ((Intrinsic) operation.exp()).params());

            return result;
        }

        throw new IllegalStateException("no builder found for: " + operation);
    }
}
