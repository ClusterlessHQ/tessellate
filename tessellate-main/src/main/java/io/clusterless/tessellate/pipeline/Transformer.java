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

import java.util.List;

public class Transformer {
    private final FieldsParser fieldsParser;
    Statement statement;

    public Transformer(Statement statement) {
        this.statement = statement;
        fieldsParser = FieldsParser.INSTANCE;
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
            throw new UnsupportedOperationException("unsupported: " + operation);
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

        if (operation.exp() != null) {
            throw new UnsupportedOperationException("unsupported: " + operation);
        }

        Fields fromFields = fieldsParser.asFields(operation.arguments());
        Fields toFields = fieldsParser.asFields(operation.results());
        context.log.info("transform copy: from: {}, to: {}", fromFields, toFields);
        Pipe pipe = new Copy(context.pipe, fromFields, toFields);
        Fields currentFields = context.currentFields.append(toFields);
        return context.update(currentFields, pipe);
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
        Fields insertFields = fieldsParser.asFields(((Assignment) statement).result(), null);
        Object literal = Coercions.coerce(value, insertFields.getType(0));
        context.log.info("transform insert: fields: {}, value: {}", insertFields, literal);
        Pipe pipe = new Each(context.pipe, new Insert(insertFields, literal), Fields.ALL);
        Fields currentFields = context.currentFields.append(insertFields);

        return context.update(currentFields, pipe);
    }
}
