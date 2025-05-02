/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.pipeline;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.*;
import cascading.pipe.joiner.*;
import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import io.clusterless.tessellate.parser.FieldsParser;
import io.clusterless.tessellate.parser.ast.*;
import io.clusterless.tessellate.pipeline.intrinsic.IntrinsicBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class Transformer {
    private final FieldsParser fieldsParser = FieldsParser.INSTANCE;
    private final Statement statement;

    public Transformer(Statement statement) {
        this.statement = statement;
    }

    PipelineContext resolve(PipelineContext context) {
        if (statement.isJoin()) {
            return handleJoin(context);
        }
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

            boolean argsIsAll = result.arguments().isAll() ||
                    containsAll(context.currentFields, result.arguments());

            boolean resultsEqualsDeclared = containsAll(result.function().getFieldDeclaration(), result.results());

            Fields selector;

            if ((argsIsAll && result.results().isNone()) || resultsEqualsDeclared) {
                selector = Fields.RESULTS;
            } else if (argsIsAll) {
                selector = result.results();
            } else {
                // SWAP allows for efficient replacement of arguments with non-argument and results
                selector = Fields.SWAP;
            }

            Pipe pipe = new Each(context.pipe, result.arguments(), result.function(), selector);

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

    private static boolean containsAll(Fields lhs, Fields rhs) {
        return lhs.contains(rhs) && lhs.size() == rhs.size();
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

            IntrinsicBuilder.Result result = intrinsicBuilder.create(context.currentFields, operation);

            context.log.info("transform {}: from: {}, to: {}, having: {}", intrinsicBuilder.name(), result.arguments(), result.results(), ((Intrinsic) operation.exp()).params());

            return result;
        }

        throw new IllegalStateException("no builder found for: " + operation);
    }

    private PipelineContext handleJoin(PipelineContext context) {
        Join join = (Join) statement;

        List<Rel> lhs = join.lhsRelations();
        List<Rel> rhs = join.rhsRelations();

        if (lhs.size() != 1) {
            throw new IllegalArgumentException("lhs must have exactly one relation");
        }

        Rel lhsRel = lhs.get(0);
        Rel rhsRel = rhs.get(0);
        Fields joinLhsFields = fieldsParser.asFields(lhsRel.fields());
        Fields joinRhsFields = fieldsParser.asFields(rhsRel.fields());

        // all fields from lhs
        Fields toFields = fieldsParser.asFields(join.results());

        Pipe lhsPipe = new Pipe(lhsRel.name());

        // toFields may declare fields used in the join
        Fields lhsFields = Fields.merge(joinLhsFields, toFields);
        lhsPipe = new Retain(lhsPipe, lhsFields);

        Pipe rhsPipe = new Pipe(rhsRel.name(), context.pipe);

        Joiner joiner = findJoiner(join);

        context.log.info("join {}: from lhs: {}, to: {}", join.joinType(), lhsFields, toFields);

        String name = String.format("%s+%s", lhsRel.name(), rhsRel.name());
        Pipe pipe = new HashJoin(name, lhsPipe, joinLhsFields, rhsPipe, joinRhsFields, joiner);

        Fields currentFields;

        // `lhs(fromField1+fromField2) rhs(fromField1+fromField2+...) +inner{} +> fromField3` - copy lhs `fromField1+fromField2+fromField3` to results
        // `lhs(fromField1+fromField2) rhs(fromField1+fromField2+...) +inner{} -> fromField3` - copy lhs `fromField3` to results
        // `lhs(fromField1+fromField2) rhs(fromField1+fromField2+...) +inner{}` - as a filter
        switch (join.op().op()) {
            case "+>":
                currentFields = lhsFields.append(context.currentFields);
                break;
            case "->":
                pipe = new Discard(pipe, joinLhsFields);
                currentFields = toFields.append(context.currentFields);
                break;
            case "":
                pipe = new Discard(pipe, lhsFields);
                currentFields = context.currentFields;
                break;
            default:
                throw new IllegalArgumentException("unsupported join op: " + join.op());
        }

        context.joins.add(lhsPipe);

        return context.update(currentFields, pipe);
    }

    private static @NotNull Joiner findJoiner(Join join) {
        switch (join.joinType()) {
            case inner:
                return new InnerJoin();
            case left:
                return new LeftJoin();
            case right:
                return new RightJoin();
            case outer:
                return new OuterJoin();
            default:
                throw new IllegalArgumentException("unsupported join type: " + join.joinType());
        }
    }
}
