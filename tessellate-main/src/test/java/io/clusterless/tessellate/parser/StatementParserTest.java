/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import io.clusterless.tessellate.parser.ast.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StatementParserTest {

    @Test
    void literals() {
        assertThat(StatementParser.parseLiteral("foo")).isEqualTo("foo");
        assertThat(StatementParser.parseLiteral(" foo ")).isEqualTo("foo");
        assertThat(StatementParser.parseLiteral("\"f oo\"")).isEqualTo("f oo");
        assertThat(StatementParser.parseLiteral(" \"f oo\" ")).isEqualTo("f oo");
        assertThat(StatementParser.parseLiteral("\"f\\\"oo\"")).isEqualTo("f\"oo");
        assertThat(StatementParser.parseLiteral(" \"f\\\"oo\" ")).isEqualTo("f\"oo");
        assertThat(StatementParser.parseLiteral("'f oo'")).isEqualTo("f oo");
        assertThat(StatementParser.parseLiteral(" 'f oo' ")).isEqualTo("f oo");
        assertThat(StatementParser.parseLiteral("'f''oo'")).isEqualTo("f'oo");
        assertThat(StatementParser.parseLiteral(" 'f''oo' ")).isEqualTo("f'oo");
        assertThat(StatementParser.parseLiteral("'f{oo'")).isEqualTo("f{oo");
        assertThat(StatementParser.parseLiteral("'f}oo'")).isEqualTo("f}oo");
        assertThat(StatementParser.parseLiteral("'f:oo'")).isEqualTo("f:oo");
    }

    @Test
    void parse() {
        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("fromField1+fromField2+fromFieldN ^siphash{} +> intoField|type")).exp());
        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("fromField1 + fromField2 + fromFieldN ^siphash{} +> intoField|type")).exp());
        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("^tsid{node:1,nodeCount:10,signed:true,epoch:123} +> intoField|type")).exp());

        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("fromField1+fromField2+fromFieldN ^siphash{} -> intoField|type")).exp());
        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("fromField1 + fromField2 + fromFieldN ^siphash{} -> intoField|type")).exp());
        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("fromField1 + fromField2 + fromFieldN ^siphash{prefix:'{:}'} -> intoField|type")).exp());
        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("^tsid{node:1,nodeCount:10,signed:true,epoch:123} -> intoField|type")).exp());
        assertInstanceOf(Intrinsic.class, assertInstanceOf(Operation.class, StatementParser.parse("^toJson{} -> json")).exp());

        assertNotNull(StatementParser.parse("five => intoField|type"));
        assertNotNull(StatementParser.parse("=> emptyValue"));
    }

    @Test
    void transforms() {
        Field field = new Field(new FieldName("time_ymd"), Optional.of(new FieldType(new FieldTypeName("DateTime"), Optional.of(new FieldTypeParam("yyyyMMdd", Optional.empty())))));
        List<Field> results = ((UnaryOperation) StatementParser.parse("time   +>   time_ymd|DateTime|yyyyMMdd")).results();
        assertThat(results).hasSize(1).map(Objects::toString).contains(field.toString());

        assertNotNull(StatementParser.parse("one"));
        assertNotNull(StatementParser.parse("one|string"));
        assertNotNull(StatementParser.parse("two->@two"));
        assertNotNull(StatementParser.parse("three+>@three|DateTime|yyyyMMdd"));
        assertNotNull(StatementParser.parse("four->"));
        assertNotNull(StatementParser.parse("\"five\"=>_five"));
        assertNotNull(StatementParser.parse("five=>_five"));
        assertNotNull(StatementParser.parse("1689820455=>six|DateTime|yyyyMMdd"));
    }

    @Test
    void intrinsic() {
        Operation operation = StatementParser.parse("fromField1+fromField2+fromFieldN ^siphash{prefix:\"{sip-\",postfix:\"-xx\",returnNull:true} +> intoField|string");
        assertThat(operation.arguments())
                .hasSize(3);
        assertThat(operation
                .<Intrinsic>exp()
                .params()
                .params())
                .containsEntry("prefix", "{sip-") // confirm { is retained
                .containsEntry("postfix", "-xx")
                .containsEntry("returnNull", "true");

        assertThat(operation.results())
                .hasSize(1);
    }

    @Test
    void joins() {
        Join join = StatementParser.parse("lhs(lhsField1+lhsField2) rhs(rhsField1+rhsField2) +inner{} +> intoField|string");
        assertThat(join.joinType())
                .isEqualTo(JoinType.inner);

        assertThat(join.relations())
                .hasSize(2);

        assertThat(join.relations())
                .contains(
                        new Rel("lhs", List.of(FieldParser.parseField("lhsField1"), FieldParser.parseField("lhsField2"))),
                        new Rel("rhs", List.of(FieldParser.parseField("rhsField1"), FieldParser.parseField("rhsField2")))
                );

        assertThat(join.results())
                .hasSize(1);
    }
}
