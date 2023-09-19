/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import io.clusterless.tessellate.parser.ast.Intrinsic;
import io.clusterless.tessellate.parser.ast.Operation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
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
        assertNotNull(StatementParser.parse("fromField1+fromField2+fromFieldN ^siphash{} +> intoField|type"));
        assertNotNull(StatementParser.parse("fromField1 + fromField2 + fromFieldN ^siphash{} +> intoField|type"));
        assertNotNull(StatementParser.parse("^tsid{node:1,nodeCount:10,signed:true,epoch:123} +> intoField|type"));

        assertNotNull(StatementParser.parse("fromField1+fromField2+fromFieldN ^siphash{} -> intoField|type"));
        assertNotNull(StatementParser.parse("fromField1 + fromField2 + fromFieldN ^siphash{} -> intoField|type"));
        assertNotNull(StatementParser.parse("fromField1 + fromField2 + fromFieldN ^siphash{prefix:'{:}'} -> intoField|type"));
        assertNotNull(StatementParser.parse("^tsid{node:1,nodeCount:10,signed:true,epoch:123} -> intoField|type"));

        assertNotNull(StatementParser.parse("five => intoField|type"));
    }

    @Test
    void transforms() {
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
}
