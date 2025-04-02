/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import io.clusterless.tessellate.parser.ast.*;
import org.jetbrains.annotations.NotNull;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Scanners;
import org.jparsec.Terminals;
import org.jparsec.pattern.CharPredicates;
import org.jparsec.pattern.Patterns;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.jparsec.Parsers.EOF;
import static org.jparsec.pattern.CharPredicates.*;

public class StatementParser {

    private static Parser<Void> op(String op) {
        return Patterns.string(op).toScanner(op);
    }

    private static final Parser<String> QUOTED_LITERAL = Parsers.or(
            Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER,
            Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER);

    public static final Parser<Op> ASSIGNMENT = op("=>").source().map(Op::new); // literal arguments only
    public static final Parser<Op> RETAIN = op("+>").source().map(Op::new);
    public static final Parser<Op> DISCARD = op("->").source().map(Op::new);

    public static Parser<Op> operators =
            Parsers.or(
                    ASSIGNMENT,
                    RETAIN,
                    DISCARD
            ).source().map(Op::new);

    public static Parser<Op> VARIABLE_OPS =
            Parsers.or(
                    RETAIN,
                    DISCARD
            ).source().map(Op::new);


    private static final Parser<Void> PARAM_DELIM = Parsers.sequence(Scanners.many(IS_WHITESPACE), Scanners.isChar(','), Scanners.many(IS_WHITESPACE));

    public static final Parser<String> LITERAL_VALUE =
            Parsers.or(
                    QUOTED_LITERAL,
                    Parsers.sequence(
                            Scanners.isChar(notAmong("'\"")),
                            Parsers.sequence(
                                    operators.not(),
                                    Scanners.isChar(
                                            CharPredicates.and(ALWAYS, CharPredicates.not(IS_WHITESPACE), CharPredicates.notAmong(",:{}"))
                                    )
                            ).many()
                    ).source()
            ).between(Scanners.many(IS_WHITESPACE), Scanners.many(IS_WHITESPACE));

    private static final Parser<Map.Entry<String, String>> PARAM_ENTRY =
            Parsers.sequence(
                    Scanners.isChar(CharPredicates.IS_ALPHA)
                            .followedBy(Scanners.many1(IS_ALPHA_NUMERIC_)).source()
                            .followedBy(Scanners.many(IS_WHITESPACE))
                            .followedBy(Scanners.isChar(':')
                                    .followedBy(Scanners.many(IS_WHITESPACE))),
                    LITERAL_VALUE
                            .followedBy(Scanners.many(IS_WHITESPACE)),
                    (Map::entry)
            );

    private static final Parser<Map<String, String>> PARAMS =
            PARAM_ENTRY.sepBy(PARAM_DELIM).map(l -> l.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    private static final Parser<IntrinsicName> INTRINSIC_NAME = Parsers.sequence(
            Scanners.isChar('^'),
            Scanners.many1(CharPredicates.IS_ALPHA).source(),
            (unused, intrinsic) -> new IntrinsicName(intrinsic)
    );
    public static final Parser<IntrinsicParams> INTRINSIC_PARAMS = Parsers.sequence(
            Scanners.isChar('{'),
            Scanners.many(IS_WHITESPACE),
            PARAMS.asOptional(),
            Scanners.many(IS_WHITESPACE),
            Scanners.isChar('}'),
            (unused, unused2, params, unused3, unused4) -> new IntrinsicParams(params)
    );

    // ^intrinsic{param1:value1, param2:value2}
    public static Parser<Intrinsic> INTRINSIC =
            Parsers.sequence(
                    INTRINSIC_NAME,
                    INTRINSIC_PARAMS,
                    Intrinsic::new
            );

    public static Parser<Operation> INTRINSIC_OPERATION_WITH_ARGUMENTS =
            Parsers.sequence(
                    FieldParser.FIELD_LIST.followedBy(Scanners.many(IS_WHITESPACE)),
                    INTRINSIC.followedBy(Scanners.many(IS_WHITESPACE)),
                    VARIABLE_OPS.followedBy(Scanners.many(IS_WHITESPACE)),
                    FieldParser.FIELD_LIST.followedBy(EOF),
                    NAryOperation::new
            );

    public static Parser<Operation> INTRINSIC_OPERATION_WITHOUT_ARGUMENTS =
            Parsers.sequence(
                    INTRINSIC.followedBy(Scanners.many(IS_WHITESPACE)),
                    VARIABLE_OPS.followedBy(Scanners.many(IS_WHITESPACE)),
                    FieldParser.FIELD_LIST.followedBy(EOF),
                    NAryOperation::new
            );

    public static Parser<Operation> TRANSFORM_COERCE = FieldParser.fullFieldDeclaration.followedBy(EOF)
            .map(UnaryOperation::new);

    public static Parser<Operation> TRANSFORM_DISCARD =
            Parsers.sequence(
                    FieldParser.fullFieldDeclaration.followedBy(Scanners.many(IS_WHITESPACE)),
                    DISCARD.followedBy(Scanners.many(IS_WHITESPACE)).followedBy(EOF),
                    UnaryOperation::new
            );

    public static Parser<Operation> TRANSFORM_RENAME =
            Parsers.sequence(
                    FieldParser.fullFieldDeclaration.followedBy(Scanners.many(IS_WHITESPACE)),
                    DISCARD.followedBy(Scanners.many(IS_WHITESPACE)),
                    FieldParser.fullFieldDeclaration.followedBy(EOF),
                    UnaryOperation::new
            );

    public static Parser<Operation> TRANSFORM_COPY =
            Parsers.sequence(
                    FieldParser.fullFieldDeclaration.followedBy(Scanners.many(IS_WHITESPACE)),
                    RETAIN.followedBy(Scanners.many(IS_WHITESPACE)),
                    FieldParser.fullFieldDeclaration.followedBy(EOF),
                    UnaryOperation::new
            );

    public static Parser<Assignment> LITERAL_ASSIGNMENT =
            Parsers.or(
                    Parsers.sequence(
                            LITERAL_VALUE,
                            ASSIGNMENT.followedBy(Scanners.many(IS_WHITESPACE)),
                            FieldParser.fullFieldDeclaration.followedBy(EOF),
                            Assignment::new
                    ),
                    Parsers.sequence(
                            ASSIGNMENT.followedBy(Scanners.many(IS_WHITESPACE)),
                            FieldParser.fullFieldDeclaration.followedBy(EOF),
                            Assignment::new
                    )
            );

    private static final Parser<JoinType> JOIN_NAME = Parsers.sequence(
            Scanners.isChar('+'),
            joins()
                    .source(),
            (unused, type) -> JoinType.valueOf(type)
    );

    private static @NotNull Parser<Void> joins() {
        return Parsers.or(
                Arrays.stream(JoinType.values())
                        .map(type -> Scanners.stringCaseInsensitive(type.name()))
                        .collect(Collectors.toList())
        );
    }

    public static final Parser<Void> JOIN_PARAMS = Parsers.sequence(
            Scanners.isChar('{'),
            Scanners.many(IS_WHITESPACE),
            Scanners.isChar('}')
    );

    // +inner{}
    public static Parser<JoinType> JOIN =
            Parsers.sequence(
                    JOIN_NAME,
                    JOIN_PARAMS,
                    (type, unused) -> type
            );

    public static Parser<Join> JOIN_STATEMENT =
            Parsers.or(
                    Parsers.sequence(
                            FieldParser.RELATION_LIST.followedBy(Scanners.many(IS_WHITESPACE)),
                            JOIN.followedBy(Scanners.many(IS_WHITESPACE)),
                            Parsers.or(RETAIN, DISCARD).followedBy(Scanners.many(IS_WHITESPACE)),
                            FieldParser.FIELD_LIST.followedBy(EOF),
                            Join::new
                    ),
                    Parsers.sequence(
                            FieldParser.RELATION_LIST.followedBy(Scanners.many(IS_WHITESPACE)),
                            JOIN.followedBy(EOF),
                            Join::new
                    ));

    public static Parser<Statement> STATEMENTS = Parsers.or(
            LITERAL_ASSIGNMENT,
            TRANSFORM_COERCE,
            TRANSFORM_DISCARD,
            TRANSFORM_RENAME,
            TRANSFORM_COPY,
            INTRINSIC_OPERATION_WITH_ARGUMENTS,
            INTRINSIC_OPERATION_WITHOUT_ARGUMENTS,
            JOIN_STATEMENT
    );

    public static <T extends Statement> T parse(String parse) {
        //noinspection unchecked
        return (T) BaseParser.parse(STATEMENTS, parse);
    }

    public static String parseLiteral(String parse) {
        return BaseParser.parse(LITERAL_VALUE.followedBy(EOF), parse);
    }
}
