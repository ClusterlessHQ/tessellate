/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Scanners;
import org.jparsec.error.ParserException;
import org.jparsec.pattern.CharPredicate;
import org.jparsec.pattern.CharPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static org.jparsec.pattern.CharPredicates.*;


public class FieldParser {
    private static final Logger LOG = LoggerFactory.getLogger(FieldParser.class);
    private static final CharPredicate FIELD_NAME_EXTRA = CharPredicates.among("~@#$%^&_");
    private static final CharPredicate PARAM_EXTRA = CharPredicates.not(among("|+")); // | delimits params, + delimits fields
    private static final Parser<FieldName> FIELD_NAME =
            Scanners.isChar(
                            or(CharPredicates.IS_ALPHA, FIELD_NAME_EXTRA)
                    )
                    .followedBy(Scanners.many(
                            or(CharPredicates.IS_ALPHA_NUMERIC, FIELD_NAME_EXTRA)
                    ))
                    .source()
                    .map(FieldName::new);

    private static final Parser<FieldOrdinal> FIELD_ORDINAL =
            Scanners.many1(
                            CharPredicates.range('0', '9')
                    )
                    .source()
                    .map(FieldOrdinal::new);

    private static final Parser<FieldTypeName> TYPE_NAME =
            Scanners.isChar(CharPredicates.IS_ALPHA)
                    .followedBy(Scanners.many(CharPredicates.IS_ALPHA_NUMERIC_))
                    .source()
                    .map(FieldTypeName::new);

    private static final Parser<String> TYPE_PARAM =
            Scanners.isChar(
                            or(
                                    CharPredicates.IS_ALPHA,
                                    CharPredicates.isChar('-') // null token for primitive values
                            )
                    )
                    .followedBy(Scanners.many(or(CharPredicates.IS_ALPHA_NUMERIC_, PARAM_EXTRA)))
                    .source();

    private static final Parser<Optional<FieldTypeParam>> typeParam =
            Parsers.sequence(
                            Scanners.isChar('|'),
                            TYPE_PARAM,
                            Parsers.sequence(
                                            Scanners.isChar('|'),
                                            TYPE_PARAM
                                    )
                                    .asOptional(),
                            (unused, s, d) -> new FieldTypeParam(s, d)
                    )
                    .asOptional();
    private static final Parser<FieldTypeName> typeName = Parsers.sequence(Scanners.isChar('|'), TYPE_NAME);

    private static final Parser<FieldType> types =
            Parsers.sequence(typeName, typeParam, FieldType::new);

    private static final Parser<Field> fullFieldDeclaration =
            Parsers.sequence(Parsers.or(FIELD_NAME, FIELD_ORDINAL), types.asOptional(), Field::new);

    private static final Parser<Void> FIELD_DELIM = Parsers.sequence(Scanners.many(IS_WHITESPACE), Scanners.isChar('+'), Scanners.many(IS_WHITESPACE));
    private static final Parser<List<Field>> fieldList =
            fullFieldDeclaration.sepBy(FIELD_DELIM);

    public static Field parseField(String field) {
        try {
            return fullFieldDeclaration.parse(field, Parser.Mode.DEBUG);
        } catch (ParserException e) {
            LOG.error("unable to parse: {}, got: {}, tree: {}", field, e.getMessage(), e.getParseTree());

            throw new RuntimeException(e);
        }
    }

    public static List<Field> parseFieldList(String field) {
        try {
            return fieldList.parse(field, Parser.Mode.DEBUG);
        } catch (ParserException e) {
            LOG.error("unable to parse: {}, got: {}, tree: {}", field, e.getMessage(), e.getParseTree());

            throw new RuntimeException(e);
        }
    }
}
