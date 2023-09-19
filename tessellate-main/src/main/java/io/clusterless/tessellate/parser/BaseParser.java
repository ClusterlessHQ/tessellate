/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import org.jparsec.Parser;
import org.jparsec.error.ParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(BaseParser.class);

    static <T> T parse(Parser<T> parser, String field) {
        try {
            return parser.parse(field, Parser.Mode.DEBUG);
        } catch (ParserException e) {
            LOG.error("unable to parse: {}, got: {}", field, e.getMessage().replace("\n", ""));

            throw new RuntimeException(e);
        }
    }
}
