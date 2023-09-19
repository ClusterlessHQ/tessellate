/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonValue;
import io.clusterless.tessellate.parser.StatementParser;
import io.clusterless.tessellate.parser.ast.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Supports the following transforms
 * <p>
 * - copy - ts->ymd|DateTime|yyyyMMdd
 * - coerce - ts|DateTime|yyyyMMdd
 * - rename - ts->ymd|DateTime|yyyyMMdd
 * - insert - value=ymd|DateTime|yyyyMMdd
 */
public class Transform implements Model {
    private List<Statement> statements = new ArrayList<>();

    public Transform(String... transforms) {
        this(List.of(transforms));
    }

    @JsonCreator
    public Transform(List<String> transforms) {
        transforms.forEach(this::addStatement);
    }

    public Transform() {
    }

    @JsonSetter
    public void addStatement(String statement) {
        statements.add(StatementParser.parse(statement));
    }

    public List<Statement> statements() {
        return statements;
    }

    @JsonValue
    public List<String> statementsToString() {
        return statements.stream().map(Object::toString).collect(Collectors.toList());
    }
}
