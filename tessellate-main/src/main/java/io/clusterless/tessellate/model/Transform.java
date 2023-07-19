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
    private List<TransformOp> transforms = new ArrayList<>();

    public Transform(String... transforms) {
        this(List.of(transforms));
    }

    @JsonCreator
    public Transform(List<String> transforms) {
        transforms.forEach(this::addTransform);
    }

    public Transform() {
    }

    @JsonSetter
    public void addTransform(String transform) {
        // this needs to be more robust
        if (transform.endsWith("->")) {
            transforms.add(new DiscardOp(transform));
        } else if (transform.contains("->")) {
            transforms.add(new RenameOp(transform));
        } else if (transform.contains("+>")) {
            transforms.add(new CopyOp(transform));
        } else if (transform.contains("=>")) {
            transforms.add(new InsertOp(transform));
        } else if (transform.contains("!>")) {
            transforms.add(new EvalInsertOp(transform));
        } else {
            transforms.add(new CoerceOp(transform));
        }
    }

    public List<TransformOp> transformOps() {
        return transforms;
    }

    @JsonValue
    public List<String> transforms() {
        return transforms.stream().map(Object::toString).collect(Collectors.toList());
    }
}
