/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

public class Intrinsic implements Exp {
    IntrinsicName name;
    IntrinsicParams params;

    public Intrinsic(IntrinsicName name, IntrinsicParams params) {
        this.name = name;
        this.params = params;
    }

    public IntrinsicName name() {
        return name;
    }

    public IntrinsicParams params() {
        return params;
    }

    @Override
    public String toString() {
        return "^" + name + "{" + params() + "}";
    }
}
