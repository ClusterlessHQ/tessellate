/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import io.clusterless.tessellate.parser.Printer;

import java.util.Optional;

public class FieldTypeParam {
    String param1;
    String param2;

    public FieldTypeParam(String param1, Optional<String> param2) {
        this.param1 = param1;
        this.param2 = param2.orElse(null);
    }

    public String param1() {
        return param1;
    }

    public String param2() {
        return param2;
    }

    @Override
    public String toString() {
        return Printer.withParams(param1, param2);
    }
}
