/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

import java.util.Optional;

public class FieldType {
    FieldTypeName name;
    FieldTypeParam param;

    public FieldType(FieldTypeName name, Optional<FieldTypeParam> param) {
        this.name = name;
        this.param = param.orElse(null);
    }

    public FieldTypeName name() {
        return name;
    }

    public Optional<FieldTypeParam> param() {
        return Optional.ofNullable(param);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FieldType{");
        sb.append("name='").append(name).append('\'');
        sb.append(", param=").append(param);
        sb.append('}');
        return sb.toString();
    }
}
