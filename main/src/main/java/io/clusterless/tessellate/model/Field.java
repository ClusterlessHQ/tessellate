/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import cascading.tuple.Fields;
import io.clusterless.tessellate.util.FieldsParser;

public class Field {
    private Fields fields;

    public Field(String field) {
        this.fields = FieldsParser.INSTANCE.parseSingleFields(field, null);
    }

    public Field(Fields fields) {
        this.fields = fields;
    }

    public Fields fields() {
        return fields;
    }
}
