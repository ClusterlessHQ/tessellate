/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import cascading.tuple.Fields;
import io.clusterless.tessellate.model.Field;
import io.clusterless.tessellate.model.Partition;
import io.clusterless.tessellate.model.Translate;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Models {
    public static Fields fieldAsFields(List<Field> fields, Type defaultType, Fields defaultFields) {
        Fields result = Fields.NONE;

        for (Field field : fields) {
            Fields to = field.fields();

            if (to.isAll() || to.isUnknown()) {
                continue;
            }

            if (!to.isDefined()) {
                throw new IllegalArgumentException("unsupported field: " + to);
            }

            if (!to.hasTypes()) {
                to = to.applyTypes(defaultType);
            }

            result = result.append(to);
        }

        if (result.isNone()) {
            return defaultFields;
        }

        return result;
    }

    public static Fields partitionsAsFields(List<? extends Partition> partitions, Type defaultType) {
        Fields result = Fields.NONE;

        for (Partition partition : partitions) {
            Fields to = ((Translate) partition).to().fields();

            if (!to.hasTypes()) {
                to = to.applyTypes(defaultType);
            }

            result = result.append(to);
        }

        return result;
    }

    public static Map<String, String> partitionsAsMap(List<? extends Partition> partitions) {
        Map<String, String> result = new HashMap<>();

        for (Partition partition : partitions) {
            Field to = ((Translate) partition).to();
            Field from = ((Translate) partition).from().orElse(to);

            result.put(from.fields().get(0).toString(), to.fields().get(0).toString());
        }

        return result;
    }
}
