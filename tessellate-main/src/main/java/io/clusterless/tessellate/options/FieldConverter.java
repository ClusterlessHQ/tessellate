/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.options;

import io.clusterless.tessellate.model.Field;
import picocli.CommandLine;

public class FieldConverter implements CommandLine.ITypeConverter<Field> {
    @Override
    public Field convert(String value) throws Exception {
        return new Field(value);
    }
}
