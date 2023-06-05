/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import cascading.tuple.Fields;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class FieldsHasher {

    public static long hash(Fields fields) {
        if (fields.isUnknown()) {
            return -1;
        }

        if (fields.isNone()) {
            return 0;
        }

        if (fields.isAll()) {
            return 1;
        }

        Hasher hasher = Hashing.sipHash24().newHasher();

        Iterator<Fields> fieldsIterator = fields.fieldsIterator();

        while (fieldsIterator.hasNext()) {
            Fields next = fieldsIterator.next();
            hasher.putUnencodedChars(next.get(0).toString());

            if (next.hasTypes()) {
                hasher.putString(next.getType(0).getClass().getName(), StandardCharsets.UTF_8);
            }
        }

        return hasher.hash().asLong();
    }
}
