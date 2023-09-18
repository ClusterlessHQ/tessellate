/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser;

public class FieldOrdinal implements FieldRef {
    Integer ordinal;

    public FieldOrdinal(CharSequence ordinal) {
        this.ordinal = Integer.parseInt(ordinal.toString());
    }

    @Override
    public boolean isOrdinal() {
        return true;
    }

    @Override
    public Comparable<?> asComparable() {
        return ordinal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FieldOrdinal{");
        sb.append("ordinal=").append(ordinal);
        sb.append('}');
        return sb.toString();
    }
}
