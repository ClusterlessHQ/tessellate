/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import java.util.Map;
import java.util.Optional;

public class IntrinsicParams {
    Map<String, String> params;

    public IntrinsicParams(Optional<Map<String, String>> params) {
        this.params = params.orElse(null);
    }

    public Map<String, String> params() {
        return params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IntrinsicParams{");
        sb.append("params=").append(params);
        sb.append('}');
        return sb.toString();
    }
}
