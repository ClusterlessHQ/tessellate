/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.parser.ast;

import io.clusterless.tessellate.parser.Printer;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class IntrinsicParams {
    Map<String, String> params;

    public IntrinsicParams(Optional<Map<String, String>> params) {
        this.params = params.orElse(null);
    }

    public Map<String, String> params() {
        return params;
    }

    public String getString(String param) {
        return params.get(param);
    }

    public Boolean getBoolean(String param) {
        String value = params.get(param);

        if (value == null) {
            return null;
        }

        return Boolean.parseBoolean(value);
    }

    public Integer getInteger(String param, Function<String, Integer> otherwise) {
        try {
            return getInteger(param);
        } catch (NumberFormatException e) {
            return otherwise.apply(getString(param));
        }
    }

    public Integer getInteger(String param) {
        String value = params.get(param);

        if (value == null) {
            return null;
        }

        return Integer.parseInt(value);
    }

    public Long getLong(String param) {
        String value = params.get(param);

        if (value == null) {
            return null;
        }

        return Long.parseLong(value);
    }

    @Override
    public String toString() {
        return Printer.params(params);
    }
}
