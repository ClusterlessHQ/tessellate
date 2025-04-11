/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringHelper {
    public static String convertConsecutiveUpperCase(String input) {
        if (input == null || input.isEmpty()) {
            return null;
        }

        Pattern pattern = Pattern.compile("(?<=[A-Z])([A-Z]+)(?=[A-Z]|$)");
        Matcher matcher = pattern.matcher(input);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String group = matcher.group(1);
            matcher.appendReplacement(result, group.toLowerCase());
        }

        matcher.appendTail(result);

        return result.toString();
    }
}
