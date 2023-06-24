/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory;

import cascading.tap.Tap;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.options.PipelineOptions;

import java.io.IOException;
import java.util.Properties;

public interface SourceFactory extends TapFactory {
    Tap<Properties, ?, ?> getSource(PipelineOptions pipelineOptions, Source sourceModel) throws IOException;
}
