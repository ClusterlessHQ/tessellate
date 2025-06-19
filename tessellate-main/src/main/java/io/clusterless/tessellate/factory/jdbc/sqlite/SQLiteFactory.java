/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.jdbc.sqlite;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.SinkFactory;
import io.clusterless.tessellate.factory.TapFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;
import io.clusterless.tessellate.util.Models;
import io.clusterless.tessellate.util.Protocol;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Set;

public class SQLiteFactory implements SinkFactory {
    public static TapFactory INSTANCE = new SQLiteFactory();

    @Override
    public Set<Protocol> getSinkProtocols() {
        return Set.of(Protocol.sqlite);
    }

    @Override
    public Set<Format> getFormats() {
        return Set.of(Format.sql);
    }

    @Override
    public Set<Compression> getCompressions() {
        return Set.of(Compression.none);
    }

    @Override
    public Tap<Properties, ?, ?> getSink(PipelineOptions pipelineOptions, Sink sinkModel, Fields currentFields) throws IOException {
        Fields declaredFields = declaredFields(sinkModel, currentFields);
        SQLiteScheme scheme = new SQLiteScheme(declaredFields);
        
        // Determine which tap type to use based on URI parameters
        // Use SQLiteTableTap for multi-table databases if 'mode=table' parameter is present
        // Otherwise use SQLiteTap for single-table databases (default behavior)
        boolean isTableMode = isTableMode(sinkModel);
        
        if (isTableMode) {
            return new SQLiteTableTap(scheme, sinkModel);
        } else {
            return new SQLiteTap(scheme, sinkModel);
        }
    }
    
    private boolean isTableMode(Sink sinkModel) {
        if (sinkModel.uris().isEmpty()) {
            return false;
        }
        
        URI uri = sinkModel.uris().get(0);
        String query = uri.getQuery();
        if (query != null) {
            for (String param : query.split("&")) {
                String[] keyValue = param.split("=", 2);
                if (keyValue.length == 2 && SQLiteConfig.MODE_PARAM.equals(keyValue[0])) {
                    return SQLiteConfig.TABLE_MODE_VALUE.equals(keyValue[1]);
                }
            }
        }
        
        // Default to single-table mode (SQLiteTap)
        return false;
    }

    private Fields declaredFields(Dataset dataset, Fields currentFields) {
        return currentFields.isDefined() ? currentFields : declaredFields(dataset);
    }

    private Fields declaredFields(Dataset dataset) {
        return Models.fieldAsFields(dataset.schema().declared(), null, Fields.ALL);
    }
}
