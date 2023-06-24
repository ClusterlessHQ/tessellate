/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.local;

import cascading.tap.Tap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tap.partition.NamedPartition;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import io.clusterless.tessellate.factory.SinkFactory;
import io.clusterless.tessellate.factory.SourceFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.model.Source;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Models;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public abstract class FilesFactory implements SourceFactory, SinkFactory {
    public static final String PART_NAME_DEFAULT = "part";

    protected static boolean isSink(Dataset dataset) {
        return dataset instanceof Sink;
    }

    protected String getPartFileName(Sink sinkModel, Fields declaredFields) {
        String result = PART_NAME_DEFAULT;

        if (sinkModel.filename().prefix() != null && !sinkModel.filename().prefix().isEmpty()) {
            result = sinkModel.filename().prefix();
        }

        if (sinkModel.filename().includeFieldsHash()) {
            result = appendHyphen(result, Integer.toHexString(declaredFields.hashCode()));
        }

        if (sinkModel.filename().includeGuid()) {
            result = appendHyphen(result, getGUID(sinkModel));
        }

        return result;
    }

    protected String appendHyphen(String part, String append) {
        return String.format("%s-%s", part, append);
    }

    protected String getGUID(Sink options) {
        if (options.filename().providedGuid() != null) {
            return options.filename().providedGuid();
        }

        return UUID.randomUUID().toString();
    }

    @Override
    public Tap<Properties, ?, ?> getSource(PipelineOptions pipelineOptions, Source sourceModel) throws IOException {
        return createTap(pipelineOptions, sourceModel, Fields.NONE);
    }

    @Override
    public Tap<Properties, ?, ?> getSink(PipelineOptions pipelineOptions, Sink sinkModel, Fields currentFields) throws IOException {
        return createTap(pipelineOptions, sinkModel, currentFields);
    }

    public abstract int openWritesThreshold();

    protected abstract Tap<Properties, ?, ?> createTap(PipelineOptions pipelineOptions, Dataset dataset, Fields currentFields) throws IOException;

    protected boolean isLocalDirectory(URI uri) {
        Path path = uri.getScheme() == null ? Paths.get(uri.getPath()) : Paths.get(uri);

        if (Files.exists(path)) {
            return Files.isDirectory(path);
        } else {
            // assuming directory if no extension
            return path.getFileName().toString().lastIndexOf('.') == -1;
        }
    }

    protected Fields declaredFields(Dataset dataset) {
        return Models.fieldAsFields(dataset.schema().declared(), String.class, isSink(dataset) ? Fields.ALL : Fields.UNKNOWN);
    }

    protected Optional<Partition> createPartition(Dataset dataset) {
        if (dataset.partitions().isEmpty()) {
            return Optional.empty();
        }

        Fields partitionFields = Models.partitionsAsFields(dataset.partitions(), String.class);

        if (dataset.namedPartitions()) {
            return Optional.of(new NamedPartition(partitionFields, "/"));
        } else {
            return Optional.of(new DelimitedPartition(partitionFields, "/"));
        }
    }

    protected Fields declaredFields(Dataset dataset, Fields currentFields) {
        return isSink(dataset) && currentFields.isDefined() ? currentFields : declaredFields(dataset);
    }
}
