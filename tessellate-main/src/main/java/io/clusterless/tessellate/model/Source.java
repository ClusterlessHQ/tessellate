/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Source implements Dataset, Model {
    private URI manifest;
    private String manifestLot;
    private List<URI> inputs = new ArrayList<>();
    private Schema schema = new Schema();
    private List<Partition> partitions = new ArrayList<>();
    private boolean namedPartitions = true;
    private LineOptions lines = new LineOptions();
    private List<Field> select = new ArrayList<>();

    public static Builder builder() {
        return Builder.builder();
    }

    public URI manifest() {
        return manifest;
    }

    @Override
    public boolean hasManifest() {
        return manifest() != null;
    }

    public String manifestLot() {
        return manifestLot;
    }

    public List<URI> inputs() {
        return inputs;
    }

    public Schema schema() {
        return schema;
    }

    @Override
    public List<URI> uris() {
        return inputs();
    }

    public List<Field> select() {
        return select;
    }

    public List<Partition> partitions() {
        return partitions;
    }

    @Override
    public boolean namedPartitions() {
        return namedPartitions;
    }

    public LineOptions lines() {
        return lines;
    }

    public static final class Builder {
        private URI manifest;
        private String manifestLot;
        private List<URI> inputs = new ArrayList<>();
        private Schema schema = new Schema();
        private List<Partition> partitions = new ArrayList<>();
        private boolean namedPartitions = true;
        private LineOptions lines = new LineOptions();
        private List<Field> select = new ArrayList<>();

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withManifest(URI manifest) {
            this.manifest = manifest;
            return this;
        }

        public Builder withManifestLot(String manifestLot) {
            this.manifestLot = manifestLot;
            return this;
        }

        public Builder withInputs(List<URI> inputs) {
            this.inputs = inputs;
            return this;
        }

        public Builder withSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder withPartitions(List<Partition> partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder withNamedPartitions(boolean namedPartitions) {
            this.namedPartitions = namedPartitions;
            return this;
        }

        public Builder withLines(LineOptions lines) {
            this.lines = lines;
            return this;
        }

        public Builder withSelect(List<Field> select) {
            this.select = select;
            return this;
        }

        public Source build() {
            Source source = new Source();
            source.manifestLot = this.manifestLot;
            source.manifest = this.manifest;
            source.lines = this.lines;
            source.inputs = this.inputs;
            source.schema = this.schema;
            source.select = this.select;
            source.partitions = this.partitions;
            source.namedPartitions = this.namedPartitions;
            return source;
        }
    }
}
