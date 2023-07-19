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

public class Sink implements Dataset, Model {
    private URI manifest;
    private String manifestLot;
    private URI output;
    private Schema schema = new Schema();
    private List<Partition> partitions = new ArrayList<>();
    private boolean namedPartitions = true;

    private Filename filename = new Filename();

    public static Builder builder() {
        return Builder.builder();
    }

    public URI output() {
        return output;
    }

    @Override
    public URI manifest() {
        return manifest;
    }

    public String manifestLot() {
        return manifestLot;
    }

    public Schema schema() {
        return schema;
    }

    @Override
    public List<URI> uris() {
        return List.of(output());
    }

    public List<Partition> partitions() {
        return partitions;
    }

    public boolean namedPartitions() {
        return namedPartitions;
    }

    public Filename filename() {
        return filename;
    }

    public static final class Builder {
        private URI manifest;
        private String manifestLot;
        private URI output;
        private Schema schema = new Schema();
        private List<Partition> partitions = new ArrayList<>();
        private boolean namedPartitions = true;
        private Filename filename = new Filename();

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

        public Builder withOutput(URI output) {
            this.output = output;
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

        public Builder withFilename(Filename filename) {
            this.filename = filename;
            return this;
        }

        public Sink build() {
            Sink sink = new Sink();
            sink.namedPartitions = this.namedPartitions;
            sink.manifest = this.manifest;
            sink.manifestLot = this.manifestLot;
            sink.schema = this.schema;
            sink.filename = this.filename;
            sink.output = this.output;
            sink.partitions = this.partitions;
            return sink;
        }
    }
}
