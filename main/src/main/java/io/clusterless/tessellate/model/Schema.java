/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import io.clusterless.tessellate.util.Compression;
import io.clusterless.tessellate.util.Format;

import java.util.ArrayList;
import java.util.List;

public class Schema implements Model {
    private String name;
    private List<String> documentation = new ArrayList<>();
    private List<Field> declared = new ArrayList<>();
    private Format format;
    private Compression compression = Compression.none;
    private boolean embedsSchema = false;
    private String pattern;

    public static Builder builder() {
        return Builder.builder();
    }

    public String name() {
        return name;
    }

    public List<String> documentation() {
        return documentation;
    }

    public boolean embedsSchema() {
        return embedsSchema;
    }

    public List<Field> declared() {
        return declared;
    }

    public Format format() {
        return format;
    }

    public Compression compression() {
        return compression;
    }

    public String pattern() {
        return pattern;
    }

    public static final class Builder {
        private String name;
        private List<String> documentation = new ArrayList<>();
        private List<Field> declared = new ArrayList<>();
        private Format format;
        private Compression compression = Compression.none;
        private boolean embedsSchema = false;
        private String pattern;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withDocumentation(List<String> documentation) {
            this.documentation = documentation;
            return this;
        }

        public Builder withDeclared(List<Field> declared) {
            this.declared = declared;
            return this;
        }

        public Builder withFormat(Format format) {
            this.format = format;
            return this;
        }

        public Builder withCompression(Compression compression) {
            this.compression = compression;
            return this;
        }

        public Builder withEmbedsSchema(boolean embedsSchema) {
            this.embedsSchema = embedsSchema;
            return this;
        }

        public Builder withPattern(String pattern) {
            this.pattern = pattern;
            return this;
        }

        public Schema build() {
            Schema schema = new Schema();
            schema.pattern = this.pattern;
            schema.format = this.format;
            schema.compression = this.compression;
            schema.documentation = this.documentation;
            schema.name = this.name;
            schema.embedsSchema = this.embedsSchema;
            schema.declared = this.declared;
            return schema;
        }
    }
}
