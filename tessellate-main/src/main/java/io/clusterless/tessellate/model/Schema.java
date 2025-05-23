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
import io.clusterless.tessellate.util.json.JsonSimpleView;

import java.util.ArrayList;
import java.util.List;

public class Schema implements Model {
    private String name;
    private List<String> documentation = new ArrayList<>();
    @JsonSimpleView
    private List<Field> declared = new ArrayList<>();
    @JsonSimpleView
    private Format format;
    @JsonSimpleView
    private Compression compression = Compression.none;
    @JsonSimpleView
    private boolean embedsSchema = false;
    private String pattern;
    private boolean strictParsing = true;
    private String delimiterChar = null;
    private String quoteChar = "\"";
    @JsonSimpleView
    private String tableName;
    @JsonSimpleView
    private boolean createTable = true;

    public Schema() {
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

    public boolean strictParsing() {
        return strictParsing;
    }

    public String delimiterChar() {
        return delimiterChar;
    }

    public String quoteChar() {
        return quoteChar;
    }

    public String tableName() {
        return tableName;
    }

    public boolean createTable() {
        return createTable;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private List<String> documentation = new ArrayList<>();
        private List<Field> declared = new ArrayList<>();
        private Format format;
        private Compression compression = Compression.none;
        private boolean embedsSchema = false;
        private String pattern;
        private boolean strictParsing = true;
        private String delimiterChar = null;
        private String quoteChar = "\"";
        private String tableName;
        private boolean createTable = true;

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

        public Builder withStrictParsing(boolean strictParsing) {
            this.strictParsing = strictParsing;
            return this;
        }

        public Builder withDelimiterChar(String delimiterChar) {
            this.delimiterChar = delimiterChar;
            return this;
        }

        public Builder withQuoteChar(String quoteChar) {
            this.quoteChar = quoteChar;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withCreateTable(boolean createTable) {
            this.createTable = createTable;
            return this;
        }

        public Schema build() {
            Schema schema = new Schema();
            schema.name = this.name;
            schema.documentation = this.documentation;
            schema.declared = this.declared;
            schema.format = this.format;
            schema.compression = this.compression;
            schema.embedsSchema = this.embedsSchema;
            schema.pattern = this.pattern;
            schema.strictParsing = this.strictParsing;
            schema.delimiterChar = this.delimiterChar;
            schema.quoteChar = this.quoteChar;
            schema.tableName = this.tableName;
            schema.createTable = this.createTable;
            return schema;
        }
    }
}
