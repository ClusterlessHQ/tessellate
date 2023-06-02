/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

public class Filename implements Model {
    private String prefix;
    private boolean includeGuid;
    private String providedGuid;
    private boolean includeFieldsHash;

    public static Builder builder() {
        return Builder.builder();
    }

    public String prefix() {
        return prefix;
    }

    public boolean includeGuid() {
        return includeGuid;
    }

    public String providedGuid() {
        return providedGuid;
    }

    public boolean includeFieldsHash() {
        return includeFieldsHash;
    }

    public static final class Builder {
        private String prefix;
        private boolean includeGuid;
        private String providedGuid;
        private boolean includeFieldsHash;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder withIncludeGuid(boolean includeGuid) {
            this.includeGuid = includeGuid;
            return this;
        }

        public Builder withProvidedGuid(String providedGuid) {
            this.providedGuid = providedGuid;
            return this;
        }

        public Builder withIncludeFieldsHash(boolean includeFieldsHash) {
            this.includeFieldsHash = includeFieldsHash;
            return this;
        }

        public Filename build() {
            Filename filename = new Filename();
            filename.providedGuid = this.providedGuid;
            filename.includeGuid = this.includeGuid;
            filename.prefix = this.prefix;
            filename.includeFieldsHash = this.includeFieldsHash;
            return filename;
        }
    }
}
