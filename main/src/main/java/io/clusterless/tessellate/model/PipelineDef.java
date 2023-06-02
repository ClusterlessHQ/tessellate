/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

public class PipelineDef implements Model {
    private String name;
    private AWS aws = new AWS();
    private Source source = new Source();
    private Sink sink = new Sink();

    public static Builder builder() {
        return Builder.builder();
    }

    public String name() {
        return name;
    }

    public AWS aws() {
        return aws;
    }

    public Source source() {
        return source;
    }

    public Sink sink() {
        return sink;
    }


    public static final class Builder {
        private String name;
        private AWS aws = new AWS();
        private Source source = new Source();
        private Sink sink = new Sink();

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withAws(AWS aws) {
            this.aws = aws;
            return this;
        }

        public Builder withSource(Source source) {
            this.source = source;
            return this;
        }

        public Builder withSink(Sink sink) {
            this.sink = sink;
            return this;
        }

        public PipelineDef build() {
            PipelineDef pipelineDef = new PipelineDef();
            pipelineDef.aws = this.aws;
            pipelineDef.source = this.source;
            pipelineDef.name = this.name;
            pipelineDef.sink = this.sink;
            return pipelineDef;
        }
    }
}
