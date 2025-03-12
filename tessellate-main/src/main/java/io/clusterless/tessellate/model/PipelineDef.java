/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

import io.clusterless.tessellate.util.json.JsonSimpleView;

import java.util.LinkedHashMap;
import java.util.Map;

public class PipelineDef implements Model {
    private String name;
    private AWS aws = new AWS();

    private Source source; // null if not used
    private Map<String, Source> sources = new LinkedHashMap<>();

    private Transform transform = new Transform();

    private Sink sink = new Sink();

    public static Builder builder() {
        return Builder.builder();
    }

    public static PipelineDef defaultPipelineDef() {
        return builder().withSource(new Source()).build();
    }

    public PipelineDef() {
    }


    public String name() {
        return name;
    }

    public AWS aws() {
        return aws;
    }

    @JsonSimpleView
    public Source source() {
        return source;
    }

    public Map<String, Source> sources() {
        return sources;
    }

    @JsonSimpleView
    public Transform transform() {
        return transform;
    }

    @JsonSimpleView
    public Sink sink() {
        return sink;
    }

    public static final class Builder {
        private String name;
        private AWS aws = new AWS();
        private Source source;
        private Map<String, Source> sources = new LinkedHashMap<>();
        private Transform transform = new Transform();
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

        public Builder withSources(Map<String, Source> sources) {
            this.sources = sources;
            return this;
        }

        public Builder withTransform(Transform transform) {
            this.transform = transform;
            return this;
        }

        public Builder withSink(Sink sink) {
            this.sink = sink;
            return this;
        }

        public PipelineDef build() {
            PipelineDef pipelineDef = new PipelineDef();
            pipelineDef.name = this.name;
            pipelineDef.aws = this.aws;
            pipelineDef.transform = this.transform;
            pipelineDef.sink = this.sink;
            pipelineDef.source = this.source;
            pipelineDef.sources = this.sources;
            return pipelineDef;
        }
    }
}
