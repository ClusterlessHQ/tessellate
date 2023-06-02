/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.model;

public class AWS implements Model {
    private String awsEndpoint;
    private String awsRegion;
    private String awsAssumedRoleARN;

    public static Builder builder() {
        return Builder.builder();
    }

    public String awsEndpoint() {
        return awsEndpoint;
    }

    public String awsRegion() {
        return awsRegion;
    }

    public String awsAssumedRoleARN() {
        return awsAssumedRoleARN;
    }

    public static final class Builder {
        private String awsEndpoint;
        private String awsRegion;
        private String awsAssumedRoleARN;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withAwsEndpoint(String awsEndpoint) {
            this.awsEndpoint = awsEndpoint;
            return this;
        }

        public Builder withAwsRegion(String awsRegion) {
            this.awsRegion = awsRegion;
            return this;
        }

        public Builder withAwsAssumedRoleARN(String awsAssumedRoleARN) {
            this.awsAssumedRoleARN = awsAssumedRoleARN;
            return this;
        }

        public AWS build() {
            AWS aWS = new AWS();
            aWS.awsRegion = this.awsRegion;
            aWS.awsAssumedRoleARN = this.awsAssumedRoleARN;
            aWS.awsEndpoint = this.awsEndpoint;
            return aWS;
        }
    }
}
