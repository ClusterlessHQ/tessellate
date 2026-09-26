/*
 * Copyright (c) 2023-2025 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs.aws;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * The aws sdk default credentials chain (system properties, environment, web identity, profile, container,
 * instance profile) as an s3a credentials provider.
 * <p>
 * s3a instantiates each listed provider per filesystem and closes it with the filesystem.
 * {@link DefaultCredentialsProvider#create()} returns a jvm-wide shared instance, so closing one filesystem
 * would close the chain every other filesystem holds; this builds a chain per instance instead.
 */
public class DefaultChainCredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {
    private final DefaultCredentialsProvider provider = DefaultCredentialsProvider.builder().build();

    @Override
    public AwsCredentials resolveCredentials() {
        return provider.resolveCredentials();
    }

    @Override
    public void close() {
        provider.close();
    }

    @Override
    public String toString() {
        return provider.toString();
    }
}
