/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.local.hadoop.LocalHfsAdaptor;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.clusterless.tessellate.factory.ManifestWriter;
import io.clusterless.tessellate.factory.Observed;
import io.clusterless.tessellate.factory.hdfs.fs.ObserveLocalFileSystem;
import io.clusterless.tessellate.factory.hdfs.fs.ObserveS3AFileSystem;
import io.clusterless.tessellate.factory.local.FilesFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.options.AWSOptions;
import io.clusterless.tessellate.options.PipelineOptions;
import io.clusterless.tessellate.util.Property;
import io.clusterless.tessellate.util.URIs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class FSFactory extends FilesFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FSFactory.class);

    static {
        // prevents npe when run inside a docker container on some hosts
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(System.getProperty("user.name", "nobody")));
    }

    @NotNull
    private static String logPrefix(boolean isSink) {
        return isSink ? "writing" : "reading";
    }

    @Override
    public int openWritesThreshold() {
        return 10;
    }

    @Override
    protected Tap<Properties, ?, ?> createTap(PipelineOptions pipelineOptions, Dataset dataset, Fields currentFields) throws IOException {
        boolean isSink = isSink(dataset);

        Fields declaredFields = declaredFields(dataset, currentFields);

        Properties local = getProperties(pipelineOptions, dataset, declaredFields);

        List<URI> uris = dataset.uris();

        URI commonRoot = uris.get(0);

        // uri is likely a directory or single file, let the Hfs tap handle it
        if (!isSink && dataset.hasManifest()) {
            commonRoot = URIs.findCommonPrefix(uris, dataset.partitions().size());
        }

        LOG.info("{}: handling uris: {}, with common: {}", logPrefix(isSink), uris.size(), commonRoot);

        Scheme scheme = createScheme(dataset, declaredFields);

        Tap tap;

        if (isSink) {
            tap = createSinkTap(local, scheme, commonRoot, uris);
        } else {
            tap = createSourceTap(local, scheme, commonRoot, uris);
        }

        Optional<Partition> partition = createPartition(dataset);

        if (partition.isPresent()) {
            LOG.info("{}: partitioning on: {}", logPrefix(isSink), partition.get().getPartitionFields());
            tap = new PartitionTap((Hfs) tap, partition.get(), openWritesThreshold());
        }

        ManifestWriter manifestWriter = ManifestWriter.from(dataset, commonRoot);

        return new LocalHfsAdaptor(tap) {
            @Override
            public boolean commitResource(Properties conf) throws IOException {
                boolean result = super.commitResource(conf);

                manifestWriter.writeManifest(conf);

                return result;
            }
        };
    }

    @NotNull
    private Hfs createSourceTap(Properties local, Scheme scheme, URI commonURI, List<URI> uris) {
        Observed.INSTANCE.reads(commonURI);

        String[] identifiers = uris.stream()
                .map(URI::toString)
                .toArray(String[]::new);

        return new Hfs(scheme, commonURI.toString(), SinkMode.UPDATE) {
            @Override
            public boolean isSink() {
                return false;
            }

            @Override
            public void sourceConfInit(FlowProcess<? extends Configuration> process, Configuration conf) {
                HadoopUtil.copyConfiguration(local, conf);

                applySourceConfInitIdentifiers(process, conf, identifiers);

                verifyNoDuplicates(conf);
            }

            @Override
            public Fields retrieveSourceFields(FlowProcess<? extends Configuration> flowProcess) {
                HadoopUtil.copyConfiguration(local, flowProcess.getConfig());
                return super.retrieveSourceFields(flowProcess);
            }
        };
    }

    @NotNull
    private Hfs createSinkTap(Properties local, Scheme scheme, URI commonURI, List<URI> uris) {
        if (uris.size() > 1) {
            throw new IllegalArgumentException("cannot write to multiple uris, got: " + uris.stream().limit(10));
        }

        Observed.INSTANCE.writes(commonURI);

        return new Hfs(scheme, commonURI.toString(), SinkMode.UPDATE) {
            @Override
            public void sourceConfInit(FlowProcess<? extends Configuration> process, Configuration conf) {
                HadoopUtil.copyConfiguration(local, conf);
                super.sourceConfInit(process, conf);
            }

            @Override
            public void sinkConfInit(FlowProcess<? extends Configuration> process, Configuration conf) {
                HadoopUtil.copyConfiguration(local, conf);
                super.sinkConfInit(process, conf);
            }
        };
    }

    @Override
    public void applyGlobalProperties(Properties properties) {
        properties.setProperty("mapred.output.direct." + S3AFileSystem.class.getSimpleName(), "true");
        properties.setProperty("mapreduce.input.fileinputformat.input.dir.recursive", "true");

        // intercept all writes against the s3a: and file: filesystem
        properties.setProperty("mapred.output.direct." + ObserveS3AFileSystem.class.getSimpleName(), "true");
        properties.setProperty("fs.s3a.impl", ObserveS3AFileSystem.class.getName());
        properties.setProperty("fs.s3.impl", ObserveS3AFileSystem.class.getName());
        properties.setProperty("mapred.output.direct." + ObserveLocalFileSystem.class.getSimpleName(), "true");
        properties.setProperty("fs.file.impl", ObserveLocalFileSystem.class.getName());

        properties.setProperty(Constants.AWS_CREDENTIALS_PROVIDER, getAWSCredentialProviders());
    }

    protected Properties getProperties(PipelineOptions pipelineOptions, Dataset dataset, Fields declaredFields) {
        Properties local = new Properties();

        // covers case when reading manifest
        applyGlobalProperties(local);

        local = applyAWSProperties(pipelineOptions, local, isSink(dataset));

        return applySinkProperties(dataset, declaredFields, local);
    }

    protected Properties applySinkProperties(Dataset dataset, Fields declaredFields, Properties local) {
        if (!isSink(dataset)) {
            return local;
        }

        // hdfs always treat paths as directories, so we need to provide a prefix for the part files
        String prefix = getPartFileName((Sink) dataset, declaredFields);
        String format = String.format("%%s%%s%s-%%05d-%%05d", prefix);
        local.setProperty("cascading.tapcollector.partname", format);

        LOG.info("sinking to prefix: {}", prefix);

        return local;
    }

    protected abstract Scheme createScheme(Dataset dataset, Fields declaredFields);

    protected Properties applyAWSProperties(PipelineOptions pipelineOptions, Properties properties, boolean isSink) {
        AWSOptions overrideAWSOptions = isSink ? pipelineOptions.outputOptions() : pipelineOptions.inputOptions();
        List<AWSOptions> awsOptions = List.of(overrideAWSOptions, pipelineOptions);

        Optional<AWSOptions> hasAssumedRoleARN = awsOptions.stream()
                .filter(AWSOptions::hasAWSAssumedRoleARN)
                .findFirst();

        hasAssumedRoleARN.ifPresent(o -> properties.setProperty(Constants.ASSUMED_ROLE_ARN, o.awsAssumedRoleARN()));

        Property.setIfNotNullFromSystem(properties, Constants.ASSUMED_ROLE_ARN);

        if (properties.containsKey(Constants.ASSUMED_ROLE_ARN)) {
            properties.setProperty(Constants.ASSUMED_ROLE_SESSION_NAME, "role-session-" + System.currentTimeMillis());
            properties.setProperty(Constants.AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.class.getName());
            properties.setProperty(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER, getAWSCredentialProviders());
        } else {
            properties.setProperty(Constants.AWS_CREDENTIALS_PROVIDER, getAWSCredentialProviders());
        }

        Optional<String> hasAWSEndpoint = awsOptions.stream()
                .filter(AWSOptions::hasAwsEndpoint)
                .map(AWSOptions::awsEndpoint)
                .findFirst();

        Property.setIfNotNullFromEnvThenSystem(properties, "AWS_S3_ENDPOINT", Constants.ENDPOINT, hasAWSEndpoint.orElse(null));
        Property.setIfNotNullFromEnvThenSystem(properties, "AWS_ACCESS_KEY_ID", Constants.ACCESS_KEY);
        Property.setIfNotNullFromEnvThenSystem(properties, "AWS_SECRET_ACCESS_KEY", Constants.SECRET_KEY);
        Property.setIfNotNullFromSystem(properties, Constants.SESSION_TOKEN);
        Property.setIfNotNullFromSystem(properties, Constants.PROXY_HOST);
        Property.setIfNotNullFromSystem(properties, Constants.PROXY_PORT);

        return properties;
    }

    private String getAWSCredentialProviders() {
        LinkedList<Class> list = new LinkedList<>(S3AUtils.STANDARD_AWS_PROVIDERS);

        list.addFirst(DefaultAWSCredentialsProviderChain.class);

        return list.stream()
                .map(Class::getName)
                .collect(Collectors.joining(","));
    }
}
