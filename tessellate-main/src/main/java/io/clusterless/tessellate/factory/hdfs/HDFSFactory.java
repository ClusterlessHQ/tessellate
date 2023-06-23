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
import io.clusterless.tessellate.factory.ManifestReader;
import io.clusterless.tessellate.factory.ManifestWriter;
import io.clusterless.tessellate.factory.Observed;
import io.clusterless.tessellate.factory.hdfs.fs.ObserveLocalFileSystem;
import io.clusterless.tessellate.factory.hdfs.fs.ObserveS3AFileSystem;
import io.clusterless.tessellate.factory.local.FilesFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.pipeline.AWSOptions;
import io.clusterless.tessellate.pipeline.PipelineOptions;
import io.clusterless.tessellate.util.Property;
import io.clusterless.tessellate.util.Protocol;
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
import java.util.*;
import java.util.stream.Collectors;

public abstract class HDFSFactory extends FilesFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSFactory.class);

    static {
        // prevents npe when run inside a docker container on some hosts
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(System.getProperty("user.name", "nobody")));
    }

    @Override
    public Set<Protocol> getProtocols() {
        return Set.of(Protocol.file, Protocol.hdfs, Protocol.s3);
    }

    @Override
    public int openWritesThreshold() {
        return 10;
    }

    @Override
    protected Tap createTap(PipelineOptions pipelineOptions, Dataset dataset, Fields currentFields) throws IOException {
        boolean isSink = isSink(dataset);

        Fields declaredFields = declaredFields(dataset, currentFields);

        Properties local = initLocalProperties(pipelineOptions, dataset, declaredFields);

        ManifestReader manifestReader = ManifestReader.from(dataset);

        List<URI> uris = manifestReader.uris(local);

        URI commonRoot = manifestReader.findCommonRoot(local);

        LOG.info("{}: handing uris: {}, with common: {}", logPrefix(isSink), uris.size(), commonRoot);

        Scheme scheme = createScheme(pipelineOptions, dataset, declaredFields);

        Tap tap;

        if (isSink) {
            tap = createSinkTap(local, scheme, commonRoot, uris, ((Sink) dataset).manifest());
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

                manifestWriter.writeSuccess(conf);

                return result;
            }
        };
    }

    @NotNull
    private static String logPrefix(boolean isSink) {
        return isSink ? "writing" : "reading";
    }

    @NotNull
    protected Properties initLocalProperties(PipelineOptions pipelineOptions, Dataset dataset, Fields declaredFields) {
        String prefix = PART_NAME_DEFAULT;

        // hdfs always treat paths as directories, so we need to provide a prefix for the part files
        if (isSink(dataset)) {
            prefix = getPartFileName((Sink) dataset, declaredFields);
        }

        Properties local = new Properties();

        local.setProperty("mapred.output.direct." + S3AFileSystem.class.getSimpleName(), "true");
        local.setProperty("cascading.tapcollector.partname", String.format("%%s%%s%s-%%05d-%%05d", prefix));
        local.setProperty("mapreduce.input.fileinputformat.input.dir.recursive", "true");

        // intercept all writes against the s3a: and file: filesystem
        local.setProperty("mapred.output.direct." + ObserveS3AFileSystem.class.getSimpleName(), "true");
        local.setProperty("fs.s3a.impl", ObserveS3AFileSystem.class.getName());
        local.setProperty("fs.s3.impl", ObserveS3AFileSystem.class.getName());
        local.setProperty("mapred.output.direct." + ObserveLocalFileSystem.class.getSimpleName(), "true");
        local.setProperty("fs.file.impl", ObserveLocalFileSystem.class.getName());

        return applyAWSProperties(pipelineOptions, dataset, local);
    }

    protected abstract Scheme createScheme(PipelineOptions pipelineOptions, Dataset dataset, Fields declaredFields);

    protected Properties applyAWSProperties(PipelineOptions pipelineOptions, Dataset dataset, Properties local) {
        AWSOptions overrideAWSOptions = isSink(dataset) ? pipelineOptions.outputOptions() : pipelineOptions.inputOptions();
        List<AWSOptions> awsOptions = List.of(overrideAWSOptions, pipelineOptions);

        Optional<AWSOptions> hasAssumedRoleARN = awsOptions.stream()
                .filter(AWSOptions::hasAWSAssumedRoleARN)
                .findFirst();

        hasAssumedRoleARN.ifPresent(o -> local.setProperty(Constants.ASSUMED_ROLE_ARN, o.awsAssumedRoleARN()));

        Property.setIfNotNullFromSystem(local, Constants.ASSUMED_ROLE_ARN);

        if (local.containsKey(Constants.ASSUMED_ROLE_ARN)) {
            local.setProperty(Constants.ASSUMED_ROLE_SESSION_NAME, "role-session-" + System.currentTimeMillis());
            local.setProperty(Constants.AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.class.getName());
            local.setProperty(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER, getAWSCredentialProviders());
        } else {
            local.setProperty(Constants.AWS_CREDENTIALS_PROVIDER, getAWSCredentialProviders());
        }

        Optional<String> hasAWSEndpoint = awsOptions.stream()
                .filter(AWSOptions::hasAwsEndpoint)
                .map(AWSOptions::awsEndpoint)
                .findFirst();

        Property.setIfNotNullFromEnvThenSystem(local, "AWS_S3_ENDPOINT", Constants.ENDPOINT, hasAWSEndpoint.orElse(null));
        Property.setIfNotNullFromEnvThenSystem(local, "AWS_ACCESS_KEY_ID", Constants.ACCESS_KEY);
        Property.setIfNotNullFromEnvThenSystem(local, "AWS_SECRET_ACCESS_KEY", Constants.SECRET_KEY);
        Property.setIfNotNullFromSystem(local, Constants.SESSION_TOKEN);
        Property.setIfNotNullFromSystem(local, Constants.PROXY_HOST);
        Property.setIfNotNullFromSystem(local, Constants.PROXY_PORT);

        return local;
    }

    @NotNull
    private static Hfs createSourceTap(Properties local, Scheme scheme, URI commonURI, List<URI> uris) {
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
    private static Hfs createSinkTap(Properties local, Scheme scheme, URI commonURI, List<URI> uris, URI manifest) {
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

    private String getAWSCredentialProviders() {
        LinkedList<Class> list = new LinkedList<>(S3AUtils.STANDARD_AWS_PROVIDERS);

        list.addFirst(DefaultAWSCredentialsProviderChain.class);

        return list.stream().map(Class::getName).collect(Collectors.joining(","));
    }
}
