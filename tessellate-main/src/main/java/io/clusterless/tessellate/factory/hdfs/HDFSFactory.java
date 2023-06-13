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
import io.clusterless.tessellate.factory.local.FilesFactory;
import io.clusterless.tessellate.model.Dataset;
import io.clusterless.tessellate.model.Sink;
import io.clusterless.tessellate.pipeline.AWSOptions;
import io.clusterless.tessellate.pipeline.PipelineOptions;
import io.clusterless.tessellate.util.Property;
import io.clusterless.tessellate.util.Protocol;
import io.clusterless.tessellate.util.URIs;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public abstract class HDFSFactory extends FilesFactory {
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
    protected Tap createTap(PipelineOptions pipelineOptions, Dataset dataset, Fields currentFields) {
        List<URI> uris = dataset.uris().stream()
                .map(URIs::copyWithoutQuery)
                .map(this::s3ToS3a)
                .collect(Collectors.toList());

        URI commonRoot = findCommonRoot(uris);

        Fields declaredFields = isSink(dataset) && currentFields.isDefined() ? currentFields : declaredFields(dataset);

        Properties local = initLocalProperties(pipelineOptions, dataset, declaredFields);

        Scheme scheme = createScheme(pipelineOptions, dataset, declaredFields);

        Tap tap;

        if (isSink(dataset)) {
            tap = createSinkTap(local, scheme, commonRoot, uris);
        } else {
            tap = createSourceTap(local, scheme, commonRoot, uris);
        }

        Optional<Partition> partition = createPartition(dataset);

        if (partition.isPresent()) {
            tap = new PartitionTap((Hfs) tap, partition.get(), openWritesThreshold());
        }

        return new LocalHfsAdaptor(tap);
    }

    @NotNull
    private Properties initLocalProperties(PipelineOptions pipelineOptions, Dataset dataset, Fields declaredFields) {
        String prefix = PART_NAME_DEFAULT;

        // hdfs always treat paths as directories, so we need to provide a prefix for the part files
        if (isSink(dataset)) {
            prefix = getPartFileName((Sink) dataset, declaredFields);
        }

        Properties local = new Properties();

        local.setProperty("mapred.output.direct." + S3AFileSystem.class.getSimpleName(), "true");
        local.setProperty("cascading.tapcollector.partname", String.format("%%s%%s%s-%%05d-%%05d", prefix));
        local.setProperty("mapreduce.input.fileinputformat.input.dir.recursive", "true");

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

    private URI findCommonRoot(List<URI> uris) {
        if (uris.size() == 1) {
            return uris.get(0);
        }

        Set<String> roots = uris.stream()
                .map(Objects::toString)
                .collect(Collectors.toSet());

        String commonPrefix = StringUtils.getCommonPrefix(roots.toArray(new String[0]));

        if (commonPrefix.isEmpty()) {
            throw new IllegalArgumentException("to many unique roots, got: " + roots);
        }

        if (commonPrefix.endsWith("/")) {
            return URI.create(commonPrefix);
        }

        // remove the filename part of the path
        return URIs.trim(URI.create(commonPrefix), 1);
    }

    @NotNull
    private static Hfs createSourceTap(Properties local, Scheme scheme, URI finalURI, List<URI> uris) {
        String[] identifiers = uris.stream()
                .map(URI::toString)
                .collect(Collectors.toList())
                .toArray(new String[uris.size()]);

        return new Hfs(scheme, finalURI.toString(), SinkMode.UPDATE) {
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
    private static Hfs createSinkTap(Properties local, Scheme scheme, URI finalURI, List<URI> uris) {
        if (uris.size() > 1) {
            throw new IllegalArgumentException("cannot write to multiple uris, got: " + uris.stream().limit(10));
        }

        return new Hfs(scheme, finalURI.toString(), SinkMode.UPDATE) {
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

    protected URI s3ToS3a(URI uri) {
        if (uri.getScheme().equals("s3")) {
            uri = URIs.copyWithScheme(uri, "s3a");
        }

        return uri;
    }
}
