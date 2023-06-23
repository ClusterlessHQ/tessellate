/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.factory.hdfs.fs;

import io.clusterless.tessellate.factory.Observed;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class ObserveS3AFileSystem extends S3AFileSystem {
    public ObserveS3AFileSystem() {
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        Observed.INSTANCE.addWrite(f.toUri());
        return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        Observed.INSTANCE.addWrite(f.toUri());
        return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        Observed.INSTANCE.addRead(f.toUri());
        return super.open(f, bufferSize);
    }
}
