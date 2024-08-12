/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ObjectStorageTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStorageTestBase.class);
  protected Configuration conf;
  protected Configuration tosConf;
  protected Path testDir;
  protected FileSystem fs;
  protected String scheme;
  protected ObjectStorage storage;

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    LOG.info("The test temporary folder is {}", tempDir.getRoot().getAbsolutePath());

    String tempDirPath = tempDir.getRoot().getAbsolutePath();
    conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key("filestore"), tempDirPath);
    conf.set("fs.filestore.impl", LocalFileSystem.class.getName());
    tosConf = new Configuration(conf);
    // Set the environment variable for ObjectTestUtils#assertObject
    TestUtility.setSystemEnv(FileStore.ENV_FILE_STORAGE_ROOT, tempDirPath);

    testDir = new Path("filestore://" + FileStore.DEFAULT_BUCKET + "/", UUIDUtils.random());
    fs = testDir.getFileSystem(conf);
    scheme = testDir.toUri().getScheme();
    storage = ObjectStorageFactory.create(scheme, testDir.toUri().getAuthority(), tosConf);
  }

  @After
  public void tearDown() throws IOException {
    if (storage != null) {
      // List all keys with test dir prefix and delete them.
      String prefix = ObjectUtils.pathToKey(testDir);
      CommonUtils.runQuietly(() -> storage.deleteAll(prefix));
      // List all multipart uploads and abort them.
      CommonUtils.runQuietly(() -> {
        for (MultipartUpload upload : storage.listUploads(prefix)) {
          LOG.info("Abort the multipart upload {}", upload);
          storage.abortMultipartUpload(upload.key(), upload.uploadId());
        }
      });

      storage.close();
    }
  }
}
