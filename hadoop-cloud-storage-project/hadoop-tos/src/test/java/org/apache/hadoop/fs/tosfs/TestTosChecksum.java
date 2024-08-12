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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.FileStoreKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.ChecksumType;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.util.TempFiles;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestTosChecksum {
  private static final String FILE_STORE_ROOT = TempFiles.newTempDir("TestTosChecksum");
  private static final String ALGORITHM_NAME = "mock-algorithm";
  private static final String PREFIX = UUIDUtils.random();

  @Parameterized.Parameters(name = "checksumType = {0}, conf = {1}, uri = {2}, objectStorage = {3}")
  public static Iterable<Object[]> createStorage() throws URISyntaxException {
    Assume.assumeTrue(TestEnv.checkTestEnabled());
    return createTestObjectStorage(FILE_STORE_ROOT);
  }

  public static Iterable<Object[]> createTestObjectStorage(String fileStoreRoot)
      throws URISyntaxException {
    List<Object[]> list = new ArrayList<>();

    // The 1st argument.
    Configuration fileStoreConf = new Configuration();
    fileStoreConf.set(FileStoreKeys.FS_FILESTORE_CHECKSUM_ALGORITHM, ALGORITHM_NAME);
    fileStoreConf.set(FileStoreKeys.FS_FILESTORE_CHECKSUM_TYPE, ChecksumType.MD5.name());
    fileStoreConf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key("filestore"), fileStoreRoot);

    URI uri0 = new URI("filestore://" + TestUtility.bucket() + "/");
    Object[] objs = new Object[] { ChecksumType.MD5, fileStoreConf, uri0,
        ObjectStorageFactory.create(uri0.getScheme(), uri0.getAuthority(), fileStoreConf) };
    list.add(objs);

    // The 2nd argument.
    Configuration tosConf = new Configuration();
    tosConf.set(TosKeys.FS_TOS_CHECKSUM_ALGORITHM, ALGORITHM_NAME);
    tosConf.set(TosKeys.FS_TOS_CHECKSUM_TYPE, ChecksumType.CRC32C.name());

    URI uri1 = new URI(TOS_SCHEME + "://" + TestUtility.bucket() + "/");
    objs = new Object[] { ChecksumType.CRC32C, tosConf, uri1,
        ObjectStorageFactory.create(uri1.getScheme(), uri1.getAuthority(), tosConf) };
    list.add(objs);

    return list;
  }

  @After
  public void tearDown() {
    objectStorage.deleteAll(PREFIX);
  }

  private ChecksumType type;
  private Configuration conf;
  private URI uri;
  private ObjectStorage objectStorage;

  public TestTosChecksum(ChecksumType type, Configuration conf, URI uri,
      ObjectStorage objectStorage) {
    this.type = type;
    this.conf = conf;
    this.uri = uri;
    this.objectStorage = objectStorage;
  }

  @Test
  public void testChecksumInfo() {
    assertEquals(ALGORITHM_NAME, objectStorage.checksumInfo().algorithm());
    assertEquals(type, objectStorage.checksumInfo().checksumType());
  }

  @Test
  public void testFileChecksum() throws Exception {
    try (RawFileSystem fs = new RawFileSystem()) {
      fs.initialize(uri, conf);
      Path file = new Path("/" + PREFIX, "testFileChecksum");
      fs.create(file).close();
      FileChecksum checksum = fs.getFileChecksum(file, Long.MAX_VALUE);
      assertEquals(ALGORITHM_NAME, checksum.getAlgorithmName());

      String key = file.toString().substring(1);
      byte[] checksumData = objectStorage.head(key).checksum();
      assertArrayEquals(checksumData, checksum.getBytes());
    }
  }
}
