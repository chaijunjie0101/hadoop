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

package org.apache.hadoop.fs.tosfs.ops;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TempFiles;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestRenameOp extends TestBaseOps {
  private static final String FILE_STORE_ROOT = TempFiles.newTempDir("TestRenameOp");

  private final ExecutorService renamePool;
  private ExtendedRenameOp operation;

  public TestRenameOp(ObjectStorage storage) {
    super(storage);
    this.renamePool = ThreadPools.newWorkerPool("renamePool");
  }

  @Parameterized.Parameters
  public static List<ObjectStorage> createStorage() {
    Assume.assumeTrue(TestEnv.checkTestEnabled());
    return TestUtility.createTestObjectStorage(FILE_STORE_ROOT);
  }

  @After
  public void tearDown() {
    CommonUtils.runQuietly(() -> storage.deleteAll(""));
    CommonUtils.runQuietly(renamePool::shutdown);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    CommonUtils.runQuietly(() -> TempFiles.deleteDir(FILE_STORE_ROOT));
  }

  @Test
  public void testRenameFileDirectly() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(ConfKeys.FS_MULTIPART_COPY_THRESHOLD.key(storage.scheme()), 1L << 20);
    operation = new ExtendedRenameOp(conf, storage, renamePool);

    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    int dataSize = 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    Path dstFile = new Path(renameDest, filename);
    byte[] data = writeData(srcFile, dataSize);
    mkdir(renameDest);

    assertFileExist(srcFile);
    assertFileDoesNotExist(dstFile);
    assertDirExist(renameDest);

    operation.renameFile(srcFile, dstFile, data.length);
    assertFileDoesNotExist(srcFile);
    assertFileExist(dstFile);
    Map<String, List<Part>> uploadInfos = operation.uploadInfos;
    assertEquals("use put method when rename file, upload info's size should be 0", 0, uploadInfos.size());

    try (InputStream in = storage.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  @Test
  public void testRenameFileByUploadParts() throws IOException {
    Assume.assumeFalse(storage.bucket().isDirectory());
    Configuration conf = new Configuration();
    conf.setLong(ConfKeys.FS_MULTIPART_COPY_THRESHOLD.key(storage.scheme()), 1L << 20);
    operation = new ExtendedRenameOp(conf, storage, renamePool);

    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    int dataSize = 10 * 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    Path dstFile = new Path(renameDest, filename);
    byte[] data = writeData(srcFile, dataSize);
    mkdir(renameDest);

    assertFileExist(srcFile);
    assertFileDoesNotExist(dstFile);
    assertDirExist(renameDest);

    operation.renameFile(srcFile, dstFile, data.length);
    assertFileDoesNotExist(srcFile);
    assertFileExist(dstFile);
    Map<String, List<Part>> uploadInfos = operation.uploadInfos;
    assertTrue("use upload parts method when rename file, upload info's size should not be 0",
        uploadInfos.size() != 0);
    List<Part> parts = uploadInfos.get(ObjectUtils.pathToKey(dstFile));
    assertNotNull("use upload parts method when rename file, upload info should not be null", parts);
    assertTrue("use upload parts method when rename file, the num of upload parts should be greater than or equal" +
        " to 2", parts.size() >= 2);
    long fileLength = parts.stream().mapToLong(Part::size).sum();
    assertEquals(dataSize, fileLength);

    try (InputStream in = storage.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  private byte[] writeData(Path path, int size) {
    byte[] data = TestUtility.rand(size);
    touchFile(path, data);
    return data;
  }

  static class ExtendedRenameOp extends RenameOp {
    public Map<String, List<Part>> uploadInfos = Maps.newHashMap();

    ExtendedRenameOp(Configuration conf, ObjectStorage storage, ExecutorService pool) {
      super(conf, storage, pool);
    }

    @Override
    protected void finishUpload(String key, String uploadId, List<Part> uploadParts) {
      super.finishUpload(key, uploadId, uploadParts);
      if (!uploadInfos.isEmpty()) {
        uploadInfos.clear();
      }
      uploadInfos.put(key, uploadParts);
    }
  }
}
