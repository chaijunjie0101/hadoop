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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.internal.model.CRC64Checksum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.common.Bytes;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.ChecksumType;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.object.MultipartUpload;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.object.exceptions.NotAppendableException;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.util.PureJavaCrc32C;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Checksum;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public class TestTOSObjectStorage {

  private final ObjectStorage tos;
  private final Checksum checksum;
  private final ChecksumType type;

  public TestTOSObjectStorage(ObjectStorage tos, Checksum checksum, ChecksumType checksumType) {
    this.tos = tos;
    this.checksum = checksum;
    this.type = checksumType;
  }

  @Parameterized.Parameters(name = "ObjectStorage = {0}, Checksum = {1}, ChecksumType = {2}")
  public static Iterable<Object[]> collections() {
    Assume.assumeTrue(TestEnv.checkTestEnabled());

    List<Object[]> values = new ArrayList<>();

    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_CHECKSUM_TYPE, ChecksumType.CRC64ECMA.name());
    values.add(new Object[] {
        ObjectStorageFactory.createWithPrefix(String.format("tos-%s/", UUIDUtils.random()),
            TOS_SCHEME, TestUtility.bucket(), conf), new CRC64Checksum(), ChecksumType.CRC64ECMA });

    conf = new Configuration();
    conf.set(TosKeys.FS_TOS_CHECKSUM_TYPE, ChecksumType.CRC32C.name());
    values.add(new Object[] {
        ObjectStorageFactory.createWithPrefix(String.format("tos-%s/", UUIDUtils.random()),
            TOS_SCHEME, TestUtility.bucket(), conf), new PureJavaCrc32C(), ChecksumType.CRC32C });

    return values;
  }

  @After
  public void tearDown() throws Exception {
    checksum.reset();

    CommonUtils.runQuietly(() -> tos.deleteAll(""));
    for (MultipartUpload upload : tos.listUploads("")) {
      tos.abortMultipartUpload(upload.key(), upload.uploadId());
    }
    tos.close();
  }

  @Test
  public void testHeadObj() {
    String key = "testPutChecksum";
    byte[] data = TestUtility.rand(1024);
    checksum.update(data, 0, data.length);
    assertEquals(checksum.getValue(), parseChecksum(tos.put(key, data)));

    ObjectInfo objInfo = tos.head(key);
    assertEquals(checksum.getValue(), parseChecksum(objInfo.checksum()));
  }

  @Test
  public void testGetFileStatus() {
    Assume.assumeFalse(tos.bucket().isDirectory());

    Configuration conf = new Configuration(tos.conf());
    conf.setBoolean(TosKeys.FS_TOS_GET_FILE_STATUS_ENABLED, true);
    tos.initialize(conf, tos.bucket().name());

    String key = "testFileStatus";
    byte[] data = TestUtility.rand(256);
    byte[] checksum = tos.put(key, data);

    ObjectInfo obj1 = tos.objectStatus(key);
    Assert.assertArrayEquals(checksum, obj1.checksum());
    Assert.assertEquals(key, obj1.key());
    Assert.assertEquals(obj1, tos.head(key));

    ObjectInfo obj2 = tos.objectStatus(key + "/");
    Assert.assertNull(obj2);

    String dirKey = "testDirStatus/";
    checksum = tos.put(dirKey, new byte[0]);

    ObjectInfo obj3 = tos.objectStatus("testDirStatus");
    Assert.assertArrayEquals(checksum, obj3.checksum());
    Assert.assertEquals(dirKey, obj3.key());
    Assert.assertEquals(obj3, tos.head(dirKey));
    Assert.assertNull(tos.head("testDirStatus"));
    ObjectInfo obj4 = tos.objectStatus(dirKey);
    Assert.assertArrayEquals(checksum, obj4.checksum());
    Assert.assertEquals(dirKey, obj4.key());
    Assert.assertEquals(obj4, tos.head(dirKey));

    String prefix = "testPrefix/";
    tos.put(prefix + "subfile", data);
    ObjectInfo obj5 = tos.objectStatus(prefix);
    Assert.assertEquals(prefix, obj5.key());
    Assert.assertArrayEquals(Constants.MAGIC_CHECKSUM, obj5.checksum());
    Assert.assertNull(tos.head(prefix));
    ObjectInfo obj6 = tos.objectStatus("testPrefix");
    Assert.assertEquals(prefix, obj6.key());
    Assert.assertArrayEquals(Constants.MAGIC_CHECKSUM, obj6.checksum());
    Assert.assertNull(tos.head("testPrefix"));
  }

  @Test
  public void testObjectStatus() {
    Assume.assumeFalse(tos.bucket().isDirectory());

    String key = "testObjectStatus";
    byte[] data = TestUtility.rand(1024);
    checksum.update(data, 0, data.length);
    assertEquals(checksum.getValue(), parseChecksum(tos.put(key, data)));

    ObjectInfo objInfo = tos.objectStatus(key);
    assertEquals(checksum.getValue(), parseChecksum(objInfo.checksum()));

    objInfo = tos.head(key);
    assertEquals(checksum.getValue(), parseChecksum(objInfo.checksum()));

    String dir = key + "/";
    tos.put(dir, new byte[0]);
    objInfo = tos.objectStatus(dir);
    assertEquals(Constants.MAGIC_CHECKSUM, objInfo.checksum());

    objInfo = tos.head(dir);
    assertEquals(Constants.MAGIC_CHECKSUM, objInfo.checksum());
  }

  @Test
  public void testListObjs() {
    String key = "testListObjs";
    byte[] data = TestUtility.rand(1024);
    checksum.update(data, 0, data.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(checksum.getValue(), parseChecksum(tos.put(key, data)));
    }

    ListObjectsRequest request =
        ListObjectsRequest.builder().prefix(key).startAfter(null).maxKeys(-1).delimiter("/")
            .build();
    Iterator<ListObjectsResponse> iter = tos.list(request).iterator();
    while (iter.hasNext()) {
      List<ObjectInfo> objs = iter.next().objects();
      for (ObjectInfo obj : objs) {
        assertEquals(checksum.getValue(), parseChecksum(obj.checksum()));
      }
    }
  }

  @Test
  public void testPutChecksum() {
    String key = "testPutChecksum";
    byte[] data = TestUtility.rand(1024);
    checksum.update(data, 0, data.length);

    byte[] checksumStr = tos.put(key, data);

    assertEquals(checksum.getValue(), parseChecksum(checksumStr));
  }

  @Test
  public void testMPUChecksum() {
    int partNumber = 2;
    String key = "testMPUChecksum";
    MultipartUpload mpu = tos.createMultipartUpload(key);
    byte[] data = TestUtility.rand(mpu.minPartSize() * partNumber);
    checksum.update(data, 0, data.length);

    List<Part> parts = new ArrayList<>();
    for (int i = 0; i < partNumber; i++) {
      final int index = i;
      Part part = tos.uploadPart(key, mpu.uploadId(), index + 1,
          () -> new ByteArrayInputStream(data, index * mpu.minPartSize(), mpu.minPartSize()),
          mpu.minPartSize());
      parts.add(part);
    }

    byte[] checksumStr = tos.completeUpload(key, mpu.uploadId(), parts);
    assertEquals(checksum.getValue(), parseChecksum(checksumStr));
  }

  @Test
  public void testAppendable() {
    Assume.assumeFalse(tos.bucket().isDirectory());

    // Test create object with append then append.
    byte[] data = TestUtility.rand(256);
    String prefix = "a/testAppendable/";
    String key = prefix + "object.txt";
    tos.append(key, data);

    tos.append(key, new byte[0]);

    // Test create object with put then append.
    data = TestUtility.rand(256);
    tos.put(key, data);

    assertThrows("Expect not appendable.", NotAppendableException.class,
        () -> tos.append(key, new byte[0]));

    tos.delete(key);
  }

  @Test
  public void testDirectoryBucketAppendable() {
    Assume.assumeTrue(tos.bucket().isDirectory());

    byte[] data = TestUtility.rand(256);
    String prefix = "a/testAppendable/";
    String key = prefix + "object.txt";
    tos.put(key, data);

    tos.append(key, new byte[1024]);

    tos.delete(key);
  }

  private long parseChecksum(byte[] checksum) {
    switch (type) {
    case CRC32C:
    case CRC64ECMA:
      return Bytes.toLong(checksum);
    default:
      throw new IllegalArgumentException(
          String.format("Checksum type %s is not supported by TOS.", type.name()));
    }
  }
}
