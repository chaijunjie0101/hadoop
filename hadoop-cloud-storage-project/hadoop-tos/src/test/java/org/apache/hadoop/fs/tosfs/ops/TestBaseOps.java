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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.RawFileSystem;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.junit.Assert;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public abstract class TestBaseOps {
  protected ObjectStorage storage;

  public TestBaseOps(ObjectStorage storage) {
    this.storage = storage;
  }

  public Path path(String... keys) {
    return new Path(String.format("/%s", Joiner.on("/").join(keys)));
  }

  public void assertFileExist(Path file) {
    assertNotNull(ObjectUtils.pathToKey(file));
  }

  public void assertFileDoesNotExist(String key) {
    assertNull(storage.head(key));
  }

  public void assertFileDoesNotExist(Path file) {
    assertFileDoesNotExist(ObjectUtils.pathToKey(file));
  }

  public void assertDirExist(String key) {
    assertNotNull(storage.head(key));
  }

  public void assertDirExist(Path path) {
    assertDirExist(ObjectUtils.pathToKey(path, true));
  }

  public void assertDirDoesNotExist(String key) {
    assertNull(storage.head(key));
  }

  public void assertDirDoesNotExist(Path path) {
    assertDirDoesNotExist(ObjectUtils.pathToKey(path, true));
  }

  public void mkdir(Path path) {
    storage.put(ObjectUtils.pathToKey(path, true), new byte[0]);
    assertDirExist(path);
  }

  public ObjectInfo touchFile(Path path, byte[] data) {
    byte[] checksum = storage.put(ObjectUtils.pathToKey(path), data);
    ObjectInfo obj = storage.head(ObjectUtils.pathToKey(path));
    Assert.assertArrayEquals(checksum, obj.checksum());
    return obj;
  }

  public FileStatus getFileStatus(Path path) {
    String key = ObjectUtils.pathToKey(path);
    return toFileStatus(storage.objectStatus(key));
  }

  public RawFileStatus toFileStatus(ObjectInfo obj) {
    long modifiedTime = RawFileSystem.dateToLong(obj.mtime());
    String path = String.format("%s://%s/%s", storage.scheme(), storage.bucket().name(), obj.key());
    return new RawFileStatus(obj.size(), obj.isDir(), 0, modifiedTime, new Path(path), "fake", obj.checksum());
  }
}
