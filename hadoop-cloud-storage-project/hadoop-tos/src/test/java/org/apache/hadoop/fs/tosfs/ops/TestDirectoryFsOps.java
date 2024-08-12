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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.object.DirectoryStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.Assume;
import org.junit.BeforeClass;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;

// TODO change to directory bucket configuration.
public class TestDirectoryFsOps extends TestBaseFsOps {
  private final FsOps fsOps;

  public TestDirectoryFsOps() {
    super(ObjectStorageFactory.createWithPrefix(
        String.format("tos-%s/", UUIDUtils.random()), TOS_SCHEME, TestUtility.bucket(), new Configuration()));
    this.fsOps = new DirectoryFsOps((DirectoryStorage) storage, this::toFileStatus);
  }

  @BeforeClass
  public static void beforeClass() {
    Assume.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  public void testRenameDir() {
    // Will remove this test case once test environment support
    Assume.assumeTrue(storage.bucket().isDirectory());
  }

  @Override
  public void testRenameFile() {
    // Will remove this test case once test environment support
    Assume.assumeTrue(storage.bucket().isDirectory());
  }

  @Override
  public FsOps fsOps() {
    return fsOps;
  }

  @Override
  public void testCreateDirRecursive() {
    // Will remove this test case once test environment support
    Assume.assumeTrue(storage.bucket().isDirectory());
  }
}
