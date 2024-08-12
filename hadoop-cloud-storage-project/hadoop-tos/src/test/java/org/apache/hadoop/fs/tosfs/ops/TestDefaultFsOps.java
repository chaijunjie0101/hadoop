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
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.util.Lists;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;

@RunWith(Parameterized.class)
public class TestDefaultFsOps extends TestBaseFsOps {
  private static ExecutorService threadPool;
  private final FsOps fsOps;

  public TestDefaultFsOps(Configuration conf) {
    super(ObjectStorageFactory.createWithPrefix(
        String.format("tos-%s/", UUIDUtils.random()), TOS_SCHEME, TestUtility.bucket(), conf));

    this.fsOps = new DefaultFsOps(storage, new Configuration(conf), threadPool, this::toFileStatus);
  }

  @Parameterized.Parameters(name = "conf = {0}")
  public static List<Configuration> createConf() {
    Configuration directRenameConf = new Configuration();
    directRenameConf.setBoolean(ConfKeys.FS_OBJECT_RENAME_ENABLED.key("tos"), true);
    directRenameConf.setBoolean(ConfKeys.FS_ASYNC_CREATE_MISSED_PARENT.key("tos"), false);

    Configuration copiedRenameConf = new Configuration();
    copiedRenameConf.setLong(ConfKeys.FS_MULTIPART_COPY_THRESHOLD.key("tos"), 1L << 20);
    copiedRenameConf.setBoolean(ConfKeys.FS_ASYNC_CREATE_MISSED_PARENT.key("tos"), false);
    return Lists.newArrayList(directRenameConf, copiedRenameConf);
  }

  @BeforeClass
  public static void beforeClass() {
    Assume.assumeTrue(TestEnv.checkTestEnabled());
    threadPool = ThreadPools.newWorkerPool("TestDefaultFsHelper-pool");
  }

  @AfterClass
  public static void afterClass() {
    if (!TestEnv.checkTestEnabled()) {
      return;
    }

    if (!threadPool.isShutdown()) {
      threadPool.shutdown();
    }
  }

  @Override
  public FsOps fsOps() {
    return fsOps;
  }
}
