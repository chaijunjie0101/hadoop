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

import org.junit.Assert;
import org.junit.Test;

public class TestRawFSUtils {

  @Test
  public void testIsAncestor() {
    Assert.assertTrue(RawFSUtils.inSubtree("/", "/"));
    Assert.assertTrue(RawFSUtils.inSubtree("/", "/a"));
    Assert.assertTrue(RawFSUtils.inSubtree("/a", "/a"));
    Assert.assertFalse(RawFSUtils.inSubtree("/a", "/"));
    Assert.assertTrue(RawFSUtils.inSubtree("/", "/a/b/c"));
    Assert.assertFalse(RawFSUtils.inSubtree("/a/b/c", "/"));
    Assert.assertTrue(RawFSUtils.inSubtree("/", "/a/b/c.txt"));
    Assert.assertFalse(RawFSUtils.inSubtree("/a/b/c.txt", "/"));
    Assert.assertTrue(RawFSUtils.inSubtree("/a/b/", "/a/b"));
    Assert.assertTrue(RawFSUtils.inSubtree("/a/b/", "/a/b/c"));
    Assert.assertFalse(RawFSUtils.inSubtree("/a/b/c", "/a/b"));
  }
}
