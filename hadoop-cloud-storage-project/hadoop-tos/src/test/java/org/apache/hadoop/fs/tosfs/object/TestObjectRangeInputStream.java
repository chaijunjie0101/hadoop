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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestObjectRangeInputStream extends ObjectStorageTestBase {

  @Test
  public void testRead() throws IOException {
    Path outPath = new Path(testDir, "testRead.txt");
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(1 << 10);
    storage.put(key, rawData);
    ObjectContent content = storage.get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    int position = 100;
    int len = 200;
    try (ObjectRangeInputStream ri =
             new ObjectRangeInputStream(storage, key, Range.of(position, len), content.checksum())) {
      // Test read byte.
      assertEquals(rawData[position] & 0xff, ri.read());

      // Test read buffer.
      byte[] buffer = new byte[len];
      assertEquals(buffer.length - 1, ri.read(buffer, 0, buffer.length));
      assertArrayEquals(
          Arrays.copyOfRange(rawData, position + 1, position + len),
          Arrays.copyOfRange(buffer, 0, buffer.length - 1));
      assertEquals(0, ri.available());

      assertEquals(-1, ri.read());
      assertEquals(-1, ri.read(buffer, 0, buffer.length));
    }
  }

  @Test
  public void testRangeExceedInnerStream() throws IOException {
    Path outPath = new Path(testDir, "testRangeExceedInnerStream.txt");
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(10);
    storage.put(key, rawData);
    ObjectContent content = storage.get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    int position = 10;
    int badLen = 10;
    try (ObjectRangeInputStream ri =
             new ObjectRangeInputStream(storage, key, Range.of(position, badLen), content.checksum())) {
      byte[] buffer = new byte[1];
      assertEquals(-1, ri.read());
      assertEquals(-1, ri.read(buffer, 0, buffer.length));
    }
  }

  @Test
  public void testRangeInclude() throws IOException {
    Path outPath = new Path(testDir, "testRangeInclude.txt");
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(10);
    storage.put(key, rawData);
    ObjectContent content = storage.get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    long pos = 100;
    long len = 300;

    try (ObjectRangeInputStream in = new ObjectRangeInputStream(storage, key, Range.of(pos, len), content.checksum())) {
      assertEquals(Range.of(pos, len), in.range());

      assertTrue(in.include(pos));
      assertTrue(in.include((pos + len) / 2));
      assertTrue(in.include(pos + len - 1));

      assertFalse(in.include(pos - 1));
      assertFalse(in.include(pos + len));
    }
  }

  @Test
  public void testSeek() throws IOException {
    Path outPath = new Path(testDir, "testSeek.txt");
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(1 << 10);
    storage.put(key, rawData);
    ObjectContent content = storage.get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    long pos = 100;
    long len = 300;

    try (ObjectRangeInputStream in = new ObjectRangeInputStream(storage, key, Range.of(pos, len), content.checksum())) {
      assertEquals(pos, in.getPos());

      Exception error = assertThrows("Overflow", IllegalArgumentException.class, () -> in.seek(-1));
      assertTrue(error.getMessage().contains("must be in range Range{offset=100, length=300}"));
      error = assertThrows("Overflow", IllegalArgumentException.class, () -> in.seek(99));
      assertTrue(error.getMessage().contains("must be in range Range{offset=100, length=300}"));
      error = assertThrows("Overflow", IllegalArgumentException.class, () -> in.seek(401));
      assertTrue(error.getMessage().contains("must be in range Range{offset=100, length=300}"));
      error = assertThrows("Overflow", IllegalArgumentException.class, () -> in.seek(1 << 20));
      assertTrue(error.getMessage().contains("must be in range Range{offset=100, length=300}"));

      in.seek(399);
      assertTrue(0 <= in.read());
      assertEquals(-1, in.read());

      in.seek(100);
      assertTrue(in.read() >= 0);
    }
  }
}
