/**
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
package org.apache.hadoop.crypto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test crypto streams using normal stream which does not support the 
 * additional interfaces that the Hadoop FileSystem streams implement 
 * (Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor, 
 * CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess, Syncable, 
 * CanSetDropBehind)
 */
public class TestCryptoStreamsNormal extends CryptoStreamsTestBase {
  /**
   * Data storage.
   * {@link #getOutputStream(int, byte[], byte[])} will write to this buffer.
   * {@link #getInputStream(int, byte[], byte[])} will read from this buffer.
   */
  private byte[] buffer;
  private int bufferLen;
  
  @BeforeAll
  public static void init() throws Exception {
    Configuration conf = new Configuration();
    codec = CryptoCodec.getInstance(conf);
  }
  
  @AfterAll
  public static void shutdown() throws Exception {
  }

  @Override
  protected OutputStream getOutputStream(int bufferSize, byte[] key, byte[] iv)
      throws IOException {
    OutputStream out = new ByteArrayOutputStream() {
      @Override
      public void flush() throws IOException {
        buffer = buf;
        bufferLen = count;
      }
      @Override
      public void close() throws IOException {
        buffer = buf;
        bufferLen = count;
      }
    };
    return new CryptoOutputStream(out, codec, bufferSize, key, iv);
  }

  @Override
  protected InputStream getInputStream(int bufferSize, byte[] key, byte[] iv)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(buffer, 0, bufferLen);
    return new CryptoInputStream(in, codec, bufferSize, 
        key, iv);
  }
  
  @Disabled("Wrapped stream doesn't support Syncable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testSyncable() throws IOException {}
  
  @Disabled("Wrapped stream doesn't support PositionedRead")
  @Override
  @Test
  @Timeout(value = 10)
  public void testPositionedRead() throws IOException {}

  @Disabled("Wrapped stream doesn't support ByteBufferPositionedReadable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testPositionedReadWithByteBuffer() throws IOException {}

  @Disabled("Wrapped stream doesn't support ByteBufferPositionedReadable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testByteBufferReadFully() throws Exception {}

  @Disabled("Wrapped stream doesn't support ReadFully")
  @Override
  @Test
  @Timeout(value = 10)
  public void testReadFully() throws IOException {}
  
  @Disabled("Wrapped stream doesn't support Seek")
  @Override
  @Test
  @Timeout(value = 10)
  public void testSeek() throws IOException {}
  
  @Disabled("Wrapped stream doesn't support ByteBufferRead")
  @Override
  @Test
  @Timeout(value = 10)
  public void testByteBufferRead() throws IOException {}

  @Disabled("Wrapped stream doesn't support ByteBufferPositionedReadable")
  @Override
  @Test
  @Timeout(value = 10)
  public void testByteBufferPread() throws IOException {}
  
  @Disabled("Wrapped stream doesn't support ByteBufferRead, Seek")
  @Override
  @Test
  @Timeout(value = 10)
  public void testCombinedOp() throws IOException {}
  
  @Disabled("Wrapped stream doesn't support SeekToNewSource")
  @Override
  @Test
  @Timeout(value = 10)
  public void testSeekToNewSource() throws IOException {}
  
  @Disabled("Wrapped stream doesn't support HasEnhancedByteBufferAccess")
  @Override
  @Test
  @Timeout(value = 10)
  public void testHasEnhancedByteBufferAccess() throws IOException {}

  @Disabled("ByteArrayInputStream does not support unbuffer")
  @Override
  @Test
  public void testUnbuffer() throws Exception {}
}
