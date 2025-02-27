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
package org.apache.hadoop.mapreduce.v2.hs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreService.HistoryServerState;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

public class TestHistoryServerFileSystemStateStoreService {

  private static final File testDir = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      "TestHistoryServerFileSystemStateStoreService");

  private Configuration conf;

  @BeforeEach
  public void setup() {
    FileUtil.fullyDelete(testDir);
    testDir.mkdirs();
    conf = new Configuration();
    conf.setBoolean(JHAdminConfig.MR_HS_RECOVERY_ENABLE, true);
    conf.setClass(JHAdminConfig.MR_HS_STATE_STORE,
        HistoryServerFileSystemStateStoreService.class,
        HistoryServerStateStoreService.class);
    conf.set(JHAdminConfig.MR_HS_FS_STATE_STORE_URI,
        testDir.getAbsoluteFile().toURI().toString());
  }

  @AfterEach
  public void cleanup() {
    FileUtil.fullyDelete(testDir);
  }

  private HistoryServerStateStoreService createAndStartStore()
      throws IOException {
    HistoryServerStateStoreService store =
        HistoryServerStateStoreServiceFactory.getStore(conf);
    assertTrue(store instanceof HistoryServerFileSystemStateStoreService,
        "Factory did not create a filesystem store");
    store.init(conf);
    store.start();
    return store;
  }

  private void testTokenStore(String stateStoreUri) throws IOException {
    conf.set(JHAdminConfig.MR_HS_FS_STATE_STORE_URI, stateStoreUri);
    HistoryServerStateStoreService store = createAndStartStore();

    HistoryServerState state = store.loadState();
    assertTrue(state.tokenState.isEmpty(), "token state not empty");
    assertTrue(state.tokenMasterKeyState.isEmpty(), "key state not empty");

    final DelegationKey key1 = new DelegationKey(1, 2, "keyData1".getBytes());
    final MRDelegationTokenIdentifier token1 =
        new MRDelegationTokenIdentifier(new Text("tokenOwner1"),
            new Text("tokenRenewer1"), new Text("tokenUser1"));
    token1.setSequenceNumber(1);
    final Long tokenDate1 = 1L;
    final MRDelegationTokenIdentifier token2 =
        new MRDelegationTokenIdentifier(new Text("tokenOwner2"),
            new Text("tokenRenewer2"), new Text("tokenUser2"));
    token2.setSequenceNumber(12345678);
    final Long tokenDate2 = 87654321L;

    store.storeTokenMasterKey(key1);
    try {
      store.storeTokenMasterKey(key1);
      fail("redundant store of key undetected");
    } catch (IOException e) {
      // expected
    }
    store.storeToken(token1, tokenDate1);
    store.storeToken(token2, tokenDate2);
    try {
      store.storeToken(token1, tokenDate1);
      fail("redundant store of token undetected");
    } catch (IOException e) {
      // expected
    }
    store.close();

    store = createAndStartStore();
    state = store.loadState();
    assertEquals(2, state.tokenState.size(), "incorrect loaded token count");
    assertTrue(state.tokenState.containsKey(token1), "missing token 1");
    assertEquals(tokenDate1,
        state.tokenState.get(token1), "incorrect token 1 date");
    assertTrue(state.tokenState.containsKey(token2), "missing token 2");
    assertEquals(tokenDate2,
        state.tokenState.get(token2), "incorrect token 2 date");
    assertEquals(1,
        state.tokenMasterKeyState.size(), "incorrect master key count");
    assertTrue(state.tokenMasterKeyState.contains(key1), "missing master key 1");

    final DelegationKey key2 = new DelegationKey(3, 4, "keyData2".getBytes());
    final DelegationKey key3 = new DelegationKey(5, 6, "keyData3".getBytes());
    final MRDelegationTokenIdentifier token3 =
        new MRDelegationTokenIdentifier(new Text("tokenOwner3"),
            new Text("tokenRenewer3"), new Text("tokenUser3"));
    token3.setSequenceNumber(12345679);
    final Long tokenDate3 = 87654321L;

    store.removeToken(token1);
    store.storeTokenMasterKey(key2);
    final Long newTokenDate2 = 975318642L;
    store.updateToken(token2, newTokenDate2);
    store.removeTokenMasterKey(key1);
    store.storeTokenMasterKey(key3);
    store.storeToken(token3, tokenDate3);
    store.close();

    store = createAndStartStore();
    state = store.loadState();
    assertEquals(2, state.tokenState.size(), "incorrect loaded token count");
    assertFalse(state.tokenState.containsKey(token1), "token 1 not removed");
    assertTrue(state.tokenState.containsKey(token2), "missing token 2");
    assertEquals(newTokenDate2,
        state.tokenState.get(token2), "incorrect token 2 date");
    assertTrue(state.tokenState.containsKey(token3), "missing token 3");
    assertEquals(tokenDate3,
        state.tokenState.get(token3), "incorrect token 3 date");
    assertEquals(2,
        state.tokenMasterKeyState.size(), "incorrect master key count");
    assertFalse(state.tokenMasterKeyState.contains(key1),
        "master key 1 not removed");
    assertTrue(state.tokenMasterKeyState.contains(key2),
        "missing master key 2");
    assertTrue(state.tokenMasterKeyState.contains(key3),
        "missing master key 3");
  }

  @Test
  public void testTokenStore() throws IOException {
    testTokenStore(testDir.getAbsoluteFile().toURI().toString());
  }

  @Test
  public void testTokenStoreHdfs() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    conf = cluster.getConfiguration(0);
    try {
      testTokenStore("/tmp/historystore");
    } finally {
        cluster.shutdown();
    }
  }

  @Test
  public void testUpdatedTokenRecovery() throws IOException {
    IOException intentionalErr = new IOException("intentional error");
    FileSystem fs = FileSystem.getLocal(conf);
    final FileSystem spyfs = spy(fs);
    // make the update token process fail halfway through where we're left
    // with just the temporary update file and no token file
    ArgumentMatcher<Path> updateTmpMatcher =
        arg -> arg.getName().startsWith("update");
    doThrow(intentionalErr)
        .when(spyfs).rename(argThat(updateTmpMatcher), isA(Path.class));

    conf.set(JHAdminConfig.MR_HS_FS_STATE_STORE_URI,
        testDir.getAbsoluteFile().toURI().toString());
    HistoryServerStateStoreService store =
        new HistoryServerFileSystemStateStoreService() {
          @Override
          FileSystem createFileSystem() throws IOException {
            return spyfs;
          }
    };
    store.init(conf);
    store.start();

    final MRDelegationTokenIdentifier token1 =
        new MRDelegationTokenIdentifier(new Text("tokenOwner1"),
            new Text("tokenRenewer1"), new Text("tokenUser1"));
    token1.setSequenceNumber(1);
    final Long tokenDate1 = 1L;
    store.storeToken(token1, tokenDate1);
    final Long newTokenDate1 = 975318642L;
    try {
      store.updateToken(token1, newTokenDate1);
      fail("intentional error not thrown");
    } catch (IOException e) {
      assertEquals(intentionalErr, e);
    }
    store.close();

    // verify the update file is seen and parsed upon recovery when
    // original token file is missing
    store = createAndStartStore();
    HistoryServerState state = store.loadState();
    assertEquals(1, state.tokenState.size(), "incorrect loaded token count");
    assertTrue(state.tokenState.containsKey(token1), "missing token 1");
    assertEquals(newTokenDate1,
        state.tokenState.get(token1), "incorrect token 1 date");
    store.close();
  }
}
