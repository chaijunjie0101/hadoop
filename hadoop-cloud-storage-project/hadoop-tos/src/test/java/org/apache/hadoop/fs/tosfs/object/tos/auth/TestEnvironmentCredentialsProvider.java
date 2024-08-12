/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object.tos.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestEnvironmentCredentialsProvider extends TestAbstractCredentialsProvider {

  @Before
  public void setUp() {
    saveOsCredEnv();
  }

  @Test
  public void testLoadAkSkFromEnvProvider() {
    TestUtility.setSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID, "AccessKeyId");
    TestUtility.setSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY, "SecretAccessKey");

    EnvironmentCredentialsProvider provider = new EnvironmentCredentialsProvider();
    provider.initialize(new Configuration(), null);

    ExpireableCredential oldCred = provider.credential();
    Assert.assertEquals("provider ak must be equals to env ak", oldCred.getAccessKeyId(), "AccessKeyId");
    Assert.assertEquals("provider sk must be equals to env sk", oldCred.getAccessKeySecret(), "SecretAccessKey");

    TestUtility.setSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID, "newAccessKeyId");
    TestUtility.setSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY, "newSecretAccessKey");
    TestUtility.setSystemEnv(TOS.ENV_TOS_SESSION_TOKEN, "newSessionToken");

    Assert.assertFalse(oldCred.isExpired());

    ExpireableCredential newCred = provider.credential();
    Assert.assertEquals("provider ak must be equals to env ak", newCred.getAccessKeyId(), "AccessKeyId");
    Assert.assertEquals("provider sk must be equals to env sk", newCred.getAccessKeySecret(), "SecretAccessKey");

    Assert.assertFalse(newCred.isExpired());
  }

  @After
  public void resetEnv() {
    resetOsCredEnv();
  }
}
