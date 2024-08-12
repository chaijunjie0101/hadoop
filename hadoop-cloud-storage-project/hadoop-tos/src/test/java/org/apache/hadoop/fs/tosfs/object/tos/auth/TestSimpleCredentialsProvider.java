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

import com.volcengine.tos.auth.Credential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestSimpleCredentialsProvider extends TestAbstractCredentialsProvider {

  @Test
  public void testStaticCredentials() {
    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_ACCESS_KEY_ID, "ACCESS_KEY");
    conf.set(TosKeys.FS_TOS_SECRET_ACCESS_KEY, "SECRET_KEY");
    conf.set(TosKeys.FS_TOS_SESSION_TOKEN, "STS_TOKEN");
    SimpleCredentialsProvider provider = new SimpleCredentialsProvider();
    provider.initialize(conf, "test");
    Credential credentials = provider.credential();
    Assert.assertEquals("access key must be ACCESS_KEY", "ACCESS_KEY",
        credentials.getAccessKeyId());
    Assert.assertEquals("secret key must be SECRET_KEY", "SECRET_KEY",
        credentials.getAccessKeySecret());
    Assert.assertEquals("sts token must be STS_TOKEN", "STS_TOKEN",
        credentials.getSecurityToken());
  }

  @Test
  public void testStaticCredentialsWithBucket() {
    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key("test"), "ACCESS_KEY");
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key("test"), "SECRET_KEY");
    conf.set(TosKeys.FS_TOS_BUCKET_SESSION_TOKEN.key("test"), "STS_TOKEN");
    SimpleCredentialsProvider provider = new SimpleCredentialsProvider();
    provider.initialize(conf, "test");
    Credential credentials = provider.credential();
    Assert.assertEquals("access key must be ACCESS_KEY", "ACCESS_KEY",
        credentials.getAccessKeyId());
    Assert.assertEquals("secret key must be SECRET_KEY", "SECRET_KEY",
        credentials.getAccessKeySecret());
    Assert.assertEquals("sts token must be STS_TOKEN", "STS_TOKEN",
        credentials.getSecurityToken());
  }

  @Test
  public void testStaticCredentialsWithPriority() {
    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_ACCESS_KEY_ID, "ACCESS_KEY");
    conf.set(TosKeys.FS_TOS_SECRET_ACCESS_KEY, "SECRET_KEY");
    conf.set(TosKeys.FS_TOS_SESSION_TOKEN, "STS_TOKEN");
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key("test"), "ACCESS_KEY_BUCKET");
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key("test"), "SECRET_KEY_BUCKET");
    conf.set(TosKeys.FS_TOS_BUCKET_SESSION_TOKEN.key("test"), "STS_TOKEN_BUCKET");

    SimpleCredentialsProvider provider = new SimpleCredentialsProvider();
    provider.initialize(conf, "test");
    Credential credentials = provider.credential();
    Assert.assertEquals("access key must be ACCESS_KEY_BUCKET", "ACCESS_KEY_BUCKET",
        credentials.getAccessKeyId());
    Assert.assertEquals("secret key must be SECRET_KEY_BUCKET", "SECRET_KEY_BUCKET",
        credentials.getAccessKeySecret());
    Assert.assertEquals("sts token must be STS_TOKEN_BUCKET", "STS_TOKEN_BUCKET",
        credentials.getSecurityToken());
  }
}
