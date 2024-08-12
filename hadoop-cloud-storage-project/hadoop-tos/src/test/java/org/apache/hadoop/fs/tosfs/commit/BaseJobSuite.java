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

package org.apache.hadoop.fs.tosfs.commit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.object.MultipartUpload;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.util.ParseUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class BaseJobSuite {
  private static final Logger LOG = LoggerFactory.getLogger(BaseJobSuite.class);
  public static final int DEFAULT_APP_ATTEMPT_ID = 1;
  protected static final Text KEY_1 = new Text("key1");
  protected static final Text KEY_2 = new Text("key2");
  protected static final Text VAL_1 = new Text("val1");
  protected static final Text VAL_2 = new Text("val2");

  protected Job job;
  protected String jobId;
  protected FileSystem fs;
  protected Path outputPath;
  protected ObjectStorage storage;

  private final boolean dumpObjectStorage = ParseUtils.envAsBoolean("DUMP_OBJECT_STORAGE", false);

  protected abstract Path magicPartPath();

  protected abstract Path magicPendingSetPath();

  protected abstract void assertSuccessMarker() throws IOException;

  protected abstract void assertSummaryReport(Path reportDir) throws IOException;

  protected abstract void assertNoTaskAttemptPath() throws IOException;

  protected void assertMagicPathExist(Path outputPath) throws IOException {
    Path magicPath = CommitUtils.magicPath(outputPath);
    Assert.assertTrue(String.format("Magic path: %s should exist", magicPath), fs.exists(magicPath));
  }

  protected void assertMagicPathNotExist(Path outputPath) throws IOException {
    Path magicPath = CommitUtils.magicPath(outputPath);
    Assert.assertFalse(String.format("Magic path: %s should not exist", magicPath), fs.exists(magicPath));
  }

  protected abstract boolean skipTests();

  public Path magicPendingPath() {
    Path magicPart = magicPartPath();
    return new Path(magicPart.getParent(), magicPart.getName() + ".pending");
  }

  public Path magicJobPath() {
    return CommitUtils.magicPath(outputPath);
  }

  public String magicPartKey() {
    return ObjectUtils.pathToKey(magicPartPath());
  }

  public String destPartKey() {
    return MagicOutputStream.toDestKey(magicPartPath());
  }

  public FileSystem fs() {
    return fs;
  }

  public ObjectStorage storage() {
    return storage;
  }

  public Job job() {
    return job;
  }

  public void assertHasMagicKeys() {
    Iterable<ObjectInfo> objects = storage.listAll(ObjectUtils.pathToKey(magicJobPath(), true), "");
    Assert.assertTrue("Should have some __magic object keys", Iterables.any(objects, o -> o.key().contains(
        CommitUtils.MAGIC) && o.key().contains(jobId)));
  }

  public void assertHasBaseKeys() {
    Iterable<ObjectInfo> objects = storage.listAll(ObjectUtils.pathToKey(magicJobPath(), true), "");
    Assert.assertTrue("Should have some __base object keys", Iterables.any(objects, o -> o.key().contains(
        CommitUtils.BASE) && o.key().contains(jobId)));
  }

  public void assertNoMagicPendingFile() {
    String magicPendingKey = String.format("%s.pending", magicPartKey());
    Assert.assertNull("Magic pending key should exist", storage.head(magicPendingKey));
  }

  public void assertHasMagicPendingFile() {
    String magicPendingKey = String.format("%s.pending", magicPartKey());
    Assert.assertNotNull("Magic pending key should exist", storage.head(magicPendingKey));
  }

  public void assertNoMagicMultipartUpload() {
    Iterable<MultipartUpload> uploads = storage.listUploads(ObjectUtils.pathToKey(magicJobPath(), true));
    boolean anyMagicUploads = Iterables.any(uploads, u -> u.key().contains(CommitUtils.MAGIC));
    Assert.assertFalse("Should have no magic multipart uploads", anyMagicUploads);
  }

  public void assertNoMagicObjectKeys() {
    Iterable<ObjectInfo> objects = storage.listAll(ObjectUtils.pathToKey(magicJobPath(), true), "");
    boolean anyMagicUploads =
        Iterables.any(objects, o -> o.key().contains(CommitUtils.MAGIC) && o.key().contains(jobId));
    Assert.assertFalse("Should not have any magic keys", anyMagicUploads);
  }

  public void assertHasPendingSet() {
    Iterable<ObjectInfo> objects = storage.listAll(ObjectUtils.pathToKey(magicJobPath(), true), "");
    boolean anyPendingSet =
        Iterables.any(objects, o -> o.key().contains(CommitUtils.PENDINGSET_SUFFIX) && o.key().contains(jobId));
    Assert.assertTrue("Should have the expected .pendingset file", anyPendingSet);
  }

  public void assertPendingSetAtRightLocation() {
    Iterable<ObjectInfo> objects = storage.listAll(ObjectUtils.pathToKey(magicJobPath(), true), "");
    Path magicJobAttemptPath =
        CommitUtils.magicJobAttemptPath(job().getJobID().toString(), DEFAULT_APP_ATTEMPT_ID, outputPath);
    String inQualifiedPath = magicJobAttemptPath.toUri().getPath().substring(1);
    Iterable<ObjectInfo> filtered =
        Iterables.filter(objects, o -> o.key().contains(CommitUtils.PENDINGSET_SUFFIX) && o.key().contains(jobId));
    boolean pendingSetAtRightLocation =
        Iterables.any(filtered, o -> o.key().startsWith(inQualifiedPath) && o.key().contains(jobId));
    Assert.assertTrue("The .pendingset file should locate at the job's magic output path.", pendingSetAtRightLocation);
  }

  public void assertMultipartUpload(int expectedUploads) {
    // Note: should be care in concurrent case: they need to check the same output path.
    Iterable<MultipartUpload> uploads = storage.listUploads(ObjectUtils.pathToKey(outputPath, true));
    long actualUploads = StreamSupport.stream(uploads.spliterator(), false).count();
    Assert.assertEquals(expectedUploads, actualUploads);
  }

  public void assertPartFiles(int num) throws IOException {
    FileStatus[] files = fs.listStatus(outputPath,
        f -> !MagicOutputStream.isMagic(new Path(f.toUri())) && f.toUri().toString().contains("part-"));
    Assert.assertEquals(num, files.length);
    Iterable<ObjectInfo> objects = storage.listAll(ObjectUtils.pathToKey(outputPath, true), "");
    List<ObjectInfo> infos = Arrays.stream(Iterables.toArray(objects, ObjectInfo.class))
        .filter(o -> o.key().contains("part-")).collect(Collectors.toList());
    Assert.assertEquals(
        String.format("Part files number should be %d, but got %d", num, infos.size()), num, infos.size());
  }

  public void assertNoPartFiles() throws IOException {
    FileStatus[] files = fs.listStatus(outputPath,
        f -> !MagicOutputStream.isMagic(new Path(f.toUri())) && f.toUri().toString().contains("part-"));
    Assert.assertEquals(0, files.length);
    Iterable<ObjectInfo> objects = storage.listAll(ObjectUtils.pathToKey(outputPath, true), "");
    boolean anyPartFile = Iterables.any(objects, o -> o.key().contains("part-"));
    Assert.assertFalse("Should have no part files", anyPartFile);
  }

  public void dumpObjectStorage() {
    if (dumpObjectStorage) {
      LOG.info("===> Dump object storage - Start <===");
      dumpObjectKeys();
      dumpMultipartUploads();
      LOG.info("===> Dump object storage -  End  <===");
    }
  }

  public void dumpObjectKeys() {
    String prefix = ObjectUtils.pathToKey(magicJobPath());
    LOG.info("Dump object keys with prefix {}", prefix);
    storage.listAll("", "").forEach(o -> LOG.info("Dump object keys - {}", o));
  }

  public void dumpMultipartUploads() {
    String prefix = ObjectUtils.pathToKey(magicJobPath());
    LOG.info("Dump multi part uploads with prefix {}", prefix);
    storage.listUploads("")
        .forEach(u -> LOG.info("Dump multipart uploads - {}", u));
  }

  public void verifyPartContent() throws IOException {
    String partKey = destPartKey();
    LOG.info("Part key to verify is: {}", partKey);
    try (InputStream in = storage.get(partKey).stream()) {
      byte[] data = IOUtils.toByteArray(in);
      String expected = String.format("%s\t%s\n%s\t%s\n", KEY_1, VAL_1, KEY_2, VAL_2);
      Assert.assertEquals(expected, new String(data, StandardCharsets.UTF_8));
    }
  }

  public void assertSuccessMarkerNotExist() throws IOException {
    Path succPath = CommitUtils.successMarker(outputPath);
    Assert.assertFalse(String.format("%s should not exists", succPath), fs.exists(succPath));
  }
}
