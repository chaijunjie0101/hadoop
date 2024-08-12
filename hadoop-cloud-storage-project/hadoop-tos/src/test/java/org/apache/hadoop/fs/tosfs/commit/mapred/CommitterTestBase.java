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

package org.apache.hadoop.fs.tosfs.commit.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.commit.CommitUtils;
import org.apache.hadoop.fs.tosfs.commit.Pending;
import org.apache.hadoop.fs.tosfs.commit.PendingSet;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public abstract class CommitterTestBase {
  private Configuration conf;
  private FileSystem fs;
  private Path outputPath;
  private TaskAttemptID taskAttempt0;
  private Path reportDir;

  @Before
  public void setup() throws IOException {
    conf = newConf();
    fs = FileSystem.get(conf);
    String uuid = UUIDUtils.random();
    outputPath = fs.makeQualified(new Path("/test/" + uuid));
    taskAttempt0 = JobSuite.createTaskAttemptId(randomTrimmedJobId(), 0);

    reportDir = fs.makeQualified(new Path("/report/" + uuid));
    fs.mkdirs(reportDir);
    conf.set(org.apache.hadoop.fs.tosfs.commit.Committer.COMMITTER_SUMMARY_REPORT_DIR,
        reportDir.toUri().toString());
  }

  protected abstract Configuration newConf();

  @After
  public void teardown() {
    CommonUtils.runQuietly(() -> fs.delete(outputPath, true));
    IOUtils.closeStream(fs);
  }

  @BeforeClass
  public static void beforeClass() {
    Assume.assumeTrue(TestEnv.checkTestEnabled());
  }

  @AfterClass
  public static void afterClass() {
    if (!TestEnv.checkTestEnabled()) {
      return;
    }

    List<String> committerThreads = Thread.getAllStackTraces().keySet()
        .stream()
        .map(Thread::getName)
        .filter(n -> n.startsWith(org.apache.hadoop.fs.tosfs.commit.Committer.THREADS_PREFIX))
        .collect(Collectors.toList());
    Assert.assertTrue("Outstanding committer threads", committerThreads.isEmpty());
  }

  private static String randomTrimmedJobId() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    return String.format("%s%04d_%04d", formatter.format(new Date()),
        (long) (Math.random() * 1000),
        (long) (Math.random() * 1000));
  }

  private static String randomFormedJobId() {
    return String.format("job_%s", randomTrimmedJobId());
  }

  @Test
  public void testSetupJob() throws IOException {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());

    // Setup job.
    suite.setupJob();
    suite.dumpObjectStorage();
    suite.assertHasMagicKeys();
  }

  @Test
  public void testSetupJobWithOrphanPaths() throws IOException, InterruptedException {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());

    // Orphan success marker.
    Path successPath = CommitUtils.successMarker(outputPath);
    CommitUtils.save(fs, successPath, new byte[]{});
    Assert.assertTrue(fs.exists(successPath));

    // Orphan job path.
    Path jobPath = CommitUtils.magicJobPath(suite.committer().jobId(), outputPath);
    fs.mkdirs(jobPath);
    Assert.assertTrue("The job path should be existing", fs.exists(jobPath));
    Path subPath = new Path(jobPath, "tmp.pending");
    CommitUtils.save(fs, subPath, new byte[]{});
    Assert.assertTrue("The sub path under job path should be existing.", fs.exists(subPath));
    FileStatus jobPathStatus = fs.getFileStatus(jobPath);

    Thread.sleep(1000L);
    suite.setupJob();
    suite.dumpObjectStorage();
    suite.assertHasMagicKeys();

    Assert.assertFalse("Should have deleted the success path", fs.exists(successPath));
    Assert.assertTrue("Should have re-created the job path", fs.exists(jobPath));
    Assert.assertFalse("Should have deleted the sub path under the job path", fs.exists(subPath));
  }

  @Test
  public void testSetupTask() throws IOException {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());

    // Remaining attempt task path.
    Path taskAttemptBasePath = CommitUtils.magicTaskAttemptBasePath(suite.taskAttemptContext(), outputPath);
    Path subTaskAttemptPath = new Path(taskAttemptBasePath, "tmp.pending");
    CommitUtils.save(fs, subTaskAttemptPath, new byte[]{});
    Assert.assertTrue(fs.exists(taskAttemptBasePath));
    Assert.assertTrue(fs.exists(subTaskAttemptPath));

    // Setup job.
    suite.setupJob();
    suite.assertHasMagicKeys();
    // It will clear all the job path once we've set up the job.
    Assert.assertFalse(fs.exists(taskAttemptBasePath));
    Assert.assertFalse(fs.exists(subTaskAttemptPath));

    // Left some the task paths.
    CommitUtils.save(fs, subTaskAttemptPath, new byte[]{});
    Assert.assertTrue(fs.exists(taskAttemptBasePath));
    Assert.assertTrue(fs.exists(subTaskAttemptPath));

    // Setup task.
    suite.setupTask();
    Assert.assertFalse(fs.exists(subTaskAttemptPath));
  }

  @Test
  public void testCommitTask() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());
    // Setup job
    suite.setupJob();
    suite.dumpObjectStorage();
    suite.assertHasMagicKeys();

    // Setup task
    suite.setupTask();

    // Write records.
    suite.assertNoMagicPendingFile();
    suite.assertMultipartUpload(0);
    suite.writeOutput();
    suite.dumpObjectStorage();
    suite.assertHasMagicPendingFile();
    suite.assertNoMagicMultipartUpload();
    suite.assertMultipartUpload(1);
    // Assert the pending file content.
    Path pendingPath = suite.magicPendingPath();
    byte[] pendingData = CommitUtils.load(suite.fs(), pendingPath);
    Pending pending = Pending.deserialize(pendingData);
    Assert.assertEquals(suite.destPartKey(), pending.destKey());
    Assert.assertEquals(20, pending.length());
    Assert.assertEquals(1, pending.parts().size());

    // Commit the task.
    suite.commitTask();

    // Verify the pending set file.
    suite.assertHasPendingSet();
    // Assert the pending set file content.
    Path pendingSetPath = suite.magicPendingSetPath();
    byte[] pendingSetData = CommitUtils.load(suite.fs(), pendingSetPath);
    PendingSet pendingSet = PendingSet.deserialize(pendingSetData);
    Assert.assertEquals(suite.job().getJobID().toString(), pendingSet.jobId());
    Assert.assertEquals(1, pendingSet.commits().size());
    Assert.assertEquals(pending, pendingSet.commits().get(0));
    Assert.assertEquals(pendingSet.extraData(),
        ImmutableMap.of(CommitUtils.TASK_ATTEMPT_ID, suite.taskAttemptContext().getTaskAttemptID().toString()));

    // Complete the multipart upload and verify the results.
    ObjectStorage storage = suite.storage();
    storage.completeUpload(pending.destKey(), pending.uploadId(), pending.parts());
    suite.verifyPartContent();
  }

  @Test
  public void testAbortTask() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();

    // Pre-check before the output write.
    suite.assertNoMagicPendingFile();
    suite.assertMultipartUpload(0);

    // Execute the output write.
    suite.writeOutput();

    // Post-check after the output write.
    suite.assertHasMagicPendingFile();
    suite.assertNoMagicMultipartUpload();
    suite.assertMultipartUpload(1);
    // Assert the pending file content.
    Path pendingPath = suite.magicPendingPath();
    byte[] pendingData = CommitUtils.load(suite.fs(), pendingPath);
    Pending pending = Pending.deserialize(pendingData);
    Assert.assertEquals(suite.destPartKey(), pending.destKey());
    Assert.assertEquals(20, pending.length());
    Assert.assertEquals(1, pending.parts().size());

    // Abort the task.
    suite.abortTask();

    // Verify the state after aborting task.
    suite.assertNoMagicPendingFile();
    suite.assertNoMagicMultipartUpload();
    suite.assertMultipartUpload(0);
    suite.assertNoTaskAttemptPath();
  }

  @Test
  public void testCommitJob() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();
    suite.writeOutput();
    suite.commitTask();

    // Commit the job.
    suite.assertNoPartFiles();
    suite.commitJob();
    // Verify the output.
    suite.assertNoMagicMultipartUpload();
    suite.assertNoMagicObjectKeys();
    suite.assertSuccessMarker();
    suite.assertSummaryReport(reportDir);
    suite.verifyPartContent();
  }


  @Test
  public void testCommitJobFailed() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();
    suite.writeOutput();
    suite.commitTask();

    // Commit the job.
    suite.assertNoPartFiles();
    suite.commitJob();
  }

  @Test
  public void testTaskCommitAfterJobCommit() throws Exception {
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());
    suite.setupJob();
    suite.setupTask();
    suite.writeOutput();
    suite.commitTask();

    // Commit the job
    suite.assertNoPartFiles();
    suite.commitJob();
    // Verify the output.
    suite.assertNoMagicMultipartUpload();
    suite.assertNoMagicObjectKeys();
    suite.assertSuccessMarker();
    suite.verifyPartContent();

    // Commit the task again.
    Assert.assertThrows(FileNotFoundException.class, suite::commitTask);
  }

  @Test
  public void testTaskCommitWithConsistentJobId() throws Exception {
    Configuration conf = newConf();
    String consistentJobId = randomFormedJobId();
    conf.set(CommitUtils.SPARK_WRITE_UUID, consistentJobId);
    JobSuite suite = JobSuite.create(conf, taskAttempt0, outputPath);
    Assume.assumeFalse(suite.skipTests());

    // By now, we have two "jobId"s, one is spark uuid, and the other is the jobId in taskAttempt.
    // The job committer will adopt the former.
    suite.setupJob();

    // Next, we clear spark uuid, and set the jobId of taskAttempt to another value. In this case,
    // the committer will take the jobId of taskAttempt as the final jobId, which is not consistent
    // with the one that committer holds.
    conf.unset(CommitUtils.SPARK_WRITE_UUID);
    JobConf jobConf = new JobConf(conf);
    String anotherJobId = randomTrimmedJobId();
    TaskAttemptID taskAttemptId1 =
        JobSuite.createTaskAttemptId(anotherJobId, JobSuite.DEFAULT_APP_ATTEMPT_ID);
    final TaskAttemptContext attemptContext1 =
        JobSuite.createTaskAttemptContext(jobConf, taskAttemptId1, JobSuite.DEFAULT_APP_ATTEMPT_ID);

    Assert.assertThrows("JobId set in the context", IllegalArgumentException.class,
        () -> suite.setupTask(attemptContext1));

    // Even though we use another taskAttempt, as long as we ensure the spark uuid is consistent,
    // the jobId in committer is consistent.
    conf.set(CommitUtils.SPARK_WRITE_UUID, consistentJobId);
    conf.set(FileOutputFormat.OUTDIR, outputPath.toString());
    jobConf = new JobConf(conf);
    anotherJobId = randomTrimmedJobId();
    TaskAttemptID taskAttemptId2 =
        JobSuite.createTaskAttemptId(anotherJobId, JobSuite.DEFAULT_APP_ATTEMPT_ID);
    TaskAttemptContext attemptContext2 =
        JobSuite.createTaskAttemptContext(jobConf, taskAttemptId2, JobSuite.DEFAULT_APP_ATTEMPT_ID);

    suite.setupTask(attemptContext2);
    // Write output must use the same task context with setup task.
    suite.writeOutput(attemptContext2);
    // Commit task must use the same task context with setup task.
    suite.commitTask(attemptContext2);
    suite.assertPendingSetAtRightLocation();

    // Commit the job
    suite.assertNoPartFiles();
    suite.commitJob();

    // Verify the output.
    suite.assertNoMagicMultipartUpload();
    suite.assertNoMagicObjectKeys();
    suite.assertSuccessMarker();
    suite.verifyPartContent();
  }
}
