/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.s3a.impl.AwsSdkWorkarounds;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.commons.io.FileUtils.ONE_KB;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enablePrefetching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.getExternalData;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.isUsingDefaultExternalDataFile;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ANALYTICS_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_OPENED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS;
import static org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs;

import org.assertj.core.api.Assertions;

import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ParquetMetadataParsingTask;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;

public class ITestS3AAnalyticsAcceleratorStream extends AbstractS3ATestBase {

  private static final String PHYSICAL_IO_PREFIX = "physicalio";
  private static final String LOGICAL_IO_PREFIX = "logicalio";


  private Configuration conf;
  private Path testFile;

  @Before
  public void setUp() throws Exception {
    super.setup();
    conf = createConfiguration();
    testFile = getExternalData(conf);
  }

  @Override
  public Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    if (isUsingDefaultExternalDataFile(configuration)) {
      S3ATestUtils.removeBaseAndBucketOverrides(configuration,
          ENDPOINT);
    }
    enableAnalyticsAccelerator(configuration);
    return configuration;
  }

  @Test
  public void testConnectorFrameWorkIntegration() throws IOException {
    describe("Verify S3 connector framework integration");

    S3AFileSystem fs =
        (S3AFileSystem) FileSystem.get(testFile.toUri(), conf);
    byte[] buffer = new byte[500];
    IOStatistics ioStats;

    try (FSDataInputStream inputStream = fs.open(testFile)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.seek(5);
      inputStream.read(buffer, 0, 500);
    }
    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
  }

  @Test
  public void testMalformedParquetFooter() throws IOException {
    describe("Reading a malformed parquet file should not throw an exception");

    // File with malformed footer take from https://github.com/apache/parquet-testing/blob/master/bad_data/PARQUET-1481.parquet.
    // This test ensures AAL does not throw exceptions if footer parsing fails. It will only emit a WARN log,
    // "Unable to parse parquet footer for test/malformedFooter.parquet, parquet prefetch optimisations will be disabled for this key."
    Path dest = path("malformed_footer.parquet");

    File file = new File("src/test/resources/malformed_footer.parquet");
    Path sourcePath = new Path(file.toURI().getPath());
    getFileSystem().copyFromLocalFile(false, true, sourcePath, dest);

    byte[] buffer = new byte[500];
    IOStatistics ioStats;

    try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
      ioStats = inputStream.getIOStatistics();
      inputStream.seek(5);
      inputStream.read(buffer, 0, 500);
    }

    verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
  }

 @Test
 public void testMultiRowGroupParquet() throws IOException {
    describe("A parquet file is read successfully");

    Path dest = path("multi_row_group.parquet");

   File file = new File("src/test/resources/multi_row_group.parquet");
   Path sourcePath = new Path(file.toURI().getPath());
   getFileSystem().copyFromLocalFile(false, true, sourcePath, dest);

   FileStatus fileStatus = getFileSystem().getFileStatus(dest);

   byte[] buffer = new byte[3000];
   IOStatistics ioStats;

   try (FSDataInputStream inputStream = getFileSystem().open(dest)) {
     ioStats = inputStream.getIOStatistics();
     inputStream.readFully(buffer, 0, (int) fileStatus.getLen());
   }

   verifyStatisticCounterValue(ioStats, STREAM_READ_ANALYTICS_OPENED, 1);
 }

  @Test
  public void testConnectorFrameworkConfigurable() {
    describe("Verify S3 connector framework reads configuration");

    Configuration conf = getConfiguration();
    removeBaseAndBucketOverrides(conf);

    //Disable Predictive Prefetching
    conf.set(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
        "." + LOGICAL_IO_PREFIX + ".prefetching.mode", "all");

    //Set Blobstore Capacity
    conf.setInt(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
        "." + PHYSICAL_IO_PREFIX + ".blobstore.capacity", 1);

    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(conf, ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX);

    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration);

    Assertions.assertThat(configuration.getLogicalIOConfiguration().getPrefetchingMode())
            .as("AnalyticsStream configuration is not set to expected value")
            .isSameAs(PrefetchMode.ALL);

    Assertions.assertThat(configuration.getPhysicalIOConfiguration().getBlobStoreCapacity())
            .as("AnalyticsStream configuration is not set to expected value")
            .isEqualTo(1);
  }

  @Test
  public void testInvalidConfigurationThrows() throws Exception {
    describe("Verify S3 connector framework throws with invalid configuration");

    Configuration conf = getConfiguration();
    removeBaseAndBucketOverrides(conf);
    //Disable Sequential Prefetching
    conf.setInt(ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX +
        "." + PHYSICAL_IO_PREFIX + ".blobstore.capacity", -1);

    ConnectorConfiguration connectorConfiguration =
        new ConnectorConfiguration(conf, ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX);
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() ->
                    S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration));
  }

}
