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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertArrayEquals;

public abstract class TestBaseFsOps extends TestBaseOps {
  public TestBaseFsOps(ObjectStorage storage) {
    super(storage);
  }

  public abstract FsOps fsOps();

  @After
  public void tearDown() {
    CommonUtils.runQuietly(() -> storage.deleteAll(""));
  }

  @Test
  public void testDeleteFile() throws IOException {
    Path path = new Path("/a/b");
    touchFile(path, TestUtility.rand(8));
    assertFileExist(path);

    fsOps().deleteFile(path);
    assertFileDoesNotExist(path);
  }

  @Test
  public void testDeleteEmptyDir() throws IOException {
    Path path = new Path("/a/b/");
    mkdir(path);

    fsOps().deleteDir(path, false);
    assertDirDoesNotExist(path);
  }

  @Test
  public void testDeleteNonEmptyDir() throws IOException {
    Path dirPath = new Path("/a/b/");
    Path subDirPath = new Path("/a/b/c/");
    Path filePath = new Path("/a/b/file.txt");
    mkdir(dirPath);
    mkdir(subDirPath);
    touchFile(filePath, new byte[10]);

    Assert.assertThrows(PathIsNotEmptyDirectoryException.class, () -> fsOps().deleteDir(dirPath, false));
    assertDirExist(dirPath);
    assertDirExist(subDirPath);
    assertFileExist(filePath);

    fsOps().deleteDir(dirPath, true);
    assertDirDoesNotExist(dirPath);
    assertDirDoesNotExist(subDirPath);
    assertFileDoesNotExist(filePath);
  }

  @Test
  public void testCreateDirRecursive() throws IOException {
    Path path = new Path("/aa/bb/cc");
    String key = ObjectUtils.pathToKey(path, true);
    String parentKey = ObjectUtils.pathToKey(path.getParent(), true);
    String grandparents = ObjectUtils.pathToKey(path.getParent().getParent(), true);

    assertDirDoesNotExist(parentKey);
    assertDirDoesNotExist(grandparents);

    fsOps().mkdirs(path);
    assertDirExist(key);
    assertDirExist(parentKey);
    assertDirExist(grandparents);

    storage.delete(key);
    assertDirExist(parentKey);
    assertDirExist(grandparents);
  }

  @Test
  public void testListEmptyDir() {
    Path dir = path("testListEmptyDir");
    mkdir(dir);

    Assert.assertFalse(listDir(dir, false).iterator().hasNext());
    Assert.assertFalse(listDir(dir, true).iterator().hasNext());
    Assert.assertTrue(fsOps().isEmptyDirectory(dir));
  }

  @Test
  public void testListNonExistDir() {
    Path dir = path("testListNonExistDir");
    assertDirDoesNotExist(dir);

    Assert.assertFalse(listDir(dir, false).iterator().hasNext());
    Assert.assertFalse(listDir(dir, false).iterator().hasNext());
    Assert.assertTrue(fsOps().isEmptyDirectory(dir));
  }

  private Iterable<RawFileStatus> listDir(Path dir, boolean recursive) {
    return fsOps().listDir(dir, recursive, s -> true);
  }

  private Iterable<RawFileStatus> listFiles(Path dir, boolean recursive) {
    return fsOps().listDir(dir, recursive, s -> !ObjectInfo.isDir(s));
  }

  @Test
  public void testListAFileViaListDir() {
    Path file = new Path("testListFileViaListDir");
    touchFile(file, TestUtility.rand(8));
    Assert.assertFalse(listDir(file, false).iterator().hasNext());
    Assert.assertFalse(listDir(file, true).iterator().hasNext());

    Path nonExistFile = new Path("testListFileViaListDir-nonExist");
    assertFileDoesNotExist(nonExistFile);
    Assert.assertFalse(listDir(nonExistFile, false).iterator().hasNext());
    Assert.assertFalse(listDir(nonExistFile, true).iterator().hasNext());
  }

  @Test
  public void testListFiles() {
    Path dir = path("testListEmptyFiles");
    mkdir(dir);

    Assert.assertFalse(listFiles(dir, false).iterator().hasNext());
    Assert.assertFalse(listFiles(dir, true).iterator().hasNext());

    mkdir(new Path(dir, "subDir"));
    Assert.assertFalse(listFiles(dir, false).iterator().hasNext());
    Assert.assertFalse(listFiles(dir, true).iterator().hasNext());

    RawFileStatus subDir = listDir(dir, false).iterator().next();
    Assert.assertFalse(subDir.isFile());
    Assert.assertEquals("subDir", subDir.getPath().getName());

    ObjectInfo fileObj = touchFile(new Path(dir, "subFile"), TestUtility.rand(8));
    RawFileStatus subFile = listFiles(dir, false).iterator().next();
    assertArrayEquals(fileObj.checksum(), subFile.checksum());
    Assert.assertTrue(subFile.isFile());

    Assert.assertFalse(fsOps().isEmptyDirectory(dir));
  }

  @Test
  public void testRecursiveList() {
    Path root = path("root");
    Path file1 = path("root", "file1");
    Path file2 = path("root", "afile2");
    Path dir1 = path("root", "dir1");
    Path file3 = path("root", "dir1", "file3");

    mkdir(root);
    mkdir(dir1);
    touchFile(file1, TestUtility.rand(8));
    touchFile(file2, TestUtility.rand(8));
    touchFile(file3, TestUtility.rand(8));

    // List result is in sorted lexicographical order if recursive is false
    Assertions.assertThat(listDir(root, false))
        .hasSize(3)
        .extracting(f -> f.getPath().getName())
        .contains("afile2", "dir1", "file1");

    // List result is in sorted lexicographical order if recursive is false
    Assertions.assertThat(listFiles(root, false))
        .hasSize(2)
        .extracting(f -> f.getPath().getName())
        .contains("afile2", "file1");

    // listDir with recursive=true doesn't guarantee the return result in a sorted order
    Assertions.assertThat(listDir(root, true))
        .hasSize(4)
        .extracting(f -> f.getPath().getName())
        .containsExactlyInAnyOrder("afile2", "dir1", "file1", "file3");

    // listFiles with recursive=true doesn't guarantee the return result in a sorted order
    Assertions.assertThat(listFiles(root, true))
        .hasSize(3)
        .extracting(f -> f.getPath().getName())
        .containsExactlyInAnyOrder("afile2", "file1", "file3");
  }

  @Test
  public void testRenameFile() throws IOException {
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    int dataSize = 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    byte[] data = writeData(srcFile, dataSize);
    Path dstFile = new Path(renameDest, filename);

    // The dest file and dest parent don't exist.
    assertFileExist(srcFile);
    assertDirDoesNotExist(renameDest);
    assertFileDoesNotExist(dstFile);

    fsOps().renameFile(srcFile, dstFile, data.length);
    assertFileDoesNotExist(srcFile);
    assertDirExist(renameSrc);
    assertFileExist(dstFile);

    try (InputStream in = storage.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  @Test
  public void testRenameDir() throws IOException {
    Path renameSrc = path("renameSrc");
    Path renameDest = path("renameDst");

    mkdir(renameSrc);
    int dataSize = 1024 * 1024;
    String filename = String.format("%sMB.txt", dataSize >> 20);
    Path srcFile = new Path(renameSrc, filename);
    Path dstFile = new Path(renameDest, filename);
    byte[] data = writeData(srcFile, dataSize);

    assertFileExist(srcFile);
    assertFileDoesNotExist(dstFile);
    assertDirExist(renameSrc);
    assertDirDoesNotExist(renameDest);

    fsOps().renameDir(renameSrc, renameDest);
    assertFileDoesNotExist(srcFile);
    assertDirDoesNotExist(renameSrc);
    assertFileExist(dstFile);
    assertDirExist(renameDest);

    try (InputStream in = storage.get(ObjectUtils.pathToKey(dstFile)).stream()) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  private byte[] writeData(Path path, int size) {
    byte[] data = TestUtility.rand(size);
    touchFile(path, data);
    return data;
  }
}
