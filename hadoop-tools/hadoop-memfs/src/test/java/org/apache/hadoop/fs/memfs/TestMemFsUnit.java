/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.memfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

// Simple tests for in memory fs, built before extending existing FS tests.
public class TestMemFsUnit {

  private static final Path root = new Path("/");
  private static final Path noSuchFile = new Path("/no_such_file");

  private FileSystem fs;

  @Before
  public void createFs() throws IOException {
    InMemoryFs.reset();
    fs = new InMemoryFs();
    fs.setConf(new Configuration());
  }

  @Test
  public void uri() {
    fs.getUri();
    Assert.assertFalse(fs.supportsSymlinks());
  }

  @Test
  public void cwd() {
    Assert.assertEquals("memfs:/", fs.getWorkingDirectory().toString());
    fs.setWorkingDirectory(new Path("/bla"));
    Assert.assertEquals("/bla", fs.getWorkingDirectory().toString());
  }

  @Test
  public void makeSingleDir() throws IOException {
    FileStatus[] stats = fs.listStatus(root);
    Assert.assertNotNull(stats);
    Assert.assertEquals(0, stats.length);

    fs.mkdirs(new Path("/a"));
    stats = fs.listStatus(root);
    Assert.assertNotNull(stats);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/a", stats[0].getPath().toString());
    fs.setWorkingDirectory(new Path("/a"));

    fs.mkdirs(new Path("b"));
    stats = fs.listStatus(root);
    Assert.assertNotNull(stats);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/a", stats[0].getPath().toString());
    stats = fs.listStatus(new Path("/a"));
    Assert.assertNotNull(stats);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/a/b", stats[0].getPath().toString());
  }

  @Test
  public void makeMultiDirs() throws IOException {
    fs.mkdirs(new Path("/a/b/c"));

    FileStatus[] stats = fs.listStatus(root);
    Assert.assertNotNull(stats);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/a", stats[0].getPath().toString());

    stats = fs.listStatus(new Path("/a"));
    Assert.assertNotNull(stats);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/a/b", stats[0].getPath().toString());

    stats = fs.listStatus(new Path("/a/b"));
    Assert.assertNotNull(stats);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/a/b/c", stats[0].getPath().toString());

    fs.setWorkingDirectory(new Path("/a"));

    fs.mkdirs(new Path("beta/gamma"));
    stats = fs.listStatus(new Path("/a"));
    Assert.assertNotNull(stats);
    Assert.assertEquals(2, stats.length);
    sortStats(stats);
    Assert.assertEquals("memfs:/a/b", stats[0].getPath().toString());
    Assert.assertEquals("memfs:/a/beta", stats[1].getPath().toString());

    stats = fs.listStatus(new Path("/a/beta"));
    Assert.assertNotNull(stats);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/a/beta/gamma", stats[0].getPath().toString());
  }

  @Test
  public void mkdirPerms() throws IOException {
    fs.mkdirs(new Path("/a"), new FsPermission(FsAction.WRITE_EXECUTE, FsAction.NONE, FsAction.NONE));

    // Should work
    fs.mkdirs(new Path("/a/b"));
  }

  @Test(expected = AccessControlException.class)
  public void mkdirPermsNoUserPerms() throws IOException {
    fs.mkdirs(new Path("/a"), new FsPermission(FsAction.NONE, FsAction.EXECUTE, FsAction.EXECUTE));
    fs.mkdirs(new Path("/a/b"));
  }

  @Test
  public void gettingFileStatus() throws IOException {
    fs.mkdirs(new Path("/a"));
    FileStatus stat = fs.getFileStatus(new Path("/a"));
    Assert.assertTrue(stat.isDirectory());
    Assert.assertEquals("memfs:/a", stat.getPath().toString());

    fs.setWorkingDirectory(root);
    stat = fs.getFileStatus(new Path("a"));
    Assert.assertTrue(stat.isDirectory());
    Assert.assertEquals("memfs:/a", stat.getPath().toString());

    stat = fs.getFileStatus(new Path("./a"));
    Assert.assertTrue(stat.isDirectory());
    Assert.assertEquals("memfs:/a", stat.getPath().toString());

  }

  @Test(expected = FileNotFoundException.class)
  public void getFileStatusNoSuchFile() throws IOException {
    fs.getFileStatus(noSuchFile);
  }

  @Test
  public void writeAndRead() throws IOException {
    final String line1 = "Mary had a little lamb";
    final String line2 = "Its fleece was white as snow";
    final String line3 = "And everywhere that Mary went";
    final String line4 = "The lamb was sure to go";

    FSDataOutputStream out = fs.create(new Path("/mary"));
    out.writeBytes(line1);
    out.writeBytes(line2);
    out.writeBytes(line3);
    out.writeBytes(line4);
    out.close();

    FSDataInputStream in = fs.open(new Path("/mary"));
    byte[] buf = new byte[line1.getBytes().length];
    long pos = 0;
    pos += in.read(pos, buf, 0, buf.length);
    Assert.assertArrayEquals(line1.getBytes(), buf);

    buf = new byte[line2.getBytes().length];
    pos += in.read(pos, buf, 0, buf.length);
    Assert.assertArrayEquals(line2.getBytes(), buf);

    buf = new byte[line3.getBytes().length];
    pos += in.read(pos, buf, 0, buf.length);
    Assert.assertArrayEquals(line3.getBytes(), buf);

    buf = new byte[line4.getBytes().length];
    in.readFully(pos, buf);
    Assert.assertArrayEquals(line4.getBytes(), buf);
    in.close();
  }

  @Test
  public void readAndWriteRelativePath() throws IOException {
    final String line = "When in the course of human events";
    final String line2 = "it becomes necessary for one people to disolve";

    fs.setWorkingDirectory(root);
    FSDataOutputStream out = fs.create(new Path("declaration"));
    out.writeBytes(line);
    out.writeBytes(line2);
    out.close();

    FSDataInputStream in = fs.open(new Path("declaration"));
    byte[] buf = new byte[line.getBytes().length];
    in.readFully(buf);
    Assert.assertArrayEquals(line.getBytes(), buf);

    buf = new byte[line2.getBytes().length];
    in.readFully(buf);
    Assert.assertArrayEquals(line2.getBytes(), buf);

    // Read just a few bytes
    buf = new byte[line2.getBytes().length];
    int bytesRead = in.read(5, buf, 0, 2);
    Assert.assertEquals(2, bytesRead);
    Assert.assertEquals("in", new String(Arrays.copyOf(buf, 2)));

    // Read just the end
    long fileLen = fs.getFileStatus(new Path("/declaration")).getLen();
    int pos = (int)fileLen - "disolve".getBytes().length;
    buf = new byte[100];
    bytesRead = in.read(pos, buf, 0, 100);
    Assert.assertEquals("disolve".getBytes().length, bytesRead);
    Assert.assertEquals("disolve", new String(Arrays.copyOf(buf, 7)));

    // Read into non-zero position in buffer
    buf = new byte["When in the".getBytes().length];
    in.readFully(0, buf);
    Assert.assertEquals("When in the", new String(buf));
    bytesRead = in.read(1, buf, 5, 2);
    Assert.assertEquals(2, bytesRead);
    Assert.assertEquals("When he the", new String(buf));

    // Read without position, make sure position is moving forward
    in.seek(0);
    Assert.assertEquals(0, in.getPos());
    buf = new byte[100];
    bytesRead = in.read(buf, 0, 4);
    Assert.assertEquals(4, bytesRead);
    Assert.assertEquals("When", new String(Arrays.copyOf(buf, 4)));
    Assert.assertEquals(4, in.getPos());

    // Make sure I can seek
    in.seek(8);
    Assert.assertEquals(8, in.getPos());
    buf = new byte[100];
    bytesRead = in.read(buf, 0, 3);
    Assert.assertEquals(3, bytesRead);
    Assert.assertEquals("the", new String(Arrays.copyOf(buf, 3)));
    Assert.assertEquals(11, in.getPos());

    // Make sure read past end does the right thing
    in.seek((int)fileLen - 7);
    buf = new byte[100];
    bytesRead = in.read(buf, 0, 100);
    Assert.assertEquals(7, bytesRead);
    Assert.assertEquals("disolve", new String(Arrays.copyOf(buf, 7)));
  }

  @Test(expected = AccessControlException.class)
  public void noReadPermissions() throws IOException {
    FSDataOutputStream out = fs.create(new Path("/unreadable"),
        new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL), false, 1, (short)1, 1L, null);
    out.writeBytes("You'll never read me!");
    out.close();

    fs.open(new Path("/unreadable"));
  }

  @Test(expected = EOFException.class)
  public void readTooMuch() throws IOException {
    final String line = "abc";

    FSDataOutputStream out = fs.create(new Path("/alphabet"));
    out.writeBytes(line);
    out.close();

    FSDataInputStream in = fs.open(new Path("/alphabet"));
    byte[] buf = new byte[10];
    in.readFully(1, buf);
  }

  @Test(expected = FileNotFoundException.class)
  public void noSuchFile() throws IOException {
    FSDataInputStream in = fs.open(noSuchFile);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void overwriteOverwriteNotSet() throws IOException {
    final String line = "abc";
    FSDataOutputStream out = fs.create(new Path("/alphabet"));
    out.writeBytes(line);
    out.close();

    out = fs.create(new Path("/alphabet"), false);
  }

  @Test
  public void overwrite() throws IOException {
    final String line = "abc";
    FSDataOutputStream out = fs.create(new Path("/alphabet"));
    out.writeBytes(line);
    out.close();

    out = fs.create(new Path("/alphabet"));
    final String newLine = "alpha beta gamma";
    out.writeBytes(newLine);
    out.close();

    FSDataInputStream in = fs.open(new Path("/alphabet"));
    byte[] buf = new byte[newLine.getBytes().length];
    in.readFully(buf);
    Assert.assertArrayEquals(newLine.getBytes(), buf);
  }

  @Test
  public void append() throws IOException {
    final String line = "abc";
    FSDataOutputStream out = fs.create(new Path("/alphabet"));
    out.writeBytes(line);
    out.close();

    final String line2 = "def";
    out = fs.append(new Path("/alphabet"));
    out.writeBytes(line2);
    out.close();

    FSDataInputStream in = fs.open(new Path("/alphabet"));
    byte[] buf = new byte[6];
    in.readFully(buf);
    Assert.assertEquals(line + line2, new String(buf));
  }

  @Test(expected = FileNotFoundException.class)
  public void appendNoSuchFile() throws IOException {
    fs.append(noSuchFile);
  }

  @Test(expected = AccessControlException.class)
  public void appendNoWritePerms() throws IOException {
    final String line = "abc";
    FSDataOutputStream out = fs.create(new Path("/unwritable"),
        new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL), false, 1, (short)1, 1L, null);
    out.writeBytes(line);
    out.close();

    out = fs.append(new Path("/unwritable"));
  }

  @Test
  public void deleteNonExistentFile() throws IOException {
    Assert.assertFalse(fs.delete(noSuchFile, false));
  }

  @Test(expected = IOException.class)
  public void deleteDirNonRecursive() throws IOException {
    fs.mkdirs(new Path("/a"));
    final String line = "abc";
    FSDataOutputStream out = fs.create(new Path("/a/lphabet"));
    out.writeBytes(line);
    out.close();
    fs.delete(new Path("/a"), false);
  }

  @Test
  public void absolutePath() throws IOException {
    final String line = "abc";
    FSDataOutputStream out = fs.create(new Path("/alphabet"));
    out.writeBytes(line);
    out.close();

    Assert.assertTrue(fs.delete(new Path("/alphabet"), true));
    Assert.assertEquals(0, fs.listStatus(root).length);
  }

  @Test
  public void relativePath() throws IOException {
    final String line = "abc";
    fs.setWorkingDirectory(root);
    FSDataOutputStream out = fs.create(new Path("alphabet"));
    out.writeBytes(line);
    out.close();

    Assert.assertTrue(fs.delete(new Path("alphabet"), true));
    Assert.assertEquals(0, fs.listStatus(root).length);
  }

  @Test
  public void deleteRecursive() throws IOException {
    fs.mkdirs(new Path("/a/b"));
    FSDataOutputStream out = fs.create(new Path("/alphabet"));
    out.writeBytes("abc");
    out.close();

    out = fs.create(new Path("/a/greek"));
    out.writeBytes("alpha beta gamma");
    out.close();

    out = fs.create(new Path("/a/b/hebrew"));
    out.writeBytes("aleph beth gimel");
    out.close();

    Assert.assertTrue(fs.delete(new Path("/a"), true));
    FileStatus[] stats = fs.listStatus(root);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/alphabet", stats[0].getPath().toString());
  }

  @Test(expected = FileNotFoundException.class)
  public void renameToNoSuchFile() throws IOException {
    final String line = "abc";
    FSDataOutputStream out = fs.create(new Path("/alphabet"));
    out.writeBytes(line);
    out.close();

    fs.rename(new Path("/alphabet"), new Path("/no/such/file"));
  }

  @Test(expected = FileNotFoundException.class)
  public void renameNoSuchFile() throws IOException {
    fs.rename(noSuchFile, root);
  }

  @Test
  public void renameFile() throws IOException {
    final String line = "abc";
    Path alphabet = new Path("/alphabet");
    Path newAlphabet = new Path("memfs:/newalphabet");
    FSDataOutputStream out = fs.create(alphabet);
    out.writeBytes(line);
    out.close();

    fs.rename(alphabet, newAlphabet);

    FileStatus[] stats = fs.listStatus(root);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals(newAlphabet, stats[0].getPath());

    FSDataInputStream in = fs.open(newAlphabet);
    byte[] buf = new byte[3];
    in.readFully(buf);
    Assert.assertEquals(line, new String(buf));
  }

  @Test
  public void renameRelativeFile() throws IOException {
    fs.setWorkingDirectory(root);
    final String line = "abc";
    Path alphabet = new Path("alphabet");
    Path newAlphabet = new Path("memfs:/newalphabet");
    FSDataOutputStream out = fs.create(alphabet);
    out.writeBytes(line);
    out.close();

    fs.rename(alphabet, newAlphabet);

    FileStatus[] stats = fs.listStatus(root);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals(new Path(root, newAlphabet), stats[0].getPath());

    FSDataInputStream in = fs.open(newAlphabet);
    byte[] buf = new byte[3];
    in.readFully(buf);
    Assert.assertEquals(line, new String(buf));
  }

  @Test
  public void renameBetweenDirs() throws IOException {
    Path latin = new Path("memfs:/latin");
    Path greek = new Path("/greek");
    fs.mkdirs(latin);
    fs.mkdirs(greek);
    final String line = "abc";
    Path alphabet = new Path(greek, "alphabet");
    Path newAlphabet = new Path(latin, "alphabet");
    FSDataOutputStream out = fs.create(alphabet);
    out.writeBytes(line);
    out.close();

    fs.rename(alphabet, newAlphabet);

    FileStatus[] stats = fs.listStatus(latin);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals(newAlphabet, stats[0].getPath());

    FSDataInputStream in = fs.open(newAlphabet);
    byte[] buf = new byte[3];
    in.readFully(buf);
    Assert.assertEquals(line, new String(buf));
  }

  @Test
  public void renameToDir() throws IOException {
    Path latin = new Path("memfs:/latin");
    fs.mkdirs(latin);
    final String line = "abc";
    Path alphabet = new Path("/alphabet");
    FSDataOutputStream out = fs.create(alphabet);
    out.writeBytes(line);
    out.close();

    fs.rename(alphabet, latin);

    Path newAlphabet = new Path(latin, "alphabet");
    FileStatus[] stats = fs.listStatus(latin);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals(newAlphabet, stats[0].getPath());

    FSDataInputStream in = fs.open(newAlphabet);
    byte[] buf = new byte[3];
    in.readFully(buf);
    Assert.assertEquals(line, new String(buf));
  }

  @Test
  public void renameDirTree() throws IOException {
    Path abc = new Path("/a/b/c");
    fs.mkdirs(abc);
    final String line = "O say can you see";
    Path d = new Path(abc, "d");
    FSDataOutputStream out = fs.create(d);
    out.writeBytes(line);
    out.close();

    Path alpha = new Path("/alpha");
    fs.mkdirs(alpha);

    fs.rename(new Path("/a/b"), alpha);

    FileStatus[] stats = fs.listStatus(alpha);
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/alpha/b", stats[0].getPath().toString());

    stats = fs.listStatus(stats[0].getPath());
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/alpha/b/c", stats[0].getPath().toString());

    stats = fs.listStatus(stats[0].getPath());
    Assert.assertEquals(1, stats.length);
    Assert.assertEquals("memfs:/alpha/b/c/d", stats[0].getPath().toString());

    FSDataInputStream in = fs.open(new Path("/alpha/b/c/d"));
    byte[] buf = new byte[line.length()];
    in.readFully(buf);
    Assert.assertEquals(line, new String(buf));
  }

  @Test(expected = AccessControlException.class)
  public void renameNoPermission() throws IOException {
    Path a = new Path("/a");
    fs.mkdirs(a, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL));
    Path b = new Path("/b");
    fs.mkdirs(b);
    fs.rename(b, a);
  }

  @Test
  public void dumpFs() throws IOException {
    String line1 = "abc";
    String line23 = "def";

    Path dir1 = new Path("/dir1");
    Path dir23 = new Path("/dir2/dir3");

    Path file1 = new Path(dir1, "file1");
    Path file23 = new Path(dir23, "file23");

    FSDataOutputStream out = fs.create(file1);
    out.writeBytes(line1);
    out.close();

    out = fs.create(file23);
    out.writeBytes(line23);
    out.close();

    FileSystem localFs = new LocalFileSystem();
    localFs.initialize(localFs.getUri(), new Configuration());
    Path dumpPath = new Path(System.getProperty("hadoop.tmp.dir") + "/memfs-dump");
    localFs.delete(dumpPath, true);

    ((InMemoryFs)fs).dumpToDurableFiles(localFs, dumpPath);

    Path localFile1 = new Path(dumpPath.toString() + file1.toString());
    Assert.assertTrue(localFs.exists(localFile1));
    FSDataInputStream in = localFs.open(localFile1);
    byte[] buf = new byte[line1.length()];
    in.readFully(buf);
    Assert.assertEquals(line1, new String(buf));

    Path localFile23 = new Path(dumpPath.toString() + file23.toString());
    Assert.assertTrue(localFs.exists(localFile23));
    in = localFs.open(localFile23);
    buf = new byte[line23.length()];
    in.readFully(buf);
    Assert.assertEquals(line23, new String(buf));

  }

  @Test
  public void instantiateFromConf() throws IOException {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "memfs:///");
    FileSystem memFs = FileSystem.get(conf);
    Assert.assertTrue(memFs instanceof InMemoryFs);
  }

  private void sortStats(FileStatus[] stats) {
    Arrays.sort(stats, Comparator.comparing(o -> o.getPath().getName()));
  }

}
