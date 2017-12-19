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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class InMemoryFs extends FileSystem {

  private static final String URI_SCHEME = "memfs";

  private static InMemoryDirectory root;

  private final CurrentWorkingDirectory cwd;

  public InMemoryFs() throws IOException {
    synchronized (InMemoryFs.class) {
      if (root == null) {
        root = new InMemoryDirectory(this);
      }
    }

    cwd = new CurrentWorkingDirectory();
    LOG.info("Instantiating in memory file system");
  }


  @Override
  public URI getUri() {
    try {
      return new URI(URI_SCHEME, null, "/", null);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Help me Obi Wan Kenobi", e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    InMemoryRegularFile target = findFileMustExist(path).asRegularFile();
    checkCanRead(target);
    return new FSDataInputStream(new InputStreamWrapper(target.readData()));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
                                   int bufferSize, short replication,
                                   long blockSize, Progressable progressable) throws IOException {
    InMemoryFile target = findFileNotExistsOk(path);
    InMemoryRegularFile toWriteTo;
    if (target != null) {
      if (!overwrite) throw new FileAlreadyExistsException(path.toString() + " already exists");
      checkCanWrite(target);
      toWriteTo = target.asRegularFile();
    } else {
      // Determine the directory to create the file in
      InMemoryDirectory parentDir;
      if (path.depth() > 1) {
        InMemoryFile parent = findFileNotExistsOk(path.getParent());
        // It's possible the file's parent doesn't exist.  If so, create the directory first
        if (parent == null) {
          FsPermission dirPerms =
              new FsPermission(fsPermission.getUserAction().or(FsAction.EXECUTE),
                  fsPermission.getGroupAction(), fsPermission.getOtherAction());
          mkdirs(path.getParent(), dirPerms);
          parent = findFileNotExistsOk(path.getParent());
          assert parent != null;
        }
        parentDir = parent.asDirectory();

      } else {
        parentDir = path.isAbsolute() ? root : cwd.getCwd();
      }
      checkCanWrite(parentDir);
      toWriteTo =
          new InMemoryRegularFile(parentDir, path.getName(), fsPermission, getUser(), getGroup());
    }
    return new FSDataOutputStream(new OutputStreamWrapper(toWriteTo, false), statistics, 0);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable)
      throws IOException {
    InMemoryRegularFile toAppendTo = findFileMustExist(path).asRegularFile();
    checkCanWrite(toAppendTo);
    toAppendTo.updateModificationTime();
    return new FSDataOutputStream(new OutputStreamWrapper(toAppendTo, true), statistics,
        toAppendTo.getLen());
  }

  @Override
  public boolean rename(Path src, Path dest) throws IOException {
    InMemoryFile srcFile = findFileMustExist(src);

    // First, figure out whether dest is a directory or a new entry in a directory
    InMemoryFile maybeDest = findFileNotExistsOk(dest);
    InMemoryDirectory destDir;
    String targetName;
    if (maybeDest == null || !maybeDest.isDirectory()) {
      if (maybeDest != null) delete(dest, false);
      // Okay, it looks like dest will be a new entry in an existing directory.  Find that
      // directory.
      destDir = findFileMustExist(dest.getParent()).asDirectory();
      targetName = dest.getName();
    } else {
      destDir = maybeDest.asDirectory();
      targetName = src.getName(); // keep the same name, move it into the directory
    }
    checkCanWrite(destDir);

    srcFile.move(destDir, targetName);
    return true;
  }


  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    InMemoryFile target = findFileNotExistsOk(path);
    if (target == null) return false;

    if (target.isDirectory() && !recursive) {
      // Check to see if the directory is empty
      if (target.asDirectory().getFiles().size() > 0) {
        throw new IOException("Directory " + path.toString() + " is not empty");

      }
    }
    InMemoryDirectory parentDir;
    if (path.depth() > 1) {
      parentDir = findFileMustExist(path.getParent()).asDirectory();
    } else {
      parentDir = path.isAbsolute() ? root : cwd.getCwd();
    }
    checkCanWrite(parentDir);
    target.delete();
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    InMemoryDirectory dir = findFileMustExist(path).asDirectory();
    checkCanRead(dir);
    dir.updateAccessTime();
    List<FileStatus> stats = new ArrayList<>();
    for (InMemoryFile f : dir.getFiles()) stats.add(f.stat());
    return stats.toArray(new FileStatus[stats.size()]);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    cwd.setCwd(path);
  }

  @Override
  public Path getWorkingDirectory() {
    return cwd.getPath();
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    DicedPath pair = determineWhichPiecesOfPathExist(path);
    if (pair.nonExisting.isEmpty()) {
      if (pair.existing.isDirectory()) return false;
      else throw new FileAlreadyExistsException(path.toString());
    }

    mkdir(pair.existing, pair.nonExisting, fsPermission);
    return true;
  }

  // Assumes path already exists, makes the next directory down in levels
  private void mkdir(InMemoryFile file, Stack<String> levels, FsPermission fsPermission)
      throws IOException {
    assert file.isDirectory();
    InMemoryDirectory parentDir = file.asDirectory();
    checkCanWrite(parentDir);
    InMemoryDirectory newDir = new InMemoryDirectory(parentDir, levels.pop(), fsPermission,
        getUser(), getGroup());
    if (!levels.isEmpty()) mkdir(newDir, levels, fsPermission);
  }


  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    InMemoryFile file = findFileMustExist(path);
    if (path.isAbsolute() && path.depth() == 0) {
      checkCanRead(root);
      root.updateAccessTime();
    } else {
      checkCanRead(file.getParent());
      file.getParent().updateAccessTime();
    }
    return file.stat();
  }

  @Override
  public String getScheme() {
    return URI_SCHEME;
  }

  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    InMemoryFile file = findFileMustExist(path);
    if (path.isAbsolute() && path.depth() == 0) checkCanWrite(root);
    else checkCanWrite(file.getParent());
    file.setPerms(permission);
  }

  @Override
  public void setOwner(Path path, String username, String groupname) throws IOException {
    InMemoryFile file = findFileMustExist(path);
    if (path.isAbsolute() && path.depth() == 0) checkCanWrite(root);
    else checkCanWrite(file.getParent());
    if (username != null) file.setOwner(username);
    if (groupname != null) file.setGroup(groupname);
  }

  @Override
  public void setTimes(Path path, long lastModTime, long lastAccessTime) throws IOException {
    InMemoryFile file = findFileMustExist(path);
    if (lastModTime > -1) file.setLastModificationTime(lastModTime);
    if (lastAccessTime > -1) file.setLastAccessTime(lastAccessTime);
  }

  @Override
  public boolean exists(Path path) throws IOException {
    return findFileNotExistsOk(path) != null;
  }

  /**
   * Dump the entire file system into another file system.  This is intended for use in error
   * cases where the user wants to see what was written to the files.  There is no locking of
   * files or directories in this dump, so if you do it while the file system is in use you may
   * see partial changes.
   * @param otherFs durable file system to write to
   * @param base path in the file system to start writing at
   * @throws IOException if an underlying operation throws it
   */
  public void dumpToDurableFiles(FileSystem otherFs, Path base) throws IOException {
    root.dump(otherFs, base);
  }

  /**
   * Adds the file system's scheme to the path, if it is not already there.
   * @param path path to possibly add the scheme to
   * @return path with the scheme
   */
  static Path addSchemeToPath(Path path) {
    URI asUri = path.toUri();
    if (asUri.getScheme() == null) {
      try {
        URI withScheme = new URI(URI_SCHEME, null, asUri.getPath(), null, null);
        return new Path(withScheme);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Help me Obi Wan Kenobi", e);
      }
    } else {
      return path;
    }
  }

  static Path mergePathsIntelligently(Path base, Path part2) {
    if (part2.toString().equals(".")) return base;
    if (part2.toString().equals("..")) return base.getParent();
    StringBuilder buf = new StringBuilder(base.toString());
    buf.append('/');
    buf.append(part2.toUri().getPath());
    return new Path(buf.toString());
  }

  // Find a file when you expect it to exist.  If it does not exist a FileNotFoundException will
  // be thrown.  This method should never return null.
  private InMemoryFile findFileMustExist(Path path) throws IOException {
    DicedPath p = determineWhichPiecesOfPathExist(path);
    // If the file exists, the stack will be empty
    if (!p.nonExisting.isEmpty()) {
      throw new FileNotFoundException(path + ", no such file or directory");
    }
    assert p.existing != null;
    return p.existing;
  }

  // Find a file when you are not sure if it exists.  If it does not, null will be returned.
  private InMemoryFile findFileNotExistsOk(Path path) {
    // This can throw an IOException if a user passes a bogus path (like something with a file
    // instead of a directory in the middle).  Catch the exception and return null, it just means
    // the file doesn't exist.
    try {
      DicedPath p = determineWhichPiecesOfPathExist(path);
      return p.nonExisting.isEmpty() ? p.existing : null;
    } catch (IOException e) {
      LOG.debug("Caught exception while checking to see if file exists", e);
      return null;
    }
  }

  private static final Stack<String> emptyStack = new Stack<>();

  // Determine which pieces of a path exist and which don't.  Returns the longest path from the
  // passed in path that already exists, and a stack of strings for further elements of the path
  // that do not exist.
  private DicedPath determineWhichPiecesOfPathExist(Path path)
      throws IOException {
    // We have to handle root as a special case
    if (path.getName().length() == 0) return new DicedPath(root, emptyStack);
    InMemoryDirectory curDir = path.isAbsolute() ? root : cwd.getCwd();
    Stack<String> components = new Stack<>();
    Path curPath = path;
    do {
      components.push(curPath.getName());
      curPath = curPath.getParent();
    } while (curPath.getName().length() > 0);
    InMemoryFile file = null;
    while (components.size() > 0) {
      checkCanSearch(curDir);
      String name = components.pop();
      file = curDir.getFile(name);
      if (file == null) {
        // Be sure to push the name back onto the stack
        components.push(name);
        return new DicedPath(curDir, components);
      }
      // entries should be directories until we get to the leaf
      if (components.size() > 0) curDir = file.asDirectory();
    }
    // Components should be empty by now
    return new DicedPath(file, components);
  }

  private void checkCanExecute(InMemoryFile file) throws IOException {
    checkCan(file.stat(), FsAction.EXECUTE, "execute");
  }

  private void checkCanSearch(InMemoryDirectory dir) throws IOException {
    checkCanExecute(dir);
  }

  private void checkCanRead(InMemoryFile file) throws IOException {
    checkCan(file.stat(), FsAction.READ, "read");
  }

  private void checkCanWrite(InMemoryFile file) throws IOException {
    checkCan(file.stat(), FsAction.WRITE, "write");
  }

  private void checkCan(FileStatus stat, FsAction action, String verb) throws IOException {
    if (stat.getOwner().equals(getUser())) {
      if (stat.getPermission().getUserAction().and(action) != action) {
        throw new AccessControlException("User " + getUser() + " does not have permission to " +
            verb + " " + stat.getPath().toString());
      }
      return;
    }

    for (String group : getGroups()) {
      if (stat.getGroup().equals(group)) {
        if (stat.getPermission().getGroupAction().and(action) != action) {
          throw new AccessControlException("Group " + getGroup() + " does not have permission to " +
              verb + " " + stat.getPath().toString());
        }
        return;
      }
    }

    if (stat.getPermission().getOtherAction().and(action) != action) {
      throw new AccessControlException("World does not have permission to " + verb +
          " " + stat.getPath().toString());
    }

  }

  private String getUser() throws IOException {
    return UserGroupInformation.getCurrentUser().getUserName();
  }

  private String getGroup() throws IOException {
    return UserGroupInformation.getCurrentUser().getPrimaryGroupName();
  }

  private String[] getGroups() throws IOException {
    return UserGroupInformation.getCurrentUser().getGroupNames();
  }

  @VisibleForTesting
  static void reset() {
    root = null;
  }

  // So here's weirdness.  FS allows setting the working directory to path that does not exist.
  // This is painful later as we need to reference the cwd but it may not be there.  This class
  // is an attempt to encapsulate the handling of this insanity.
  private class CurrentWorkingDirectory {
    private InMemoryDirectory dir;
    private Path path;

    CurrentWorkingDirectory() {
      setCwd(root.getPath());
    }

    void setCwd(Path p) {
      path = p.isAbsolute() ? p : mergePathsIntelligently(path, p);
      assert path.isAbsolute();
      dir = null;
    }

    Path getPath() {
      return path;
    }

    InMemoryDirectory getCwd() throws IOException {
      if (dir == null) {
        dir = findFileMustExist(path).asDirectory();
      }
      return dir;
    }
  }

  private static class DicedPath {
    final InMemoryFile existing;
    final Stack<String> nonExisting;

    DicedPath(InMemoryFile existing, Stack<String> nonExisting) {
      this.existing = existing;
      this.nonExisting = nonExisting;
    }
  }

}
