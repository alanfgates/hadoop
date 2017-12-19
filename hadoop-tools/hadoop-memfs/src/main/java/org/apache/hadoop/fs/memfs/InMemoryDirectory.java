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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class InMemoryDirectory extends InMemoryFile {

  private final ConcurrentMap<String, InMemoryFile> files;

  public InMemoryDirectory(InMemoryFs fs) {
    super(fs);
    files = new ConcurrentHashMap<>();
  }

  InMemoryDirectory(InMemoryDirectory parent, String name, FsPermission perms, String owner,
                    String group) throws IOException {
    super(parent, name, perms, owner, group);
    files = new ConcurrentHashMap<>();
  }

  @Override
  FileStatus stat() {
    return new FileStatus(0, true, 1, 1, getLastModificationTime(), getLastAccessTime(), getPerms(),
        getOwner(), getGroup(), null, InMemoryFs.addSchemeToPath(getPath()));

  }

  @Override
  void dump(FileSystem otherFs, Path base) throws IOException {
    // Try to make our directory.  If we're root, it will likely already exist
    Path ourPath = InMemoryFs.mergePathsIntelligently(base, getPath());
    if (!otherFs.mkdirs(ourPath) && !getPath().toString().equals("/")) {
      throw new FileAlreadyExistsException(ourPath.toString());
    }
    for (InMemoryFile child : files.values()) child.dump(otherFs, base);
  }

  @Override
  boolean isDirectory() {
    return true;
  }

  @Override
  void setPath(Path parent, String name) {
    super.setPath(parent, name);
    for (InMemoryFile child : files.values()) child.setPath(getPath(), child.getPath().getName());
  }

  void addFile(String name, InMemoryFile file) throws IOException {
    if (files.containsKey(name)) {
      throw new IOException("File " + name + " already exists");
    }
    files.put(name, file);
    updateModificationTime();
  }

  void removeFile(String name) {
    files.remove(name);
    updateModificationTime();
  }

  @Override
  InMemoryDirectory asDirectory() throws IOException {
    return this;
  }

  InMemoryFile getFile(String name) {
    updateAccessTime();
    return files.get(name);
  }

  Collection<InMemoryFile> getFiles() {
    updateAccessTime();
    return files.values();
  }
}
