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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

abstract class InMemoryFile {

  private InMemoryDirectory parent;
  private Path path;
  private long lastModificationTime;
  private long lastAccessTime;
  private FsPermission perms;
  private String owner;
  private String group;


  /**
   * Only for creating the root directory.
   */
  InMemoryFile(InMemoryFs fs) {
    path = new Path(fs.getUri() + "/");
    this.perms = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    this.owner = "root";
    this.group = "root";
  }

  InMemoryFile(InMemoryDirectory parent, String name, FsPermission perms, String owner,
               String group) throws IOException {
    this.parent = parent;
    parent.addFile(name, this);
    this.path = new Path(parent.getPath(), name);
    assert path.isAbsolute();
    lastModificationTime = lastAccessTime = System.currentTimeMillis();
    this.perms = perms;
    this.owner = owner;
    this.group = group;
  }

  abstract FileStatus stat();

  /**
   * Dump the contents of this file to another files system.
   * @param otherFs Other filesystem to dump to
   * @param base path in that filesystem to start at
   * @throws IOException if thrown by an underlying operation
   */
  abstract void dump(FileSystem otherFs, Path base) throws IOException;

  boolean isDirectory() {
    return false;
  }

  InMemoryDirectory asDirectory() throws IOException {
    throw new ParentNotDirectoryException(path.toString() + " is not a directory");
  }

  InMemoryRegularFile asRegularFile() throws IOException {
    throw new IOException(path.toString() + " is not a regular file");
  }

  Path getPath() {
    return path;
  }

  InMemoryDirectory getParent() {
    return parent;
  }

  void move(InMemoryDirectory newParent, String newName) throws IOException {
    parent.removeFile(path.getName());
    this.parent = newParent;
    setPath(newParent.getPath(), newName);
    parent.addFile(newName, this);
  }

  void setPath(Path parent, String name) {
    this.path = new Path(parent, name);
  }

  void delete() {
    parent.removeFile(path.getName());
  }

  long getLastModificationTime() {
    return lastModificationTime;
  }

  long getLastAccessTime() {
    return lastAccessTime;
  }

  FsPermission getPerms() {
    return perms;
  }

  String getOwner() {
    return owner;
  }

  String getGroup() {
    return group;
  }

  void setPerms(FsPermission perms) {
    this.perms = perms;
    if (parent != null) parent.updateModificationTime();
  }

  void setOwner(String owner) {
    this.owner = owner;
    if (parent != null) parent.updateModificationTime();
  }

  void setGroup(String group) {
    this.group = group;
    if (parent != null) parent.updateModificationTime();
  }

  void updateModificationTime() {
    lastModificationTime = System.currentTimeMillis();
  }

  void updateAccessTime() {
    lastAccessTime = System.currentTimeMillis();
  }

  void setLastModificationTime(long lastModificationTime) {
    this.lastModificationTime = lastModificationTime;
  }

  void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }
}
