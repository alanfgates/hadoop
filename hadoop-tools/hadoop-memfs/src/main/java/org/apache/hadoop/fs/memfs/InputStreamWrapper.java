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

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamWrapper extends InputStream implements Seekable, PositionedReadable{
  private final byte[] data;
  private int pos;
  private boolean closed;
  private int mark;

  public InputStreamWrapper(byte[] data) {
    this.data = data;
    pos = 0;
    closed = false;
  }

  @Override
  public int read() throws IOException {
    checkClosed();
    return pos >= data.length ? -1 : data[pos++];
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkClosed();
    if (pos >= data.length) return -1;
    if (b.length == 0) return 0;
    int length = Math.min(len, data.length - pos);
    System.arraycopy(data, pos, b, 0, length);
    pos += length;
    return length;
  }

  @Override
  public long skip(long n) throws IOException {
    checkClosed();
    if (pos >= data.length) return 0;
    if (n < 0) return 0;
    long toSkip = Math.min(n, data.length - pos);
    pos += toSkip;
    return toSkip;
  }

  @Override
  public int available() throws IOException {
    checkClosed();
    return data.length - pos;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }

  @Override
  public synchronized void mark(int readlimit) {
    mark = pos;
  }

  @Override
  public synchronized void reset() throws IOException {
    pos = mark;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    if (position >= data.length) return 0;
    int length = Math.min(len, data.length - (int)position);
    System.arraycopy(data, (int)position, buffer, offset, length);
    return length;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int len) throws IOException {
    int bytesRead = read(position, buffer, offset, len);
    if (bytesRead < len) throw new EOFException();
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void seek(long offset) throws IOException {
    checkClosed();
    if (offset > Integer.MAX_VALUE) offset = Integer.MAX_VALUE;
    pos = (int)offset;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long l) throws IOException {
    // I have no idea what this is supposed to do
    throw new UnsupportedOperationException();
  }

  private void checkClosed() throws IOException {
    if (closed) throw new IOException("Attempt to operate on closed stream");
  }
}
