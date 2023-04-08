/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.iq80.leveldb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.iq80.leveldb.util.Snappy.SPI;

/**
 * Some glue code that uses the java.util.zip classes to implement ZLIB
 * compression for leveldb.
 */
public class Zlib
{
  private Zlib()
  {
  }

  private static final int DEFAULT_COMPRESSION_LEVEL = 8;

  /**
   * From:
   * http://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-
   * an-inputstream
   */
  public static class ByteBufferBackedInputStream extends InputStream
  {
    ByteBuffer buf;

    public ByteBufferBackedInputStream(ByteBuffer buf)
    {
      this.buf = buf;
    }

    public int read() throws IOException
    {
      if (!buf.hasRemaining()) {
        return -1;
      }
      return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException
    {
      if (!buf.hasRemaining()) {
        return -1;
      }

      len = Math.min(len, buf.remaining());
      buf.get(bytes, off, len);
      return len;
    }
  }

  public static class ByteBufferBackedOutputStream extends OutputStream
  {
    ByteBuffer buf;

    public ByteBufferBackedOutputStream(ByteBuffer buf)
    {
      this.buf = buf;
    }

    public void write(int b) throws IOException
    {
      buf.put((byte) b);
    }

    public void write(byte[] bytes, int off, int len) throws IOException
    {
      buf.put(bytes, off, len);
    }

  }

  /**
   * Use the same SPI interface as Snappy, for the case if leveldb ever gets
   * a compression plug-in type.
   */
  private static class ZLibSPI implements SPI
  {
    boolean raw;

    ZLibSPI(boolean raw)
    {
      this.raw = raw;
    }

    private int copy(InputStream in, OutputStream out) throws IOException
    {
      byte[] buffer = new byte[1024];
      int read;
      int count = 0;
      while (-1 != (read = in.read(buffer))) {
        out.write(buffer, 0, read);
        count += read;
      }
      return count;
    }

    @Override
    public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
        throws IOException
    {
      int count = copy(new InflaterInputStream(new ByteBufferBackedInputStream(
          compressed), new Inflater(raw)), new ByteBufferBackedOutputStream(uncompressed));
      // Prepare the output buffer for reading.
      uncompressed.flip();
      return count;
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int length,
        byte[] output, int outputOffset) throws IOException
    {
      return copy(
          new InflaterInputStream(new ByteArrayInputStream(input, inputOffset,
              length), new Inflater(raw)),
          new ByteBufferBackedOutputStream(ByteBuffer.wrap(output,
              outputOffset, output.length - outputOffset)));
    }

    @Override
    public int compress(byte[] input, int inputOffset, int length,
        byte[] output, int outputOffset) throws IOException
    {
      return copy(
          new DeflaterInputStream(new ByteArrayInputStream(input, inputOffset,
              length), new Deflater(DEFAULT_COMPRESSION_LEVEL, raw)),
          new ByteBufferBackedOutputStream(ByteBuffer.wrap(output,
              outputOffset, output.length - outputOffset)));
    }

    @Override
    public byte[] compress(String text) throws IOException
    {
      byte[] input = text.getBytes();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      copy(new DeflaterInputStream(new ByteArrayInputStream(input, 0,
          input.length), new Deflater(DEFAULT_COMPRESSION_LEVEL, raw)), baos);
      return baos.toByteArray();
    }

    @Override
    public int maxCompressedLength(int length)
    {
      // unused
      return 0;
    }
  }

  private static final SPI ZLIB;
  private static final SPI ZLIB_RAW;
  static
  {
    ZLIB = new ZLibSPI(false);
    ZLIB_RAW = new ZLibSPI(true);
  }

  public static void uncompress(ByteBuffer compressed, ByteBuffer uncompressed, boolean raw)
      throws IOException
  {
    if (raw) {
      ZLIB_RAW.uncompress(compressed, uncompressed);
    }
    else {
      ZLIB.uncompress(compressed, uncompressed);
    }
  }

  public static void uncompress(byte[] input, int inputOffset, int length,
      byte[] output, int outputOffset, boolean raw) throws IOException
  {
    if (raw) {
      ZLIB_RAW.uncompress(input, inputOffset, length, output, outputOffset);
    }
    else {
      ZLIB.uncompress(input, inputOffset, length, output, outputOffset);
    }
  }

  public static int compress(byte[] input, int inputOffset, int length,
      byte[] output, int outputOffset, boolean raw) throws IOException
  {
    if (raw) {
      return ZLIB_RAW.compress(input, inputOffset, length, output, outputOffset);
    }
    else {
      return ZLIB.compress(input, inputOffset, length, output, outputOffset);
    }
  }

  public static byte[] compress(String text, boolean raw) throws IOException
  {
    if (raw) {
      return ZLIB_RAW.compress(text);
    }
    else {
      return ZLIB.compress(text);
    }
  }
}
