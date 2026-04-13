/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.codec;

import io.airlift.compress.lz4.Lz4Decompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.compress.DirectDecompressor;

public class Lz4RawDecompressor extends NonBlockedDecompressor implements DirectDecompressor {

  private static final int LENGTH_MASK = 0x0F;
  private static final int MIN_MATCH_LENGTH = 4;
  private static final int MIN_LAST_LITERAL_LENGTH_WITH_MATCH = 5;

  private final Lz4Decompressor decompressor = new Lz4Decompressor();

  @Override
  protected int maxUncompressedLength(ByteBuffer compressed, int maxUncompressedLength) throws IOException {
    return Math.max(maxUncompressedLength, compressed.remaining());
  }

  @Override
  protected int maxUncompressedLengthOnDecompressionError(
      ByteBuffer compressed, int currentMaxUncompressedLength, Throwable error) throws IOException {
    return Math.max(currentMaxUncompressedLength, exactUncompressedLength(compressed));
  }

  private static int exactUncompressedLength(ByteBuffer compressed) throws IOException {
    ByteBuffer input = compressed.duplicate();
    long uncompressedLength = 0;
    boolean sawMatch = false;

    while (input.hasRemaining()) {
      int token = input.get() & 0xFF;

      long literalLength = token >>> 4;
      literalLength = readLength(input, literalLength);
      if (literalLength > input.remaining()) {
        throw new IOException("Malformed LZ4 input: literal length exceeds remaining compressed input");
      }
      uncompressedLength = checkedAdd(uncompressedLength, literalLength);
      input.position(input.position() + (int) literalLength);

      if (!input.hasRemaining()) {
        if (sawMatch && literalLength < MIN_LAST_LITERAL_LENGTH_WITH_MATCH) {
          throw new IOException(
              "Malformed LZ4 input: last literals must be at least " + MIN_LAST_LITERAL_LENGTH_WITH_MATCH
                  + " bytes");
        }
        break;
      }

      if (input.remaining() < 2) {
        throw new IOException("Malformed LZ4 input: missing match offset");
      }

      int offset = (input.get() & 0xFF) | ((input.get() & 0xFF) << 8);
      if (offset == 0 || offset > uncompressedLength) {
        throw new IOException("Malformed LZ4 input: invalid match offset " + offset);
      }

      long matchLength = token & LENGTH_MASK;
      matchLength = readLength(input, matchLength);
      matchLength = checkedAdd(matchLength, MIN_MATCH_LENGTH);
      uncompressedLength = checkedAdd(uncompressedLength, matchLength);
      sawMatch = true;

      if (!input.hasRemaining()) {
        throw new IOException("Malformed LZ4 input: missing trailing literals");
      }
    }

    return (int) uncompressedLength;
  }

  @Override
  protected int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    decompressor.decompress(compressed, uncompressed);
    int uncompressedSize = uncompressed.position();
    uncompressed.limit(uncompressedSize);
    uncompressed.rewind();
    return uncompressedSize;
  }

  @Override
  public void decompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    uncompress(compressed, uncompressed);
  }

  private static long readLength(ByteBuffer input, long length) throws IOException {
    if (length != LENGTH_MASK) {
      return length;
    }

    while (true) {
      if (!input.hasRemaining()) {
        throw new IOException("Malformed LZ4 input: truncated length");
      }

      int value = input.get() & 0xFF;
      length = checkedAdd(length, value);
      if (value != 255) {
        return length;
      }
    }
  }

  private static long checkedAdd(long left, long right) throws IOException {
    long result = left + right;
    if (result < left || result > Integer.MAX_VALUE) {
      throw new IOException("LZ4 uncompressed length exceeds supported maximum: " + result);
    }
    return result;
  }
}
