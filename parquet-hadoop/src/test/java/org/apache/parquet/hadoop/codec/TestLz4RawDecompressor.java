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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class TestLz4RawDecompressor {

  private static final int TOKEN_MAX_NIBBLE = 0x0F;
  private static final int TOKEN_LITERAL_SHIFT = 4;
  private static final int MIN_MATCH_LENGTH = 4;

  @Test
  public void decompressWithSmallReadBufferHandlesExtendedLengths() throws IOException {
    byte[] literals = "abcdefghijklmnopqrst".getBytes(StandardCharsets.US_ASCII);
    int matchLength = 9000;
    byte[] trailingLiterals = "VWXYZ".getBytes(StandardCharsets.US_ASCII);

    byte[] compressed = createExtendedLiteralAndMatchBlock(literals, matchLength, trailingLiterals);

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    expected.write(literals);
    for (int i = 0; i < matchLength; i++) {
      expected.write(literals[literals.length - 1]);
    }
    expected.write(trailingLiterals);

    byte[] actual = decompressWithChunkSize(compressed, 8 * 1024);
    Assert.assertArrayEquals(expected.toByteArray(), actual);
  }

  @Test
  public void parseLengthRejectsInvalidOffset() {
    ExposedLz4RawDecompressor decompressor = new ExposedLz4RawDecompressor();
    IOException error = expectIOException(() -> decompressor.exactUncompressedLength(new byte[] {0x00, 0x00, 0x00}));
    Assert.assertTrue(error.getMessage().contains("invalid match offset"));
  }

  @Test
  public void parseLengthRejectsTruncatedLength() {
    ExposedLz4RawDecompressor decompressor = new ExposedLz4RawDecompressor();
    IOException error =
        expectIOException(() -> decompressor.exactUncompressedLength(new byte[] {(byte) 0xF0, (byte) 0xFF}));
    Assert.assertTrue(error.getMessage().contains("truncated length"));
  }

  @Test
  public void parseLengthRejectsOverflow() {
    ExposedLz4RawDecompressor decompressor = new ExposedLz4RawDecompressor();
    byte[] compressed = overflowLiteralLengthBlock();
    IOException error = expectIOException(() -> decompressor.exactUncompressedLength(compressed));
    Assert.assertTrue(error.getMessage().contains("exceeds supported maximum"));
  }

  @Test
  public void parseLengthRejectsMissingTrailingLiterals() {
    ExposedLz4RawDecompressor decompressor = new ExposedLz4RawDecompressor();
    IOException error =
        expectIOException(() -> decompressor.exactUncompressedLength(createBlockWithoutTrailingLiterals()));
    Assert.assertTrue(error.getMessage().contains("missing trailing literals"));
  }

  @Test
  public void parseLengthRejectsShortTrailingLiteralsAfterMatch() {
    byte[] literals = "abcdefghijklmnopqrst".getBytes(StandardCharsets.US_ASCII);
    int matchLength = 9000;
    byte[] shortTrailingLiterals = "WXYZ".getBytes(StandardCharsets.US_ASCII);

    ExposedLz4RawDecompressor decompressor = new ExposedLz4RawDecompressor();
    IOException error = expectIOException(
        () -> decompressor.exactUncompressedLength(
            createExtendedLiteralAndMatchBlock(literals, matchLength, shortTrailingLiterals)));
    Assert.assertTrue(error.getMessage().contains("last literals must be at least"));
  }

  @Test
  public void decompressPropagatesLengthParsingErrorWithSuppressedInitialFailure() throws IOException {
    byte[] compressed = createBlockWithoutTrailingLiterals();

    Lz4RawDecompressor decompressor = new Lz4RawDecompressor();
    decompressor.setInput(compressed, 0, compressed.length);
    try {
      byte[] chunk = new byte[8 * 1024];
      decompressor.decompress(chunk, 0, chunk.length);
      Assert.fail("Expected IOException for malformed LZ4 data");
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("missing trailing literals"));
      Assert.assertEquals(1, expected.getSuppressed().length);
      Assert.assertTrue(expected.getSuppressed()[0] instanceof RuntimeException);
    } finally {
      decompressor.end();
    }
  }

  private static byte[] decompressWithChunkSize(byte[] compressed, int chunkSize) throws IOException {
    Lz4RawDecompressor decompressor = new Lz4RawDecompressor();
    decompressor.setInput(compressed, 0, compressed.length);

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] chunk = new byte[chunkSize];
      while (!decompressor.finished()) {
        int bytesRead = decompressor.decompress(chunk, 0, chunk.length);
        if (bytesRead == 0) {
          Assert.fail("Unexpected zero-byte read while decompressing LZ4 data");
        }
        out.write(chunk, 0, bytesRead);
      }
      return out.toByteArray();
    } finally {
      decompressor.end();
    }
  }

  private static byte[] createExtendedLiteralAndMatchBlock(byte[] literals, int matchLength, byte[] trailingLiterals) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    out.write((TOKEN_MAX_NIBBLE << TOKEN_LITERAL_SHIFT) | TOKEN_MAX_NIBBLE);
    writeLengthExtension(out, literals.length - TOKEN_MAX_NIBBLE);
    out.write(literals, 0, literals.length);

    out.write(0x01);
    out.write(0x00);
    writeLengthExtension(out, matchLength - TOKEN_MAX_NIBBLE - MIN_MATCH_LENGTH);

    out.write(trailingLiterals.length << TOKEN_LITERAL_SHIFT);
    out.write(trailingLiterals, 0, trailingLiterals.length);
    return out.toByteArray();
  }

  /**
   * Creates an LZ4 block that violates the LZ4 spec: the block ends with a match
   * and has no trailing literal sequence.
   */
  private static byte[] createBlockWithoutTrailingLiterals() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // Sequence: 20 literal bytes + match(offset=1, length=9005), no trailing literals.
    out.write((TOKEN_MAX_NIBBLE << TOKEN_LITERAL_SHIFT) | TOKEN_MAX_NIBBLE);
    writeLengthExtension(out, 20 - TOKEN_MAX_NIBBLE);
    for (int i = 0; i < 20; i++) {
      out.write('A');
    }

    // Match offset 1 (little-endian): copies the last decoded byte
    out.write(0x01);
    out.write(0x00);
    // Match length: nibble(15) + extension -> readLength returns 9001, plus MIN_MATCH_LENGTH = 9005
    writeLengthExtension(out, 9001 - TOKEN_MAX_NIBBLE);

    return out.toByteArray();
  }

  private static byte[] overflowLiteralLengthBlock() {
    int extensionBytes = Integer.MAX_VALUE / 255 + 2;
    ByteArrayOutputStream out = new ByteArrayOutputStream(extensionBytes + 1);
    out.write(TOKEN_MAX_NIBBLE << TOKEN_LITERAL_SHIFT);
    for (int i = 0; i < extensionBytes; i++) {
      out.write(0xFF);
    }
    return out.toByteArray();
  }

  private static void writeLengthExtension(ByteArrayOutputStream out, int extensionLength) {
    int remaining = extensionLength;
    while (remaining >= 255) {
      out.write(0xFF);
      remaining -= 255;
    }
    out.write(remaining);
  }

  private static IOException expectIOException(IoCallable callable) {
    try {
      callable.call();
      Assert.fail("Expected IOException");
      return null;
    } catch (IOException e) {
      return e;
    }
  }

  private interface IoCallable {
    void call() throws IOException;
  }

  private static final class ExposedLz4RawDecompressor extends Lz4RawDecompressor {

    private int exactUncompressedLength(byte[] compressed) throws IOException {
      return maxUncompressedLengthOnDecompressionError(ByteBuffer.wrap(compressed), 0, null);
    }
  }
}
