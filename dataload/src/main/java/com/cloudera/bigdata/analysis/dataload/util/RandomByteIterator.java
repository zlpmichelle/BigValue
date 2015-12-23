package com.cloudera.bigdata.analysis.dataload.util;

import com.cloudera.bigdata.analysis.generated.FieldPattern;

/**
 * A RandomByteIterator generates a sequence of random bytes.
 */
public class RandomByteIterator extends ByteIterator {

  private static final int BUFFER_SIZE = 6;
  private static final byte ZERO = (byte) '0';
  private static final byte LOWER_A = (byte) 'a';
  private static final byte UPPER_A = (byte) 'A';

  // Required length of this generation
  private int length;

  // Start offset of the buffer
  private int off;

  // Relative offset in the buffer
  private int bufOff;

  // Buffer to hold the byte sequence
  private byte[] buf;

  private FieldPattern pattern;

  public RandomByteIterator(int length, FieldPattern pattern) {
    this.length = length;
    this.off = 0;
    this.buf = new byte[BUFFER_SIZE];
    this.pattern = pattern;
    fillBytesImpl(buf, 0);
  }

  private void fillBytes() {
    if (bufOff == buf.length) {
      fillBytesImpl(buf, 0);
      off += bufOff;
      bufOff = 0;
    }
  }

  /**
   * Fills six bytes once
   */
  private void fillBytesImpl(byte[] buf, int baseOff) {
    int toBeFilled = 0;
    if (baseOff < buf.length) {
      int bytes = Util.getRandom().nextInt();
      if (buf.length - baseOff >= BUFFER_SIZE) {
        toBeFilled = BUFFER_SIZE;
      } else {
        toBeFilled = buf.length - baseOff;
      }
      for (int i = 0; i < toBeFilled; i++) {
        buf[baseOff + i] = (byte) determineByte(bytes >> (i * 5));
      }
    }
  }

  @Override
  public boolean hasNext() {
    return off + bufOff < length;
  }

  @Override
  public Byte next() {
    fillBytes();

    return buf[bufOff++];
  }

  @Override
  public int bytesLeft() {
    return length - off - bufOff;
  }

  @Override
  public int nextBuf(byte[] buffer, int bufferOffset) {
    // calculate the number of bytes to fill
    int ret = 0;
    if (bytesLeft() < buffer.length - bufferOffset) {
      ret = bytesLeft();
    } else {
      ret = buffer.length - bufferOffset;
    }

    int i = 0;
    for (i = 0; i < ret; i += 6) {
      fillBytesImpl(buffer, i + bufferOffset);
    }
    off += ret;
    return ret;
  }

  private int determineByte(int bytes) {
    switch (pattern) {
    case DIGIT:
      return determineDigit(bytes);
    case ALPHABET:
      return determineAlphabet(bytes);
    default:
      return 0;
    }
  }

  private int determineDigit(int bytes) {
    return (bytes & 7) + ((bytes & 8) == 8 ? 1 : 0)
        + ((bytes & 16) == 16 ? 1 : 0) + ZERO;
  }

  private int determineAlphabet(int bytes) {
    return (bytes & 15) + ((bytes & 16) == 16 ? 9 : 0)
        + ((bytes & 16) == 16 ? LOWER_A : UPPER_A);
  }

  public static void main(String args[]) {
    // all digits
    ByteIterator digitIter = new RandomByteIterator(100, FieldPattern.DIGIT);
    System.out.println(new String(digitIter.toString()));

    // all alphabet
    ByteIterator alphabetIter = new RandomByteIterator(32,
        FieldPattern.ALPHABET);
    System.out.println(new String(alphabetIter.toArray()));
  }

}
