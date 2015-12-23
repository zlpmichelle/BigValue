package com.cloudera.bigdata.analysis.dataload.util;

import java.util.Iterator;

public abstract class ByteIterator implements Iterator<Byte> {

  @Override
  public abstract boolean hasNext();

  @Override
  public abstract Byte next();

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public abstract int bytesLeft();

  /**
   * Fill external buffer with bytes in this iterator
   * 
   * @return number filled
   */
  public int nextBuf(byte[] buffer, int bufferOffset) {
    int index = bufferOffset;
    while (index < buffer.length && hasNext()) {
      buffer[index++] = next().byteValue();
    }

    return index - bufferOffset;
  }

  /**
   * Consumes the remaining string of the iterator
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    while (hasNext()) {
      builder.append((char) (next().byteValue()));
    }

    return builder.toString();
  }

  /**
   * Consumers the remaining bytes of the iterator
   */
  public byte[] toArray() {
    int left = bytesLeft();
    byte[] ret = new byte[left];
    int index = 0;
    while (index < ret.length) {
      index += nextBuf(ret, index);
    }

    return ret;
  }

}
