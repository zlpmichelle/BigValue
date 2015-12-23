package com.cloudera.bigdata.analysis.dataload.util;

public class ByteArray {
  private static final int BUFFER_SIZE = 1024;

  private byte[] buffer;
  private int size;

  public ByteArray() {
    buffer = new byte[BUFFER_SIZE];
    size = 0;
  }

  public void add(byte item) {
    if (size >= buffer.length) {
      byte[] newBuffer = new byte[buffer.length * 2];
      System.arraycopy(buffer, 0, newBuffer, 0, size);
      buffer = newBuffer;
    }

    buffer[size++] = item;
  }

  public void add(byte[] items) {
    if (size + items.length > buffer.length) {
      int desiredLength = buffer.length;
      while (size + items.length > desiredLength) {
        desiredLength <<= 2;
      }
      byte[] newBuffer = new byte[desiredLength];
      System.arraycopy(buffer, 0, newBuffer, 0, size);
      buffer = newBuffer;
    }

    System.arraycopy(items, 0, buffer, size, items.length);
    size += items.length;
  }

  public byte[] toBytes() {
    byte[] res = new byte[size];
    System.arraycopy(buffer, 0, res, 0, size);
    return res;
  }
}
