package com.cloudera.bigdata.analysis.dataload.salter;

/**
 * This will prepend N byte before the rowkey
 *
 */
public abstract class NBytePrefixKeySalter implements KeySalter {
  protected int prefixLength;

  /**
   * 
   * @param limit
   *          , how many salts is allowed.
   */
  public NBytePrefixKeySalter(int prefixLength) {
    this.prefixLength = prefixLength;
  }

  @Override
  public int getSaltLength() {
    return prefixLength;
  }

  @Override
  public byte[] unSalt(byte[] row) {
    byte[] newRow = new byte[row.length - prefixLength];
    System.arraycopy(row, prefixLength, newRow, 0, newRow.length);
    return newRow;
  }

  @Override
  public byte[] salt(byte[] key) {
    return concat(hash(key), key);
  }

  protected abstract byte[] hash(byte[] key);

  private byte[] concat(byte[] prefix, byte[] row) {
    if (null == prefix || prefix.length == 0) {
      return row;
    }
    if (null == row || row.length == 0) {
      return prefix;
    }
    byte[] newRow = new byte[row.length + prefix.length];
    if (row.length != 0) {
      System.arraycopy(row, 0, newRow, prefix.length, row.length);
    }
    if (prefix.length != 0) {
      System.arraycopy(prefix, 0, newRow, 0, prefix.length);
    }
    return newRow;
  }
}
