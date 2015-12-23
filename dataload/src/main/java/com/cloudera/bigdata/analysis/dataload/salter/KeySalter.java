package com.cloudera.bigdata.analysis.dataload.salter;

/**
 * A slater will prepend some hashed prefix before the row, So that the row key
 * distribution is more even to avoid hotspoting.
 *
 */
public interface KeySalter {

  /**
   * Get salt length of this salter
   * 
   * @return
   */
  public int getSaltLength();

  /**
   * get all possible salts prefix The returned array should be sorted.
   * 
   * @return
   */
  public byte[][] getAllSalts();

  /**
   * Salt a rowkey
   * 
   * @param rowKey
   * @return
   */
  byte[] salt(byte[] rowKey);

  /**
   * revert the salted row key to original row key
   * 
   * @param row
   * @return
   */
  public byte[] unSalt(byte[] row);
}
