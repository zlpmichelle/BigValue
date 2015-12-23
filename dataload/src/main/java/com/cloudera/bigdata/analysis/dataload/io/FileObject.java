package com.cloudera.bigdata.analysis.dataload.io;

import org.apache.hadoop.io.WritableComparable;

/**
 * FileObject stands for the description of a single file entity. User can extract name,
 * length and location from this object.
 * 
 * For each different DataSource, usually it will have different FileObject implementation
 */
public interface FileObject extends WritableComparable<FileObject> {
  /** Return the name of the file */
  public String getName();

  /** Return the file's size. It may be used for map/reduce split */
  public long getSize();

  /** Return the canonical form of this pathname */
  public String getCanonicalPath();

  /**
   * Return the preferred hostname. Used to set co-location for map/reduce split
   */
  public String getHostname();

  public String getPath();
}
