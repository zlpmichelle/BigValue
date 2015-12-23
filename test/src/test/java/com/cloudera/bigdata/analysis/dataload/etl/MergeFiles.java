package com.cloudera.bigdata.analysis.dataload.etl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

public class MergeFiles {
  public static void main(String[] args) throws Exception {
    IOCopier.joinFiles(new File("/root/MRGeneralHBaseETLTool/c.txt"),
        new File[] { new File("/root/MRGeneralHBaseETLTool/a.txt"),
            new File("/root/MRGeneralHBaseETLTool/b.txt") });
  }
}

class IOCopier {
  public static void joinFiles(File destination, File[] sources)
      throws IOException {
    OutputStream output = null;
    try {
      output = createAppendableStream(destination);
      for (File source : sources) {
        appendFile(output, source);
      }
    } finally {
      IOUtils.closeQuietly(output);
    }
  }

  private static BufferedOutputStream createAppendableStream(File destination)
      throws FileNotFoundException {
    return new BufferedOutputStream(new FileOutputStream(destination, true));
  }

  private static void appendFile(OutputStream output, File source)
      throws IOException {
    InputStream input = null;
    try {
      input = new BufferedInputStream(new FileInputStream(source));
      IOUtils.copy(input, output);
    } finally {
      IOUtils.closeQuietly(input);
    }
  }
}