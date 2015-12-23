package com.cloudera.bigdata.analysis.dataload.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class CatCommandTest {
  private static Logger logger = Logger.getLogger(CatCommandTest.class);
  private CatCommand executeCommand;

  public static void main(String[] args) throws IOException {
    CatCommandTest test = new CatCommandTest();
    test.populateCatCommand();
    test.executeCatCommand();
  }

  public void executeCatCommand() throws IOException {

    System.out.println("Executing Cat Script:\n" + executeCommand.toString());
    Runtime rt = Runtime.getRuntime();
    System.out.println(rt.availableProcessors());
    Process process = rt.exec(new String[] { executeCommand.getCatExecutable(),
        executeCommand.getSourceFile1(), executeCommand.getSourceFile2(),
        executeCommand.getCatTarget() });

    String line;
    InputStream es = process.getErrorStream();
    BufferedReader br = new BufferedReader(new InputStreamReader(es));
    while ((line = br.readLine()) != null) {
      logger.info(line);
    }
    br.close();
  }

  private void populateCatCommand() {
    executeCommand = new CatCommand("/root/MRGeneralHBaseETLTool/cat.sh",
        "/root/MRGeneralHBaseETLTool/a.txt",
        "/root/MRGeneralHBaseETLTool/b.txt",
        "/root/MRGeneralHBaseETLTool/c.txt");
  }
}
