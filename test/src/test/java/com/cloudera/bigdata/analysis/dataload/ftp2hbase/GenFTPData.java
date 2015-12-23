package com.cloudera.bigdata.analysis.dataload.ftp2hbase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

public class GenFTPData {

  static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
  final static Calendar calendar = Calendar.getInstance();;

  public static void main(String[] args) throws ParseException, IOException {

    Date date = dateFormat.parse(args[0]);
    int numPerSecond = Integer.parseInt(args[1]);
    double numRecordsInFile = Double.parseDouble(args[2]);
    double totalrecords = Double.parseDouble(args[3]);
    calendar.setTime(date);

    final ThreadGroup root = Thread.currentThread().getThreadGroup();
    Thread[] threads = new Thread[30];
    double i = 0, n = 0;
    for (i = 0; i <= totalrecords / numRecordsInFile; i++) {

      calendar.add(Calendar.SECOND, (int) numRecordsInFile / numPerSecond);

      Thread thr1 = new Thread(new FTPThread(i, calendar.getTime(),
          numPerSecond, numRecordsInFile, totalrecords));
      thr1.start();
      n = root.enumerate(threads, true);
      while (n > 25) {
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          System.out.println("Exception " + e);
          e.printStackTrace();

        }

        n = root.enumerate(threads, true);
      }
    }
  }
}

class FTPThread implements Runnable {
  private double ctr;
  private Date date;
  private int numPerSecond;
  private double numRecordsInFile;
  private double totalrecords;

  public FTPThread(double parameter, Date date, int numPerSecond,
      double numRecordsInFile, double totalrecords) {
    this.ctr = parameter;
    this.date = date;
    this.numPerSecond = numPerSecond;
    this.numRecordsInFile = numRecordsInFile;
    this.totalrecords = totalrecords;
  }

  static HashMap<String, Integer> DataLengthMap = new HashMap<String, Integer>();
  static HashMap<String, Integer> DataStartMap = new HashMap<String, Integer>();

  Random random = new Random();
  static char[] phone = new char[] { '1', '3', '8', '0', '0', '0', '0', '0',
      '0', '0', '0' };
  static char[] phone_2 = new char[] { '3', '5', '8', '3' };
  static char[] DIGIT = new char[] { '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9' };

  char[] DEMO = "1212345678901212345678901234123456781234567810000067812345678901112345123456789012112345678901234123456781234512345678"
      .toCharArray();

  char[] genPhone() {
    String localphone = "00" + String.format("%03d", random.nextInt(170) + 630)
        + "6" + String.format("%06d", random.nextInt(1000000));
    return localphone.toCharArray();
  }

  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

  final Calendar calendar = Calendar.getInstance();;

  char[] genDateTime() {
    calendar.add(Calendar.SECOND, 1);
    String str = dateFormat.format(calendar.getTime());
    return str.toCharArray();
  }

  public void run() {

    try {
      DataStartMap.put("EventType", 0);
      DataLengthMap.put("EventType", 2);
      DataStartMap.put("Phone", 2);
      DataLengthMap.put("Phone", 12);
      DataStartMap.put("Start_Time", 14);
      DataLengthMap.put("Start_Time", 14);
      DataStartMap.put("Period", 28);
      DataLengthMap.put("Period", 8);
      DataStartMap.put("Tower", 36);
      DataLengthMap.put("Tower", 8);
      DataStartMap.put("DropMarker", 44);
      DataLengthMap.put("DropMarker", 1);
      DataStartMap.put("IMEI", 45);
      DataLengthMap.put("IMEI", 8);
      DataStartMap.put("PlanLimits", 54);
      DataLengthMap.put("PlanLimits", 11);
      DataStartMap.put("Roaming", 64);
      DataLengthMap.put("Roaming", 1);
      DataStartMap.put("WAPfee", 65);
      DataLengthMap.put("WAPfee", 5);
      DataStartMap.put("TargetPhone", 70);
      DataLengthMap.put("TargetPhone", 12);
      DataStartMap.put("IntExtCall", 82);
      DataLengthMap.put("IntExtCall", 1);
      DataStartMap.put("PlanEnd", 83);
      DataLengthMap.put("PlanEnd", 14);
      DataStartMap.put("BytesTrans", 97);
      DataLengthMap.put("BytesTrans", 8);
      DataStartMap.put("TargetCarrier", 105);
      DataLengthMap.put("TargetCarrier", 5);
      DataStartMap.put("Cost", 110);
      DataLengthMap.put("Cost", 8);

      /*
       * start date, number of record per second, number records per file, total
       * records
       */

      System.out.println(date);
      calendar.setTime(date);

      SimpleDateFormat df = new SimpleDateFormat("yyyyMM");
      String month = df.format(calendar.getTime());

      char[] phone = String.format("%012d", 0).toCharArray();
      Random random = new Random();
      Integer randtime1;
      String file = "demo." + ctr + ".0.1." + month + ".GD.XD";

      /* Voice is first 5, data is next 6 */
      String Planlimits = String.format("%05d", random.nextInt(50000) + 10000)
          + String.format("%06d", random.nextInt(1000000));
      SetFTPValue("PlanLimits", Planlimits.toCharArray());
      SetFTPValue("WAPfee", String.format("%05d", 0).toCharArray());
      randtime1 = random.nextInt(100000000) + 2000000;
      calendar.add(Calendar.SECOND, randtime1);
      SetFTPValue("PlanEnd", dateFormat.format(calendar.getTime())
          .toCharArray());
      calendar.add(Calendar.SECOND, -1 * randtime1);

      BufferedWriter bw = new BufferedWriter(new FileWriter(file));
      bw.write("==================================================\r\n");
      char[] dateTime = genDateTime();
      for (int j = 0; j < numRecordsInFile
          && ctr * numRecordsInFile + j < totalrecords; j++) {
        // double k = ctr*numRecordsInFile+j;

        if (j % numPerSecond == 0) {
          dateTime = genDateTime();
        }

        phone = genPhone();
        SetFTPValue("Start_Time", dateTime);

        /* use phone number as a indicator for phone type */
        System.arraycopy(phone, 3, DEMO, GetFTPStart("IMEI") + 4, 4);
        /* we only have 8k phone types, so if over 8k set to 5k range */
        if (phone[3] == '9' || phone[3] == '8') {
          DEMO[GetFTPStart("IMEI") + 4] = '3';
        }

        if (random.nextInt(500000) == 1) {
          SetFTPValue("IMEI", String.format("%08d", random.nextInt(8000))
              .toCharArray());
        }

        /* write a singlerecord for each phone number */

        /* ET of 01 is voice, 03 is data */
        if (random.nextInt(10) != 1) {
          SetFTPValue("EventType", String.format("%02d", 1).toCharArray());
          SetFTPValue("BytesTrans", String.format("%08d", 0).toCharArray());
          SetFTPValue("Period", String.format("%08d", random.nextInt(1200))
              .toCharArray());
          SetFTPValue("Cost", String.format("%08d", random.nextInt(100))
              .toCharArray());
        } else if (phone[11] == '2') {
          SetFTPValue("EventType", String.format("%02d", 1).toCharArray());
          SetFTPValue("BytesTrans", String.format("%08d", 0).toCharArray());
          SetFTPValue("Period", String.format("%08d", random.nextInt(2400))
              .toCharArray());
          SetFTPValue("Cost", String.format("%08d", random.nextInt(1000))
              .toCharArray());
        } else {
          SetFTPValue("EventType", String.format("%02d", 3).toCharArray());
          SetFTPValue("BytesTrans",
              String.format("%08d", random.nextInt(100000000)).toCharArray());
          SetFTPValue("Period", String.format("%08d", 0).toCharArray());
          SetFTPValue("Cost", String.format("%08d", random.nextInt(1000))
              .toCharArray());
        }

        if (phone[11] == '2' && random.nextInt(10000) == 1) {
          SetFTPValue("WAPfee", String.format("%05d", random.nextInt(10001))
              .toCharArray());
        } else {
          SetFTPValue("WAPfee", String.format("%05d", 0).toCharArray());
        }
        SetFTPValue("Phone", phone);
        SetFTPValue("Tower", String.format("%08d", random.nextInt(10000))
            .toCharArray());
        if (phone[10] == '7' && phone[9] == '4'
            && DEMO[GetFTPStart("Tower") + 6] == '3'
            && random.nextInt(10000) == 1) {
          SetFTPValue("DropMarker", String.format("%01d", random.nextInt(2))
              .toCharArray());
          if (random.nextInt(100) == 1) {
            SetFTPValue("DropMarker", String.format("%01d", 2).toCharArray());
            if (random.nextInt(100) == 1) {
              SetFTPValue("DropMarker", String.format("%01d", 3).toCharArray());
            }
          }
        } else {
          SetFTPValue("DropMarker", String.format("%01d", 0).toCharArray());
        }

        if (phone[10] == '9') {
          if (phone[9] == '2' && random.nextInt(10) > 1) {
            SetFTPValue("Roaming", String.format("%01d", 1).toCharArray());
          } else {
            SetFTPValue("Roaming", String.format("%01d", random.nextInt(2))
                .toCharArray());
          }
        } else {
          SetFTPValue("Roaming", String.format("%01d", 0).toCharArray());
        }

        String targetphone = String.format("%05d", random.nextInt(2000))
            + String.format("%07d", random.nextInt(10000000));
        SetFTPValue("TargetPhone", targetphone.toCharArray());
        if (phone[5] == '3' && phone[9] == '6') {
          /* 90% of calls are external at min for these numbers */
          if (random.nextInt(10) == 1) {
            SetFTPValue("IntExtCall", String.format("%01d", 0).toCharArray());
          }
          {
            SetFTPValue("IntExtCall", String.format("%01d", 1).toCharArray());
          }
        } else {
          if (random.nextInt(10) == 1) {
            SetFTPValue("IntExtCall", String.format("%01d", random.nextInt(2))
                .toCharArray());
          } else {
            SetFTPValue("IntExtCall", String.format("%01d", 0).toCharArray());
          }

        }
        SetFTPValue("TargetCarrier",
            String.format("%05d", random.nextInt(10000)).toCharArray());

        bw.write(DEMO);
        bw.write("\r\n");

      }

      bw.close();
    } catch (Exception e) {
      System.out.println("Exception " + e);
      e.printStackTrace();

    }
  }

  public void SetFTPValue(String key, char[] value) {
    Integer sp = GetFTPStart(key);
    Integer sl = GetFTPLength(key);
    System.arraycopy(value, 0, DEMO, sp, sl);
    /*
     * for(int index = 0;index<sl;index++) { DEMO[sp+index] = value[index]; }
     */
  }

  public static Integer GetFTPLength(String key) {
    /*
     * System.out.println("length key " + key + " is "+DataStartMap.get(key));
     */
    return (Integer) DataLengthMap.get(key);
  }

  public static Integer GetFTPStart(String key) {
    /*
     * System.out.println("start  key " + key + " is "+DataLengthMap.get(key));
     */
    return (Integer) DataStartMap.get(key);
  }
};
