package com.cloudera.bigdata.analysis.datagen;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Util {
  private static final ThreadLocal<MessageDigest> tls = 
      new ThreadLocal<MessageDigest>();
      
  public static String getMD5Str(String str) {
    MessageDigest messageDigest = null;
    try {
      if ((messageDigest=tls.get())== null) {
        messageDigest = MessageDigest.getInstance("MD5");
        tls.set(messageDigest);
      }
      
      messageDigest.reset();
      messageDigest.update(str.getBytes("UTF-8"));
    } catch (NoSuchAlgorithmException e) {
      System.out.println("NoSuchAlgorithmException caught!");
      System.exit(-1);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    byte[] byteArray = messageDigest.digest();
    StringBuffer md5StrBuff = new StringBuffer();
    for (int i = 0; i < byteArray.length; i++) {
      String hexString = Integer.toHexString(0xFF & byteArray[i]);
      if (hexString.length() == 1)
        md5StrBuff.append("0").append(hexString);
      else
        md5StrBuff.append(hexString);
    }
    return md5StrBuff.toString().toUpperCase();
  }

}
