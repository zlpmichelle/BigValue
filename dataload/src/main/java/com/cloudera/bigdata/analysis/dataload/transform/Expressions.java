package com.cloudera.bigdata.analysis.dataload.transform;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.CharSetUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;

import com.cloudera.bigdata.analysis.dataload.exception.ETLException;

public class Expressions {

  public static enum ConvertType {
    MOBILEPHONE, CHINESENAME, IDCARD
  }

  public static int indexOf(String str, String searchStr) {
    return StringUtils.indexOf(str, searchStr);
  }

  public static int lastIndexOf(String str, String searchStr) {
    return StringUtils.lastIndexOf(str, searchStr);
  }

  public static String trim(String str) {
    return str.trim();
  }

  public static String reverse(String str) {
    return StringUtils.reverse(str);
  }

  public static String rightPad(String str, int size, String padChar) {
    return StringUtils.rightPad(str, size, padChar);
  }

  public static String leftPad(String str, int size, String padChar) {
    return StringUtils.leftPad(str, size, padChar);
  }

  public static String substring(String str, int begin, int end) {
    return str.substring(begin, end);
  }

  public static String concat(String... catStrings) {
    StringBuilder sb = new StringBuilder();
    for (String catString : catStrings) {
      sb.append(catString);
    }
    return sb.toString();
  }

  public static String randomAlphanumeric(int count) {
    return RandomStringUtils.randomAlphanumeric(count);
  }

  public static boolean isBlank(String str) {
    return StringUtils.isBlank(str);
  }

  public static boolean isEmpty(String str) {
    return StringUtils.isEmpty(str);
  }

  public static boolean isNotBlank(String str) {
    return StringUtils.isNotBlank(str);
  }

  public static boolean isNotEmpty(String str) {
    return StringUtils.isNotEmpty(str);
  }

  public static String delete(String str, String deleteChar) {
    return CharSetUtils.delete(str, deleteChar);
  }

  public static String keep(String str, String keepChar) {
    return CharSetUtils.keep(str, keepChar);
  }

  public static String squeeze(String str, String squeezeChar) {
    return CharSetUtils.squeeze(str, squeezeChar);
  }

  public static String repeat(String str, int repeatCount) {
    return StringUtils.repeat(str, repeatCount);
  }

  public static String center(String str, int total, String appendChar) {
    return StringUtils.center(str, total, appendChar);
  }

  public static String abbreviate(String str, int total) {
    return StringUtils.abbreviate(str, total);
  }

  public static String strip(String str, String stripChar) {
    return StringUtils.strip(str, stripChar);
  }

  public static String stripStart(String str, String stripStartChar) {
    return StringUtils.stripStart(str, stripStartChar);
  }

  public static String stripEnd(String str, String stripEndChar) {
    return StringUtils.stripEnd(str, stripEndChar);
  }

  public static String replace(String str, String searchChars,
      String replacement) {
    return StringUtils.replace(str, searchChars, replacement);
  }

  public static String replaceChars(String str, char searchChar,
      char replaceChar) {
    return StringUtils.replaceChars(str, searchChar, replaceChar);
  }

  public static String deleteWhitespace(String str) {
    return StringUtils.deleteWhitespace(str);
  }

  public static String removeBlank(String str) {
    if (str == null || str.isEmpty())
      str = "";
    Pattern p = Pattern.compile("\\s*|\t|\r|\n");
    Matcher m = p.matcher(str);
    return m.replaceAll("");
  }

  public static String removeDigits(String str) {
    return str.replaceAll("\\d+", "");
  }

  public static String keepDigits(String str) {
    return str.replaceAll("\\D+", "");
  }

  public static String convertTo(String str, String typeStr, String defaultStr)
      throws ETLException {
    ConvertType type = ConvertType.valueOf(ConvertType.class,
        typeStr.toUpperCase());
    switch (type) {
    case MOBILEPHONE:
      return convertToMobilePhoneNumber(str, defaultStr);
    case CHINESENAME:
      return convertToChineseName(str, false, defaultStr);
    default:
      throw new ETLException("Upsupported ConvertType!");
    }
  }

  public static String convertToMobilePhoneNumber(String str, String msg) {
    String dest = keepDigits(str);
    Pattern p = Pattern.compile("^((13[0-9])|(15[^4,\\D])|(18[0-9]))\\d{8}$");
    p.matcher(dest);
    Matcher m = p.matcher(dest);
    if (m.matches())
      return dest;
    else
      return msg;
  }

  public static String convertToChineseName(String str,
      boolean chineseCharactersOnly, String msg) {
    String dest = deleteWhitespace(str);
    dest = removeDigits(dest);
    Pattern p = Pattern.compile("[\u4e00-\u9fa5\\w]+");
    if (chineseCharactersOnly)
      p = Pattern.compile("[\u4e00-\u9fa5]+");
    Matcher m = p.matcher(dest);
    if (m.matches())
      return dest;
    else
      return msg;
  }
}
