package com.cloudera.bigdata.analysis.dataload.etl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.transform.Expressions;

public class TestRegexExpression {

  public static void main(String[] args) {

    String rowkeyNormalized = "a,bc";

    String[] rowkeySpecs = StringUtils.splitByWholeSeparatorPreserveAllTokens(
        rowkeyNormalized, Constants.CELL_SPLIT_CHARACTER);

    String a = rowkeySpecs[0];
    String b = rowkeySpecs[1];

    byte[] ab = a.getBytes();
    byte[] bb = b.getBytes();

    if (ab != null && ab.length != 0)
      System.out.println(ab.length);

    if (bb != null && bb.length != 0)
      System.out.println(bb.length);

    System.out.println(StringUtils.deleteWhitespace("aa			  " + "		  bb  cc  "
        + "\n" + "\t" + "\r" + "dachao"));

    // TODO Auto-generated method stub
    String str = "123assume345contribute";
    System.out.println(str.replaceAll("\\d+", ""));
    String str1 = "123assume345contribute \t !@#$% \n sfssfslll";
    System.out.println(str1.replaceAll("\\D+", ""));

    replaceBlank();
    replaceBlank("just do it!,\r ");
    replaceBlank(null);
    mobilePhoneTest("lidach \t dafa \n jjj 135243066911lidad");
    System.out.println(Expressions.convertToMobilePhoneNumber(
        "lidach \t dafa \n jjj 13524306691lidad", ""));
    ChineseNameTest("da���_123 456 \r\nsfsf");
    FixedLinePhoneTest("���04-53173188sfsf ss");
    System.out.println(Expressions.convertToChineseName(
        "da���_123 456 \r\nsfsf", false, "msg"));
  }

  public static void replaceBlank() {

    Pattern p = Pattern.compile("\\s*|\t|\r|\n");
    String str = "I am a, I am Hello ok, \n new line ffdsa! \r new \t tab";
    System.out.println("before:" + str);
    Matcher m = p.matcher(str);
    String after = m.replaceAll("");
    System.out.println("after:" + after);
  }

  public static String replaceBlank(String str) {
    String dest = "";
    // if (str!=null) {
    if (str == null || str.isEmpty())
      str = "";
    Pattern p = Pattern.compile("\\s*|\t|\r|\n");
    Matcher m = p.matcher(str);
    dest = m.replaceAll("");
    // }
    System.out.println(dest);
    return dest;
  }

  public static String mobilePhoneTest(String str) {
    System.out.println("str: " + str);
    String dest = "";
    Pattern p = null, p1 = null;
    Matcher m = null, m1 = null;
    boolean b;
    p1 = Pattern.compile("\\D+");
    m1 = p1.matcher(str);
    dest = m1.replaceAll("");
    System.out.println(dest);
    // p = Pattern.compile("^[1][35]+\\d{9}");
    p = Pattern.compile("^((13[0-9])|(15[^4,\\D])|(18[0-9]))\\d{8}$");
    m = p.matcher(dest);
    b = m.matches();
    System.out.println(dest);
    System.out.println("Phone Number correct:" + b);
    return dest;
  }

  public static String FixedLinePhoneTest(String str) {
    System.out.println("str: " + str);
    String dest = "";
    Pattern p = null, p1 = null;
    Matcher m = null, m1 = null;
    boolean b;
    p1 = Pattern.compile("\\D+");
    m1 = p1.matcher(str);
    dest = m1.replaceAll("");
    p = Pattern.compile("^[0-9]{3,4}[0-9]{7,8}$");
    m = p.matcher(dest);
    b = m.matches();
    System.out.println(dest);
    System.out.println("Phone Number correct:" + b);
    return dest;
  }

  public static String ChineseNameTest(String str) {
    System.out.println("str: " + str);
    String dest = "";
    Pattern p = null, p1 = null, p2 = null;
    Matcher m = null, m1 = null, m2 = null;
    boolean b;
    p1 = Pattern.compile("\\s*|\t|\r|\n");
    m1 = p1.matcher(str);
    dest = m1.replaceAll("");

    p2 = Pattern.compile("\\d+");
    m2 = p2.matcher(dest);
    dest = m2.replaceAll("");
    // p = Pattern.compile("[\u4e00-\u9fa5\\w]+");//���У�\u4e00-\u9fa5
    // �������ģ�\\w����Ӣ�ġ����ֺ͡�_"�������Ŵ������е������ַ������ļӺŴ������ٳ���һ�Ρ�
    p = Pattern.compile("[\u4e00-\u9fa5_a-zA-Z]+");// ���У�\u4e00-\u9fa5
                                                   // �������ģ�\\��

    m = p.matcher(dest);
    b = m.matches();
    System.out.println(dest);
    System.out.println("Chinese Name correct:" + b);
    return dest;
  }

}
