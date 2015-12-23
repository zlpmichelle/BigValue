package com.cloudera.bigdata.analysis.dataload.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;

public class TestCommonUtils {

  @Test
  public void testGetFirstParenthesisClosingPosition() {
    try {
      assertTrue(CommonUtils
          .getTopParenthesisClosingPositionOfFirstExpression("firstName,secondName") == -1);
      assertTrue(CommonUtils
          .getTopParenthesisClosingPositionOfFirstExpression("concat(firstName,secondName)") == 27);
      assertTrue(CommonUtils
          .getTopParenthesisClosingPositionOfFirstExpression("concat(substr(firstName,1,3),substr(secondName),age),age") == 51);
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }

  @Test
  public void testGetExpressions() {
    try {
      String[] expressions = CommonUtils.getExpressions("firstName,secondName");
      assertEquals(expressions[0], "firstName");
      assertEquals(expressions[1], "secondName");
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }

  @Test
  public void testGetExpressionsWithParenthesis() {

    try {
      String[] expressions = CommonUtils
          .getExpressions("concat(firstName,secondName)");
      assertEquals(expressions[0], "concat(firstName,secondName)");
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }

  @Test
  public void testGetExpressionsWithNestedParenthesis() {

    try {
      String[] expressions = CommonUtils
          .getExpressions("concat(substr(firstName,1,3),substr(secondName),age),age");
      assertEquals(expressions[0],
          "concat(substr(firstName,1,3),substr(secondName),age)");
      assertEquals(expressions[1], "age");
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }

  @Test
  public void testGetExpressionsWithNestedParenthesis2() {

    try {
      String[] expressions = CommonUtils
          .getExpressions("age,concat(substr(firstName,1,3),substr(secondName),age)");
      assertEquals(expressions[0], "age");
      assertEquals(expressions[1],
          "concat(substr(firstName,1,3),substr(secondName),age)");
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }

  @Test
  public void testGetEscapedDelimiter() {

    // try{
    // String escaped = CommonUtils.getEscapedDelimiter("|!");
    // assertEquals(escaped,"\\|!");
    // }catch(Exception ex){
    // fail(ex.toString());
    // }

    try {
      String textLimiter = "\\001";
      String escaped = CommonUtils.getEscapedDelimiter(textLimiter);
      assertEquals(escaped, "\001");
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }

  @Test
  public void testApshRowKey() {

    try {
      String[] expressions = CommonUtils
          .getExpressions("concat(trim(APSHPROCOD),trim(APSHACTNO),trim(APSHPRDNO),trim(APSHTRDAT),trim(APSHJRNNO),trim(APSHSEQNO))");
      assertEquals(
          expressions[0],
          "concat(trim(APSHPROCOD),trim(APSHACTNO),trim(APSHPRDNO),trim(APSHTRDAT),trim(APSHJRNNO),trim(APSHSEQNO))");
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }

  @Test
  public void testGmccRowKey() {

    try {
      String[] expressions = CommonUtils
          .getExpressions("concat(reverse(trim(f6)),trim(f24),trim(f0))");
      assertEquals(expressions[0],
          "concat(reverse(trim(f6)),trim(f24),trim(f0))");
    } catch (Exception ex) {
      fail(ex.toString());
    }
  }
}
