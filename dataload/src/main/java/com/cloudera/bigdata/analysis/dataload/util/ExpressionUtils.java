package com.cloudera.bigdata.analysis.dataload.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.exception.FormatException;
import com.cloudera.bigdata.analysis.dataload.transform.Expressions;

public class ExpressionUtils {

  public static enum Expression {
    TRIM, RPAD, LPAD, SUBSTR, CONCAT, REVERSE, RAND, DEFAULTIFBLANK, DEFAULTIFEMPTY, DEFAULTIFNOTBLANK, DEFAULTIFNOTEMPTY, DELETE, KEEP, SQUEEZE, REPEAT, CENTER, ABBREVIATE, STRIP, STRIPSTART, STRIPEND, REPLACE, REPLACECHARS, DELETEWHITESPACE, REMOVEBLANK, REMOVEDIGITS, KEEPDIGITS, CONVERTTO
  }

  /**
   * calculate real value of columnSpec
   * 
   * @param columnSpec
   *          Right Value of "=" in table specification
   * @param fieldMap
   *          The Parsed source data Line converted into Map
   * @return the real value
   * @throws ETLException
   */
  public static String calculate(String columnSpec, Map<String, String> fieldMap)
      throws ETLException {
    StringBuilder sb = new StringBuilder();
    String fieldName = null, fieldValue = null;

    if (CommonUtils.isAtomicExpression(columnSpec)) {// just a plain field
      fieldName = columnSpec;
      fieldValue = fieldMap.get(fieldName);
      sb.append(fieldValue);
      return sb.toString();
    }

    int start = Expressions.indexOf(columnSpec, Constants.OPEN_PARENTHESIS);
    int stop = Expressions.lastIndexOf(columnSpec, Constants.CLOSE_PARENTHESIS);

    if (start > 0 && stop > 0 && start < stop) {// an expression starting with a
                                                // function
      Expression expression = Expression.valueOf(Expression.class, columnSpec
          .substring(0, start).toUpperCase());
      String innerSpec = columnSpec.substring(start + 1, stop);

      String[] ss = CommonUtils.getExpressions(innerSpec);
      switch (expression) {
      case TRIM:// trim
        if (CommonUtils.isAtomicExpression(ss[0])) {// trim on a field
          fieldName = innerSpec;
          fieldValue = fieldMap.get(fieldName);
          if (fieldMap.containsKey(fieldName)) {
            fieldValue = fieldMap.get(fieldName);
          } else {
            throw new ETLException("fieldMap DO NOT contains " + fieldName
                + "!");
          }
          sb.append(Expressions.trim(fieldValue));
          return sb.toString();
        }
        // trim on an expression: recursion call
        return Expressions.trim(calculate(ss[0], fieldMap));

      case REVERSE:
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.reverse(fieldValue));
          return sb.toString();
        }
        return Expressions.reverse(calculate(ss[0], fieldMap));

      case RPAD:
        int size = Integer.parseInt(ss[1]);
        String padChar = ss[2];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.rightPad(fieldValue, size, padChar));
          return sb.toString();
        }
        return Expressions.rightPad(calculate(ss[0], fieldMap), size, padChar);

      case LPAD:
        size = Integer.parseInt(ss[1]);
        padChar = ss[2];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.leftPad(fieldValue, size, padChar));
          return sb.toString();
        }
        return Expressions.leftPad(calculate(ss[0], fieldMap), size, padChar);

      case SUBSTR:
        int begin = Integer.parseInt(ss[1]);
        int end = Integer.parseInt(ss[2]);
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.substring(fieldValue, begin, end));
          return sb.toString();
        }
        return Expressions.substring(calculate(ss[0], fieldMap), begin, end);

      case CONCAT:
        for (int i = 0; i <= ss.length - 2; i++) {
          if (CommonUtils.isAtomicExpression(ss[i])) {
            if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[i])) {
              sb.append(Expressions.concat(CommonUtils
                  .getSingleQuotationMarksWrapedExpressionValue(ss[i]),
                  Constants.COLUMN_ENTRY_SEPARATOR));
            } else {
              fieldName = ss[i];
              fieldValue = fieldMap.get(fieldName);
              sb.append(Expressions.concat(fieldValue,
                  Constants.COLUMN_ENTRY_SEPARATOR));
            }
          } else {
            sb.append(Expressions.concat(calculate(ss[i], fieldMap),
                Constants.COLUMN_ENTRY_SEPARATOR));
          }
        }

        if (CommonUtils.isAtomicExpression(ss[ss.length - 1])) {
          if (CommonUtils
              .isSingleQuotationMarksWrapedExpression(ss[ss.length - 1])) {
            sb.append(Expressions.concat(CommonUtils
                .getSingleQuotationMarksWrapedExpressionValue(ss[ss.length - 1])));
          } else {
            fieldName = ss[ss.length - 1];
            fieldValue = fieldMap.get(fieldName);
            sb.append(Expressions.concat(fieldValue));
          }
        } else {
          sb.append(calculate(ss[ss.length - 1], fieldMap));
        }
        return sb.toString();

      case RAND:
        int count = Integer.parseInt(ss[0]);
        sb.append(Expressions.randomAlphanumeric(count));
        return sb.toString();

      case DEFAULTIFBLANK:
        if (Expressions.isBlank(fieldMap.get(ss[0]))) {
          if (CommonUtils.isAtomicExpression(ss[1])) {
            if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[1])) {
              sb.append(CommonUtils
                  .getSingleQuotationMarksWrapedExpressionValue(ss[1]));
            } else {
              fieldName = ss[1];
              fieldValue = fieldMap.get(fieldName);
              sb.append(fieldValue);
            }
          } else {
            sb.append(calculate(ss[1], fieldMap));
          }
        }

        return sb.toString();

      case DEFAULTIFEMPTY:
        if (Expressions.isEmpty(fieldMap.get(ss[0]))) {
          if (CommonUtils.isAtomicExpression(ss[1])) {
            if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[1])) {
              sb.append(CommonUtils
                  .getSingleQuotationMarksWrapedExpressionValue(ss[1]));
            } else {
              fieldName = ss[1];
              fieldValue = fieldMap.get(fieldName);
              sb.append(fieldValue);
            }
          } else {
            sb.append(calculate(ss[1], fieldMap));
          }
        }
        return sb.toString();

      case DEFAULTIFNOTBLANK:
        if (Expressions.isNotBlank(fieldMap.get(ss[0]))) {
          if (CommonUtils.isAtomicExpression(ss[1])) {
            if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[1])) {
              sb.append(CommonUtils
                  .getSingleQuotationMarksWrapedExpressionValue(ss[1]));
            } else {
              fieldName = ss[1];
              fieldValue = fieldMap.get(fieldName);
              sb.append(fieldValue);
            }
          } else {
            sb.append(calculate(ss[1], fieldMap));
          }
        }
        return sb.toString();

      case DEFAULTIFNOTEMPTY:
        if (Expressions.isNotEmpty(fieldMap.get(ss[0]))) {
          if (CommonUtils.isAtomicExpression(ss[1])) {
            if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[1])) {
              sb.append(CommonUtils
                  .getSingleQuotationMarksWrapedExpressionValue(ss[1]));
            } else {
              fieldName = ss[1];
              fieldValue = fieldMap.get(fieldName);
              sb.append(fieldValue);
            }
          } else {
            sb.append(calculate(ss[1], fieldMap));
          }
        }
        return sb.toString();

      case DELETE:
        String deleteChar = ss[1];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.delete(fieldValue, deleteChar));
          return sb.toString();
        }
        return Expressions.delete(calculate(ss[0], fieldMap), deleteChar);

      case KEEP:
        String keepChar = ss[1];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.keep(fieldValue, keepChar));
          return sb.toString();
        }
        return Expressions.keep(calculate(ss[0], fieldMap), keepChar);

      case SQUEEZE:
        String squeezeChar = ss[1];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.squeeze(fieldValue, squeezeChar));
          return sb.toString();
        }
        return Expressions.squeeze(calculate(ss[0], fieldMap), squeezeChar);

      case REPEAT:
        int repeatCount = Integer.parseInt(ss[1]);
        if (CommonUtils.isAtomicExpression(ss[0])) {
          if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[0])) {
            sb.append(Expressions.repeat(
                CommonUtils.getSingleQuotationMarksWrapedExpressionValue(ss[0]),
                repeatCount));
          } else {
            fieldName = ss[0];
            fieldValue = fieldMap.get(fieldName);
            sb.append(Expressions.repeat(fieldValue, repeatCount));
          }
        } else {
          sb.append(Expressions.repeat(calculate(ss[0], fieldMap), repeatCount));
        }
        return sb.toString();

      case CENTER:
        int totalLength = Integer.parseInt(ss[1]);
        String appendChar = ss[2];
        if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[2])) {
          appendChar = CommonUtils
              .getSingleQuotationMarksWrapedExpressionValue(ss[2]);
        }
        if (CommonUtils.isAtomicExpression(ss[0])) {
          if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[0])) {
            sb.append(Expressions.center(
                CommonUtils.getSingleQuotationMarksWrapedExpressionValue(ss[0]),
                totalLength, appendChar));
          } else {
            fieldName = ss[0];
            fieldValue = fieldMap.get(fieldName);
            sb.append(Expressions.center(fieldValue, totalLength, appendChar));
          }
        } else {
          sb.append(Expressions.center(calculate(ss[0], fieldMap), totalLength,
              appendChar));
        }
        return sb.toString();

      case ABBREVIATE:
        int totalAbbLength = Integer.parseInt(ss[1]);
        if (CommonUtils.isAtomicExpression(ss[0])) {
          if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[0])) {
            sb.append(Expressions.abbreviate(
                CommonUtils.getSingleQuotationMarksWrapedExpressionValue(ss[0]),
                totalAbbLength));
          } else {
            fieldName = ss[0];
            fieldValue = fieldMap.get(fieldName);
            sb.append(Expressions.abbreviate(fieldValue, totalAbbLength));
          }
        } else {
          sb.append(Expressions.abbreviate(calculate(ss[0], fieldMap),
              totalAbbLength));
        }
        return sb.toString();

      case STRIP:
        String stripChar = ss[1];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.strip(fieldValue, stripChar));
          return sb.toString();
        }
        return Expressions.strip(calculate(ss[0], fieldMap), stripChar);

      case STRIPSTART:
        String stripStartChar = ss[1];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.stripStart(fieldValue, stripStartChar));
          return sb.toString();
        }
        return Expressions.stripStart(calculate(ss[0], fieldMap),
            stripStartChar);

      case STRIPEND:
        String stripEndChar = ss[1];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.stripEnd(fieldValue, stripEndChar));
          return sb.toString();
        }
        return Expressions.stripEnd(calculate(ss[0], fieldMap), stripEndChar);

      case REPLACE:
        String searchChars = ss[1];
        String replacement = ss[2];
        if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[1])) {
          searchChars = CommonUtils
              .getSingleQuotationMarksWrapedExpressionValue(ss[1]);
        }
        if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[2])) {
          replacement = CommonUtils
              .getSingleQuotationMarksWrapedExpressionValue(ss[2]);
        }
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.replace(fieldValue, searchChars, replacement));
          return sb.toString();
        }
        return Expressions.replace(calculate(ss[0], fieldMap), searchChars,
            replacement);

      case REPLACECHARS:
        if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[1])) {
          ss[1] = CommonUtils
              .getSingleQuotationMarksWrapedExpressionValue(ss[1]);
        }
        if (CommonUtils.isSingleQuotationMarksWrapedExpression(ss[2])) {
          ss[2] = CommonUtils
              .getSingleQuotationMarksWrapedExpressionValue(ss[2]);
        }
        char searchChar = ss[1].toCharArray()[0];// only one char
        char replaceChar = ss[2].toCharArray()[0];
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.replaceChars(fieldValue, searchChar,
              replaceChar));
          return sb.toString();
        }
        return Expressions.replaceChars(calculate(ss[0], fieldMap), searchChar,
            replaceChar);

      case DELETEWHITESPACE:
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.deleteWhitespace(fieldValue));
          return sb.toString();
        }
        return Expressions.deleteWhitespace(calculate(ss[0], fieldMap));

      case REMOVEBLANK:
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.removeBlank(fieldValue));
          return sb.toString();
        }
        return Expressions.removeBlank(calculate(ss[0], fieldMap));

      case REMOVEDIGITS:
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.removeDigits(fieldValue));
          return sb.toString();
        }
        return Expressions.removeDigits(calculate(ss[0], fieldMap));

      case KEEPDIGITS:
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions.keepDigits(fieldValue));
          return sb.toString();
        }
        return Expressions.keepDigits(calculate(ss[0], fieldMap));

      case CONVERTTO:
        String convertType = ss[1].trim();
        String defaultValue = ss[2].trim();
        if (CommonUtils.isAtomicExpression(ss[0])) {
          fieldName = ss[0];
          fieldValue = fieldMap.get(fieldName);
          sb.append(Expressions
              .convertTo(fieldValue, convertType, defaultValue));
          return sb.toString();
        }
        return Expressions.convertTo(calculate(ss[0], fieldMap), convertType,
            defaultValue);

      default:
        throw new ETLException("Upsupported operation!");
      }
    }
    return sb.toString();
  }

  public static boolean isAtomicExpression(String columnSpec) {
    if (StringUtils.indexOf(columnSpec, "(") < 0
        && StringUtils.indexOf(columnSpec, ")") < 0
        && StringUtils.indexOf(columnSpec, ",") < 0) {
      return true;
    }

    return false;
  }

  public static String[] getExpressions(String commaSeperatedExpressions)
      throws FormatException {

    List<String> expressions = new ArrayList<String>();

    spawnExpressions(commaSeperatedExpressions, expressions);

    return expressions.toArray(new String[expressions.size()]);
  }

  // return the postion of the top closing parenthesis in the first expression
  // in a common seperated expressions
  public static int getTopParenthesisClosingPositionOfFirstExpression(
      String commaSeperatedExpressions) throws FormatException {

    int openPostion = StringUtils.indexOf(commaSeperatedExpressions, "(");
    if (openPostion < 0)
      return -1; // no parenthesis

    int commaPostion = StringUtils.indexOf(commaSeperatedExpressions, ",");

    if (commaPostion < openPostion)
      return -1;// no parenthesis in the first expression

    int index = openPostion + 1;
    int netCount = 1;
    while (index < commaSeperatedExpressions.length()) {
      char c = commaSeperatedExpressions.charAt(index);
      if (c == '(') {
        netCount++;
      } else if (c == ')') {
        netCount--;
        if (netCount == 0)
          return index;// arrive at the closing parenthesis of the first open
                       // parenthesis
      }

      index++;
    }

    throw new FormatException("Parenthesis is not closed properly: "
        + commaSeperatedExpressions);
  }

  // return the first comma postion in the top level in a common seperated
  // expressions
  public static int getFirstCommaPostionOnTopLevel(
      String commaSeperatedExpressions) throws FormatException {

    int openPostion = StringUtils.indexOf(commaSeperatedExpressions, "(");
    if (openPostion < 0)
      return -1; // no parenthesis

    int commaPostion = StringUtils.indexOf(commaSeperatedExpressions, ",");

    if (commaPostion < openPostion)
      return -1;// no parenthesis in the first expression

    int index = openPostion + 1;
    int netCount = 1;
    while (index < commaSeperatedExpressions.length()) {
      char c = commaSeperatedExpressions.charAt(index);
      if (c == '(') {
        netCount++;
      } else if (c == ')') {
        netCount--;
        if (netCount == 0)
          return index;// arrive at the closing parenthesis of the first open
                       // parenthesis
      }

      index++;
    }

    throw new FormatException("Parenthesis is not closed properly: "
        + commaSeperatedExpressions);
  }

  private static void spawnExpressions(String commaSeperatedExpressions,
      List<String> expressions) throws FormatException {

    int closingParenthesisPosition = getTopParenthesisClosingPositionOfFirstExpression(commaSeperatedExpressions);

    if (closingParenthesisPosition < 0) {// no parenthesis in first expression

      int firstCommaPostionOnTopLevel = StringUtils.indexOf(
          commaSeperatedExpressions, ",");

      if (firstCommaPostionOnTopLevel < 0) {// only one expression
        String firstExpression = commaSeperatedExpressions;
        expressions.add(firstExpression);
        return;
      }

      String firstExpression = commaSeperatedExpressions.substring(0,
          firstCommaPostionOnTopLevel);
      expressions.add(firstExpression);
      String remaining = commaSeperatedExpressions
          .substring(firstCommaPostionOnTopLevel + 1);// skip the "," after the
                                                      // first expression
      spawnExpressions(remaining, expressions);
      return;
    }

    // first expression with parenthesis

    String firstExpression = commaSeperatedExpressions.substring(0,
        closingParenthesisPosition + 1);
    expressions.add(firstExpression);

    if (firstExpression.length() == commaSeperatedExpressions.length())
      return;// only one expression

    String remaining = commaSeperatedExpressions
        .substring(closingParenthesisPosition + 2);// skip the "," after the
                                                   // first expression
    spawnExpressions(remaining, expressions);
  }

  // if there is a \t in file delimiter, will construct escaped delimiter string
  // which appends with "\\", because split() needs to handle escaped character
  // TODO add more escaped characters
  public static String getEscapedDelimiter(String delimiter) {
    StringBuilder sb = new StringBuilder();

    for (char c : delimiter.toCharArray()) {
      if (c == '|' || c == '$') {
        sb.append("\\").append(c);
      } else {
        sb.append(c);
      }
    }

    return sb.toString();
  }
}
