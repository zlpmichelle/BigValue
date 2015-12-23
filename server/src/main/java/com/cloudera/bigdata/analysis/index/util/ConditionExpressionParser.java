package com.cloudera.bigdata.analysis.index.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.query.AndCondition;
import com.cloudera.bigdata.analysis.query.Condition;
import com.cloudera.bigdata.analysis.query.InvalidQueryException;
import com.cloudera.bigdata.analysis.query.Operator;
import com.cloudera.bigdata.analysis.query.OrCondition;
import com.cloudera.bigdata.analysis.query.SimpleCondition;

/**
 * A util to parse condition expression, and build an
 * {@link com.cloudera.bigdata.analysis.query.Condition} as output. Condition
 * expression looks like following:
 * [(f,locId,EQ,370101){(f,plate,EQ,A00001)(f,plate,EQ,A00002)}] this
 * expression means: (f:locId = 370101 and (f:plate=A00001 or
 * f:plate=A00002))
 */
public class ConditionExpressionParser {
  private Random rand = new Random();
  private String[] randomArgs;
  private int min, max;
  private long minL, maxL;
  private long minDate = 0;
  private long maxDate = 0;
  private SimpleDateFormat df;

  private static final Logger LOG = LoggerFactory
      .getLogger(ConditionExpressionParser.class);

  public Condition parse(String table, String expression, boolean ramdonGenValue) {
    Stack<Object> expStack = new Stack<Object>();
    int i = 0;
    int simpleConditionStartPosition = -1;
    while (i < expression.length()) {
      Character c = expression.charAt(i);
      if (c.equals('{') || c.equals('[')) {
        expStack.push(c);
      } else if (c.equals('(')) {
        simpleConditionStartPosition = i + 1;
      } else if (c.equals(')')) {
        if (simpleConditionStartPosition == -1
            || i <= simpleConditionStartPosition) {
          throw new RuntimeException("Invalid Condition Expression!");
        }
        String[] simpleConditionArgs = expression.substring(
            simpleConditionStartPosition, i).split(",");
        String columnFamily = simpleConditionArgs[0];
        String qualifier = simpleConditionArgs[1];
        Operator operator = Operator.valueOf(simpleConditionArgs[2]);
        ByteArray value = null;

        if (IndexUtil.isIndexConfAvailableForQuery(table)) {
          // randomly generate value
          if (ramdonGenValue) {
            randomArgs = simpleConditionArgs[3].split("_");
            if (randomArgs[0].equals("RandomIntegerGenerator")
                && randomArgs.length == 3) {
              min = Integer.valueOf(randomArgs[1]);
              max = Integer.valueOf(randomArgs[2]);
              value = new ByteArray(Bytes.toBytes(String.valueOf(Math.abs(rand
                  .nextInt()) % (max - min) + min)));
            } else if (randomArgs[0].equals("RandomLongGenerator")
                && randomArgs.length == 3) {
              minL = Long.parseLong(randomArgs[1]);
              maxL = Long.parseLong(randomArgs[2]);
              value = new ByteArray(Bytes.toBytes(String.valueOf(Math.abs(rand
                  .nextInt()) % (maxL - minL) + minL)));
            } else if (randomArgs[0].equals("RandomTimeGenerator")
                && randomArgs.length == 4) {
              df = new SimpleDateFormat(randomArgs[3]);
              try {
                minDate = df.parse(randomArgs[1]).getTime();
                maxDate = df.parse(randomArgs[2]).getTime();
                if (maxDate == minDate) {
                  value = new ByteArray(Bytes.toBytes(df.format(new Date(
                      minDate))));
                } else {
                  long random = rand.nextLong();
                  value = new ByteArray(Bytes.toBytes(df.format(Math.abs(random
                      % (maxDate - minDate))
                      + minDate)));
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          } else {
            // value = new
            // ByteArray(Bytes.toBytes(Long.parseLong(simpleConditionArgs[3])));
            value = new ByteArray(Bytes.toBytes(simpleConditionArgs[3]));
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("value : " + Bytes.toString(value.getBytes()));
          }
        } else {
          throw new InvalidQueryException("There is no search table [" + table
              + "] in index-conf.xml!");
        }
        expStack.push(new SimpleCondition(columnFamily, qualifier, operator,
            value));
      } else if (c.equals(']')) {
        Collection<Condition> conditions = new ArrayList<Condition>();
        while (expStack.peek() instanceof Condition) {
          conditions.add((Condition) expStack.pop());
        }
        Object leftBracket = expStack.pop();
        if (!((Character) leftBracket).equals('[')) {
          throw new RuntimeException("Invalid Condition Expression!");
        }
        expStack.push(new AndCondition(conditions));
      } else if (c.equals('}')) {
        Collection<Condition> conditions = new ArrayList<Condition>();
        while (expStack.peek() instanceof Condition) {
          conditions.add((Condition) expStack.pop());
        }
        Object leftBracket = expStack.pop();
        if (!((Character) leftBracket).equals('{')) {
          throw new RuntimeException("Invalid Condition Expression!");
        }
        expStack.push(new OrCondition(conditions));
      }
      i++;
    }
    if (expStack.size() != 1 || !(expStack.peek() instanceof Condition)) {
      throw new RuntimeException("Invalid Condition Expression!");
    }
    return (Condition) expStack.pop();
  }
}
