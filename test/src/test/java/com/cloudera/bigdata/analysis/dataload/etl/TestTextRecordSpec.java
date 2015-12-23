package com.cloudera.bigdata.analysis.dataload.etl;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.cloudera.bigdata.analysis.dataload.source.TextRecordSpec;

public class TestTextRecordSpec {

  @Test
  public void testSimpleSpec() {

    TextRecordSpec spec = new TextRecordSpec(
        "firstName:STRING,secondName,age:INT");

    assertTrue("[0:firstName:STRING, 1:secondName:STRING, 2:age:INT]"
        .equals(spec.toString()));

    TextRecordSpec spec2 = new TextRecordSpec(
        "firstName :string , secondName,age :int");

    assertTrue("[0:firstName:STRING, 1:secondName:STRING, 2:age:INT]"
        .equals(spec2.toString()));

    TextRecordSpec spec3 = new TextRecordSpec(
        "firstName:String,secondName,age:INT");

    assertTrue("[0:firstName:STRING, 1:secondName:STRING, 2:age:INT]"
        .equals(spec3.toString()));

    TextRecordSpec spec4 = new TextRecordSpec("firstName:STRING");

    assertTrue("[0:firstName:STRING]".equals(spec4.toString()));

  }

  @Test
  public void testApshSpec() {

    TextRecordSpec spec = new TextRecordSpec(
        "APSHPROCOD|!APSHACTNO|!APSHTRDAT|!APSHJRNNO|!APSHSEQNO|!APSHPRDNO|!APSHFRNJRN|!APSHTRTM|!APSHVCHNO|!APSHTRPROCCOD|!APSHTRBK|!APSHTRCOD|!APSHTRAMT|!APSHCSHTFR|!APSHRBIND|!APSHTRAFTBAL|!APSHERRDAT|!APSHDBKTYP|!APSHDBKPRO|!APSHBATNO|!APSHDBKNO|!APSHAPROCOD|!APSHAACNO|!APSHABS|!APSHREM|!APSHTRCHL|!APSHTRFRM|!APSHTRPLA|!APSHECIND|!APSHPRTIND|!APSHSUP1|!APSHSUP2",
        "gb2312", "|!");

    assertTrue("[0:APSHPROCOD:STRING, 1:APSHACTNO:STRING, 2:APSHTRDAT:STRING, 3:APSHJRNNO:STRING, 4:APSHSEQNO:STRING, 5:APSHPRDNO:STRING, 6:APSHFRNJRN:STRING, 7:APSHTRTM:STRING, 8:APSHVCHNO:STRING, 9:APSHTRPROCCOD:STRING, 10:APSHTRBK:STRING, 11:APSHTRCOD:STRING, 12:APSHTRAMT:STRING, 13:APSHCSHTFR:STRING, 14:APSHRBIND:STRING, 15:APSHTRAFTBAL:STRING, 16:APSHERRDAT:STRING, 17:APSHDBKTYP:STRING, 18:APSHDBKPRO:STRING, 19:APSHBATNO:STRING, 20:APSHDBKNO:STRING, 21:APSHAPROCOD:STRING, 22:APSHAACNO:STRING, 23:APSHABS:STRING, 24:APSHREM:STRING, 25:APSHTRCHL:STRING, 26:APSHTRFRM:STRING, 27:APSHTRPLA:STRING, 28:APSHECIND:STRING, 29:APSHPRTIND:STRING, 30:APSHSUP1:STRING, 31:APSHSUP2:STRING]"
        .equals(spec.toString()));

  }

  @Test
  public void testGmccSpec() {

    TextRecordSpec spec = new TextRecordSpec(
        "f0@#$f1@#$f2@#$f3@#$f4@#$f5@#$f6@#$f7@#$f8@#$f9@#$f10@#$f11@#$f12@#$f13@#$f14@#$f15@#$f16@#$f17@#$f18@#$f19@#$f20@#$f21@#$f22@#$f23@#$f24@#$f25@#$f26@#$f27@#$f28@#$f29@#$f30@#$f31@#$f32@#$f33@#$f34@#$f35@#$f36@#$f37@#$f38",
        "gb2312", "@#$");

    assertTrue("[0:f0:STRING, 1:f1:STRING, 2:f2:STRING, 3:f3:STRING, 4:f4:STRING, 5:f5:STRING, 6:f6:STRING, 7:f7:STRING, 8:f8:STRING, 9:f9:STRING, 10:f10:STRING, 11:f11:STRING, 12:f12:STRING, 13:f13:STRING, 14:f14:STRING, 15:f15:STRING, 16:f16:STRING, 17:f17:STRING, 18:f18:STRING, 19:f19:STRING, 20:f20:STRING, 21:f21:STRING, 22:f22:STRING, 23:f23:STRING, 24:f24:STRING, 25:f25:STRING, 26:f26:STRING, 27:f27:STRING, 28:f28:STRING, 29:f29:STRING, 30:f30:STRING, 31:f31:STRING, 32:f32:STRING, 33:f33:STRING, 34:f34:STRING, 35:f35:STRING, 36:f36:STRING, 37:f37:STRING, 38:f38:STRING]"
        .equals(spec.toString()));

  }

  @Test
  public void testApshSpec_escaped001() {

    TextRecordSpec spec = new TextRecordSpec(
        "APSHPROCOD\001APSHACTNO\001APSHTRDAT\001APSHJRNNO\001APSHSEQNO\001APSHPRDNO\001APSHFRNJRN\001APSHTRTM\001APSHVCHNO\001APSHTRPROCCOD\001APSHTRBK\001APSHTRCOD\001APSHTRAMT\001APSHCSHTFR\001APSHRBIND\001APSHTRAFTBAL\001APSHERRDAT\001APSHDBKTYP\001APSHDBKPRO\001APSHBATNO\001APSHDBKNO\001APSHAPROCOD\001APSHAACNO\001APSHABS\001APSHREM\001APSHTRCHL\001APSHTRFRM\001APSHTRPLA\001APSHECIND\001APSHPRTIND\001APSHSUP1\001APSHSUP2",
        "gb2312", "\001");

    assertTrue("[0:APSHPROCOD:STRING, 1:APSHACTNO:STRING, 2:APSHTRDAT:STRING, 3:APSHJRNNO:STRING, 4:APSHSEQNO:STRING, 5:APSHPRDNO:STRING, 6:APSHFRNJRN:STRING, 7:APSHTRTM:STRING, 8:APSHVCHNO:STRING, 9:APSHTRPROCCOD:STRING, 10:APSHTRBK:STRING, 11:APSHTRCOD:STRING, 12:APSHTRAMT:STRING, 13:APSHCSHTFR:STRING, 14:APSHRBIND:STRING, 15:APSHTRAFTBAL:STRING, 16:APSHERRDAT:STRING, 17:APSHDBKTYP:STRING, 18:APSHDBKPRO:STRING, 19:APSHBATNO:STRING, 20:APSHDBKNO:STRING, 21:APSHAPROCOD:STRING, 22:APSHAACNO:STRING, 23:APSHABS:STRING, 24:APSHREM:STRING, 25:APSHTRCHL:STRING, 26:APSHTRFRM:STRING, 27:APSHTRPLA:STRING, 28:APSHECIND:STRING, 29:APSHPRTIND:STRING, 30:APSHSUP1:STRING, 31:APSHSUP2:STRING]"
        .equals(spec.toString()));

  }

  @Test
  public void testPpsrSpec_PropertyFile() throws IOException {

    Properties props = new Properties();
    props
        .load(new FileInputStream(
            "C:\\Users\\HP\\Desktop\\shaocheng\\GeneralBulkLoad\\test\\load-ppsr-conf.properties"));

    String formatSpecString = props.getProperty("textRecordSpec");
    String fieldDelimiter = props.getProperty("fieldDelimiter");

    TextRecordSpec spec = new TextRecordSpec(formatSpecString, "gb2312",
        fieldDelimiter);

    assertTrue(spec.getFieldList().size() == 9);

  }

}
