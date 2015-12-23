package com.cloudera.bigdata.analysis.dataload.jaxb;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaUnmarshaller {
  private static final Logger LOG = LoggerFactory
      .getLogger(SchemaUnmarshaller.class);

  private static SchemaUnmarshaller schemaUnmashaller;

  private Map<Class<?>, JAXBContext> contextMap;

  public static synchronized SchemaUnmarshaller getInstance() {
    if (schemaUnmashaller == null) {
      schemaUnmashaller = new SchemaUnmarshaller();
    }

    return schemaUnmashaller;
  }

  private SchemaUnmarshaller() {
    contextMap = new ConcurrentHashMap<Class<?>, JAXBContext>();
  }

  @SuppressWarnings("unchecked")
  public <T> JAXBElement<T> unmarshallDocument(Class<? extends T> clazz,
      String instanceDoc) {
    try {
      JAXBContext jaxbContext = contextMap.get(clazz);
      synchronized (this) {
        if (jaxbContext == null) {
          jaxbContext = JAXBContext.newInstance(clazz);
          contextMap.put(clazz, jaxbContext);
        }
      }
      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      return (JAXBElement<T>) unmarshaller.unmarshal(new StreamSource(new File(
          instanceDoc)), clazz);
    } catch (JAXBException e) {
      LOG.error("", e);
      return null;
    }
  }
}
