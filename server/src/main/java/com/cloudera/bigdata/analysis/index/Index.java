package com.cloudera.bigdata.analysis.index;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.cloudera.bigdata.analysis.index.formatter.IndexFieldValueFormatter;
import com.cloudera.bigdata.analysis.index.formatter.PlainValueFormatter;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto;
import com.cloudera.bigdata.analysis.index.util.IndexUtil;
import com.cloudera.bigdata.analysis.index.util.WritableUtil;
import com.cloudera.bigdata.analysis.query.Order;

/**
 * The secondary-join index. it's the metadata of index entries. it described
 * the index entry structure.All indexes and its fields are global, static and
 * immutable!!
 */
public class Index {
  private static final Logger LOG = LoggerFactory.getLogger(Index.class);

  public static Map<ByteArray, Set<Index>> indexMap;
  static Configuration conf;
  static FileSystem fs;

  static {
    Set<Index> indexSet = new IndexParser().parse(null);
    indexMap = new LinkedHashMap<ByteArray, Set<Index>>();
    for (Index index : indexSet) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Load Index:" + index.toString());
      }
      ByteArray table = index.getTable();
      if (!indexMap.containsKey(table)) {
        indexMap.put(table, new LinkedHashSet<Index>());
      }
      indexMap.get(table).add(index);
    }
  }

  public static RecordServiceProto.RefreshResponse refreshIndexMap(
      String tableNameStr) {
    Set<Index> indexSet = new IndexParser().parse(tableNameStr);

    for (Index index : indexSet) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Load Index:" + index.toString());
      }
      ByteArray table = index.getTable();
      if (!indexMap.containsKey(table)) {
        indexMap.put(table, new LinkedHashSet<Index>());
      }
      indexMap.get(table).add(index);
    }

    RecordServiceProto.RefreshResponse.Builder builder = RecordServiceProto.RefreshResponse
        .newBuilder();
    builder.setIndexMapSize(indexMap.size());
    return builder.build();
  }

  // refresh indexMap
  public static void newIndexMap(String tableName) {
    if (!indexMap.isEmpty()) {
      indexMap.put(IndexUtil.convertStringToByteArray(tableName),
          new LinkedHashSet<Index>());
    }
  }

  public static Set<Index> get(ByteArray table) {
    return indexMap.get(table);
  }

  public static Map<ByteArray, Set<Index>> getAll() {
    return indexMap;
  }

  private final ByteArray name;

  private final ByteArray table;

  private final List<Field> fields;

  private final ByteArray orderByColumnFamily;

  private final ByteArray orderByQualifier;

  private final Order order;

  private Index(ByteArray name, ByteArray table, List<Field> fields) {
    this.name = name;
    this.table = table;
    this.fields = fields;
    // always order by first qualifier!
    this.orderByColumnFamily = fields.get(0).getColumnFamily();
    this.orderByQualifier = fields.get(0).getQualifier();
    this.order = fields.get(0).isOrderInverted() ? Order.DESC : Order.ASC;
  }

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  public Field getField(ByteArray columnFamily, ByteArray qualifier) {
    for (Field field : fields) {
      if (field.getColumnFamily().equals(columnFamily)
          && field.getQualifier().equals(qualifier)) {
        return field;
      }
    }
    return null;
  }

  public ByteArray orderBy() {
    return orderByQualifier;
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Index index = (Index) o;
    if (fields != null ? !fields.equals(index.fields) : index.fields != null)
      return false;
    if (name != null ? !name.equals(index.name) : index.name != null)
      return false;
    if (order != index.order)
      return false;
    if (orderByColumnFamily != null ? !orderByColumnFamily
        .equals(index.orderByColumnFamily) : index.orderByColumnFamily != null)
      return false;
    if (orderByQualifier != null ? !orderByQualifier
        .equals(index.orderByQualifier) : index.orderByQualifier != null)
      return false;
    if (table != null ? !table.equals(index.table) : index.table != null)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (table != null ? table.hashCode() : 0);
    result = 31 * result + (fields != null ? fields.hashCode() : 0);
    result = 31 * result
        + (orderByColumnFamily != null ? orderByColumnFamily.hashCode() : 0);
    result = 31 * result
        + (orderByQualifier != null ? orderByQualifier.hashCode() : 0);
    result = 31 * result + (order != null ? order.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder fieldsSb = new StringBuilder();
    for (Field field : fields) {
      fieldsSb.append(field.toString());
    }
    return "Index{" + "name="
        + IndexUtil.convertByteArrayToString(name.getBytes()) + ", table="
        + IndexUtil.convertByteArrayToString(table.getBytes()) + ", fields={"
        + fieldsSb.toString() + "}" + ", orderByColumnFamily="
        + orderByColumnFamily + ", orderByQualifier=" + orderByQualifier
        + ", order=" + order + '}';
  }

  /*--------------------------------------------    Getters/Setters    ---------------------------------------------*/

  public ByteArray getName() {
    return name;
  }

  public ByteArray getTable() {
    return table;
  }

  public List<Field> getFields() {
    return fields;
  }

  public int getFieldsCount() {
    return fields.size();
  }

  public ByteArray getOrderByColumnFamily() {
    return orderByColumnFamily;
  }

  public ByteArray getOrderByQualifier() {
    return orderByQualifier;
  }

  public Order getOrder() {
    return order;
  }

  /*--------------------------------------------     Inner Classes    ----------------------------------------------*/

  public static class Field implements Writable {
    private ByteArray columnFamily;
    private ByteArray qualifier;
    private int maxLength;
    private boolean isLengthVariable;
    private boolean isOrderInverted;
    private int startPositionInRowKey;
    private IndexFieldValueFormatter formatter;

    public Field() {
    }

    public Field(ByteArray columnFamily, ByteArray qualifier, int maxLength,
        boolean isLengthVariable, boolean isOrderInverted,
        int startPositionInRowKey, IndexFieldValueFormatter formatter) {
      this.columnFamily = columnFamily;
      this.qualifier = qualifier;
      this.maxLength = maxLength;
      this.isLengthVariable = isLengthVariable;
      this.isOrderInverted = isOrderInverted;
      this.startPositionInRowKey = startPositionInRowKey;
      this.formatter = formatter;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      Bytes.writeByteArray(dataOutput, columnFamily.getBytes());
      Bytes.writeByteArray(dataOutput, qualifier.getBytes());
      dataOutput.writeInt(maxLength);
      dataOutput.writeBoolean(isLengthVariable);
      dataOutput.writeBoolean(isOrderInverted);
      dataOutput.writeInt(startPositionInRowKey);
      WritableUtil.writeInstance(dataOutput, formatter);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      columnFamily = new ByteArray(Bytes.readByteArray(dataInput));
      qualifier = new ByteArray(Bytes.readByteArray(dataInput));
      maxLength = dataInput.readInt();
      isLengthVariable = dataInput.readBoolean();
      isOrderInverted = dataInput.readBoolean();
      startPositionInRowKey = dataInput.readInt();
      formatter = WritableUtil.readInstance(dataInput,
          IndexFieldValueFormatter.class);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      Field field = (Field) o;
      if (isLengthVariable != field.isLengthVariable)
        return false;
      if (isOrderInverted != field.isOrderInverted)
        return false;
      if (maxLength != field.maxLength)
        return false;
      if (startPositionInRowKey != field.startPositionInRowKey)
        return false;
      if (columnFamily != null ? !columnFamily.equals(field.columnFamily)
          : field.columnFamily != null)
        return false;
      if (formatter != null ? !formatter.equals(field.formatter)
          : field.formatter != null)
        return false;
      if (qualifier != null ? !qualifier.equals(field.qualifier)
          : field.qualifier != null)
        return false;
      return true;
    }

    @Override
    public int hashCode() {
      int result = columnFamily != null ? columnFamily.hashCode() : 0;
      result = 31 * result + (qualifier != null ? qualifier.hashCode() : 0);
      result = 31 * result + maxLength;
      result = 31 * result + (isLengthVariable ? 1 : 0);
      result = 31 * result + (isOrderInverted ? 1 : 0);
      result = 31 * result + startPositionInRowKey;
      result = 31 * result + (formatter != null ? formatter.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "Field{" + "columnFamily="
          + Bytes.toStringBinary(columnFamily.getBytes()) + ", qualifier="
          + Bytes.toStringBinary(qualifier.getBytes()) + ", maxLength="
          + maxLength + ", isLengthVariable=" + isLengthVariable
          + ", isOrderInverted=" + isOrderInverted + ", startPositionInRowKey="
          + startPositionInRowKey + ", formatter=" + formatter + '}';
    }

    public ByteArray getColumnFamily() {
      return columnFamily;
    }

    public ByteArray getQualifier() {
      return qualifier;
    }

    public int getMaxLength() {
      return maxLength;
    }

    public boolean isLengthVariable() {
      return isLengthVariable;
    }

    public boolean isOrderInverted() {
      return isOrderInverted;
    }

    public IndexFieldValueFormatter getFormatter() {
      return formatter;
    }

    public int getStartPositionInRowKey() {
      return startPositionInRowKey;
    }
  }

  private static class IndexParser {

    public Set<Index> parse(String tableNameStr) {
      DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory
          .newInstance();
      documentBuilderFactory.setNamespaceAware(true);
      DocumentBuilder documentBuilder = null;
      SchemaFactory schemaFactory = null;
      Document document = null;
      try {
        documentBuilder = documentBuilderFactory.newDocumentBuilder();
        schemaFactory = SchemaFactory
            .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

        // put index conf in DistributedCache
        conf = new Configuration();
        conf.addResource(new Path(Constants.CORE_SITE_XML));

        String xsdFilePath = Constants.INDEX_XSD_HDFS;
        Path xsdPath = new Path(xsdFilePath);

        fs = FileSystem.get(conf);

        if (!fs.exists(xsdPath)) {
          LOG.warn("File " + xsdFilePath + " does not exists in HDFS");
        }
        FSDataInputStream inXsd = fs.open(xsdPath);
        Source schemaSource = new StreamSource(new BufferedInputStream(inXsd));

        Schema schema = schemaFactory.newSchema(schemaSource);
        String xmlFilePath = null;
        if (tableNameStr != null) {
          xmlFilePath = Constants.INDEX_CONF_DIR_HDFS + "/" + tableNameStr
              + "_index-conf.xml";
          if (LOG.isDebugEnabled()) {
            LOG.debug("enter parser: " + xmlFilePath);
          }
        } else {
          xmlFilePath = Constants.INDEX_CONF_DIR_HDFS + "/"
              + Constants.CURRENT_INDEX_CONF_FILE_NAME;
        }
        Path xmlPath = new Path(xmlFilePath);
        if (!fs.exists(xmlPath)) {
          LOG.warn("File " + xmlFilePath + " does not exists in HDFS");
        }
        FSDataInputStream inXml = fs.open(xmlPath);

        document = documentBuilder.parse(new BufferedInputStream(inXml));
        Validator validator = schema.newValidator();
        validator.validate(new DOMSource(document));
      } catch (ParserConfigurationException e) {
        e.printStackTrace();
      } catch (SAXException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      Set<Index> indexes = new LinkedHashSet<Index>();
      NodeList indexNodeList = document.getElementsByTagName("index");
      for (int i = 0; i < indexNodeList.getLength(); i++) {
        if (indexNodeList.item(i).getNodeType() == Node.ELEMENT_NODE) { // skip
                                                                        // white
                                                                        // space
                                                                        // text.
          NodeList indexProperties = indexNodeList.item(i).getChildNodes();
          // start position will skip prefix and a delimiter.
          int currentPositionInRowKey = Constants.ROWKEY_PREFIX_LENGTH + 1;
          ByteArray indexName = null;
          ByteArray table = null;
          List<Field> fields = new ArrayList<Field>();
          for (int j = 0; j < indexProperties.getLength(); j++) {
            if (indexProperties.item(j).getNodeType() == Node.ELEMENT_NODE) { // skip
                                                                              // white
                                                                              // space
                                                                              // text.
              Node indexPropertyNode = indexProperties.item(j);
              if ("name".equals(indexPropertyNode.getNodeName())) {
                byte[] indexNameBytes = Bytes.toBytes(indexPropertyNode
                    .getTextContent().trim());
                indexName = new ByteArray(indexNameBytes);
                // move pointer to key part of index row key. 1 is length of a
                // delimiter.
                currentPositionInRowKey += indexNameBytes.length + 1;
              }
              if ("table".equals(indexPropertyNode.getNodeName())) {
                table = new ByteArray(Bytes.toBytes(indexPropertyNode
                    .getTextContent().trim()));
              }
              if ("fields".equals(indexPropertyNode.getNodeName())) {
                Element columnFamilyElement = (Element) indexPropertyNode;
                NodeList qualifierNodeList = columnFamilyElement
                    .getChildNodes();
                for (int k = 0; k < qualifierNodeList.getLength(); k++) {
                  if (qualifierNodeList.item(k).getNodeType() == Node.ELEMENT_NODE) {
                    Element qualifierElement = (Element) qualifierNodeList
                        .item(k);
                    ByteArray columnFamily = new ByteArray(
                        Bytes.toBytes(qualifierElement.getAttribute(
                            "columnFamily").trim()));
                    ByteArray qualifier = new ByteArray(
                        Bytes.toBytes(qualifierElement
                            .getAttribute("qualifier").trim()));
                    Integer maxLength = Integer.parseInt(qualifierElement
                        .getAttribute("maxLength").trim());
                    Boolean isLengthVariable = Boolean
                        .parseBoolean(qualifierElement.getAttribute(
                            "isLengthVariable").trim());
                    Boolean isOrderInverted = Boolean
                        .parseBoolean(qualifierElement.getAttribute(
                            "isOrderInverted").trim());
                    IndexFieldValueFormatter formatter = null;
                    if (qualifierElement.hasAttribute("formatter")) {
                      String formatterClass = qualifierElement.getAttribute(
                          "formatter").trim();
                      try {
                        formatter = (IndexFieldValueFormatter) Class.forName(
                            formatterClass).newInstance();
                      } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                      } catch (InstantiationException e) {
                        e.printStackTrace();
                      } catch (IllegalAccessException e) {
                        e.printStackTrace();
                      }
                    } else {
                      formatter = new PlainValueFormatter();
                    }
                    int startPositionInRowKey = currentPositionInRowKey;
                    Field field = new Field(columnFamily, qualifier, maxLength,
                        isLengthVariable, isOrderInverted,
                        startPositionInRowKey, formatter);
                    if (LOG.isDebugEnabled()) {
                      LOG.debug(field.toString());
                    }
                    fields.add(field);
                    // move pointer to next qualifier. 1 is length of
                    // a delimiter.
                    currentPositionInRowKey += (maxLength + 1);
                  }
                }
              }
            }
          }
          indexes.add(new Index(indexName, table, fields));
        }
      }
      return indexes;
    }
  }

  public static void main(String args[]) {
    Index.refreshIndexMap(args[0]);
    LOG.info("index refreshed mape size: " + Index.indexMap.size() + ": "
        + Index.indexMap.toString());
  }

}
