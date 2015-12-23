package com.cloudera.bigdata.analysis.dataload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.io.HDFSFileObject;

public class RecordConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(RecordConsumer.class);
  private final ConsumerConnector consumer;
  private final String topic = "FileMessage";
  private ConsumerIterator<byte[], byte[]> it;

  public RecordConsumer() {
      consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
      
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(1));
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
      KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
      
      it = stream.iterator();
  }

  private ConsumerConfig createConsumerConfig() {
      Properties props = new Properties();
      //ResourceBundle resource = ResourceBundle.getBundle("its-kafka-conf");
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("metadata.broker.list", "gzp1:9092,gzp2:9092,gzp3:9092");
      props.put("zookeeper.connect", "gzp1:2181,gzp2:2181,gzp3:2181");
      props.put("group.id", "recordGroup");
      props.put("zookeeper.session.timeout.ms", "400");
      props.put("zookeeper.sync.time.ms", "200");
      props.put("auto.commit.interval.ms", "1000");
      
      ConsumerConfig config = new ConsumerConfig(props);
      return config;
  }

  public FileInfo getNextFile() {
      while(true){
        MessageAndMetadata<byte[], byte[]> nextMsg = it.next();
        if(nextMsg == null){
          try {
            Thread.sleep(500);
            continue;
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }else{
          byte[] message = nextMsg.message();
          LOG.info("+++++ Get Message: " + new String(message));
          return new FileInfo(new KafkaFileObject(message));
        }
      }
  }
  
  public static class KafkaFileObject extends HDFSFileObject {
    private String path;
    public KafkaFileObject(byte[] message){
      path = new String(message);
    }
    
    @Override
    public void readFields(DataInput arg0) throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public int compareTo(FileObject o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public long getSize() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String getCanonicalPath() {
      return path;
    }

    @Override
    public String getHostname() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getPath() {
      return path;
    }
    
    public String toString(){
      return path;
    }
    
  }
}
