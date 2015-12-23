/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.bigdata.analysis.datagen;


import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

public class RecordProducer {
    private final Producer<String, String> producer;
    private final String topic = "FileMessage";
    private final Properties props = new Properties();
    
    static Logger logger = Logger.getLogger(RecordProducer.class);

    public RecordProducer() {
        //ResourceBundle resource = ResourceBundle.getBundle("its-kafka-conf");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "gzp1:9092,gzp2:9092,gzp3:9092");
        props.put("zk.connect", "gzp1:2181,gzp2:2181,gzp3:2181");
        props.put("zookeeper.connect", "gzp1:2181,gzp2:2181,gzp3:2181");
        props.put("group.id", "recordGroup");
        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    public void put(String filePath, String id) {       
        KeyedMessage<String, String> fileMessage = new KeyedMessage<String, String>(topic, id, filePath);
        logger.info("#####in the RecordProducer, before sent");
        producer.send(fileMessage);
        logger.info("#########in the RecordProducer, after sent");
    }

}
